package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/store"
)

func recoveryStore(t *testing.T) (*store.Store, *store.Store, model.Config, string) {
	t.Helper()
	if os.Getenv("POSTGRES_TEST") != "1" {
		t.Skip("set POSTGRES_TEST=1 with local PostgreSQL running")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = store.LocalDSN
	}
	namespace := fmt.Sprintf("store-recovery-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := store.Open(ctx, dsn, namespace)
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.Open(ctx, dsn, namespace)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer db.Close()
		defer other.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Error("connect for scoped recovery fixture cleanup")
			return
		}
		defer conn.Close(ctx)
		for _, table := range []string{"nexus_batch_requests", "nexus_batch_batches", "nexus_batch_runs"} {
			if _, err := conn.Exec(ctx, "DELETE FROM "+table+" WHERE namespace=$1", namespace); err != nil {
				t.Errorf("cleanup %s fixture", table)
			}
		}
	})
	cfg := model.DefaultConfig("fake")
	cfg.Endpoint = namespace
	cfg.MaxBatchRequests = 2
	return db, other, cfg, namespace
}

func recoveryEnrollment(namespace, run, id string, deadline time.Time) model.Enrollment {
	r := model.Request{RunID: run, RequestID: id, Input: "durable request"}
	return model.Enrollment{Request: r, Fingerprint: model.Fingerprint(r), DeadlineAt: deadline,
		Target: model.CompletionTarget{Namespace: namespace, ActivityID: "activity/" + run + "/" + id,
			ActivityRunID: "execution/" + run + "/" + id, Attempt: 1, TaskToken: []byte("token/" + id)}}
}

func requireStoreFailure(t *testing.T, err error, code string) {
	t.Helper()
	var failure *model.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestPostgresRecoveryDeadlineAndResources(t *testing.T) {
	db, other, cfg, namespace := recoveryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.CreateRun(ctx, model.RunInput{RunID: "deadline", Config: cfg}); err != nil {
		t.Fatal(err)
	}
	firstDeadline := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	lastDeadline := firstDeadline.Add(-10 * time.Second)
	first := recoveryEnrollment(namespace, "deadline", "one", firstDeadline)
	second := recoveryEnrollment(namespace, "deadline", "two", lastDeadline)
	firstAdmission, err := db.Enroll(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Enroll(ctx, second); err != nil {
		t.Fatal(err)
	}
	refs, err := other.ReadyBatches(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(refs) != 1 || !refs[0].DeadlineAt.Equal(lastDeadline) {
		t.Fatalf("earliest actual recipient deadline was not preserved: %+v, %v", refs, err)
	}
	// A retry must not move the stored recipient or batch deadline forward.
	second.DeadlineAt = firstDeadline.Add(time.Hour)
	if _, err := db.Enroll(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := db.StopRun(ctx, "deadline"); err != nil {
		t.Fatal(err)
	}
	batches, targets, err := other.RunResources(ctx, "deadline")
	if err != nil || !reflect.DeepEqual(batches, []string{firstAdmission.BatchID}) || len(targets) != 2 {
		t.Fatalf("durable cleanup inventory missing resources: %v, %d, %v", batches, len(targets), err)
	}
	if !reflect.DeepEqual(targets[0], first.Target) || !reflect.DeepEqual(targets[1], second.Target) {
		t.Fatal("cleanup inventory changed exact completion targets")
	}
	called := false
	err = other.WithSubmission(ctx, firstAdmission.BatchID, func(context.Context) error { called = true; return nil })
	requireStoreFailure(t, err, "RUN_STOPPED")
	if called {
		t.Fatal("stopped run entered provider submission")
	}
}

func TestPostgresRecoveryStopWaitsForSubmissionFence(t *testing.T) {
	db, other, cfg, namespace := recoveryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg.MaxBatchRequests = 1
	if err := db.CreateRun(ctx, model.RunInput{RunID: "stop", Config: cfg}); err != nil {
		t.Fatal(err)
	}
	admission, err := db.Enroll(ctx, recoveryEnrollment(namespace, "stop", "one", time.Now().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce a dispatch claim held across cleanup.
	if _, err := db.ReadyBatches(ctx, cfg.Mode, cfg.Endpoint, 10); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	submissionDone := make(chan error, 1)
	go func() {
		submissionDone <- db.WithSubmission(ctx, admission.BatchID, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("submission did not acquire its fence")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- other.StopRun(ctx, "stop") }()
	select {
	case err := <-stopDone:
		close(release)
		t.Fatalf("stop returned while provider submission still held its fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-submissionDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	called := false
	err = db.WithSubmission(ctx, admission.BatchID, func(context.Context) error { called = true; return nil })
	requireStoreFailure(t, err, "RUN_STOPPED")
	if called {
		t.Fatal("claimed batch submitted after StopRun returned")
	}
}

func TestPostgresRecoverySubmissionRejectsExpiredRecipient(t *testing.T) {
	db, _, cfg, namespace := recoveryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg.MaxBatchRequests = 1
	if err := db.CreateRun(ctx, model.RunInput{RunID: "expired", Config: cfg}); err != nil {
		t.Fatal(err)
	}
	admission, err := db.Enroll(ctx, recoveryEnrollment(namespace, "expired", "one", time.Now().Add(200*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(220 * time.Millisecond)
	called := false
	err = db.WithSubmission(ctx, admission.BatchID, func(context.Context) error { called = true; return nil })
	requireStoreFailure(t, err, "DEADLINE_EXCEEDED")
	if called {
		t.Fatal("expired batch entered provider submission")
	}
}

func TestPostgresRecoveryReattachesNewAttemptAndFencesOldDelivery(t *testing.T) {
	db, other, cfg, namespace := recoveryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg.MaxBatchRequests = 1
	if err := db.CreateRun(ctx, model.RunInput{RunID: "reattach", Config: cfg}); err != nil {
		t.Fatal(err)
	}
	first := recoveryEnrollment(namespace, "reattach", "one", time.Now().Add(time.Minute))
	admission, err := db.Enroll(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	equalAttempt := first
	equalAttempt.Target.TaskToken = []byte("different-token-same-attempt")
	_, err = other.Enroll(ctx, equalAttempt)
	requireStoreFailure(t, err, "STALE_ACTIVITY_ATTEMPT")
	heartbeats, err := db.HeartbeatTargets(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(heartbeats) != 1 || !reflect.DeepEqual(heartbeats[0], first.Target) {
		t.Fatalf("unresolved exact attempt not claimed for heartbeat: %+v, %v", heartbeats, err)
	}
	outcome := model.Outcome{RequestID: first.Request.RequestID, Fingerprint: first.Fingerprint, BatchID: admission.BatchID,
		Result: &model.Result{RequestID: first.Request.RequestID, Text: "original result"}}
	if err := db.SaveOutcomes(ctx, admission.BatchID, []model.Outcome{outcome}, nil); err != nil {
		t.Fatal(err)
	}
	active, err := other.PendingCompletions(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(active) != 0 {
		t.Fatalf("late keeper claimed active Workflow delivery: %+v, %v", active, err)
	}
	if err := db.MarkDelivered(ctx, admission.BatchID, first.Request.RequestID, first.Target, false, "old attempt timed out"); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveReport(ctx, model.BatchReport{BatchID: admission.BatchID, Status: "completed", Requests: 1, Succeeded: 1, Skipped: 1}); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Target.Attempt++
	second.Target.TaskToken = []byte("new-attempt-token")
	reattached, err := other.Enroll(ctx, second)
	if err != nil || reattached.BatchID != admission.BatchID {
		t.Fatalf("new attempt lost immutable assignment: %+v, %v", reattached, err)
	}
	// A delivery that fetched attempt 1 before reattachment cannot skip attempt 2.
	if err := db.MarkDelivered(ctx, admission.BatchID, first.Request.RequestID, first.Target, false, "late old attempt"); err != nil {
		t.Fatal(err)
	}
	pending, err := db.PendingDeliveries(ctx, admission.BatchID, 10)
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0].Target, second.Target) || pending[0].Outcome.Result.Text != "original result" {
		t.Fatalf("reattachment lost target/result or stale checkpoint won: %+v, %v", pending, err)
	}
	late, err := other.PendingCompletions(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(late) != 1 || late[0].BatchID != admission.BatchID || !reflect.DeepEqual(late[0].Target, second.Target) {
		t.Fatalf("completed batch lost its late reattachment: %+v, %v", late, err)
	}
	wrongEndpoint, err := db.PendingCompletions(ctx, cfg.Mode, cfg.Endpoint+"-other", 10)
	if err != nil || len(wrongEndpoint) != 0 {
		t.Fatalf("late completion escaped endpoint scope: %+v, %v", wrongEndpoint, err)
	}
	_, err = db.Enroll(ctx, first)
	requireStoreFailure(t, err, "STALE_ACTIVITY_ATTEMPT")
	heartbeats, err = other.HeartbeatTargets(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(heartbeats) != 1 || !reflect.DeepEqual(heartbeats[0], second.Target) {
		t.Fatalf("new exact attempt not claimed for heartbeat: %+v, %v", heartbeats, err)
	}
	heartbeats, err = db.HeartbeatTargets(ctx, cfg.Mode, cfg.Endpoint, 10)
	if err != nil || len(heartbeats) != 0 {
		t.Fatalf("heartbeat lease claimed twice: %+v, %v", heartbeats, err)
	}
	if err := db.MarkDelivered(ctx, admission.BatchID, second.Request.RequestID, second.Target, true, ""); err != nil {
		t.Fatal(err)
	}
	third := second
	third.Target.Attempt++
	third.Target.TaskToken = []byte("unexpected-after-delivery")
	_, err = other.Enroll(ctx, third)
	requireStoreFailure(t, err, "STALE_ACTIVITY_ATTEMPT")
	status, err := db.Status(ctx, "reattach")
	if err != nil || status.Admitted != 1 || len(status.Batches) != 1 || status.Batches[0].Delivered != 1 || status.Batches[0].Skipped != 0 {
		t.Fatalf("reattachment created another admission or final report hid delivery progress: %+v, %v", status, err)
	}
}
