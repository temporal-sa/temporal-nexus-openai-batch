package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/store"
)

// Uses PostgreSQL transactions and two independent pools; no Temporal server.
func TestPostgresStoreIntegration(t *testing.T) {
	if os.Getenv("POSTGRES_TEST") != "1" {
		t.Skip("set POSTGRES_TEST=1 with local PostgreSQL running")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = store.LocalDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	namespace := fmt.Sprintf("store-integration-%d", time.Now().UnixNano())
	db, err := store.Open(ctx, dsn, namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	other, err := store.Open(ctx, dsn, namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	config := model.DefaultConfig("fake")
	config.Endpoint = namespace
	config.Window = time.Second
	config.MaxBatchRequests = 25
	input := model.RunInput{RunID: "concurrent", Config: config}
	if err := db.CreateRun(ctx, input); err != nil {
		t.Fatal(err)
	}
	makeEnrollment := func(run string, i int) model.Enrollment {
		request := model.Request{RunID: run, RequestID: fmt.Sprintf("request-%04d", i), Input: "private-document-body"}
		return model.Enrollment{Request: request, Fingerprint: model.Fingerprint(request), Target: model.CompletionTarget{Namespace: namespace,
			ActivityID: fmt.Sprintf("activity/%s/%d", run, i), ActivityRunID: "00000000-0000-4000-8000-000000000000", Attempt: 1, TaskToken: []byte(fmt.Sprintf("private-token-%s-%d", run, i))}}
	}
	t.Run("concurrent_atomic_admission_dedupe_and_outbox", func(t *testing.T) {
		var group sync.WaitGroup
		failures := make(chan error, 200)
		limit := make(chan struct{}, 16)
		for i := 0; i < 200; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				limit <- struct{}{}
				defer func() { <-limit }()
				owner := db
				if i%2 == 0 {
					owner = other
				}
				e := makeEnrollment("concurrent", i)
				a, err := owner.Enroll(ctx, e)
				if err != nil {
					failures <- err
					return
				}
				b, err := owner.Enroll(ctx, e)
				if err != nil || a.BatchID != b.BatchID {
					failures <- fmt.Errorf("retried enrollment changed assignment: %v", err)
				}
			}(i)
		}
		group.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
		if t.Failed() {
			return
		}
		status, err := db.Status(ctx, "concurrent")
		if err != nil {
			t.Fatal(err)
		}
		if status.Admitted != 200 || status.Buffered != 0 {
			t.Fatalf("non-atomic count admission: %+v", status)
		}
		claimed := make(chan []model.BatchRef, 2)
		claimErrors := make(chan error, 2)
		for _, owner := range []*store.Store{db, other} {
			group.Add(1)
			go func(owner *store.Store) {
				defer group.Done()
				refs, err := owner.ReadyBatches(ctx, "fake", namespace, 100)
				claimed <- refs
				claimErrors <- err
			}(owner)
		}
		group.Wait()
		close(claimed)
		close(claimErrors)
		for err := range claimErrors {
			if err != nil {
				t.Fatal(err)
			}
		}
		seen := map[string]bool{}
		total := 0
		for refs := range claimed {
			for _, ref := range refs {
				if seen[ref.BatchID] {
					t.Fatal("two dispatchers claimed the same lease")
				}
				seen[ref.BatchID] = true
				manifest, err := db.LoadManifest(ctx, ref.BatchID)
				if err != nil {
					t.Fatal(err)
				}
				data, err := model.JSONL(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if len(manifest.Entries) != 25 || len(data) != ref.Bytes || ref.Requests != 25 {
					t.Fatalf("sealed membership/bytes inconsistent: %+v", ref)
				}
				total += ref.Requests
			}
		}
		if len(seen) != 8 || total != 200 {
			t.Fatalf("claimed %d batches/%d entries", len(seen), total)
		}
	})
	t.Run("lease_recovery_and_immutable_outcomes_survive_reopen", func(t *testing.T) {
		// Accelerate only this test namespace's lease clock; no unrelated rows.
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal("connect for scoped lease-clock fixture")
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `UPDATE nexus_batch_batches SET dispatch_claim_until=clock_timestamp()-interval '1 second' WHERE namespace=$1`, namespace); err != nil {
			t.Fatal(err)
		}
		refs, err := other.ReadyBatches(ctx, "fake", namespace, 100)
		if err != nil || len(refs) != 8 {
			t.Fatalf("expired outbox lease was not reclaimed: count=%d error=%v", len(refs), err)
		}
		ref := refs[0]
		manifest, err := db.LoadManifest(ctx, ref.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		outcomes := make([]model.Outcome, len(manifest.Entries))
		for i, entry := range manifest.Entries {
			outcomes[i] = model.Outcome{RequestID: entry.Request.RequestID, Fingerprint: entry.Fingerprint, BatchID: ref.BatchID, Result: &model.Result{RequestID: entry.Request.RequestID, Text: "original immutable result"}}
		}
		if err := db.SaveOutcomes(ctx, ref.BatchID, outcomes, nil); err != nil {
			t.Fatal(err)
		}
		outcomes[0].Result.Text = "different retry must not overwrite"
		if err := other.SaveOutcomes(ctx, ref.BatchID, outcomes, nil); err != nil {
			t.Fatal(err)
		}
		rows, err := db.PendingDeliveries(ctx, ref.BatchID, 100)
		if err != nil || len(rows) != 25 || rows[0].Outcome.Result.Text != "original immutable result" {
			t.Fatalf("outcomes were not immutable: count=%d error=%v", len(rows), err)
		}
		if err := db.MarkDelivered(ctx, ref.BatchID, rows[0].RequestID, rows[0].Target, true, ""); err != nil {
			t.Fatal(err)
		}
		if err := other.MarkDelivered(ctx, ref.BatchID, rows[0].RequestID, rows[0].Target, false, "late retry"); err != nil {
			t.Fatal(err)
		}
		reopened, err := store.Open(ctx, dsn, namespace)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		summary, err := reopened.DeliverySummary(ctx, ref.BatchID)
		if err != nil || summary.Delivered != 1 || summary.Skipped != 0 {
			t.Fatalf("delivery checkpoint regressed: %+v error=%v", summary, err)
		}
		pending, err := reopened.PendingDeliveries(ctx, ref.BatchID, 100)
		if err != nil || len(pending) != 24 {
			t.Fatalf("reopen lost delivery checkpoint: count=%d error=%v", len(pending), err)
		}
		if err := reopened.MarkDispatched(ctx, ref.BatchID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("timer_bytes_endpoint_scope_and_explicit_capacity", func(t *testing.T) {
		cfg := config
		cfg.Endpoint = namespace + "-timer"
		cfg.Window = 50 * time.Millisecond
		cfg.MaxBatchRequests = 100
		cfg.MaxAdmissions = 2
		e := makeEnrollment("timer", 0)
		line, err := model.JSONLLine(e.Request, cfg)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxBatchBytes = 2*len(line) - 1
		if err := db.CreateRun(ctx, model.RunInput{RunID: "timer", Config: cfg}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if _, err := db.Enroll(ctx, makeEnrollment("timer", i)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Enroll(ctx, makeEnrollment("timer", 2)); err == nil {
			t.Fatal("explicit optional capacity was ignored")
		} else {
			var failure *model.Failure
			if !errors.As(err, &failure) || failure.Code != "RUN_FULL" {
				t.Fatal(err)
			}
		}
		time.Sleep(80 * time.Millisecond)
		if _, err := db.SealDue(ctx, "fake", cfg.Endpoint, 100); err != nil {
			t.Fatal(err)
		}
		wrong, err := db.ReadyBatches(ctx, "fake", namespace+"-unrelated", 100)
		if err != nil || len(wrong) != 0 {
			t.Fatal("endpoint-scoped dispatcher claimed another endpoint's batches")
		}
		refs, err := db.ReadyBatches(ctx, "fake", cfg.Endpoint, 100)
		if err != nil || len(refs) != 2 {
			t.Fatalf("byte/timer sealing failed: count=%d error=%v", len(refs), err)
		}
		for _, ref := range refs {
			if ref.Requests != 1 || ref.Bytes > cfg.MaxBatchBytes {
				t.Fatalf("byte limit violated: %+v", ref)
			}
		}
	})
}
