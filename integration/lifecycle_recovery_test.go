package integration

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/orchestration"
)

type blockedInitialEnrollmentRepository struct {
	orchestration.Repository
	blocked atomic.Bool
	entered chan model.Enrollment
	release chan struct{}
}

func (r *blockedInitialEnrollmentRepository) Enroll(ctx context.Context, enrollment model.Enrollment) (model.Admission, error) {
	if r.blocked.CompareAndSwap(false, true) {
		r.entered <- enrollment
		// Model an abruptly lost process: neither commit nor return an Activity
		// result, even when the first attempt's context is canceled locally.
		<-r.release
		return model.Admission{}, context.Canceled
	}
	return r.Repository.Enroll(ctx, enrollment)
}

func TestNexusRecoveryBeforeEnrollmentCommit(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with Temporal and PostgreSQL running")
	}
	var fault *blockedInitialEnrollmentRepository
	h := newHarness(t, "success", 100*time.Millisecond, false, func(h *harness) {
		fault = &blockedInitialEnrollmentRepository{Repository: h.repo, entered: make(chan model.Enrollment, 1), release: make(chan struct{})}
		h.repo = fault
	})
	t.Cleanup(func() { close(fault.release) })
	cfg := h.config()
	cfg.MaxBatchRequests = 1
	runID := h.coordinator(cfg)
	runs, requests := h.callers(cfg, 1)
	var first model.Enrollment
	select {
	case first = <-fault.entered:
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	status, err := h.db.Status(h.ctx, runID)
	if err != nil || status.Admitted != 0 || h.persistedJobs() != 0 {
		t.Fatalf("failure injection must precede enrollment commit: admitted=%d error=%v", status.Admitted, err)
	}
	h.replaceWorkers()
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatalf("caller did not recover from worker loss before enrollment: %v", err)
	}
	assertResult(t, requests[0], result)
	status = h.finish(runID)
	assertReport(t, status, 1, 1)
	manifest, err := h.db.LoadManifest(h.ctx, status.Batches[0].BatchID)
	if err != nil || len(manifest.Entries) != 1 {
		t.Fatalf("recovered enrollment manifest: entries=%d error=%v", len(manifest.Entries), err)
	}
	recovered := manifest.Entries[0].Target
	if recovered.Attempt <= first.Target.Attempt || recovered.ActivityID != first.Target.ActivityID || recovered.ActivityRunID != first.Target.ActivityRunID {
		t.Fatalf("recovery must retry the same SAA execution: first_attempt=%d recovered_attempt=%d same_activity=%t same_execution=%t",
			first.Target.Attempt, recovered.Attempt, recovered.ActivityID == first.Target.ActivityID, recovered.ActivityRunID == first.Target.ActivityRunID)
	}
	if h.persistedJobs() != 1 {
		t.Fatal("recovery before enrollment created a duplicate provider job")
	}
	h.assertNexusHistory(runs[0], true)
	t.Logf("heartbeat recovery enrolled the original SAA on attempt %d with one admission and one provider job", recovered.Attempt)
}

type suppressedEnrollmentHeartbeatRepository struct {
	orchestration.Repository
	suppressed atomic.Bool
}

func (r *suppressedEnrollmentHeartbeatRepository) HeartbeatTargets(ctx context.Context, mode, endpoint string, limit int) ([]model.CompletionTarget, error) {
	if r.suppressed.Load() {
		return nil, nil
	}
	return r.Repository.HeartbeatTargets(ctx, mode, endpoint, limit)
}

func TestNexusRecoveryRebindsCommittedEnrollment(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with Temporal and PostgreSQL running")
	}
	var fault *suppressedEnrollmentHeartbeatRepository
	h := newHarness(t, "success", 40*time.Second, true, func(h *harness) {
		fault = &suppressedEnrollmentHeartbeatRepository{Repository: h.repo}
		fault.suppressed.Store(true)
		h.repo = fault
	})
	cfg := h.config()
	cfg.MaxBatchRequests = 1
	cfg.ResultDeadline, cfg.OperationTimeout = 90*time.Second, 3*time.Minute
	runID := h.coordinator(cfg)
	runs, requests := h.callers(cfg, 1)
	first := h.waitHandoff()
	h.awaitJobs(1)
	jobs, err := h.p.Find(h.ctx, map[string]string{"run_id": h.id})
	if err != nil || len(jobs) != 1 || jobs[0].Terminal() {
		t.Fatalf("heartbeat loss must occur while the original provider job is pending: jobs=%d error=%v", len(jobs), err)
	}
	originalJob := jobs[0].ID
	var rebound model.CompletionTarget
	select {
	case rebound = <-h.handoffs:
	case <-time.After(45 * time.Second):
		t.Fatal("committed enrollment did not reattach after losing its heartbeat")
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	fault.suppressed.Store(false)
	if rebound.Attempt <= first.Attempt || rebound.ActivityID != first.ActivityID || rebound.ActivityRunID != first.ActivityRunID || bytes.Equal(rebound.TaskToken, first.TaskToken) {
		t.Fatalf("reattachment must replace only the attempt token for the same SAA: first_attempt=%d rebound_attempt=%d", first.Attempt, rebound.Attempt)
	}
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatalf("caller did not complete through the rebound SAA: %v", err)
	}
	assertResult(t, requests[0], result)
	status := h.finish(runID)
	assertReport(t, status, 1, 1)
	report := status.Batches[0]
	if status.Admitted != 1 || report.Delivered != 1 || report.Skipped != 0 || report.ProviderBatchID != originalJob || h.persistedJobs() != 1 {
		t.Fatalf("heartbeat recovery must preserve admission and provider ownership: admitted=%d report=%+v jobs=%d", status.Admitted, report, h.persistedJobs())
	}
	h.assertNexusHistory(runs[0], true)
	t.Logf("committed enrollment rebound from attempt %d to %d and completed its original provider job", first.Attempt, rebound.Attempt)
}
