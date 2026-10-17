package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/orchestration"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/store"
	"go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
)

// These tests deliberately use a real Temporal server, real Nexus endpoints,
// and real workers. Only the OpenAI provider is replaced. They neither start a
// server nor contact OpenAI, and opt in so plain `go test ./...` stays offline.
func TestNexusBatchIntegration(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with a local Nexus-enabled Temporal server running")
	}
	t.Run("100_independent_callers_share_threshold_batches", testHundredCallers)
	t.Run("sparse_request_flushes_on_timer", testSparseWindow)
	t.Run("byte_limit_seals_before_overflow", testByteLimit)
	t.Run("saa_handoff_releases_worker_and_completes_nexus", testAsyncHandoff)
	t.Run("independent_duplicate_request_id_is_rejected", testDuplicateRequest)
	t.Run("cancellation_does_not_cancel_a_sealed_sibling", testCancellation)
	t.Run("worker_restart_resumes_existing_provider_batch", testRestart)
	t.Run("worker_restart_resumes_partial_result_delivery", testDeliveryRestart)
	t.Run("worker_restart_preserves_handed_off_saa", testHandoffRestart)
	t.Run("stale_saa_attempt_token_is_fenced_without_resubmission", testStaleAttempt)
	t.Run("successful_external_completion_ack_loss_is_reconciled", func(t *testing.T) { testCompletionAckLoss(t, "success") })
	t.Run("failed_external_completion_ack_loss_is_reconciled", func(t *testing.T) { testCompletionAckLoss(t, "expired") })
	t.Run("postgres_enrollment_commit_before_ack_is_idempotent", testEnrollmentCommitAckLoss)
	t.Run("dispatch_start_before_database_ack_is_reconciled", testDispatchCommitGap)
	t.Run("more_than_1000_callers_across_concurrent_workers", testBeyondThousand)
	for _, scenario := range []struct {
		name, code string
		failures   int
		diagnostic bool
	}{
		{"mixed", "PROVIDER_REQUEST_ERROR", 2, false},
		{"expired", "BATCH_EXPIRED", 3, false},
		{"missing", "MISSING_RESULT", 1, true},
		{"duplicate", "DUPLICATE_RESULT", 1, true},
		{"unknown", "", 0, true},
		{"malformed", "INVALID_RESULT", 1, true},
		{"submission-unknown", "", 0, false},
		{"submission-unreconciled", "SUBMISSION_UNKNOWN", 6, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			h := newHarness(t, scenario.name, 100*time.Millisecond, false)
			cfg := h.config()
			cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 6
			coord := h.createRun(cfg)
			runs, requests := h.callers(cfg, 6)
			failed := 0
			for i, run := range runs {
				var result model.Result
				err := run.Get(h.ctx, &result)
				if err != nil {
					failed++
					assertFailureCode(t, err, scenario.code)
				} else {
					assertResult(t, requests[i], result)
				}
				h.assertNexusHistory(run, err == nil)
			}
			if failed != scenario.failures {
				t.Fatalf("got %d failed callers, want %d", failed, scenario.failures)
			}
			status := h.finish(coord)
			assertReport(t, status, 6, 1)
			report := status.Batches[0]
			if report.Failed != failed || report.Succeeded != 6-failed || report.Delivered != 6 {
				t.Fatalf("incorrect fan-out accounting: %+v", report)
			}
			if failed > 0 && report.FailureCounts[scenario.code] != failed {
				t.Fatalf("batch report lacks actionable per-code failure totals: %+v", report)
			}
			if scenario.diagnostic && len(report.Diagnostics) == 0 {
				t.Fatal("invalid or unknown provider rows must appear in diagnostics")
			}
			// Count persisted jobs, including a job hidden from reconciliation.
			// An uncertain Create must not silently become a second submission.
			if got := h.persistedJobs(); got != 1 {
				t.Fatalf("provider created %d jobs for one logical batch; want exactly 1", got)
			}
		})
	}
}

func testHundredCallers(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 15*time.Second, 25
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 100)
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
		assertResult(t, requests[i], result)
		h.assertNexusHistory(run, true)
	}
	status := h.finish(coord)
	assertReport(t, status, 100, 4)
	for _, report := range status.Batches {
		if report.Requests != 25 || report.Succeeded != 25 || report.Delivered != 25 {
			t.Fatalf("threshold batch has incorrect accounting: %+v", report)
		}
		if report.CollectionWait >= cfg.Window {
			t.Fatalf("full batch waited for collection timer: %s", report.CollectionWait)
		}
	}
	if got := h.persistedJobs(); got != 4 {
		t.Fatalf("100 callers should create 4 provider jobs, got %d", got)
	}
	h.assertServiceHasNoRequestWorkflows()
}

func testSparseWindow(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window = 500 * time.Millisecond
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	assertResult(t, requests[0], result)
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if got := status.Batches[0].CollectionWait; got < cfg.Window {
		t.Fatalf("sparse batch sealed before its durable collection timer: %s < %s", got, cfg.Window)
	}
}

func testByteLimit(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window = 500 * time.Millisecond
	line, err := model.JSONLLine(h.request(0), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxBatchBytes = 2*len(line) - 1
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 4)
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatal(err)
		}
		assertResult(t, requests[i], result)
	}
	status := h.finish(coord)
	assertReport(t, status, 4, 4)
	for _, report := range status.Batches {
		if report.Requests != 1 || report.Bytes > cfg.MaxBatchBytes {
			t.Fatalf("JSONL byte limit violated: %+v", report)
		}
	}
}

func testAsyncHandoff(t *testing.T) {
	h := newHarness(t, "success", 3*time.Second, true)
	cfg := h.config()
	cfg.Window = 150 * time.Millisecond
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	target := h.waitHandoff()
	h.assertReleasedWorker(target)
	h.assertSAAStartedLink(runs[0], target)
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	assertResult(t, requests[0], result)
	h.assertNexusHistory(runs[0], true)
	description, err := h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Describe(h.ctx, client.DescribeActivityOptions{})
	if err != nil || description.Status != enums.ACTIVITY_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("external completion did not complete the Nexus-backed SAA: description=%+v error=%v", description, err)
	}
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if got := h.persistedJobs(); got != 1 {
		t.Fatalf("one handed-off SAA created %d provider batches", got)
	}
}

func testDuplicateRequest(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window = 150 * time.Millisecond
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	assertResult(t, requests[0], result)
	// A distinct caller produces a distinct Nexus operation request ID, so
	// Nexus start idempotency must not hide this logical-ID ownership conflict.
	duplicate, err := h.c.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{
		ID: "duplicate-caller/" + h.id, TaskQueue: h.callerQueue,
	}, orchestration.CallerWorkflow, model.CallerInput{Request: requests[0], Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	h.runs = append(h.runs, duplicate)
	assertFailureCode(t, duplicate.Get(h.ctx, &result), "DUPLICATE_REQUEST")
	h.assertNexusHistory(duplicate, false)
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if h.persistedJobs() != 1 {
		t.Fatal("independent duplicate request was submitted to the provider")
	}
}

func testCancellation(t *testing.T) {
	h := newHarness(t, "success", 3*time.Second, false)
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 2
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 2)
	h.awaitJobs(1)
	if err := h.c.CancelWorkflow(h.ctx, runs[0].GetID(), runs[0].GetRunID()); err != nil {
		t.Fatal(err)
	}
	var canceledResult model.Result
	if err := runs[0].Get(h.ctx, &canceledResult); !temporal.IsCanceledError(err) {
		t.Fatalf("cancelled caller returned %v; want cancellation", err)
	}
	// The public async marker does not forward post-handoff cancellation to
	// OpenAI. The SAA records the request while the immutable shared job runs.
	description, err := h.activityForCaller(runs[0]).Describe(h.ctx, client.DescribeActivityOptions{})
	if err != nil || description.Status != enums.ACTIVITY_EXECUTION_STATUS_RUNNING || description.RunState != enums.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED {
		t.Fatalf("cancellation was not delivered to the handed-off SAA: description=%+v error=%v", description, err)
	}
	var result model.Result
	if err := runs[1].Get(h.ctx, &result); err != nil {
		t.Fatalf("canceling sibling disrupted remaining caller: %v", err)
	}
	assertResult(t, requests[1], result)
	status := h.finish(coord)
	assertReport(t, status, 2, 1)
	if status.Batches[0].Requests != 2 || h.persistedJobs() != 1 {
		t.Fatal("canceling a sealed request must preserve the shared manifest and submission")
	}
}

func testRestart(t *testing.T) {
	h := newHarness(t, "success", 3*time.Second, false)
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 3
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 3)
	h.awaitJobs(1)
	jobs, err := h.p.Find(h.ctx, map[string]string{"run_id": h.id})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("read original provider batch: jobs=%v error=%v", jobs, err)
	}
	if jobs[0].Terminal() {
		t.Fatal("restart test must interrupt polling before provider completion")
	}
	originalID := jobs[0].ID
	h.awaitStatus(func(status model.RunStatus) bool {
		for _, report := range status.Batches {
			if report.ProviderBatchID == originalID && report.Status != "completed" && report.Status != "completed_with_errors" {
				return true
			}
		}
		return false
	})
	// This replacement has no in-memory state from the original provider or
	// workflow workers. Submitted jobs survive in the fake provider's files.
	h.replaceWorkers()
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("caller after worker replacement: %v", err)
		}
		assertResult(t, requests[i], result)
	}
	status := h.finish(coord)
	assertReport(t, status, 3, 1)
	if status.Batches[0].ProviderBatchID != originalID || h.persistedJobs() != 1 {
		t.Fatalf("worker replacement submitted another batch: %+v", status.Batches)
	}
}

func testDeliveryRestart(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 15*time.Second, 100
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 100)
	h.awaitJobs(1)
	jobs, err := h.p.Find(h.ctx, map[string]string{"run_id": h.id})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("read original provider batch: jobs=%v error=%v", jobs, err)
	}
	originalID := jobs[0].ID
	completedBeforeStop := h.awaitPartialDelivery(model.BatchWorkflowID(h.id+"-00000001"), len(runs))
	t.Logf("replacing service worker after %d of %d external completion Activities finished", completedBeforeStop, len(runs))
	h.replaceWorkers()
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("caller %d after worker replacement during delivery: %v", i, err)
		}
		assertResult(t, requests[i], result)
		h.assertNexusHistory(run, true)
	}
	status := h.finish(coord)
	assertReport(t, status, len(runs), 1)
	report := status.Batches[0]
	if report.ProviderBatchID != originalID || report.Delivered != len(runs) || report.Skipped != 0 || h.persistedJobs() != 1 {
		t.Fatalf("worker replacement did not resume the existing delivery manifest: %+v", report)
	}
}

func testHandoffRestart(t *testing.T) {
	h := newHarness(t, "success", 2*time.Second, true)
	cfg := h.config()
	cfg.Window = 500 * time.Millisecond
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	target := h.waitHandoff()
	h.assertReleasedWorker(target)
	h.replaceWorkers()
	description, err := h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Describe(h.ctx, client.DescribeActivityOptions{})
	if err != nil || description.Status != enums.ACTIVITY_EXECUTION_STATUS_RUNNING || description.Attempt != target.Attempt {
		t.Fatalf("worker replacement lost the original externally completing SAA: description=%+v error=%v", description, err)
	}
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatalf("caller after worker replacement with handed-off SAA: %v", err)
	}
	assertResult(t, requests[0], result)
	h.assertNexusHistory(runs[0], true)
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if h.persistedJobs() != 1 {
		t.Fatal("worker replacement after handoff created another provider batch")
	}
}

func testStaleAttempt(t *testing.T) {
	h := newHarness(t, "success", 3*time.Second, true)
	cfg := h.config()
	cfg.Window = 150 * time.Millisecond
	cfg.ResultDeadline, cfg.OperationTimeout = 6*time.Second, 8*time.Second
	coord := h.createRun(cfg)
	request := h.request(0)
	// Force a superseding attempt, then prove the old token stays fenced while
	// the new attempt reattaches to the original batch and receives its result.
	handle, err := h.c.ExecuteActivity(h.ctx, client.StartActivityOptions{
		ID: "retry-saa/" + h.id, TaskQueue: h.serviceQueue,
		StartToCloseTimeout: cfg.OperationTimeout, ScheduleToCloseTimeout: cfg.OperationTimeout,
		HeartbeatTimeout: 2 * time.Second,
		RetryPolicy:      &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 2},
	}, "SubmitRequest", model.SAAInput{Request: request, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	h.activities = append(h.activities, handle)
	target := h.waitHandoff()
	newTarget := h.waitHandoff()
	if newTarget.Attempt <= target.Attempt || newTarget.ActivityRunID != target.ActivityRunID {
		t.Fatal("retry did not preserve execution identity and advance its attempt")
	}
	var result model.Result
	err = handle.Get(h.ctx, &result)
	if err != nil {
		t.Fatalf("new attempt did not receive the original batch result: %v", err)
	}
	assertResult(t, request, result)
	err = h.c.CompleteActivity(h.ctx, target.TaskToken, model.Result{RequestID: request.RequestID, Text: "stale completion"}, nil)
	var missing *serviceerror.NotFound
	if !errors.As(err, &missing) {
		t.Fatalf("server accepted an outdated attempt's exact completion token: %v", err)
	}
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if h.persistedJobs() != 1 || status.Batches[0].Delivered != 1 {
		t.Fatalf("retry changed upstream submission or completed a stale recipient: %+v", status)
	}
}

func testCompletionAckLoss(t *testing.T, scenario string) {
	h := newHarness(t, scenario, 100*time.Millisecond, false, func(h *harness) { h.loseCompletionAck = true })
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 1
	coord := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	var result model.Result
	err := runs[0].Get(h.ctx, &result)
	if scenario == "expired" {
		assertFailureCode(t, err, "BATCH_EXPIRED")
	} else {
		if err != nil {
			t.Fatal(err)
		}
		assertResult(t, requests[0], result)
	}
	h.assertNexusHistory(runs[0], err == nil)
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if h.completionCalls.Load() != 1 || h.completionAttempts.Load() < 2 {
		t.Fatalf("completion must retry by reconciling the stored terminal outcome: RPCs=%d Activity attempts=%d", h.completionCalls.Load(), h.completionAttempts.Load())
	}
	if status.Batches[0].Delivered != 1 || status.Batches[0].Skipped != 0 || h.persistedJobs() != 1 {
		t.Fatalf("lost acknowledgment corrupted completion accounting or resubmitted: %+v", status.Batches[0])
	}
}

type enrollmentAckLossRepository struct {
	orchestration.Repository
	calls atomic.Int32
}

func (r *enrollmentAckLossRepository) Enroll(ctx context.Context, enrollment model.Enrollment) (model.Admission, error) {
	admission, err := r.Repository.Enroll(ctx, enrollment)
	if err == nil && r.calls.Add(1) == 1 {
		return model.Admission{}, serviceerror.NewUnavailable("injected acknowledgment loss after PostgreSQL enrollment committed")
	}
	return admission, err
}

func testEnrollmentCommitAckLoss(t *testing.T) {
	var fault *enrollmentAckLossRepository
	h := newHarness(t, "success", 100*time.Millisecond, true, func(h *harness) {
		fault = &enrollmentAckLossRepository{Repository: h.repo}
		h.repo = fault
	})
	cfg := h.config()
	cfg.MaxBatchRequests = 1
	runID := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	assertResult(t, requests[0], result)
	status := h.finish(runID)
	assertReport(t, status, 1, 1)
	if fault.calls.Load() < 2 || h.persistedJobs() != 1 {
		t.Fatalf("commit retry was not idempotent: admissions=%d provider jobs=%d", fault.calls.Load(), h.persistedJobs())
	}
	h.assertSmallBatchHistory(status.Batches[0].BatchID)
}

type dispatchGapRepository struct {
	orchestration.Repository
	calls             atomic.Int32
	started, repaired chan struct{}
}

func (r *dispatchGapRepository) MarkDispatched(ctx context.Context, batchID string) error {
	if r.calls.Add(1) == 1 {
		close(r.started)
		return serviceerror.NewUnavailable("injected crash boundary after Workflow start before dispatch acknowledgment")
	}
	err := r.Repository.MarkDispatched(ctx, batchID)
	if err == nil {
		select {
		case <-r.repaired:
		default:
			close(r.repaired)
		}
	}
	return err
}

func testDispatchCommitGap(t *testing.T) {
	var fault *dispatchGapRepository
	h := newHarness(t, "success", 13*time.Second, false, func(h *harness) {
		fault = &dispatchGapRepository{Repository: h.repo, started: make(chan struct{}), repaired: make(chan struct{})}
		h.repo = fault
	})
	cfg := h.config()
	cfg.MaxBatchRequests = 1
	runID := h.createRun(cfg)
	runs, requests := h.callers(cfg, 1)
	select {
	case <-fault.started:
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	workflowID := model.BatchWorkflowID(h.id + "-00000001")
	before, err := h.c.DescribeWorkflowExecution(h.ctx, workflowID, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-fault.repaired:
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	var result model.Result
	if err := runs[0].Get(h.ctx, &result); err != nil {
		t.Fatal(err)
	}
	assertResult(t, requests[0], result)
	status := h.finish(runID)
	assertReport(t, status, 1, 1)
	after, err := h.c.DescribeWorkflowExecution(h.ctx, workflowID, "")
	if err != nil {
		t.Fatal(err)
	}
	if before.WorkflowExecutionInfo.Execution.RunId != after.WorkflowExecutionInfo.Execution.RunId || h.persistedJobs() != 1 {
		t.Fatal("dispatch acknowledgment recovery started a replacement batch execution")
	}
	h.assertSmallBatchHistory(status.Batches[0].BatchID)
}

func testBeyondThousand(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 100
	cfg.ResultDeadline, cfg.OperationTimeout = 2*time.Minute, 3*time.Minute
	runID := h.createRun(cfg)
	extra := worker.New(h.c, h.serviceQueue, worker.Options{})
	orchestration.RegisterService(extra, h.c, h.p, h.repo)
	orchestration.RegisterBatch(extra, h.c, h.p, h.repo)
	if err := extra.Start(); err != nil {
		t.Fatal(err)
	}
	defer extra.Stop()
	secondCtx, stop := context.WithCancel(h.ctx)
	defer stop()
	secondDone := make(chan error, 1)
	go func() { secondDone <- orchestration.Dispatcher(secondCtx, h.c, h.repo, "fake", h.serviceQueue, h.id) }()
	defer func() {
		stop()
		if err := <-secondDone; err != nil {
			t.Errorf("second dispatcher: %v", err)
		}
	}()
	runs, requests := h.callers(cfg, 1201)
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("caller %d of 1201: %v", i, err)
		}
		assertResult(t, requests[i], result)
		if i%100 == 0 {
			h.assertNexusHistory(run, true)
		}
	}
	status := h.finish(runID)
	assertReport(t, status, 1201, 13)
	if h.persistedJobs() != 13 {
		t.Fatalf("concurrent workers dispatched %d provider jobs, want13", h.persistedJobs())
	}
	h.assertServiceHasNoRequestWorkflows()
	for _, report := range status.Batches {
		h.assertSmallBatchHistory(report.BatchID)
	}
}

type harness struct {
	t                  *testing.T
	ctx                context.Context
	c                  client.Client
	id, namespace      string
	serviceQueue       string
	callerQueue        string
	stateDir, scenario string
	delay              time.Duration
	p                  provider.Provider
	service, caller    worker.Worker
	runs               []client.WorkflowRun
	captureHandoffs    bool
	handoffs           chan model.CompletionTarget
	activities         []client.ActivityHandle
	loseCompletionAck  bool
	completionCalls    atomic.Int32
	completionAttempts atomic.Int32
	db                 *store.Store
	repo               orchestration.Repository
	dispatchStop       context.CancelFunc
	dispatchDone       chan error

	endpoint *nexuspb.Endpoint
}

func newHarness(t *testing.T, scenario string, delay time.Duration, captureHandoffs bool, configure ...func(*harness)) *harness {
	t.Helper()
	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7234"
	}
	namespace := os.Getenv("TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace,
		Logger: tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))})
	if err != nil {
		t.Fatalf("connect to Temporal at %s: %v", address, err)
	}
	h := &harness{t: t, ctx: ctx, c: c, namespace: namespace, id: fmt.Sprintf("it-%d", time.Now().UnixNano()),
		stateDir: t.TempDir(), scenario: scenario, delay: delay, captureHandoffs: captureHandoffs,
		handoffs: make(chan model.CompletionTarget, 128)}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = store.LocalDSN
	}
	db, err := store.Open(ctx, dsn, namespace)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	h.db = db
	h.repo = db
	h.serviceQueue, h.callerQueue = h.id+"-service", h.id+"-caller"
	for _, apply := range configure {
		apply(h)
	}
	t.Cleanup(h.cleanup)
	created, err := c.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{Name: h.id, Target: &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: namespace, TaskQueue: h.serviceQueue}},
		}},
	})
	if err != nil {
		t.Fatalf("create isolated Nexus endpoint (requires Nexus-enabled local server): %v", err)
	}
	h.endpoint = created.Endpoint
	h.startService()
	h.startCaller()
	return h
}

func (h *harness) startCaller() {
	h.t.Helper()
	h.caller = worker.New(h.c, h.callerQueue, worker.Options{})
	orchestration.RegisterCaller(h.caller)
	if err := h.caller.Start(); err != nil {
		h.t.Fatalf("start caller worker: %v", err)
	}
}

func (h *harness) replaceWorkers() {
	h.t.Helper()
	h.stopDispatcher()
	h.service.Stop()
	h.service = nil
	h.caller.Stop()
	h.caller = nil
	// SDK workers share a process-wide workflow cache. Purge it only after
	// both have stopped so replacement must replay Temporal history.
	worker.PurgeStickyWorkflowCache()
	h.startService()
	h.startCaller()
}

func (h *harness) startService() {
	h.t.Helper()
	p, err := provider.NewFake(provider.FakeConfig{StateDir: h.stateDir, Scenario: h.scenario, ProcessingDelay: h.delay})
	if err != nil {
		h.t.Fatal(err)
	}
	h.p = p
	options := worker.Options{WorkerStopTimeout: time.Second}
	if h.captureHandoffs {
		options.MaxConcurrentActivityExecutionSize = 1
	}
	h.service = worker.New(h.c, h.serviceQueue, options)
	orchestration.RegisterService(h.service, h.c, p, h.repo)
	orchestration.RegisterBatch(h.service, h.c, p, h.repo)
	h.service.RegisterActivityWithOptions(func(context.Context) (string, error) { return "worker-slot-released", nil },
		activity.RegisterOptions{Name: "IntegrationProbe"})
	if h.captureHandoffs {
		activities := &orchestration.ServiceActivities{Client: h.c, Provider: p, Store: h.repo}
		h.service.RegisterActivityWithOptions(func(ctx context.Context, input model.SAAInput) (model.Result, error) {
			result, err := activities.SubmitRequest(ctx, input)
			if errors.Is(err, activity.ErrResultPending) {
				info := activity.GetInfo(ctx)
				h.handoffs <- model.CompletionTarget{Namespace: info.Namespace, ActivityID: info.ActivityID,
					ActivityRunID: info.ActivityRunID, TaskToken: append([]byte(nil), info.TaskToken...), Attempt: info.Attempt}
			}
			return result, err
		}, activity.RegisterOptions{Name: "SubmitRequest", DisableAlreadyRegisteredCheck: true})
	}
	if h.loseCompletionAck {
		activities := &orchestration.ServiceActivities{Client: &lostCompletionAckClient{Client: h.c, calls: &h.completionCalls}, Provider: p, Store: h.repo}
		h.service.RegisterActivityWithOptions(func(ctx context.Context, ref model.BatchRef) (model.DeliverySummary, error) {
			h.completionAttempts.Add(1)
			return activities.Deliver(ctx, ref)
		}, activity.RegisterOptions{Name: "Deliver", DisableAlreadyRegisteredCheck: true})
	}
	if err := h.service.Start(); err != nil {
		h.t.Fatalf("start service worker: %v", err)
	}
	dispatchCtx, stop := context.WithCancel(h.ctx)
	h.dispatchStop = stop
	h.dispatchDone = make(chan error, 1)
	go func() {
		h.dispatchDone <- orchestration.Dispatcher(dispatchCtx, h.c, h.repo, "fake", h.serviceQueue, h.id)
	}()

}

type lostCompletionAckClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *lostCompletionAckClient) CompleteActivity(ctx context.Context, token []byte, result any, failure error) error {
	call := c.calls.Add(1)
	err := c.Client.CompleteActivity(ctx, token, result, failure)
	if err == nil && call == 1 {
		return serviceerror.NewUnavailable("injected lost acknowledgment after external completion committed")
	}
	return err
}

func (h *harness) config() model.Config {
	cfg := model.DefaultConfig("fake")
	cfg.Endpoint, cfg.PollInterval = h.id, 100*time.Millisecond
	cfg.ResultDeadline, cfg.OperationTimeout = 45*time.Second, 2*time.Minute
	return cfg
}

func (h *harness) createRun(cfg model.Config) string {
	h.t.Helper()
	if err := h.db.CreateRun(h.ctx, model.RunInput{RunID: h.id, Config: cfg}); err != nil {
		h.t.Fatal(err)
	}
	return h.id
}

func (h *harness) request(i int) model.Request {
	return model.Request{RunID: h.id, RequestID: fmt.Sprintf("document-%04d", i), Input: fmt.Sprintf("Summarize document %04d", i)}
}

func (h *harness) callers(cfg model.Config, count int) ([]client.WorkflowRun, []model.Request) {
	h.t.Helper()
	runs := make([]client.WorkflowRun, count)
	requests := make([]model.Request, count)
	for i := range runs {
		requests[i] = h.request(i)
		run, err := h.c.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{
			ID: model.CallerID(h.id, requests[i].RequestID), TaskQueue: h.callerQueue, WorkflowExecutionTimeout: 75 * time.Second,
		}, orchestration.CallerWorkflow, model.CallerInput{Request: requests[i], Config: cfg})
		if err != nil {
			h.t.Fatalf("start caller %d: %v", i, err)
		}
		runs[i] = run
		h.runs = append(h.runs, run)
	}
	return runs, requests
}

func (h *harness) finish(runID string) model.RunStatus {
	h.t.Helper()
	if err := h.db.CloseRun(h.ctx, runID); err != nil {
		h.t.Fatal(err)
	}
	return h.awaitStatus(func(status model.RunStatus) bool {
		if !status.Closed || status.Buffered != 0 {
			return false
		}
		count := 0
		for _, report := range status.Batches {
			if (report.Status != "completed" && report.Status != "completed_with_errors") || report.Succeeded+report.Failed != report.Requests || report.Delivered+report.Skipped != report.Requests {
				return false
			}
			count += report.Requests
		}
		return count == status.Admitted
	})
}

func (h *harness) persistedJobs() int {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.stateDir, "state.json"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		h.t.Fatal(err)
	}
	var state struct {
		Jobs map[string]json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		h.t.Fatal(err)
	}
	return len(state.Jobs)
}

func (h *harness) awaitJobs(count int) {
	h.t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.persistedJobs() == count {
			return
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-deadline.C:
			h.t.Fatalf("timed out awaiting %d persisted provider jobs", count)
		case <-tick.C:
		}
	}
}

func (h *harness) awaitStatus(ready func(model.RunStatus) bool) model.RunStatus {
	h.t.Helper()
	deadline := time.NewTimer(3 * time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var last model.RunStatus
	for {
		value, err := h.db.Status(h.ctx, h.id)
		if err == nil {
			last = value
			if ready(last) {
				return last
			}
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-deadline.C:
			h.t.Fatalf("run did not expose expected in-flight progress: %+v", last)
		case <-tick.C:
		}
	}
}

func (h *harness) awaitPartialDelivery(batchWorkflowID string, total int) int {
	h.t.Helper()
	for {
		summary, err := h.db.DeliverySummary(h.ctx, strings.TrimPrefix(batchWorkflowID, "batch/"))
		if err != nil {
			h.t.Fatal(err)
		}
		if summary.Delivered > 0 && summary.Delivered < total {
			return summary.Delivered
		}
		if summary.Delivered == total {
			h.t.Fatal("all database-checkpointed deliveries finished before interruption")
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (h *harness) waitHandoff() model.CompletionTarget {
	h.t.Helper()
	select {
	case target := <-h.handoffs:
		if target.ActivityID == "" || target.ActivityRunID == "" || target.Attempt < 1 || len(target.TaskToken) == 0 {
			h.t.Fatal("SubmitRequest returned ErrResultPending without an exact SAA completion identity")
		}
		return target
	case <-h.ctx.Done():
		h.t.Fatal(h.ctx.Err())
	case <-time.After(20 * time.Second):
		h.t.Fatal("SubmitRequest did not return ErrResultPending after durable admission")
	}
	return model.CompletionTarget{}
}

func (h *harness) assertReleasedWorker(target model.CompletionTarget) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()
	probe, err := h.c.ExecuteActivity(ctx, client.StartActivityOptions{
		ID: "integration-probe/" + h.id + fmt.Sprintf("/%d", time.Now().UnixNano()), TaskQueue: h.serviceQueue,
		ScheduleToCloseTimeout: 2 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	}, "IntegrationProbe")
	if err != nil {
		h.t.Fatalf("schedule worker-release probe: %v", err)
	}
	h.activities = append(h.activities, probe)
	var result string
	if err := probe.Get(ctx, &result); err != nil || result != "worker-slot-released" {
		h.t.Fatalf("handed-off SAA still occupies the worker's only Activity slot: result=%q error=%v", result, err)
	}
	description, err := h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Describe(h.ctx, client.DescribeActivityOptions{})
	if err != nil || description.Status != enums.ACTIVITY_EXECUTION_STATUS_RUNNING || description.RunState != enums.PENDING_ACTIVITY_STATE_STARTED {
		h.t.Fatalf("handed-off SAA must remain pending on the public API while its worker slot is released: description=%+v error=%v", description, err)
	}
}

func (h *harness) assertSAAStartedLink(run client.WorkflowRun, target model.CompletionTarget) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	for {
		iter := h.c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				h.t.Fatal(err)
			}
			if started := event.GetNexusOperationStartedEventAttributes(); started != nil {
				id, execution := h.decodeSAAOperationToken(started.OperationToken)
				if id != target.ActivityID || (execution != "" && execution != target.ActivityRunID) {
					h.t.Fatal("Nexus operation token does not identify the handed-off SAA")
				}
				for _, link := range event.Links {
					if activity := link.GetActivity(); activity != nil && activity.Namespace == target.Namespace && activity.ActivityId == target.ActivityID && activity.RunId == target.ActivityRunID {
						return
					}
					if link.GetWorkflowEvent() != nil || link.GetWorkflow() != nil {
						h.t.Fatal("Nexus operation start links to a provider Workflow instead of the SAA")
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			h.t.Fatal("NexusOperationStarted did not expose the exact SAA activity link")
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (h *harness) activityForCaller(run client.WorkflowRun) client.ActivityHandle {
	h.t.Helper()
	iter := h.c.GetWorkflowHistory(h.ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			h.t.Fatal(err)
		}
		if started := event.GetNexusOperationStartedEventAttributes(); started != nil {
			id, execution := h.decodeSAAOperationToken(started.OperationToken)
			return h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id, RunID: execution})
		}
	}
	h.t.Fatal("caller history has no standalone Activity-backed Nexus operation")
	return nil
}

func (h *harness) decodeSAAOperationToken(token string) (string, string) {
	h.t.Helper()
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		h.t.Fatalf("decode pinned SDK Nexus operation token: %v", err)
	}
	// This is the token shape implemented by the pinned SDK's temporalnexus
	// package; type 2 identifies a standalone Activity execution, not a Workflow.
	var decoded struct {
		Type       int    `json:"t"`
		Namespace  string `json:"ns"`
		ActivityID string `json:"aid"`
		RunID      string `json:"rid"`
		WorkflowID string `json:"wid"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.Type != 2 || decoded.Namespace != h.namespace || decoded.ActivityID == "" || decoded.WorkflowID != "" {
		h.t.Fatal("Nexus operation token does not reference a standalone Activity execution")
	}
	return decoded.ActivityID, decoded.RunID
}

func (h *harness) assertServiceHasNoRequestWorkflows() {
	h.t.Helper()
	response, err := h.c.ListWorkflow(h.ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.namespace, PageSize: 1000, Query: fmt.Sprintf("TaskQueue = '%s'", h.serviceQueue),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	for _, execution := range response.Executions {
		if execution.Type.Name != "BatchWorkflow" {
			h.t.Fatalf("service started a per-request or unexpected Workflow: %s", execution.Type.Name)
		}
	}
}

func (h *harness) assertNexusHistory(run client.WorkflowRun, success bool) {
	h.t.Helper()
	scheduled, started, completed, failed := 0, 0, 0, 0
	iter := h.c.GetWorkflowHistory(h.ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			h.t.Fatalf("read caller history: %v", err)
		}
		switch event.EventType {
		case enums.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
			scheduled++
			attr := event.GetNexusOperationScheduledEventAttributes()
			if attr.GetEndpoint() != h.id || attr.GetService() != model.Service || attr.GetOperation() != model.Operation {
				h.t.Fatalf("caller crossed wrong Nexus boundary: %+v", attr)
			}
		case enums.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			completed++
		case enums.EVENT_TYPE_NEXUS_OPERATION_STARTED:
			started++
			id, execution := h.decodeSAAOperationToken(event.GetNexusOperationStartedEventAttributes().OperationToken)
			description, err := h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id, RunID: execution}).Describe(h.ctx, client.DescribeActivityOptions{})
			if err != nil || description.ActivityType != "SubmitRequest" {
				h.t.Fatalf("Nexus did not invoke a real standalone SubmitRequest Activity: description=%+v error=%v", description, err)
			}
		case enums.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			failed++
		case enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, enums.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			h.t.Fatalf("caller must delegate provider orchestration through Nexus: %s", event.EventType)
		}
	}
	if scheduled != 1 || started != 1 || (success && completed != 1) || (!success && failed != 1) {
		h.t.Fatalf("unexpected Nexus history: scheduled=%d started=%d completed=%d failed=%d success=%t", scheduled, started, completed, failed, success)
	}
}

func (h *harness) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	h.stopDispatcher()
	_ = h.db.StopRun(ctx, h.id)
	// Never enumerate or mutate an unrelated queue. Clean the actual SAAs and
	// abandoned batch children if an assertion failed before external completion.
	activityList, err := h.c.ListActivities(ctx, client.ListActivitiesOptions{Query: fmt.Sprintf("TaskQueue = '%s'", h.serviceQueue)})
	if err == nil {
		for execution, listErr := range activityList.Results {
			if listErr == nil && execution.Status == enums.ACTIVITY_EXECUTION_STATUS_RUNNING {
				_ = h.c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: execution.ActivityID, RunID: execution.ActivityRunID}).Terminate(ctx, client.TerminateActivityOptions{Reason: "integration test cleanup"})
			}
		}
	}
	for _, execution := range h.activities {
		_ = execution.Terminate(ctx, client.TerminateActivityOptions{Reason: "integration test cleanup"})
	}
	for _, queue := range []string{h.serviceQueue, h.callerQueue} {
		response, err := h.c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: h.namespace, PageSize: 1000,
			Query: fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", queue),
		})
		if err == nil {
			for _, execution := range response.Executions {
				_ = h.c.TerminateWorkflow(ctx, execution.Execution.WorkflowId, execution.Execution.RunId, "integration test cleanup")
			}
		}
	}
	for _, run := range h.runs {
		_ = h.c.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "integration test cleanup")
	}
	if h.caller != nil {
		h.caller.Stop()
	}
	if h.service != nil {
		h.service.Stop()
	}
	if h.endpoint != nil {
		_, err := h.c.OperatorService().DeleteNexusEndpoint(ctx, &operatorservice.DeleteNexusEndpointRequest{
			Id: h.endpoint.Id, Version: h.endpoint.Version,
		})
		var missing *serviceerror.NotFound
		if err != nil && !errors.As(err, &missing) {
			h.t.Errorf("delete test Nexus endpoint: %v", err)
		}
	}
	h.c.Close()
	h.db.Close()
}

func assertResult(t *testing.T, request model.Request, result model.Result) {
	t.Helper()
	if result.RequestID != request.RequestID || result.Text != "Fake response: "+request.Input {
		t.Fatalf("result was correlated to the wrong caller: request=%+v result=%+v", request, result)
	}
	if result.Usage == nil || result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 20 {
		t.Fatalf("normalized token usage missing: %+v", result)
	}
}

func assertFailureCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || code == "" {
		t.Fatalf("unexpected caller failure: %v (expected code %q)", err, code)
	}
	var nexusFailure *temporal.NexusOperationError
	if !errors.As(err, &nexusFailure) {
		t.Fatalf("failure did not cross the Nexus operation boundary: %v", err)
	}
	// Nexus and workflow failure converters may add application-error wrappers.
	// Check every cause before falling back to the rendered stable code.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if application, ok := cause.(*temporal.ApplicationError); ok && application.Type() == code {
			return
		}
	}
	if !strings.Contains(err.Error(), code) {
		t.Fatalf("failure lacks typed code %s: %v", code, err)
	}
}

func assertReport(t *testing.T, status model.RunStatus, requests, batches int) {
	t.Helper()
	if status.Backing != "nexus_standalone_activity" || status.CompletionMode != "external_async_completion" {
		t.Fatalf("report must identify actual SAA backing and the public SDK's completion mode: %+v", status)
	}
	if status.Storage != "postgres" {
		t.Fatalf("run state did not come from external PostgreSQL storage: %+v", status)
	}
	if !status.Closed || status.Buffered != 0 || status.Admitted != requests || len(status.Batches) != batches {
		t.Fatalf("incorrect final run accounting: %+v; want %d admissions, %d batches", status, requests, batches)
	}
	total := 0
	for _, report := range status.Batches {
		expectedStatus := "completed"
		if report.Failed > 0 {
			expectedStatus = "completed_with_errors"
		}
		if report.Status != expectedStatus || report.Succeeded+report.Failed != report.Requests || report.Delivered+report.Skipped != report.Requests {
			t.Fatalf("incomplete batch accounting: %+v", report)
		}
		total += report.Requests
	}
	if total != requests {
		t.Fatalf("manifests contain %d entries, want %d", total, requests)
	}
}

func (h *harness) stopDispatcher() {
	if h.dispatchStop == nil {
		return
	}
	h.dispatchStop()
	err := <-h.dispatchDone
	h.dispatchStop = nil
	if err != nil && !errors.Is(err, context.Canceled) {
		h.t.Errorf("dispatcher stopped unexpectedly: %v", err)
	}
}
func (h *harness) assertSmallBatchHistory(batchID string) {
	h.t.Helper()
	iter := h.c.GetWorkflowHistory(h.ctx, model.BatchWorkflowID(batchID), "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	events, scheduled, polls := 0, 0, 0
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			h.t.Fatal(err)
		}
		events++
		data, err := proto.Marshal(event)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, forbidden := range []string{`"task_token"`, `"completion_target"`, `"request_id"`, `"entries"`, "Summarize document ", "Fake response:", "nexus_batch@"} {
			if bytes.Contains(data, []byte(forbidden)) {
				h.t.Fatalf("batch workflow history leaked per-request data or credentials: %s", forbidden)
			}
		}
		if attr := event.GetWorkflowExecutionStartedEventAttributes(); attr != nil {
			for _, p := range attr.Input.Payloads {
				if len(p.Data) > 4096 {
					h.t.Fatalf("batch reference grew to %d bytes", len(p.Data))
				}
			}
		}
		if scheduledActivity := event.GetActivityTaskScheduledEventAttributes(); scheduledActivity != nil {
			scheduled++
			if scheduledActivity.ActivityType.Name == "Retrieve" {
				polls++
			}
		}
	}
	// Durable provider polling grows with elapsed time. Exclude that fixed
	// polling loop when checking that orchestration does not grow per item.
	if events-12*polls > 150 || scheduled-polls > 20 {
		h.t.Fatalf("batch history grew with item count: events=%d Activities=%d", events, scheduled)
	}
}
