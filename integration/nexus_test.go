package integration

import (
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
	t.Run("1001st_admission_is_rejected_at_the_hard_run_limit", testAdmissionBound)
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
			coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
	runs, requests := h.callers(cfg, 100)
	h.awaitJobs(1)
	jobs, err := h.p.Find(h.ctx, map[string]string{"run_id": h.id})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("read original provider batch: jobs=%v error=%v", jobs, err)
	}
	originalID := jobs[0].ID
	completedBeforeStop := h.awaitPartialDelivery(model.BatchWorkflowID(h.id+"-0001"), len(runs))
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
	coord := h.coordinator(cfg)
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
	coord := h.coordinator(cfg)
	request := h.request(0)
	// Production Nexus starts one SAA attempt. Explicitly enable two attempts
	// here to verify that a timeout cannot silently authorize another upstream
	// submission or let the old completion token complete a new attempt.
	handle, err := h.c.ExecuteActivity(h.ctx, client.StartActivityOptions{
		ID: "retry-saa/" + h.id, TaskQueue: h.serviceQueue,
		StartToCloseTimeout: time.Second, ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 2},
	}, "SubmitRequest", model.SAAInput{Request: request, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	h.activities = append(h.activities, handle)
	target := h.waitHandoff()
	var result model.Result
	err = handle.Get(h.ctx, &result)
	var application *temporal.ApplicationError
	if !errors.As(err, &application) || application.Type() != "STALE_ACTIVITY_ATTEMPT" {
		t.Fatalf("new attempt must not replace an already-admitted completion token: %v", err)
	}
	err = h.c.CompleteActivity(h.ctx, target.TaskToken, model.Result{RequestID: request.RequestID, Text: "stale completion"}, nil)
	var missing *serviceerror.NotFound
	if !errors.As(err, &missing) {
		t.Fatalf("server accepted an outdated attempt's exact completion token: %v", err)
	}
	status := h.finish(coord)
	assertReport(t, status, 1, 1)
	if h.persistedJobs() != 1 || status.Batches[0].Skipped != 1 {
		t.Fatalf("retry changed upstream submission or completed a stale recipient: %+v", status)
	}
}

func testCompletionAckLoss(t *testing.T, scenario string) {
	h := newHarness(t, scenario, 100*time.Millisecond, false, func(h *harness) { h.loseCompletionAck = true })
	cfg := h.config()
	cfg.Window, cfg.MaxBatchRequests = 10*time.Second, 1
	coord := h.coordinator(cfg)
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

func testAdmissionBound(t *testing.T) {
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.Window, cfg.ResultDeadline, cfg.OperationTimeout = 2*time.Minute, 3*time.Minute, 4*time.Minute
	cfg.MaxAdmissions, cfg.MaxBatchRequests = model.HardMaxAdmissions, model.HardMaxBatchRequests
	coord := h.coordinator(cfg)
	for i := 0; i <= model.HardMaxAdmissions; i++ {
		request := h.request(i)
		enrollment := model.Enrollment{Request: request, Target: model.CompletionTarget{
			Namespace: h.namespace, ActivityID: fmt.Sprintf("absent-recipient/%s/%04d", h.id, i),
			ActivityRunID: "00000000-0000-4000-8000-000000000000", TaskToken: []byte("not-an-active-task-token"), Attempt: 1,
		}, Fingerprint: model.Fingerprint(request)}
		handle, err := h.c.UpdateWorkflow(h.ctx, client.UpdateWorkflowOptions{
			WorkflowID: coord.GetID(), RunID: coord.GetRunID(), UpdateID: fmt.Sprintf("admit-%04d", i),
			UpdateName: model.EnrollUpdate, Args: []any{enrollment}, WaitForStage: client.WorkflowUpdateStageCompleted,
		})
		if err != nil {
			t.Fatalf("submit admission %d: %v", i+1, err)
		}
		var admission model.Admission
		err = handle.Get(h.ctx, &admission)
		if i == model.HardMaxAdmissions {
			var application *temporal.ApplicationError
			if !errors.As(err, &application) || application.Type() != "RUN_FULL" {
				t.Fatalf("admission 1001 must fail with RUN_FULL, got %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("admission %d below the cap: %v", i+1, err)
		}
		if admission.Fingerprint != enrollment.Fingerprint || admission.BatchID == "" {
			t.Fatalf("invalid admission acknowledgment %d: %+v", i+1, admission)
		}
	}
	status := h.finish(coord)
	assertReport(t, status, model.HardMaxAdmissions, model.HardMaxAdmissions/model.HardMaxBatchRequests)
	for _, report := range status.Batches {
		if report.Requests != model.HardMaxBatchRequests || report.Skipped != report.Requests || report.Delivered != 0 {
			t.Fatalf("fictitious recipients must be skipped while preserving bounded manifests: %+v", report)
		}
	}
	if jobs := h.persistedJobs(); jobs != 10 {
		t.Fatalf("1,000 admitted items must create exactly 10 bounded batches, got %d", jobs)
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
	endpoint           *nexuspb.Endpoint
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
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace,
		Logger: tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))})
	if err != nil {
		t.Fatalf("connect to Temporal at %s: %v", address, err)
	}
	h := &harness{t: t, ctx: ctx, c: c, namespace: namespace, id: fmt.Sprintf("it-%d", time.Now().UnixNano()),
		stateDir: t.TempDir(), scenario: scenario, delay: delay, captureHandoffs: captureHandoffs,
		handoffs: make(chan model.CompletionTarget, 128)}
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
	orchestration.RegisterService(h.service, h.c, p)
	h.service.RegisterActivityWithOptions(func(context.Context) (string, error) { return "worker-slot-released", nil },
		activity.RegisterOptions{Name: "IntegrationProbe"})
	if h.captureHandoffs {
		activities := &orchestration.ServiceActivities{Client: h.c, Provider: p}
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
		activities := &orchestration.ServiceActivities{Client: &lostCompletionAckClient{Client: h.c, calls: &h.completionCalls}, Provider: p}
		h.service.RegisterActivityWithOptions(func(ctx context.Context, target model.CompletionTarget, outcome model.Outcome) (orchestration.DeliveryResult, error) {
			h.completionAttempts.Add(1)
			return activities.CompleteResult(ctx, target, outcome)
		}, activity.RegisterOptions{Name: "CompleteResult", DisableAlreadyRegisteredCheck: true})
	}
	if err := h.service.Start(); err != nil {
		h.t.Fatalf("start service worker: %v", err)
	}
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
	cfg.ResultDeadline, cfg.OperationTimeout = 45*time.Second, 60*time.Second
	return cfg
}

func (h *harness) coordinator(cfg model.Config) client.WorkflowRun {
	h.t.Helper()
	run, err := h.c.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{ID: model.CoordinatorID(h.id), TaskQueue: h.serviceQueue},
		orchestration.CoordinatorWorkflow, model.RunInput{RunID: h.id, Config: cfg})
	if err != nil {
		h.t.Fatalf("start coordinator: %v", err)
	}
	h.runs = append(h.runs, run)
	return run
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

func (h *harness) finish(run client.WorkflowRun) model.RunStatus {
	h.t.Helper()
	handle, err := h.c.UpdateWorkflow(h.ctx, client.UpdateWorkflowOptions{
		WorkflowID: run.GetID(), RunID: run.GetRunID(), UpdateID: "integration-finish", UpdateName: model.FinishUpdate,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		h.t.Fatalf("close coordinator admission: %v", err)
	}
	if err := handle.Get(h.ctx, nil); err != nil {
		h.t.Fatal(err)
	}
	var status model.RunStatus
	if err := run.Get(h.ctx, &status); err != nil {
		h.t.Fatalf("finish coordinator: %v", err)
	}
	return status
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
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var last model.RunStatus
	for {
		value, err := h.c.QueryWorkflow(h.ctx, model.CoordinatorID(h.id), "", model.StatusQuery)
		if err == nil {
			if err := value.Get(&last); err != nil {
				h.t.Fatal(err)
			}
			if ready(last) {
				return last
			}
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-deadline.C:
			h.t.Fatalf("coordinator did not expose expected in-flight progress: %+v", last)
		case <-tick.C:
		}
	}
}

func (h *harness) awaitPartialDelivery(batchWorkflowID string, total int) int {
	h.t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		completionActivities := make(map[int64]bool)
		completed := 0
		iter := h.c.GetWorkflowHistory(h.ctx, batchWorkflowID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				h.t.Fatalf("read batch delivery history: %v", err)
			}
			if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil && scheduled.ActivityType.Name == "CompleteResult" {
				completionActivities[event.EventId] = true
			}
			if activity := event.GetActivityTaskCompletedEventAttributes(); activity != nil && completionActivities[activity.ScheduledEventId] {
				completed++
			}
		}
		if completed > 0 && completed < total {
			return completed
		}
		if completed == total {
			h.t.Fatal("all external completion Activities finished before the test could interrupt the service worker")
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-deadline.C:
			h.t.Fatal("batch did not begin result delivery before worker replacement deadline")
		case <-tick.C:
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
		if execution.Type.Name != "CoordinatorWorkflow" && execution.Type.Name != "BatchWorkflow" {
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
	if status.Backing != "nexus_standalone_activity" || status.CompletionMode != "external_async_completion_legacy" {
		t.Fatalf("report must identify actual SAA backing and the public SDK's completion mode: %+v", status)
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
