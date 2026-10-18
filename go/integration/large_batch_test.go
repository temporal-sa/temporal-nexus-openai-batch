package integration

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/orchestration"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func TestPostgresLargeBatchHistory(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with Temporal and PostgreSQL running")
	}
	h := newHarness(t, "success", 100*time.Millisecond, false)
	cfg := h.config()
	cfg.MaxBatchRequests = 1000
	cfg.MaxBatchBytes = 8 * 1024 * 1024
	cfg.Window = time.Minute
	cfg.ResultDeadline = 2 * time.Minute
	cfg.OperationTimeout = 4 * time.Minute
	runID := h.createRun(cfg)
	const count = 1101
	runs := make([]client.WorkflowRun, count)
	requests := make([]model.Request, count)
	for i := range runs {
		request := h.request(i)
		request.Input += " private-document-sentinel " + strings.Repeat("x", 3072)
		requests[i] = request
		run, err := h.c.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{ID: model.CallerID(h.id, request.RequestID), TaskQueue: h.callerQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, orchestration.CallerWorkflow, model.CallerInput{Request: request, Config: cfg})
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = run
		h.runs = append(h.runs, run)
	}
	h.awaitStatus(func(status model.RunStatus) bool { return status.Admitted == count })
	if err := h.db.CloseRun(h.ctx, runID); err != nil {
		t.Fatal(err)
	}
	for i, run := range runs {
		var result model.Result
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("large document caller%d: %v", i, err)
		}
		if result.RequestID != requests[i].RequestID || !strings.HasPrefix(result.Text, "Fake response: Summarize document "+fmt.Sprintf("%04d", i)) {
			t.Fatalf("large document result correlation failed for%d", i)
		}
	}
	status := h.finish(runID)
	assertReport(t, status, count, 2)
	manifest, err := h.db.LoadManifest(h.ctx, status.Batches[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= 2*1024*1024 || len(manifest.Entries) != 1000 {
		t.Fatalf("fixture did not exceed Temporal payload limit: bytes=%d entries=%d", len(encoded), len(manifest.Entries))
	}
	t.Logf("external manifest contains%d entries and%d encoded bytes; Workflow stores only BatchRef", len(manifest.Entries), len(encoded))
	for _, report := range status.Batches {
		h.assertSmallBatchHistory(report.BatchID)
	}
	h.assertServiceHasNoRequestWorkflows()
	if h.persistedJobs() != 2 {
		t.Fatal("large batches were duplicated")
	}
}

func TestNexusEndpointIsolation(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with Temporal and PostgreSQL running")
	}
	owner := newHarness(t, "success", 100*time.Millisecond, false)
	other := newHarness(t, "success", 100*time.Millisecond, false)
	owner.createRun(owner.config())
	request := owner.request(0)
	run, err := other.c.ExecuteWorkflow(other.ctx, client.StartWorkflowOptions{ID: "misrouted/" + owner.id, TaskQueue: other.callerQueue}, orchestration.CallerWorkflow, model.CallerInput{Request: request, Config: other.config()})
	if err != nil {
		t.Fatal(err)
	}
	other.runs = append(other.runs, run)
	var result model.Result
	err = run.Get(other.ctx, &result)
	var operation *temporal.NexusOperationError
	if !errors.As(err, &operation) {
		t.Fatalf("misrouted request was not rejected at Nexus boundary: %v", err)
	}
	status, err := owner.db.Status(owner.ctx, owner.id)
	if err != nil {
		t.Fatal(err)
	}
	if status.Admitted != 0 || status.Buffered != 0 || len(status.Batches) != 0 || owner.persistedJobs() != 0 || other.persistedJobs() != 0 {
		t.Fatal("misrouted endpoint admitted or submitted a request")
	}
	iter := other.c.GetWorkflowHistory(other.ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if event.EventType == enums.EVENT_TYPE_NEXUS_OPERATION_STARTED {
			t.Fatal("misrouted request created a standalone Activity before endpoint validation")
		}
	}
}
