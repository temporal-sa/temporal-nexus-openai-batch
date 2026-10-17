package integration

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/orchestration"
)

type pausedDispatchRepository struct {
	orchestration.Repository
	paused  atomic.Bool
	claimed chan model.BatchRef
	release chan struct{}
}

func (r *pausedDispatchRepository) ReadyBatches(ctx context.Context, mode, endpoint string, limit int) ([]model.BatchRef, error) {
	refs, err := r.Repository.ReadyBatches(ctx, mode, endpoint, limit)
	if err == nil && len(refs) > 0 && r.paused.CompareAndSwap(false, true) {
		r.claimed <- refs[0]
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return refs, err
}

func TestNexusStopFencesAlreadyClaimedBatch(t *testing.T) {
	if os.Getenv("NEXUS_INTEGRATION") != "1" {
		t.Skip("set NEXUS_INTEGRATION=1 with Temporal and PostgreSQL running")
	}
	var fault *pausedDispatchRepository
	h := newHarness(t, "success", 100*time.Millisecond, false, func(h *harness) {
		fault = &pausedDispatchRepository{Repository: h.repo, claimed: make(chan model.BatchRef, 1), release: make(chan struct{})}
		h.repo = fault
	})
	var release sync.Once
	unpause := func() { release.Do(func() { close(fault.release) }) }
	t.Cleanup(unpause)
	cfg := h.config()
	cfg.MaxBatchRequests = 1
	runID := h.coordinator(cfg)
	h.callers(cfg, 1)
	var ref model.BatchRef
	select {
	case ref = <-fault.claimed:
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	if err := h.db.StopRun(h.ctx, runID); err != nil {
		t.Fatal(err)
	}
	unpause()
	// The preclaimed Workflow may start after stop. Its provider call must be
	// refused by the authoritative database fence, independently of visibility.
	h.awaitStatus(func(status model.RunStatus) bool {
		return len(status.Batches) == 1 && status.Batches[0].Status == "completed_with_errors"
	})
	var report model.BatchReport
	if err := h.c.GetWorkflow(h.ctx, model.BatchWorkflowID(ref.BatchID), "").Get(h.ctx, &report); err != nil {
		t.Fatal(err)
	}
	if h.persistedJobs() != 0 || report.FailureCounts["RUN_STOPPED"] != 1 || report.Skipped != 1 {
		t.Fatalf("stopped claim created provider work or failed to finalize: jobs=%d report=%+v", h.persistedJobs(), report)
	}
}
