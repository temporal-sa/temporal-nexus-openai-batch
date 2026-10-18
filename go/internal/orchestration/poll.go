package orchestration

import (
	"context"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/provider"
	"go.temporal.io/sdk/activity"
)

// Poll keeps provider polling out of Workflow history. Retried executions use
// the same provider ID and absolute deadline and never upload or create a job.
// It is Activity code, so ordinary clocks, timers, goroutines and HTTP are valid
// here. Workflow code instead uses workflow.Now and Temporal scheduling APIs.
func (a *ServiceActivities) Poll(ctx context.Context, job provider.Job, deadline time.Time, interval time.Duration) (provider.Job, error) {
	if interval <= 0 {
		return provider.Job{}, failure("INVALID_BATCH", "poll interval must be positive")
	}
	heartbeatCtx, stop := context.WithCancel(ctx)
	defer stop()
	jobID := job.ID
	if activity.IsActivity(ctx) {
		// Keep liveness independent of the provider poll interval and HTTP call
		// latency. A heartbeat identifies the existing job; it is not a result
		// checkpoint or permission to create replacement work on an Activity retry.
		activity.RecordHeartbeat(ctx, jobID)
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					activity.RecordHeartbeat(ctx, jobID)
				}
			}
		}()
	}
	for {
		if ctx.Err() != nil {
			return provider.Job{}, ctx.Err()
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return provider.Job{}, failure("DEADLINE_EXCEEDED", "provider result deadline reached")
		}
		if job.Terminal() {
			return controlJob(job), nil
		}
		// Bound the sleep and each retrieval by the same absolute deadline.
		// An Activity retry does not reset the provider's result budget.
		timer := time.NewTimer(min(interval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return provider.Job{}, ctx.Err()
		case <-timer.C:
		}
		if !time.Now().Before(deadline) {
			continue
		}
		callCtx, cancel := context.WithDeadline(ctx, deadline)
		next, err := a.Provider.Retrieve(callCtx, job.ID)
		cancel()
		if err != nil {
			if provider.IsPermanent(err) {
				return provider.Job{}, providerError(err)
			}
			// Transient retrieval failures can be retried because they only read
			// the existing job. Permanent errors stop this polling Activity.
			continue
		}
		job = next
	}
}
