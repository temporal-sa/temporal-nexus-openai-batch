package orchestration

import (
	"context"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"go.temporal.io/sdk/activity"
)

// Poll keeps provider polling out of Workflow history. Retried executions use
// the same provider ID and absolute deadline and never upload or create a job.
func (a *ServiceActivities) Poll(ctx context.Context, job provider.Job, deadline time.Time, interval time.Duration) (provider.Job, error) {
	if interval <= 0 {
		return provider.Job{}, failure("INVALID_BATCH", "poll interval must be positive")
	}
	heartbeatCtx, stop := context.WithCancel(ctx)
	defer stop()
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, job.ID)
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					activity.RecordHeartbeat(ctx, job.ID)
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
			continue
		}
		job = next
	}
}
