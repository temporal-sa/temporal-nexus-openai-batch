package orchestration

import (
	"context"
	"errors"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

// Repository keeps batch membership, request bodies, results and completion
// credentials outside Temporal histories. Implemented by the Postgres store.
type Repository interface {
	GetRun(context.Context, string) (model.RunInput, error)
	Enroll(context.Context, model.Enrollment) (model.Admission, error)
	SealDue(context.Context, string, string, int) (int, error)
	ReadyBatches(context.Context, string, string, int) ([]model.BatchRef, error)
	MarkDispatched(context.Context, string) error
	LoadManifest(context.Context, string) (model.Manifest, error)
	SaveOutcomes(context.Context, string, []model.Outcome, []string) error
	FailBatch(context.Context, string, string, string) error
	PendingDeliveries(context.Context, string, int) ([]model.StoredDelivery, error)
	MarkDelivered(context.Context, string, string, bool, string) error
	DeliverySummary(context.Context, string) (model.DeliverySummary, error)
	SaveReport(context.Context, model.BatchReport) error
}

// Dispatcher drains the database outbox. A crash between the Temporal start
// and MarkDispatched is safe: the stable Workflow ID rejects another execution.
func Dispatcher(ctx context.Context, c client.Client, repo Repository, mode, queue, endpoint string) error {
	backoff := 100 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return nil
		}
		wait := 100 * time.Millisecond
		if err := DispatchOnce(ctx, c, repo, mode, queue, endpoint); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// A temporary Postgres or Temporal outage must not take down
			// workers that are servicing already-started async Activities.
			// The store retains the outbox rows for this or another worker.
			wait = backoff
			backoff = min(backoff*2, 5*time.Second)
		} else {
			backoff = 100 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func DispatchOnce(ctx context.Context, c client.Client, repo Repository, mode, queue, endpoint string) error {
	if _, err := repo.SealDue(ctx, mode, endpoint, 100); err != nil {
		return err
	}
	refs, err := repo.ReadyBatches(ctx, mode, endpoint, 100)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: model.BatchWorkflowID(ref.BatchID), TaskQueue: queue,
			WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
		}, BatchWorkflow, ref)
		if err != nil {
			var exists *serviceerror.WorkflowExecutionAlreadyStarted
			if !errors.As(err, &exists) {
				return err
			}
		}
		if err := repo.MarkDispatched(ctx, ref.BatchID); err != nil {
			return err
		}
	}
	return nil
}

// Deliver pages through durable database checkpoints inside one Activity.
// Neither its input nor its result contains individual request data or tokens.
func (a *ServiceActivities) Deliver(ctx context.Context, ref model.BatchRef) (model.DeliverySummary, error) {
	for {
		if err := ctx.Err(); err != nil {
			return model.DeliverySummary{}, err
		}
		rows, err := a.Store.PendingDeliveries(ctx, ref.BatchID, 100)
		if err != nil {
			return model.DeliverySummary{}, err
		}
		if len(rows) == 0 {
			return a.Store.DeliverySummary(ctx, ref.BatchID)
		}
		for _, row := range rows {
			if activity.IsActivity(ctx) {
				activity.RecordHeartbeat(ctx, ref.BatchID)
			}
			delivery, err := a.CompleteResult(ctx, row.Target, row.Outcome)
			if err != nil {
				return model.DeliverySummary{}, err
			}
			if err := a.Store.MarkDelivered(ctx, ref.BatchID, row.RequestID, delivery.Delivered, delivery.Diagnostic); err != nil {
				return model.DeliverySummary{}, err
			}
		}
	}
}
