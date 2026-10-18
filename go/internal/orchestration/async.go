package orchestration

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// SubmitRequest is the Nexus-invoked standalone Activity. It validates and
// enrolls one request, then hands responsibility to the store-backed keeper.
// ErrResultPending tells the SDK that execution is awaiting external completion:
// it releases the worker slot without reporting a successful empty result.
// PostgreSQL must own the exact attempt token before this handoff happens.
func (a *ServiceActivities) SubmitRequest(ctx context.Context, input model.SAAInput) (model.Result, error) {
	if err := model.ValidateRequest(input.Request); err != nil {
		return model.Result{}, typedFailure(err)
	}
	if err := input.Config.Validate(); err != nil {
		return model.Result{}, failure("INVALID_INPUT", err.Error())
	}
	info := activity.GetInfo(ctx)
	// A standalone Activity has its own execution/run identity. A Workflow-owned
	// Activity must not accidentally enter this exact-SAA completion protocol.
	if info.ActivityRunID == "" || info.ActivityID == "" || info.Namespace == "" || len(info.TaskToken) == 0 {
		return model.Result{}, failure("INVALID_ACTIVITY_TARGET", "SubmitRequest requires a standalone Activity execution")
	}
	// Copy the SDK token because it is the credential for this specific attempt.
	// IDs describe the execution; the token prevents an old delivery from
	// completing a newer attempt that happens to share those execution IDs.
	enrollment := model.Enrollment{Request: input.Request, Fingerprint: model.Fingerprint(input.Request), Target: model.CompletionTarget{
		Namespace: info.Namespace, ActivityID: info.ActivityID, ActivityRunID: info.ActivityRunID,
		TaskToken: append([]byte(nil), info.TaskToken...), Attempt: info.Attempt,
	}}
	enrollment.DeadlineAt = info.Deadline
	// Heartbeat retries recover loss before enrollment. After commit the store's
	// heartbeat keeper owns liveness; a new attempt can reattach this execution
	// without admitting another request or resubmitting the provider batch.
	enrollCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		activity.RecordHeartbeat(ctx, input.Request.RequestID)
		_, err = a.Enroll(enrollCtx, enrollment)
		if err == nil {
			return model.Result{}, activity.ErrResultPending
		}
		// A commit acknowledgment can be lost. Repeating the same enrollment
		// is safe because the store checks request fingerprint and exact target;
		// a different operation claiming the logical ID is rejected instead.
		if !retryEnrollment(err) || attempt == 4 {
			break
		}
		delay := time.Duration(1<<attempt) * 100 * time.Millisecond
		select {
		case <-enrollCtx.Done():
			return model.Result{}, enrollCtx.Err()
		case <-time.After(delay):
		}
	}
	return model.Result{}, err
}

// retryEnrollment keeps a short, bounded local retry loop inside the Activity.
// Temporal's SAA retry policy remains the recovery path after this attempt ends.
// Validation/authorization failures are not made retryable by this local loop.
func retryEnrollment(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var application *temporal.ApplicationError
	if errors.As(err, &application) && application.NonRetryable() {
		return false
	}
	var permission *serviceerror.PermissionDenied
	var invalid *serviceerror.InvalidArgument
	return !errors.As(err, &permission) && !errors.As(err, &invalid)
}

// DeliveryResult describes the completion RPC, independently of whether the
// model request succeeded. A delivered Failure is still a successful delivery;
// a skipped target could have timed out, been canceled, or changed attempts.
type DeliveryResult struct {
	Delivered  bool
	Skipped    bool
	Diagnostic string
}

// CompleteResult always uses the recorded attempt token. It never falls back
// to completing an Activity by ID, which could complete a superseding attempt.
func (a *ServiceActivities) CompleteResult(ctx context.Context, target model.CompletionTarget, outcome model.Outcome) (DeliveryResult, error) {
	if len(target.TaskToken) == 0 || target.ActivityID == "" || target.ActivityRunID == "" || target.Namespace == "" || target.Attempt < 1 {
		return DeliveryResult{}, failure("INVALID_ACTIVITY_TARGET", "completion target is incomplete")
	}
	if activity.IsActivity(ctx) && target.Namespace != activity.GetInfo(ctx).Namespace {
		return DeliveryResult{}, failure("INVALID_ACTIVITY_TARGET", "completion target belongs to a different namespace")
	}
	var completionError error
	if outcome.Failure != nil {
		completionError = failure(outcome.Failure.Code, outcome.Failure.Message, *outcome.Failure)
	} else if outcome.Result == nil {
		return DeliveryResult{}, failure("INVALID_RESULT", "completion has no result or failure")
	}
	handle := a.Client.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID})
	// Inspect the exact execution before using its token. A closed execution
	// may already hold this outcome from an earlier delivery whose SQL checkpoint
	// or RPC acknowledgment was lost.
	description, inspectErr := handle.Describe(ctx, client.DescribeActivityOptions{IncludeOutcome: true})
	if inspectErr != nil {
		return completionInspectionError(inspectErr)
	}
	if description.ActivityRunID != target.ActivityRunID || description.ActivityID != target.ActivityID {
		return DeliveryResult{Skipped: true, Diagnostic: "completion target identity did not match"}, nil
	}
	if description.Status != enums.ACTIVITY_EXECUTION_STATUS_RUNNING && description.Status != enums.ACTIVITY_EXECUTION_STATUS_PAUSED {
		return reconcileCompletion(description, outcome)
	}
	if description.Attempt != target.Attempt {
		return DeliveryResult{Skipped: true, Diagnostic: "completion target has a superseding attempt"}, nil
	}
	err := a.Client.CompleteActivity(ctx, target.TaskToken, outcome.Result, completionError)
	if err == nil {
		return DeliveryResult{Delivered: true}, nil
	}
	var missing *serviceerror.NotFound
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		return DeliveryResult{Skipped: true, Diagnostic: "completion token was rejected"}, nil
	}
	if !errors.As(err, &missing) {
		return DeliveryResult{}, err
	}
	// An acknowledgment can be lost after a successful completion. A repeated
	// token is rejected, so inspect the exact execution before counting delivery.
	description, inspectErr = handle.Describe(ctx, client.DescribeActivityOptions{IncludeOutcome: true})
	if inspectErr != nil {
		return completionInspectionError(inspectErr)
	}
	return reconcileCompletion(description, outcome)
}

func completionInspectionError(err error) (DeliveryResult, error) {
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return DeliveryResult{Skipped: true, Diagnostic: "completion target no longer exists"}, nil
	}
	return DeliveryResult{}, err
}

// reconcileCompletion treats a closed execution as delivered only when its
// stored success or structured failure matches the intended outcome. Merely
// finding an execution closed is insufficient evidence that our result arrived.
func reconcileCompletion(description *client.ActivityExecutionDescription, outcome model.Outcome) (DeliveryResult, error) {
	switch description.Status {
	case enums.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		var previous model.Result
		if outcome.Failure == nil && description.GetResult(&previous) == nil && reflect.DeepEqual(previous, *outcome.Result) {
			return DeliveryResult{Delivered: true}, nil
		}
		return DeliveryResult{Skipped: true, Diagnostic: "completion target already has a different outcome"}, nil
	case enums.ACTIVITY_EXECUTION_STATUS_FAILED:
		previous := description.GetOutcomeFailure()
		if outcome.Failure != nil && errorType(previous) == outcome.Failure.Code {
			var application *temporal.ApplicationError
			var stored model.Failure
			if errors.As(previous, &application) && application.Details(&stored) == nil && reflect.DeepEqual(stored, *outcome.Failure) {
				return DeliveryResult{Delivered: true}, nil
			}
		}
		return DeliveryResult{Skipped: true, Diagnostic: "completion target already failed with a different outcome"}, nil
	case enums.ACTIVITY_EXECUTION_STATUS_CANCELED, enums.ACTIVITY_EXECUTION_STATUS_TERMINATED, enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return DeliveryResult{Skipped: true}, nil
	default:
		return DeliveryResult{Skipped: true, Diagnostic: "completion token is stale or no longer valid for the pending attempt"}, nil
	}
}
