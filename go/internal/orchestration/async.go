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

// SubmitRequest is the Nexus-invoked standalone Activity. Durable enrollment
// owns the exact attempt token before this function releases its worker slot.
// ErrResultPending releases the worker slot; durable enrollment keeps the exact
// attempt alive until its result can be completed externally.
func (a *ServiceActivities) SubmitRequest(ctx context.Context, input model.SAAInput) (model.Result, error) {
	if err := model.ValidateRequest(input.Request); err != nil {
		return model.Result{}, typedFailure(err)
	}
	if err := input.Config.Validate(); err != nil {
		return model.Result{}, failure("INVALID_INPUT", err.Error())
	}
	info := activity.GetInfo(ctx)
	if info.ActivityRunID == "" || info.ActivityID == "" || info.Namespace == "" || len(info.TaskToken) == 0 {
		return model.Result{}, failure("INVALID_ACTIVITY_TARGET", "SubmitRequest requires a standalone Activity execution")
	}
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
