package orchestration

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestConfigurationQueryFailurePreservesNexusRetryability(t *testing.T) {
	missing := configurationQueryError(serviceerror.NewNotFound("raw internal resource name"))
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, missing.Type)
	require.False(t, missing.Retryable())
	for _, cause := range []error{serviceerror.NewUnavailable("raw internal transport details"), context.DeadlineExceeded, errors.New("raw internal query-task details")} {
		transient := configurationQueryError(cause)
		require.Equal(t, nexus.HandlerErrorTypeUnavailable, transient.Type)
		require.True(t, transient.Retryable())
		require.NotContains(t, transient.Error(), "raw internal")
	}
}

type descriptionHandle struct {
	client.ActivityHandle
	describe func() (*client.ActivityExecutionDescription, error)
}

func (h descriptionHandle) Describe(context.Context, client.DescribeActivityOptions) (*client.ActivityExecutionDescription, error) {
	return h.describe()
}

func runningDescription(target model.CompletionTarget) *client.ActivityExecutionDescription {
	d := &client.ActivityExecutionDescription{Attempt: target.Attempt}
	d.ActivityID = target.ActivityID
	d.ActivityRunID = target.ActivityRunID
	d.Status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
	return d
}

func TestCompletionUsesExactRecordedAttemptTokenAndTypedFailure(t *testing.T) {
	target := testEnrollment("1").Target
	for _, code := range []string{"", "PROVIDER_ERROR"} {
		t.Run(code, func(t *testing.T) {
			var c mocks.Client
			a := ServiceActivities{Client: &c}
			outcome := model.Outcome{RequestID: "1", Result: &model.Result{RequestID: "1", Text: "result"}}
			if code != "" {
				outcome.Result = nil
				outcome.Failure = &model.Failure{Code: code, Message: "request failed", ProviderCode: "safe_provider_code"}
			}
			c.On("GetActivityHandle", client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Return(descriptionHandle{describe: func() (*client.ActivityExecutionDescription, error) { return runningDescription(target), nil }}).Once()
			c.On("CompleteActivity", mock.Anything, target.TaskToken, outcome.Result, mock.MatchedBy(func(err error) bool {
				if code == "" {
					return err == nil
				}
				app, ok := err.(*temporal.ApplicationError)
				if !ok || app.Type() != code || !app.NonRetryable() {
					return false
				}
				var details model.Failure
				return app.Details(&details) == nil && details == *outcome.Failure
			})).Return(nil).Once()
			result, err := a.CompleteResult(context.Background(), target, outcome)
			require.NoError(t, err)
			require.True(t, result.Delivered)
			c.AssertExpectations(t)
		})
	}
}

func TestCompletionSkipsMissingClosedOrSupersededTargets(t *testing.T) {
	target := testEnrollment("1").Target
	for _, scenario := range []string{"missing", "canceled", "timed_out", "superseded"} {
		t.Run(scenario, func(t *testing.T) {
			var c mocks.Client
			a := ServiceActivities{Client: &c}
			c.On("GetActivityHandle", client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Return(descriptionHandle{describe: func() (*client.ActivityExecutionDescription, error) {
				if scenario == "missing" {
					return nil, serviceerror.NewNotFound("gone")
				}
				d := runningDescription(target)
				switch scenario {
				case "canceled":
					d.Status = enums.ACTIVITY_EXECUTION_STATUS_CANCELED
				case "timed_out":
					d.Status = enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
				case "superseded":
					d.Attempt++
				}
				return d, nil
			}}).Once()
			result, err := a.CompleteResult(context.Background(), target, model.Outcome{Result: &model.Result{RequestID: "1", Text: "result"}})
			require.NoError(t, err)
			require.True(t, result.Skipped)
			require.False(t, result.Delivered)
			c.AssertNotCalled(t, "CompleteActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			c.AssertExpectations(t)
		})
	}
}

func TestCompletionReturnsTransientFailureForSafeRetry(t *testing.T) {
	target := testEnrollment("1").Target
	var c mocks.Client
	a := ServiceActivities{Client: &c}
	c.On("GetActivityHandle", client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID}).Return(descriptionHandle{describe: func() (*client.ActivityExecutionDescription, error) { return runningDescription(target), nil }}).Once()
	c.On("CompleteActivity", mock.Anything, target.TaskToken, mock.Anything, mock.Anything).Return(serviceerror.NewUnavailable("acknowledgment lost")).Once()
	_, err := a.CompleteResult(context.Background(), target, model.Outcome{Result: &model.Result{RequestID: "1", Text: "result"}})
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	c.AssertExpectations(t)
}

func TestSubmitRequestPersistsAttemptThenReturnsPending(t *testing.T) {
	var c mocks.Client
	var updateHandle mocks.WorkflowUpdateHandle
	input := model.SAAInput{Request: testEnrollment("1").Request, Config: model.DefaultConfig("fake")}
	var target model.CompletionTarget
	c.On("UpdateWorkflow", mock.Anything, mock.MatchedBy(func(options client.UpdateWorkflowOptions) bool {
		if len(options.Args) != 1 {
			return false
		}
		enrollment := options.Args[0].(model.Enrollment)
		target = enrollment.Target
		return options.UpdateID == enrollmentUpdateID(enrollment) && options.WorkflowID == model.CoordinatorID("test") && options.WaitForStage == client.WorkflowUpdateStageCompleted && enrollment.Request == input.Request && enrollment.Fingerprint == model.Fingerprint(input.Request) && target.ActivityRunID != "" && target.Namespace != "" && target.Attempt == 1 && len(target.TaskToken) > 0
	})).Return(&updateHandle, nil).Once()
	updateHandle.On("Get", mock.Anything, mock.Anything).Return(nil).Once()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetExecuteActivitiesInWorkflow(false)
	a := ServiceActivities{Client: &c}
	env.RegisterActivity(&a)
	_, err := env.ExecuteActivity(a.SubmitRequest, input)
	require.ErrorIs(t, err, activity.ErrResultPending)
	require.NotEmpty(t, target.ActivityRunID)
	c.AssertExpectations(t)
	updateHandle.AssertExpectations(t)
}
