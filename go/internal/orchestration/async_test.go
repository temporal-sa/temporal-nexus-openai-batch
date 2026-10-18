package orchestration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestConfigurationLookupFailurePreservesNexusRetryability(t *testing.T) {
	missing := configurationLookupError(serviceerror.NewNotFound("raw internal resource name"))
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, missing.Type)
	require.False(t, missing.Retryable())
	for _, cause := range []error{serviceerror.NewUnavailable("raw internal transport details"), context.DeadlineExceeded, errors.New("raw internal query-task details")} {
		transient := configurationLookupError(cause)
		require.Equal(t, nexus.HandlerErrorTypeUnavailable, transient.Type)
		require.True(t, transient.Retryable())
		require.NotContains(t, transient.Error(), "raw internal")
	}
}

func TestIngressEndpointIsolationRejectsMisroutedRun(t *testing.T) {
	require.NoError(t, validateIngressEndpoint("service-live", "service-live"))
	require.Error(t, validateIngressEndpoint("service-live", ""))
	err := validateIngressEndpoint("service-live", "service-fake")
	var handler *nexus.HandlerError
	require.ErrorAs(t, err, &handler)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handler.Type)
	require.False(t, handler.Retryable())
	require.NotContains(t, err.Error(), "service-live")
	require.NotContains(t, err.Error(), "service-fake")
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

type enrollmentRepository struct {
	Repository
	enroll func(model.Enrollment) (model.Admission, error)
}

func (r enrollmentRepository) Enroll(_ context.Context, e model.Enrollment) (model.Admission, error) {
	return r.enroll(e)
}
func TestSubmitRequestPersistsAttemptThenReturnsPending(t *testing.T) {
	input := model.SAAInput{Request: testEnrollment("1").Request, Config: model.DefaultConfig("fake")}
	var target model.CompletionTarget
	repo := enrollmentRepository{enroll: func(e model.Enrollment) (model.Admission, error) {
		require.Equal(t, input.Request, e.Request)
		require.Equal(t, model.Fingerprint(input.Request), e.Fingerprint)
		target = e.Target
		require.NotEmpty(t, target.ActivityRunID)
		require.NotEmpty(t, target.Namespace)
		require.NotEmpty(t, target.TaskToken)
		require.EqualValues(t, 1, target.Attempt)
		return model.Admission{BatchID: "test-0001", Fingerprint: e.Fingerprint, Config: input.Config}, nil
	}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetExecuteActivitiesInWorkflow(false)
	a := ServiceActivities{Store: repo}
	env.RegisterActivity(&a)
	_, err := env.ExecuteActivity(a.SubmitRequest, input)
	require.ErrorIs(t, err, activity.ErrResultPending)
	require.NotEmpty(t, target.ActivityRunID)
}

type pagedDeliveryRepository struct {
	Repository
	rows      []model.StoredDelivery
	marked    map[string]bool
	pageSizes []int
}

func (r *pagedDeliveryRepository) PendingDeliveries(_ context.Context, _ string, limit int) ([]model.StoredDelivery, error) {
	var page []model.StoredDelivery
	for _, row := range r.rows {
		if !r.marked[row.RequestID] {
			page = append(page, row)
			if len(page) == limit {
				break
			}
		}
	}
	if len(page) > 0 {
		r.pageSizes = append(r.pageSizes, len(page))
	}
	return page, nil
}
func (r *pagedDeliveryRepository) MarkDelivered(_ context.Context, _ string, requestID string, _ model.CompletionTarget, delivered bool, _ string) error {
	r.marked[requestID] = delivered
	return nil
}
func (r *pagedDeliveryRepository) DeliverySummary(context.Context, string) (model.DeliverySummary, error) {
	return model.DeliverySummary{Delivered: len(r.marked), Succeeded: len(r.rows)}, nil
}
func TestAggregateDeliveryPagesMoreThanOneThousandTargets(t *testing.T) {
	repo := &pagedDeliveryRepository{marked: map[string]bool{}}
	targets := map[string]model.CompletionTarget{}
	for i := 0; i < 1001; i++ {
		e := testEnrollment(fmt.Sprint(i))
		targets[e.Target.ActivityID] = e.Target
		repo.rows = append(repo.rows, model.StoredDelivery{RequestID: e.Request.RequestID, Target: e.Target, Outcome: model.Outcome{RequestID: e.Request.RequestID, Result: &model.Result{RequestID: e.Request.RequestID, Text: "result"}}})
	}
	var c mocks.Client
	c.On("GetActivityHandle", mock.Anything).Return(func(options client.GetActivityHandleOptions) client.ActivityHandle {
		target := targets[options.ActivityID]
		return descriptionHandle{describe: func() (*client.ActivityExecutionDescription, error) { return runningDescription(target), nil }}
	}).Times(1001)
	c.On("CompleteActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1001)
	a := ServiceActivities{Client: &c, Store: repo}
	summary, err := a.Deliver(context.Background(), model.BatchRef{BatchID: "test-0001"})
	require.NoError(t, err)
	require.Equal(t, 1001, summary.Delivered)
	require.Len(t, repo.pageSizes, 11)
	for _, size := range repo.pageSizes {
		require.LessOrEqual(t, size, 100)
	}
	c.AssertExpectations(t)
}
