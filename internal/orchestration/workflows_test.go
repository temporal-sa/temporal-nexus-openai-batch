package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"
)

func testEnrollment(id string) model.Enrollment {
	request := model.Request{RunID: "test", RequestID: id, Input: "Summarize " + id}
	return model.Enrollment{Request: request, Fingerprint: model.Fingerprint(request), Target: model.CompletionTarget{Namespace: "default-test-namespace", ActivityID: "request/" + id, ActivityRunID: "execution/" + id, TaskToken: []byte("token/" + id), Attempt: 1}}
}
func newBatchTest(requests int) (*testsuite.TestWorkflowEnvironment, model.BatchRef) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	config := model.DefaultConfig("fake")
	if requests > config.MaxBatchRequests {
		config.MaxBatchRequests = requests
	}
	ref := model.BatchRef{RunID: "test", BatchID: "test-0001", Config: config, Requests: requests, Bytes: requests * 100, SealedAt: env.Now(), FirstAdmittedAt: env.Now().Add(-time.Second)}
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	return env, ref
}
func TestBatchWorkflowHistoryCommandsDoNotScaleWithRequests(t *testing.T) {
	for _, count := range []int{1, 2500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			env, ref := newBatchTest(count)
			job := provider.Job{ID: "provider-1", Status: "completed", InputFileID: "file-1"}
			env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
			env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
			summary := model.DeliverySummary{Succeeded: count, Delivered: count}
			env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).Return(summary, nil).Once()
			env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
			env.ExecuteWorkflow(BatchWorkflow, ref)
			require.NoError(t, env.GetWorkflowError())
			var report model.BatchReport
			require.NoError(t, env.GetWorkflowResult(&report))
			require.Equal(t, count, report.Delivered)
			require.Equal(t, "completed", report.Status)
			env.AssertExpectations(t)
			encoded, err := json.Marshal(ref)
			require.NoError(t, err)
			require.Less(t, len(encoded), 2048)
		})
	}
}
func TestBatchAmbiguousCreateReconcilesWithoutResubmission(t *testing.T) {
	env, ref := newBatchTest(2)
	job := provider.Job{ID: "recovered-provider-job", Status: "completed", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{}, errors.New("worker disappeared after submission")).Once()
	env.OnActivity(serviceActivities.Find, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	summary := model.DeliverySummary{Succeeded: 2, Delivered: 2}
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestProviderCompletionDoesNotPublishTerminalLifecycleBeforeDelivery(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	ref := model.BatchRef{RunID: "test", BatchID: "test-0001", Config: model.DefaultConfig("fake"), Requests: 2, Bytes: 200, FirstAdmittedAt: env.Now(), SealedAt: env.Now()}
	var reports []model.BatchReport
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(func(_ context.Context, report model.BatchReport) error { reports = append(reports, report); return nil })
	job := provider.Job{ID: "provider-1", Status: "completed", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).Return(model.DeliverySummary{Succeeded: 2}, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(model.DeliverySummary{Succeeded: 2, Delivered: 2}, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	foundProviderCompletion := false
	for index, report := range reports {
		if report.ProviderStatus == "completed" && report.Delivered == 0 {
			foundProviderCompletion = true
			require.NotEqual(t, "completed", report.Status)
		}
		if report.Status == "completed" {
			require.Equal(t, len(reports)-1, index)
			require.Equal(t, 2, report.Delivered)
		}
	}
	require.True(t, foundProviderCompletion)
	require.Equal(t, "completed", reports[len(reports)-1].Status)
	env.AssertExpectations(t)
}
func TestBatchUnresolvedSubmissionPersistsFailuresBeforeDelivery(t *testing.T) {
	env, ref := newBatchTest(2)
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Ambiguous: true}, nil).Once()
	env.OnActivity(serviceActivities.Find, mock.Anything, ref, "file-1").Return(CreateResult{Ambiguous: true}, nil).Once()
	summary := model.DeliverySummary{Failed: 2, Delivered: 2, FailureCounts: map[string]int{"SUBMISSION_UNKNOWN": 2}}
	env.OnActivity(serviceActivities.FailBatch, mock.Anything, ref, "SUBMISSION_UNKNOWN", mock.Anything).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, "completed_with_errors", report.Status)
	require.Equal(t, 2, report.FailureCounts["SUBMISSION_UNKNOWN"])
	env.AssertExpectations(t)
}
func TestBatchDeadlinePersistsOneAggregateFailure(t *testing.T) {
	env, ref := newBatchTest(2000)
	ref.Config.Window = 500 * time.Millisecond
	ref.Config.ResultDeadline = 2 * time.Second
	ref.Config.PollInterval = time.Second
	job := provider.Job{ID: "provider-1", Status: "in_progress", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	env.OnActivity(serviceActivities.Retrieve, mock.Anything, "provider-1").Return(job, nil).Once()
	summary := model.DeliverySummary{Failed: ref.Requests, Delivered: ref.Requests, FailureCounts: map[string]int{"DEADLINE_EXCEEDED": ref.Requests}}
	env.OnActivity(serviceActivities.FailBatch, mock.Anything, ref, "DEADLINE_EXCEEDED", mock.Anything).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, 2*time.Second, report.ProviderDuration)
	require.Equal(t, "in_progress", report.ProviderStatus)
	env.AssertExpectations(t)
}

func TestBatchControlPersistenceSurvivesMoreThanFiveTransientFailures(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	ref := model.BatchRef{RunID: "test", BatchID: "test-0001", Config: model.DefaultConfig("fake"), Requests: 1, Bytes: 100, FirstAdmittedAt: env.Now(), SealedAt: env.Now()}
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(errors.New("temporary database outage")).Times(6)
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	job := provider.Job{ID: "provider-1", Status: "completed", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	summary := model.DeliverySummary{Succeeded: 1, Delivered: 1}
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

type dispatcherRepository struct {
	Repository
	ref            model.BatchRef
	marks          int
	failMark       bool
	mode, endpoint string
}

func (r *dispatcherRepository) SealDue(_ context.Context, mode, endpoint string, _ int) (int, error) {
	r.mode, r.endpoint = mode, endpoint
	return 0, nil
}
func (r *dispatcherRepository) ReadyBatches(_ context.Context, mode, endpoint string, _ int) ([]model.BatchRef, error) {
	r.mode, r.endpoint = mode, endpoint
	return []model.BatchRef{r.ref}, nil
}
func (r *dispatcherRepository) MarkDispatched(context.Context, string) error {
	r.marks++
	if r.failMark {
		r.failMark = false
		return errors.New("database acknowledgement lost")
	}
	return nil
}
func TestDispatcherRecoversCrashBetweenWorkflowStartAndOutboxCheckpoint(t *testing.T) {
	ref := model.BatchRef{RunID: "test", BatchID: "test-0001", Config: model.DefaultConfig("fake"), Requests: 2000, Bytes: 200000}
	repo := &dispatcherRepository{ref: ref, failMark: true}
	var c mocks.Client
	c.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(options client.StartWorkflowOptions) bool {
		return options.ID == model.BatchWorkflowID(ref.BatchID) && options.TaskQueue == "isolated-queue" && options.WorkflowExecutionErrorWhenAlreadyStarted
	}), mock.Anything, ref).Return(nil, nil).Once()
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, ref).Return(nil, &serviceerror.WorkflowExecutionAlreadyStarted{}).Once()
	require.Error(t, DispatchOnce(context.Background(), &c, repo, "fake", "isolated-queue", "isolated-endpoint"))
	require.NoError(t, DispatchOnce(context.Background(), &c, repo, "fake", "isolated-queue", "isolated-endpoint"))
	require.Equal(t, 2, repo.marks)
	require.Equal(t, "fake", repo.mode)
	require.Equal(t, "isolated-endpoint", repo.endpoint)
	c.AssertExpectations(t)
}

type transientDispatcherRepository struct {
	Repository
	attempts int
	cancel   context.CancelFunc
}

func (r *transientDispatcherRepository) SealDue(context.Context, string, string, int) (int, error) {
	r.attempts++
	if r.attempts == 1 {
		return 0, errors.New("temporary storage outage")
	}
	return 0, nil
}
func (r *transientDispatcherRepository) ReadyBatches(context.Context, string, string, int) ([]model.BatchRef, error) {
	r.cancel()
	return nil, nil
}
func TestDispatcherKeepsWorkersAliveThroughTemporaryStorageOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &transientDispatcherRepository{cancel: cancel}
	var c mocks.Client
	require.NoError(t, Dispatcher(ctx, &c, repo, "fake", "queue", "endpoint"))
	require.Equal(t, 2, repo.attempts)
}
