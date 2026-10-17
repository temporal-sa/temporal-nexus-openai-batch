package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

func lifecycleTestBatch() (*testsuite.TestWorkflowEnvironment, model.BatchRef) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	ref := model.BatchRef{RunID: "lifecycle", BatchID: "lifecycle-0001", Config: model.DefaultConfig("fake"),
		Requests: 1, Bytes: 100, FirstAdmittedAt: env.Now(), SealedAt: env.Now()}
	ref.DeadlineAt = ref.FirstAdmittedAt.Add(ref.Config.OperationTimeout)
	return env, ref
}

func expectLifecycleFailure(env *testsuite.TestWorkflowEnvironment, ref model.BatchRef) {
	summary := model.DeliverySummary{Failed: 1, Delivered: 1, FailureCounts: map[string]int{"DEADLINE_EXCEEDED": 1}}
	env.OnActivity(serviceActivities.FailBatch, mock.Anything, ref, "DEADLINE_EXCEEDED", mock.Anything).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
}

func TestBatchRequiresRecipientDeadline(t *testing.T) {
	env, ref := lifecycleTestBatch()
	ref.DeadlineAt = time.Time{}
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.Equal(t, "INVALID_BATCH", errorType(env.GetWorkflowError()))
}

func TestBatchOverdueSealingDoesNotRestartProviderBudget(t *testing.T) {
	env, ref := lifecycleTestBatch()
	ref.Config = model.DefaultConfig("live")
	ref.Config.Model = "test-model"
	ref.FirstAdmittedAt = env.Now().Add(-26 * time.Hour)
	ref.DeadlineAt = ref.FirstAdmittedAt.Add(ref.Config.OperationTimeout)
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	expectLifecycleFailure(env, ref)
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, 1, report.FailureCounts["DEADLINE_EXCEEDED"])
	// Unexpected Upload/Create calls have no mocks and fail this test.
	env.AssertExpectations(t)
}

func TestBatchEarliestRecipientDeadlinePreventsPaidSubmission(t *testing.T) {
	env, ref := lifecycleTestBatch()
	// This SAA was scheduled long before enrollment reached Postgres.
	ref.DeadlineAt = env.Now().Add(-time.Second)
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	expectLifecycleFailure(env, ref)
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBatchPersistenceRecoversAfterRecipientDeadline(t *testing.T) {
	env, ref := lifecycleTestBatch()
	started := env.Now()
	// Six 20-second failures plus retry backoff cross the two-minute SAA
	// lifetime. Recovery must still persist failure, delivery, and final status.
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).
		Return(errors.New("temporary database outage")).After(20 * time.Second).Times(6)
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	expectLifecycleFailure(env, ref)
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	require.Greater(t, env.Now().Sub(started), ref.Config.OperationTimeout)
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, "completed_with_errors", report.Status)
	require.Equal(t, 1, report.Delivered)
	env.AssertExpectations(t)
}

func TestBatchStorageOutageAfterUploadDoesNotCreateStaleJob(t *testing.T) {
	env, ref := lifecycleTestBatch()
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.MatchedBy(func(report model.BatchReport) bool {
		return report.Status == "submitting"
	})).Return(errors.New("temporary database outage")).After(20 * time.Second).Times(4)
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	expectLifecycleFailure(env, ref)
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBatchOutcomesRecoverAfterRecipientDeadline(t *testing.T) {
	env, ref := lifecycleTestBatch()
	started := env.Now()
	job := provider.Job{ID: "provider-1", Status: "completed", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).
		Return(model.DeliverySummary{}, errors.New("temporary database outage")).After(20 * time.Second).Times(6)
	summary := model.DeliverySummary{Succeeded: 1, Skipped: 1}
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, job).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	require.Greater(t, env.Now().Sub(started), ref.Config.OperationTimeout)
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, "completed", report.Status)
	require.Equal(t, 1, report.Skipped)
	env.AssertExpectations(t)
}

func TestBatchFullLiveDurationSchedulesOnePollingActivity(t *testing.T) {
	env, ref := lifecycleTestBatch()
	ref.Config = model.DefaultConfig("live")
	ref.Config.Model = "test-model"
	ref.DeadlineAt = ref.FirstAdmittedAt.Add(ref.Config.OperationTimeout)
	started := env.Now()
	job := provider.Job{ID: "provider-1", Status: "in_progress", InputFileID: "file-1"}
	terminal := job
	terminal.Status = "completed"
	counts := map[string]int{}
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		counts[info.ActivityType.Name]++
		if info.ActivityType.Name == "Poll" {
			require.Equal(t, 30*time.Second, info.HeartbeatTimeout)
			require.Equal(t, 25*time.Hour, info.StartToCloseTimeout)
		}
	})
	env.OnActivity(serviceActivities.SaveReport, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(serviceActivities.Upload, mock.Anything, ref).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, ref, "file-1").Return(CreateResult{Job: job}, nil).Once()
	env.OnActivity(serviceActivities.Poll, mock.Anything, job, started.Add(25*time.Hour), 30*time.Second).
		Return(terminal, nil).After(25*time.Hour - time.Second).Once()
	summary := model.DeliverySummary{Succeeded: 1, Delivered: 1}
	env.OnActivity(serviceActivities.Results, mock.Anything, ref, terminal).Return(summary, nil).Once()
	env.OnActivity(serviceActivities.Deliver, mock.Anything, ref).Return(summary, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, ref)
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 25*time.Hour-time.Second, env.Now().Sub(started))
	require.Equal(t, 1, counts["Poll"])
	require.Zero(t, counts["Retrieve"])
	total := 0
	for _, count := range counts {
		total += count
	}
	require.LessOrEqual(t, total, 12, "Activity command count must stay bounded across the full live duration")
	env.AssertExpectations(t)
}
