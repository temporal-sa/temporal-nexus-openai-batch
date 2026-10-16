package orchestration

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func testEnrollment(id string) model.Enrollment {
	r := model.Request{RunID: "test", RequestID: id, Input: "Summarize " + id}
	return model.Enrollment{Request: r, Target: model.CompletionTarget{Namespace: "default-test-namespace", ActivityID: "request/" + id, ActivityRunID: "execution/" + id, TaskToken: []byte("token/" + id), Attempt: 1}, Fingerprint: model.Fingerprint(r)}
}

func update(t *testing.T, env *testsuite.TestWorkflowEnvironment, id string, e model.Enrollment, callback func(model.Admission, error)) {
	t.Helper()
	env.UpdateWorkflow(model.EnrollUpdate, id, &testsuite.TestUpdateCallback{
		OnReject: func(err error) { callback(model.Admission{}, err) },
		OnComplete: func(value any, err error) {
			if err != nil {
				callback(model.Admission{}, err)
				return
			}
			admission, ok := value.(model.Admission)
			require.True(t, ok)
			callback(admission, nil)
		},
	}, e)
}

func finish(env *testsuite.TestWorkflowEnvironment) {
	env.UpdateWorkflow(model.FinishUpdate, "finish", &testsuite.TestUpdateCallback{})
}

func TestCoordinatorCountBoundaryAndIdentityDedupe(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	cfg := model.DefaultConfig("fake")
	cfg.MaxBatchRequests, cfg.Window = 2, 10*time.Second
	var manifests []model.Manifest
	env.OnWorkflow(BatchWorkflow, mock.Anything, mock.Anything).Return(func(_ workflow.Context, m model.Manifest) (model.BatchReport, error) {
		manifests = append(manifests, m)
		return model.BatchReport{BatchID: m.BatchID, Status: "completed", Requests: len(m.Entries), Bytes: m.Bytes}, nil
	})
	admissions := make(map[string]model.Admission)
	env.RegisterDelayedCallback(func() {
		for i := 1; i <= 5; i++ {
			id := fmt.Sprint(i)
			update(t, env, "enroll-"+id, testEnrollment(id), func(a model.Admission, err error) { require.NoError(t, err); admissions[id] = a })
		}
	}, 0)
	env.RegisterDelayedCallback(func() {
		update(t, env, "retry", testEnrollment("1"), func(a model.Admission, err error) { require.NoError(t, err); require.Equal(t, admissions["1"], a) })
		duplicate := testEnrollment("1")
		duplicate.Target.ActivityID = "independent-request"
		update(t, env, "duplicate", duplicate, func(_ model.Admission, err error) { require.Equal(t, "DUPLICATE_REQUEST", errorType(err)) })
		stale := testEnrollment("1")
		stale.Target.Attempt++
		stale.Target.TaskToken = []byte("superseding-attempt-token")
		update(t, env, "stale-attempt", stale, func(_ model.Admission, err error) { require.Equal(t, "STALE_ACTIVITY_ATTEMPT", errorType(err)) })
		foreign := testEnrollment("foreign")
		foreign.Target.Namespace = "different-namespace"
		update(t, env, "foreign-namespace", foreign, func(_ model.Admission, err error) { require.Equal(t, "INVALID_INPUT", errorType(err)) })
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() { finish(env) }, time.Second)
	env.ExecuteWorkflow(CoordinatorWorkflow, model.RunInput{RunID: "test", Config: cfg})
	require.NoError(t, env.GetWorkflowError())
	var status model.RunStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	require.Equal(t, 5, status.Admitted)
	require.True(t, status.Closed)
	require.Len(t, manifests, 3)
	counts := map[string]int{}
	for _, m := range manifests {
		counts[m.BatchID] = len(m.Entries)
	}
	require.Equal(t, map[string]int{"test-0001": 2, "test-0002": 2, "test-0003": 1}, counts)
	require.Equal(t, admissions["1"].BatchID, admissions["2"].BatchID)
	require.NotEqual(t, admissions["2"].BatchID, admissions["3"].BatchID)
	env.AssertExpectations(t)
}

func TestCoordinatorByteBoundaryAndTimer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	cfg := model.DefaultConfig("fake")
	line, err := model.JSONLLine(testEnrollment("1").Request, cfg)
	require.NoError(t, err)
	cfg.MaxBatchBytes, cfg.Window = len(line)*2-1, time.Second
	var manifests []model.Manifest
	env.OnWorkflow(BatchWorkflow, mock.Anything, mock.Anything).Return(func(_ workflow.Context, m model.Manifest) (model.BatchReport, error) {
		manifests = append(manifests, m)
		return model.BatchReport{BatchID: m.BatchID, Status: "completed", Requests: len(m.Entries)}, nil
	})
	env.RegisterDelayedCallback(func() {
		for _, id := range []string{"1", "2"} {
			update(t, env, id, testEnrollment(id), func(_ model.Admission, err error) { require.NoError(t, err) })
		}
	}, 0)
	env.RegisterDelayedCallback(func() { finish(env) }, 2*time.Second)
	env.ExecuteWorkflow(CoordinatorWorkflow, model.RunInput{RunID: "test", Config: cfg})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, manifests, 2)
	for _, m := range manifests {
		require.Len(t, m.Entries, 1)
		require.LessOrEqual(t, m.Bytes, cfg.MaxBatchBytes)
	}
	// The last buffer flushed on its durable timer, before finish was sent.
	require.Equal(t, time.Second, manifests[1].SealedAt.Sub(manifests[1].Entries[0].AdmittedAt))
}

func TestCoordinatorRejectsOverCapacityAndResolvesFailedChild(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	cfg := model.DefaultConfig("fake")
	cfg.MaxAdmissions = 1
	env.OnWorkflow(BatchWorkflow, mock.Anything, mock.Anything).Return(model.BatchReport{}, failure("TEST_FAILURE", "terminal child failure")).Once()
	env.OnActivity(serviceActivities.CompleteResult, mock.Anything, testEnrollment("1").Target,
		mock.MatchedBy(func(o model.Outcome) bool {
			return o.Fingerprint == testEnrollment("1").Fingerprint && o.BatchID == "test-0001" && o.Failure != nil && o.Failure.Code == "BATCH_WORKFLOW_FAILED"
		})).Return(DeliveryResult{Delivered: true}, nil).Once()
	env.RegisterDelayedCallback(func() {
		update(t, env, "first", testEnrollment("1"), func(_ model.Admission, err error) { require.NoError(t, err) })
		update(t, env, "over-capacity", testEnrollment("2"), func(_ model.Admission, err error) { require.Equal(t, "RUN_FULL", errorType(err)) })
	}, 0)
	env.RegisterDelayedCallback(func() { finish(env) }, time.Second)
	env.ExecuteWorkflow(CoordinatorWorkflow, model.RunInput{RunID: "test", Config: cfg})
	require.NoError(t, env.GetWorkflowError())
	var status model.RunStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	require.Equal(t, 1, status.Admitted)
	require.Equal(t, 1, status.Batches[0].Failed)
	require.Equal(t, 1, status.Batches[0].Delivered)
	env.AssertExpectations(t)
}

func testManifest(cfg model.Config, entries ...model.Enrollment) model.Manifest {
	m := model.Manifest{RunID: "test", BatchID: "test-0001", Config: cfg}
	for _, e := range entries {
		m.Entries = append(m.Entries, model.Entry{Request: e.Request, Target: e.Target, Fingerprint: e.Fingerprint})
	}
	b, _ := model.JSONL(m)
	m.Bytes = len(b)
	return m
}

func newBatchTest() (*testsuite.TestWorkflowEnvironment, model.Manifest) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&ServiceActivities{})
	m := testManifest(model.DefaultConfig("fake"), testEnrollment("1"), testEnrollment("2"))
	m.SealedAt = env.Now()
	return env, m
}

func TestBatchAmbiguousSubmissionFindsExistingJobAndDeliversByIdentity(t *testing.T) {
	env, m := newBatchTest()
	job := provider.Job{ID: "provider-1", Status: "completed", InputFileID: "file-1", Metadata: batchMetadata(m)}
	env.OnActivity(serviceActivities.Upload, mock.Anything, m).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, "file-1", batchMetadata(m)).Return(CreateResult{Ambiguous: true, Failure: "lost response"}, nil).Once()
	env.OnActivity(serviceActivities.Find, mock.Anything, batchMetadata(m)).Return([]provider.Job{job}, nil).Once()
	// Deliberately reversed to prove correlation is independent of file order.
	outcomes := []model.Outcome{{RequestID: "2", Result: &model.Result{RequestID: "2", Text: "two"}}, {RequestID: "1", Result: &model.Result{RequestID: "1", Text: "one"}}}
	env.OnActivity(serviceActivities.Results, mock.Anything, m, job).Return(ReconciledResults{Outcomes: outcomes}, nil).Once()
	for _, e := range m.Entries {
		entry := e
		env.OnActivity(serviceActivities.CompleteResult, mock.Anything, e.Target, mock.MatchedBy(func(o model.Outcome) bool {
			return o.RequestID == entry.Request.RequestID && o.Fingerprint == entry.Fingerprint && o.BatchID == m.BatchID && o.Result.RequestID == entry.Request.RequestID
		})).Return(DeliveryResult{Delivered: true}, nil).Once()
	}
	env.ExecuteWorkflow(BatchWorkflow, m)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, "provider-1", report.ProviderBatchID)
	require.Equal(t, "completed", report.ProviderStatus)
	require.Equal(t, 2, report.Succeeded)
	require.Equal(t, 2, report.Delivered)
	env.AssertExpectations(t)
}

func TestBatchUnreconciledSubmissionNeverCreatesAgain(t *testing.T) {
	env, m := newBatchTest()
	env.OnActivity(serviceActivities.Upload, mock.Anything, m).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, "file-1", batchMetadata(m)).Return(CreateResult{}, errors.New("worker lost after HTTP submission")).Once()
	env.OnActivity(serviceActivities.Find, mock.Anything, batchMetadata(m)).Return([]provider.Job{}, nil).Once()
	env.OnActivity(serviceActivities.CompleteResult, mock.Anything, mock.Anything,
		mock.MatchedBy(func(o model.Outcome) bool { return o.Failure != nil && o.Failure.Code == "SUBMISSION_UNKNOWN" })).Return(DeliveryResult{Delivered: true}, nil).Twice()
	env.ExecuteWorkflow(BatchWorkflow, m)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, 2, report.Failed)
	require.Equal(t, "completed_with_errors", report.Status)
	require.Equal(t, 2, report.FailureCounts["SUBMISSION_UNKNOWN"])
	env.AssertExpectations(t)
}

func TestBatchDeadlineAndClosedRecipient(t *testing.T) {
	env, m := newBatchTest()
	m.Config.ResultDeadline, m.Config.PollInterval = 2*time.Second, time.Second
	job := provider.Job{ID: "provider-1", Status: "in_progress", InputFileID: "file-1"}
	env.OnActivity(serviceActivities.Upload, mock.Anything, m).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, "file-1", batchMetadata(m)).Return(CreateResult{Job: job}, nil).Once()
	env.OnActivity(serviceActivities.Retrieve, mock.Anything, "provider-1").Return(job, nil).Once()
	env.OnActivity(serviceActivities.CompleteResult, mock.Anything, testEnrollment("1").Target,
		mock.MatchedBy(func(o model.Outcome) bool { return o.Failure != nil && o.Failure.Code == "DEADLINE_EXCEEDED" })).Return(DeliveryResult{Skipped: true}, nil).Once()
	env.OnActivity(serviceActivities.CompleteResult, mock.Anything, testEnrollment("2").Target, mock.Anything).Return(DeliveryResult{Delivered: true}, nil).Once()
	env.ExecuteWorkflow(BatchWorkflow, m)
	require.NoError(t, env.GetWorkflowError())
	var report model.BatchReport
	require.NoError(t, env.GetWorkflowResult(&report))
	require.Equal(t, 1, report.Skipped)
	require.Equal(t, 1, report.Delivered)
	require.Equal(t, 2*time.Second, report.ProviderDuration)
	require.Equal(t, "in_progress", report.ProviderStatus)
	require.Equal(t, 2, report.FailureCounts["DEADLINE_EXCEEDED"])
	env.AssertExpectations(t)
}

type progressHarnessResult struct {
	Final    model.BatchReport
	Progress []model.BatchReport
}

func batchProgressHarness(ctx workflow.Context, m model.Manifest) (progressHarnessResult, error) {
	var result progressHarnessResult
	progress := workflow.GetSignalChannel(ctx, model.BatchProgressSignal)
	if err := workflow.ExecuteChildWorkflow(ctx, BatchWorkflow, m).Get(ctx, &result.Final); err != nil {
		return result, err
	}
	for {
		var report model.BatchReport
		if !progress.ReceiveAsync(&report) {
			break
		}
		result.Progress = append(result.Progress, report)
	}
	return result, nil
}

func TestBatchProgressPersistsRecoveryIDsOnlyOnProviderTransitions(t *testing.T) {
	env, m := newBatchTest()
	env.RegisterWorkflow(BatchWorkflow)
	pending := provider.Job{ID: "provider-1", Status: "in_progress", InputFileID: "file-1"}
	complete := pending
	complete.Status = "completed"
	env.OnActivity(serviceActivities.Upload, mock.Anything, m).Return("file-1", nil).Once()
	env.OnActivity(serviceActivities.Create, mock.Anything, "file-1", batchMetadata(m)).Return(CreateResult{Job: pending}, nil).Once()
	env.OnActivity(serviceActivities.Retrieve, mock.Anything, "provider-1").Return(pending, nil).Once()
	env.OnActivity(serviceActivities.Retrieve, mock.Anything, "provider-1").Return(complete, nil).Once()
	env.OnActivity(serviceActivities.Results, mock.Anything, m, complete).Return(ReconciledResults{Outcomes: []model.Outcome{
		{RequestID: "1", Result: &model.Result{RequestID: "1", Text: "one"}}, {RequestID: "2", Result: &model.Result{RequestID: "2", Text: "two"}},
	}}, nil).Once()
	env.OnActivity(serviceActivities.CompleteResult, mock.Anything, mock.Anything, mock.Anything).Return(DeliveryResult{Delivered: true}, nil).Twice()
	env.ExecuteWorkflow(batchProgressHarness, m)
	require.NoError(t, env.GetWorkflowError())
	var result progressHarnessResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Len(t, result.Progress, 3)
	require.Equal(t, "file-1", result.Progress[0].InputFileID)
	require.Empty(t, result.Progress[0].ProviderBatchID)
	require.Equal(t, "provider-1", result.Progress[1].ProviderBatchID)
	require.Equal(t, "in_progress", result.Progress[1].ProviderStatus)
	require.Equal(t, "completed", result.Progress[2].ProviderStatus)
	env.AssertExpectations(t)
}
