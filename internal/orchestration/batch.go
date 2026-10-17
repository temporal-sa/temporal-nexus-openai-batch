package orchestration

import (
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func safeOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second, ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second, MaximumAttempts: 5}}
}
func boundedActivityContext(ctx workflow.Context, deadline time.Time, oneAttempt bool) workflow.Context {
	options := safeOptions()
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		remaining = time.Nanosecond
	}
	if options.ScheduleToCloseTimeout > remaining {
		options.ScheduleToCloseTimeout = remaining
	}
	if options.StartToCloseTimeout > remaining {
		options.StartToCloseTimeout = remaining
	}
	if oneAttempt {
		options.RetryPolicy.MaximumAttempts = 1
	}
	return workflow.WithActivityOptions(ctx, options)
}

func storageActivityOptions(ctx workflow.Context, deadline time.Time) workflow.ActivityOptions {
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		remaining = time.Nanosecond
	}
	return workflow.ActivityOptions{StartToCloseTimeout: min(30*time.Second, remaining), ScheduleToCloseTimeout: remaining,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second}}
}

func bulkProviderContext(ctx workflow.Context, deadline time.Time) workflow.Context {
	options := safeOptions()
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		remaining = time.Nanosecond
	}
	options.StartToCloseTimeout = min(5*time.Minute, remaining)
	options.ScheduleToCloseTimeout = remaining
	options.RetryPolicy.MaximumAttempts = 0
	return workflow.WithActivityOptions(ctx, options)
}

// BatchWorkflow keeps only one small reference and provider control metadata.
// Request bodies, manifests, outcomes and completion tokens never enter its
// history, and delivery schedules one paged Activity for the whole batch.
func BatchWorkflow(ctx workflow.Context, ref model.BatchRef) (model.BatchReport, error) {
	report := model.BatchReport{BatchID: ref.BatchID, Status: "uploading", Requests: ref.Requests, Bytes: ref.Bytes, FailureCounts: map[string]int{}}
	if err := ref.Config.Validate(); err != nil {
		return report, failure("INVALID_BATCH", err.Error())
	}
	if ref.Requests < 1 || ref.Requests > ref.Config.MaxBatchRequests || ref.Bytes < 1 || ref.Bytes > ref.Config.MaxBatchBytes || ref.BatchID == "" {
		return report, failure("INVALID_BATCH", "invalid sealed batch reference")
	}
	if !ref.FirstAdmittedAt.IsZero() {
		report.CollectionWait = ref.SealedAt.Sub(ref.FirstAdmittedAt)
	}
	if err := workflow.SetQueryHandler(ctx, model.StatusQuery, func() (model.BatchReport, error) { return report, nil }); err != nil {
		return report, err
	}
	storageDeadline := ref.FirstAdmittedAt.Add(ref.Config.OperationTimeout)
	if ref.FirstAdmittedAt.IsZero() {
		storageDeadline = ref.SealedAt.Add(ref.Config.OperationTimeout)
	}
	save := func() error {
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, storageActivityOptions(ctx, storageDeadline)), serviceActivities.SaveReport, report).Get(ctx, nil)
	}
	if err := save(); err != nil {
		return report, err
	}
	providerStart := workflow.Now(ctx)
	deadline := ref.SealedAt.Add(ref.Config.ResultDeadline)
	resolved := false
	var resolutionErr error
	failBatch := func(code, message string) {
		var summary model.DeliverySummary
		resolutionErr = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, storageActivityOptions(ctx, storageDeadline)), serviceActivities.FailBatch, ref, code, message).Get(ctx, &summary)
		if resolutionErr == nil {
			resolved = true
			applySummary(&report, summary)
		}
	}
	var inputFile string
	if err := workflow.ExecuteActivity(bulkProviderContext(ctx, deadline), serviceActivities.Upload, ref).Get(ctx, &inputFile); err != nil {
		failBatch("UPLOAD_FAILED", "provider input upload failed")
	} else {
		report.InputFileID, report.Status = inputFile, "submitting"
		if err := save(); err != nil {
			return report, err
		}
		var created CreateResult
		createErr := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, true), serviceActivities.Create, ref, inputFile).Get(ctx, &created)
		job := created.Job
		if createErr != nil || created.Ambiguous {
			report.Status = "reconciling_submission"
			var recovered CreateResult
			findErr := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Find, ref, inputFile).Get(ctx, &recovered)
			if findErr != nil || recovered.Ambiguous || recovered.Job.ID == "" {
				failBatch("SUBMISSION_UNKNOWN", "provider submission could not be uniquely reconciled; no second submission was attempted")
			} else {
				job = recovered.Job
			}
		} else if created.Failure != "" {
			failBatch("SUBMISSION_FAILED", "provider rejected the batch submission")
		}
		if !resolved && resolutionErr == nil {
			if job.ID == "" {
				failBatch("SUBMISSION_UNKNOWN", "provider returned no batch identifier")
			} else {
				report.ProviderBatchID = job.ID
				report.ProviderStatus, report.Status = job.Status, providerLifecycle(job)
				if err := save(); err != nil {
					return report, err
				}
				for !resolved && resolutionErr == nil {
					if report.ProviderStatus != job.Status {
						report.ProviderStatus, report.Status = job.Status, providerLifecycle(job)
						if err := save(); err != nil {
							return report, err
						}
					}
					if !workflow.Now(ctx).Before(deadline) {
						failBatch("DEADLINE_EXCEEDED", "provider result deadline reached")
						break
					}
					if job.Terminal() {
						var summary model.DeliverySummary
						if err := workflow.ExecuteActivity(bulkProviderContext(ctx, deadline), serviceActivities.Results, ref, job).Get(ctx, &summary); err != nil {
							failBatch("RESULT_DOWNLOAD_FAILED", "provider result download failed")
						} else {
							resolved = true
							applySummary(&report, summary)
						}
						break
					}
					wait := ref.Config.PollInterval
					if left := deadline.Sub(workflow.Now(ctx)); left < wait {
						wait = left
					}
					if err := workflow.Sleep(ctx, wait); err != nil {
						return report, err
					}
					if !workflow.Now(ctx).Before(deadline) {
						continue
					}
					if err := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Retrieve, job.ID).Get(ctx, &job); err != nil {
						if errorType(err) == "PROVIDER_ERROR" {
							failBatch("PROVIDER_FAILED", "provider polling failed permanently")
							break
						}
						report.Diagnostics = appendOnce(report.Diagnostics, "transient provider polling failures were retried")
					}
				}
			}
		}
	}
	if resolutionErr != nil {
		return report, resolutionErr
	}
	report.ProviderDuration = workflow.Now(ctx).Sub(providerStart)
	report.Status = "delivering"
	if err := save(); err != nil {
		return report, err
	}
	deliveryStart := workflow.Now(ctx)
	options := storageActivityOptions(ctx, storageDeadline)
	options.StartToCloseTimeout = options.ScheduleToCloseTimeout
	options.HeartbeatTimeout = min(30*time.Second, options.ScheduleToCloseTimeout)
	var summary model.DeliverySummary
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), serviceActivities.Deliver, ref).Get(ctx, &summary); err != nil {
		return report, err
	}
	applySummary(&report, summary)
	report.DeliveryDuration = workflow.Now(ctx).Sub(deliveryStart)
	report.Status = "completed"
	if report.Failed > 0 {
		report.Status = "completed_with_errors"
	}
	if err := save(); err != nil {
		return report, err
	}
	return report, nil
}

func providerLifecycle(job provider.Job) string {
	if job.Terminal() {
		return "processing_results"
	}
	return job.Status
}
func applySummary(report *model.BatchReport, summary model.DeliverySummary) {
	report.Delivered, report.Skipped, report.Succeeded, report.Failed = summary.Delivered, summary.Skipped, summary.Succeeded, summary.Failed
	report.FailureCounts = summary.FailureCounts
	for _, diagnostic := range summary.Diagnostics {
		report.Diagnostics = appendOnce(report.Diagnostics, diagnostic)
	}
}
func metadataEqual(actual, expected map[string]string) bool {
	for k, v := range expected {
		if actual[k] != v {
			return false
		}
	}
	return true
}
func appendOnce(values []string, value string) []string {
	for _, item := range values {
		if item == value {
			return values
		}
	}
	if len(values) < 20 {
		return append(values, value)
	}
	return values
}
