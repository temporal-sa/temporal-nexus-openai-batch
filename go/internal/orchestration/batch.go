package orchestration

import (
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/provider"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// safeOptions is the baseline for short, repeatable storage/control Activities.
// StartToClose bounds an attempt; ScheduleToClose bounds queueing plus retries.
// Paid provider creation overrides this policy to forbid an automatic replay.
func safeOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second, ScheduleToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second, MaximumAttempts: 5}}
}

// boundedActivityContext spends only the time left before an absolute deadline.
// workflow.Now is replay-safe; ordinary time.Now would make Workflow decisions
// depend on the worker's clock when history is replayed after a restart.
func boundedActivityContext(ctx workflow.Context, deadline time.Time, oneAttempt bool) workflow.Context {
	options := safeOptions()
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		// Temporal requires positive timeouts. Call sites decide whether to
		// schedule work; this floor must not grant a fresh timeout budget.
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

// bulkProviderContext lets repeatable file transfer work survive transient
// failures until the provider deadline. MaximumAttempts=0 removes the attempt
// count cap, while ScheduleToClose still caps the total elapsed retry budget.
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

// Final bookkeeping outlives the recipient's timeout. An expired recipient can
// no longer receive a result, but its durable outcome and delivery checkpoint
// still need to be recorded after a storage outage.
func finalizationActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second}}
}

func finalizationBulkOptions() workflow.ActivityOptions {
	options := finalizationActivityOptions()
	options.StartToCloseTimeout = 5 * time.Minute
	return options
}

// providerDeadline reserves time between finishing provider work and the SAA's
// completion deadline. Collection delays consume this budget rather than being
// hidden by restarting a full operation timeout when a batch finally seals.
func providerDeadline(ref model.BatchRef, recipientDeadline time.Time) time.Time {
	// Preserve the configured time left for completion instead of restarting the
	// full provider budget when an overdue collection is finally sealed.
	reserve := ref.Config.OperationTimeout - ref.Config.ResultDeadline - ref.Config.Window
	deadline := ref.SealedAt.Add(ref.Config.ResultDeadline)
	if latest := recipientDeadline.Add(-reserve); latest.Before(deadline) {
		deadline = latest
	}
	return deadline
}

// pollingActivityOptions permits polling to restart after a lost worker while
// retaining the original provider job ID and absolute deadline. The heartbeat
// timeout detects loss sooner than the potentially day-long result budget.
func pollingActivityOptions(ctx workflow.Context, deadline time.Time) workflow.ActivityOptions {
	remaining := deadline.Sub(workflow.Now(ctx))
	return workflow.ActivityOptions{StartToCloseTimeout: remaining, ScheduleToCloseTimeout: remaining,
		HeartbeatTimeout: min(30*time.Second, remaining),
		RetryPolicy:      &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second}}
}

// BatchWorkflow keeps only one small reference and provider control metadata.
// Request bodies, manifests, outcomes and completion tokens never enter its
// history, and delivery schedules one paged Activity for the whole batch.
//
// Its phases are upload -> create (or reconcile) -> poll -> persist outcomes ->
// deliver. Every I/O step is an Activity whose result is recorded by Temporal;
// replay reconstructs the decisions without directly repeating network calls.
// Activity retries can repeat their I/O, so each phase has its own recovery rule.
func BatchWorkflow(ctx workflow.Context, ref model.BatchRef) (model.BatchReport, error) {
	report := model.BatchReport{BatchID: ref.BatchID, Status: "uploading", Requests: ref.Requests, Bytes: ref.Bytes, FailureCounts: map[string]int{}}
	if err := ref.Config.Validate(); err != nil {
		return report, failure("INVALID_BATCH", err.Error())
	}
	if ref.Requests < 1 || ref.Requests > ref.Config.MaxBatchRequests || ref.Bytes < 1 || ref.Bytes > ref.Config.MaxBatchBytes || ref.BatchID == "" || ref.DeadlineAt.IsZero() {
		return report, failure("INVALID_BATCH", "invalid sealed batch reference")
	}
	if !ref.FirstAdmittedAt.IsZero() {
		report.CollectionWait = ref.SealedAt.Sub(ref.FirstAdmittedAt)
	}
	// A query reads replayed in-memory state without changing history. SaveReport
	// also persists coarse progress for CLI inspection outside this Workflow.
	if err := workflow.SetQueryHandler(ctx, model.StatusQuery, func() (model.BatchReport, error) { return report, nil }); err != nil {
		return report, err
	}
	recipientDeadline := ref.DeadlineAt
	save := func() error {
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, finalizationActivityOptions()), serviceActivities.SaveReport, report).Get(ctx, nil)
	}
	if err := save(); err != nil {
		return report, err
	}
	providerStart := workflow.Now(ctx)
	deadline := providerDeadline(ref, recipientDeadline)
	resolved := false
	var resolutionErr error
	// "Resolved" means every member has a durable outcome, not that delivery
	// has finished. Even provider-wide failures must reach the delivery phase.
	failBatch := func(code, message string) {
		var summary model.DeliverySummary
		resolutionErr = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, finalizationActivityOptions()), serviceActivities.FailBatch, ref, code, message).Get(ctx, &summary)
		if resolutionErr == nil {
			resolved = true
			applySummary(&report, summary)
		}
	}
	var inputFile string
	if !workflow.Now(ctx).Before(deadline) {
		failBatch("DEADLINE_EXCEEDED", "provider deadline reached before upload; no provider work was submitted")
	} else if err := workflow.ExecuteActivity(bulkProviderContext(ctx, deadline), serviceActivities.Upload, ref).Get(ctx, &inputFile); err != nil {
		failBatch("UPLOAD_FAILED", "provider input upload failed")
	} else {
		report.InputFileID, report.Status = inputFile, "submitting"
		if err := save(); err != nil {
			return report, err
		}
		if !workflow.Now(ctx).Before(deadline) {
			failBatch("DEADLINE_EXCEEDED", "provider deadline reached before creation; no provider batch was submitted")
		}
		var created CreateResult
		var createErr error
		if !resolved && resolutionErr == nil {
			// A timed-out Create could already have incurred paid provider work.
			// Disable Activity retries here; the only recovery action is lookup.
			createErr = workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, true), serviceActivities.Create, ref, inputFile).Get(ctx, &created)
		}
		job := created.Job
		if !resolved && resolutionErr == nil && (createErr != nil || created.Ambiguous) {
			report.Status = "reconciling_submission"
			var recovered CreateResult
			// Finding an already-created job incurs no new paid execution. Permit
			// reconciliation even if creation exhausted its provider deadline.
			findDeadline := workflow.Now(ctx).Add(2 * time.Minute)
			findErr := workflow.ExecuteActivity(boundedActivityContext(ctx, findDeadline, false), serviceActivities.Find, ref, inputFile).Get(ctx, &recovered)
			if findErr != nil || recovered.Ambiguous || recovered.Job.ID == "" {
				failBatch("SUBMISSION_UNKNOWN", "provider submission could not be uniquely reconciled; no second submission was attempted")
			} else {
				job = recovered.Job
			}
		} else if created.Failure != "" || created.FailureCode != "" {
			code := created.FailureCode
			if code == "" {
				code = "SUBMISSION_FAILED"
			}
			failBatch(code, "provider batch submission was rejected")
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
				if !job.Terminal() {
					// One long polling Activity keeps each HTTP poll out of history.
					// Saved reports show coarse phase transitions, not every poll.
					if !workflow.Now(ctx).Before(deadline) {
						failBatch("DEADLINE_EXCEEDED", "provider result deadline reached")
					} else if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, pollingActivityOptions(ctx, deadline)), serviceActivities.Poll, job, deadline, ref.Config.PollInterval).Get(ctx, &job); err != nil {
						if ctx.Err() != nil {
							return report, err
						}
						if errorType(err) == "PROVIDER_ERROR" {
							failBatch("PROVIDER_FAILED", "provider polling failed permanently")
						} else {
							failBatch("DEADLINE_EXCEEDED", "provider result deadline reached")
						}
					}
				}
				if !resolved && resolutionErr == nil {
					report.ProviderStatus, report.Status = job.Status, providerLifecycle(job)
					if err := save(); err != nil {
						return report, err
					}
					var summary model.DeliverySummary
					// Terminal provider status is not enough to call each request
					// successful. Results reconciles custom_id values and persists
					// either a normalized result or a failure for every member.
					if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, finalizationBulkOptions()), serviceActivities.Results, ref, job).Get(ctx, &summary); err != nil {
						failBatch("RESULT_DOWNLOAD_FAILED", "provider result download failed")
					} else {
						resolved = true
						applySummary(&report, summary)
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
	// The result deadline bounds provider waiting, not repair of durable
	// bookkeeping. Deliver retries from PostgreSQL checkpoints and records
	// recipients that expired or were canceled as skipped completion targets.
	options := finalizationBulkOptions()
	options.HeartbeatTimeout = 30 * time.Second
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
	// Diagnostics enter the small aggregate report, so cap their count even
	// when an upstream file contains many invalid or unexpected records.
	if len(values) < 20 {
		return append(values, value)
	}
	return values
}
