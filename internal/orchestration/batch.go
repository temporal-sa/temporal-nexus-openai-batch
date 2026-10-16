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

// BatchWorkflow owns exactly one sealed manifest and one provider submission.
func BatchWorkflow(ctx workflow.Context, manifest model.Manifest) (model.BatchReport, error) {
	report := model.BatchReport{BatchID: manifest.BatchID, Status: "uploading", Requests: len(manifest.Entries), Bytes: manifest.Bytes, FailureCounts: map[string]int{}}
	progress := func() {
		if parent := workflow.GetInfo(ctx).ParentWorkflowExecution; parent != nil {
			// Persist recovery identifiers in the coordinator while the job
			// is pending. A closed parent must not stop its abandoned child.
			_ = workflow.SignalExternalWorkflow(ctx, parent.ID, parent.RunID, model.BatchProgressSignal, report).Get(ctx, nil)
		}
	}
	if len(manifest.Entries) > 0 {
		report.CollectionWait = manifest.SealedAt.Sub(manifest.Entries[0].AdmittedAt)
	}
	if err := workflow.SetQueryHandler(ctx, model.StatusQuery, func() (model.BatchReport, error) { return report, nil }); err != nil {
		return report, err
	}
	if _, err := model.JSONL(manifest); err != nil {
		return report, failure("INVALID_BATCH", err.Error())
	}
	providerStart := workflow.Now(ctx)
	deadline := manifest.SealedAt.Add(manifest.Config.ResultDeadline)
	var outcomes []model.Outcome
	makeFailures := func(code, message string) {
		outcomes = make([]model.Outcome, 0, len(manifest.Entries))
		for _, entry := range manifest.Entries {
			outcomes = append(outcomes, model.Outcome{RequestID: entry.Request.RequestID, Fingerprint: entry.Fingerprint, BatchID: manifest.BatchID,
				Failure: &model.Failure{Code: code, Message: message}})
		}
	}
	var inputFile string
	err := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Upload, manifest).Get(ctx, &inputFile)
	if err != nil {
		makeFailures("UPLOAD_FAILED", "provider input upload failed")
	} else {
		report.InputFileID = inputFile
		report.Status = "submitting"
		progress()
		createCtx := boundedActivityContext(ctx, deadline, true)
		var created CreateResult
		createErr := workflow.ExecuteActivity(createCtx, serviceActivities.Create, inputFile, batchMetadata(manifest)).Get(ctx, &created)
		job := created.Job
		if createErr != nil || created.Ambiguous {
			report.Status = "reconciling_submission"
			var jobs []provider.Job
			findErr := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Find, batchMetadata(manifest)).Get(ctx, &jobs)
			matches := []provider.Job{}
			for _, candidate := range jobs {
				if candidate.InputFileID == inputFile && metadataEqual(candidate.Metadata, batchMetadata(manifest)) {
					matches = append(matches, candidate)
				}
			}
			if findErr != nil || len(matches) != 1 {
				makeFailures("SUBMISSION_UNKNOWN", "provider submission could not be uniquely reconciled; no second submission was attempted")
			} else {
				job = matches[0]
			}
		} else if created.Failure != "" {
			makeFailures("SUBMISSION_FAILED", "provider rejected the batch submission")
		}
		if outcomes == nil {
			if job.ID == "" {
				makeFailures("SUBMISSION_UNKNOWN", "provider returned no batch identifier")
			} else {
				report.ProviderBatchID = job.ID
				report.ProviderStatus, report.Status = job.Status, job.Status
				progress()
				for outcomes == nil {
					if report.ProviderStatus != job.Status {
						report.ProviderStatus, report.Status = job.Status, job.Status
						progress()
					}
					if !workflow.Now(ctx).Before(deadline) {
						makeFailures("DEADLINE_EXCEEDED", "provider result deadline reached")
						break
					}
					if job.Terminal() {
						var reconciled ReconciledResults
						if resultErr := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Results, manifest, job).Get(ctx, &reconciled); resultErr != nil {
							makeFailures("RESULT_DOWNLOAD_FAILED", "provider result download failed")
						} else {
							outcomes, report.Diagnostics = reconciled.Outcomes, reconciled.Diagnostics
						}
						break
					}
					wait := manifest.Config.PollInterval
					if left := deadline.Sub(workflow.Now(ctx)); left < wait {
						wait = left
					}
					if err := workflow.Sleep(ctx, wait); err != nil {
						return report, err
					}
					if !workflow.Now(ctx).Before(deadline) {
						continue
					}
					if pollErr := workflow.ExecuteActivity(boundedActivityContext(ctx, deadline, false), serviceActivities.Retrieve, job.ID).Get(ctx, &job); pollErr != nil {
						if errorType(pollErr) == "PROVIDER_ERROR" {
							makeFailures("PROVIDER_FAILED", "provider polling failed permanently")
							break
						}
						// Safe reads can be attempted again on the next durable poll.
						report.Diagnostics = appendOnce(report.Diagnostics, "transient provider polling failures were retried")
					}
				}
			}
		}
	}
	report.ProviderDuration = workflow.Now(ctx).Sub(providerStart)
	report.Status = "delivering"
	deliveryStart := workflow.Now(ctx)
	// Reconciliation yields one bounded outcome for every manifest entry.
	// Never correlate by file position or silently omit a missing result.
	byID := make(map[string]model.Outcome, len(outcomes))
	for _, outcome := range outcomes {
		byID[outcome.RequestID] = outcome
	}
	for _, entry := range manifest.Entries {
		outcome, ok := byID[entry.Request.RequestID]
		if !ok {
			outcome = model.Outcome{RequestID: entry.Request.RequestID, Failure: &model.Failure{Code: "MISSING_RESULT", Message: "provider returned no result"}}
		}
		outcome.Fingerprint, outcome.BatchID = entry.Fingerprint, manifest.BatchID
		if outcome.Failure != nil {
			report.Failed++
			report.FailureCounts[outcome.Failure.Code]++
		} else {
			report.Succeeded++
		}
		var delivery DeliveryResult
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, safeOptions()), serviceActivities.CompleteResult, entry.Target, outcome).Get(ctx, &delivery)
		if err == nil && delivery.Delivered {
			report.Delivered++
		} else {
			report.Skipped++
			if err != nil {
				report.Diagnostics = appendOnce(report.Diagnostics, "external Activity completion could not be confirmed")
			}
			if delivery.Diagnostic != "" {
				report.Diagnostics = appendOnce(report.Diagnostics, delivery.Diagnostic)
			}
		}
	}
	report.DeliveryDuration = workflow.Now(ctx).Sub(deliveryStart)
	report.Status = "completed"
	if report.Failed > 0 {
		report.Status = "completed_with_errors"
	}
	return report, nil
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
	return append(values, value)
}
