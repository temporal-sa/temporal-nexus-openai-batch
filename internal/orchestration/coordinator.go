package orchestration

import (
	"bytes"
	"fmt"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

type admittedRequest struct {
	Enrollment model.Enrollment
	Admission  model.Admission
}

// CoordinatorWorkflow is bounded to at most 1,000 admissions. Its short enroll
// Update never performs I/O or yields. Sealing swaps the buffer before any child
// start, so an admission can only belong to one immutable manifest.
func CoordinatorWorkflow(ctx workflow.Context, input model.RunInput) (model.RunStatus, error) {
	status := model.RunStatus{RunID: input.RunID, Batches: []model.BatchReport{}, Backing: "nexus_standalone_activity", CompletionMode: "external_async_completion_legacy"}
	if err := input.Config.Validate(); err != nil {
		return status, failure("INVALID_INPUT", err.Error())
	}
	if err := model.ValidateRequest(model.Request{RunID: input.RunID, RequestID: "run", Input: "run"}); err != nil {
		return status, typedFailure(err)
	}
	seen := make(map[string]admittedRequest)
	batchFinished := make(map[string]bool)
	var buffer []model.Entry
	var sealed []model.Manifest
	bufferBytes, sequence, pending, version := 0, 1, 0, 0
	var bufferSince time.Time
	seal := func() {
		if len(buffer) == 0 {
			return
		}
		manifest := model.Manifest{RunID: input.RunID, BatchID: batchName(input.RunID, sequence), Config: input.Config,
			Entries: buffer, SealedAt: workflow.Now(ctx), Bytes: bufferBytes}
		// This entire state transition is nonyielding.
		buffer, bufferBytes, bufferSince = nil, 0, time.Time{}
		sequence++
		sealed = append(sealed, manifest)
		version++
	}
	currentStatus := func() model.RunStatus {
		copy := status
		copy.Buffered = len(buffer)
		copy.Batches = append([]model.BatchReport{}, status.Batches...)
		return copy
	}
	if err := workflow.SetQueryHandler(ctx, model.StatusQuery, func() (model.RunStatus, error) { return currentStatus(), nil }); err != nil {
		return status, err
	}
	if err := workflow.SetQueryHandler(ctx, model.ConfigQuery, func() (model.Config, error) { return input.Config, nil }); err != nil {
		return status, err
	}
	if err := workflow.SetUpdateHandler(ctx, model.EnrollUpdate, func(_ workflow.Context, e model.Enrollment) (model.Admission, error) {
		if err := model.ValidateRequest(e.Request); err != nil {
			return model.Admission{}, typedFailure(err)
		}
		if e.Request.RunID != input.RunID || e.Target.ActivityID == "" || e.Target.ActivityRunID == "" || e.Target.Namespace != workflow.GetInfo(ctx).Namespace || e.Target.Attempt < 1 || len(e.Target.TaskToken) == 0 || e.Fingerprint != model.Fingerprint(e.Request) {
			return model.Admission{}, failure("INVALID_INPUT", "enrollment identity or fingerprint is invalid")
		}
		if previous, ok := seen[e.Request.RequestID]; ok {
			if sameTarget(previous.Enrollment.Target, e.Target) && previous.Enrollment.Fingerprint == e.Fingerprint {
				return previous.Admission, nil
			}
			if previous.Enrollment.Target.ActivityID == e.Target.ActivityID && previous.Enrollment.Target.ActivityRunID == e.Target.ActivityRunID {
				return model.Admission{}, failure("STALE_ACTIVITY_ATTEMPT", "request is already owned by a different Activity attempt")
			}
			return model.Admission{}, failure("DUPLICATE_REQUEST", "request_id is already owned by another operation")
		}
		if status.Closed {
			return model.Admission{}, failure("RUN_CLOSED", "batching run is closed to new requests")
		}
		if status.Admitted >= input.Config.MaxAdmissions {
			return model.Admission{}, failure("RUN_FULL", "batching run admission limit reached")
		}
		line, err := model.JSONLLine(e.Request, input.Config)
		if err != nil {
			return model.Admission{}, failure("INVALID_INPUT", err.Error())
		}
		if len(line) > input.Config.MaxBatchBytes {
			return model.Admission{}, failure("INVALID_INPUT", "request exceeds configured batch byte limit")
		}
		if len(buffer) > 0 && bufferBytes+len(line) > input.Config.MaxBatchBytes {
			seal()
		}
		a := model.Admission{BatchID: batchName(input.RunID, sequence), Fingerprint: e.Fingerprint, Config: input.Config}
		if len(buffer) == 0 {
			bufferSince = workflow.Now(ctx)
		}
		buffer = append(buffer, model.Entry{Request: e.Request, Target: e.Target, Fingerprint: e.Fingerprint, AdmittedAt: workflow.Now(ctx)})
		bufferBytes += len(line)
		status.Admitted++
		seen[e.Request.RequestID] = admittedRequest{Enrollment: e, Admission: a}
		version++
		if len(buffer) >= input.Config.MaxBatchRequests || bufferBytes >= input.Config.MaxBatchBytes {
			seal()
		}
		return a, nil
	}); err != nil {
		return status, err
	}
	if err := workflow.SetUpdateHandler(ctx, model.FinishUpdate, func(workflow.Context) (model.RunStatus, error) {
		status.Closed = true
		seal()
		version++
		return currentStatus(), nil
	}); err != nil {
		return status, err
	}
	progressReports := workflow.GetSignalChannel(ctx, model.BatchProgressSignal)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var report model.BatchReport
			if !progressReports.Receive(ctx, &report) {
				return
			}
			if batchFinished[report.BatchID] {
				continue
			}
			for index, previous := range status.Batches {
				if previous.BatchID == report.BatchID {
					status.Batches[index] = report
					version++
					break
				}
			}
		}
	})

	for {
		for len(sealed) > 0 {
			manifest := sealed[0]
			sealed = sealed[1:]
			index := len(status.Batches)
			status.Batches = append(status.Batches, model.BatchReport{BatchID: manifest.BatchID, Status: "starting", Requests: len(manifest.Entries), Bytes: manifest.Bytes})
			pending++
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: model.BatchWorkflowID(manifest.BatchID),
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE})
			future := workflow.ExecuteChildWorkflow(childCtx, BatchWorkflow, manifest)
			workflow.Go(ctx, func(ctx workflow.Context) {
				var report model.BatchReport
				if err := future.Get(ctx, &report); err != nil {
					report = status.Batches[index]
					report.Status, report.Failed = "workflow_failed", report.Requests
					report.FailureCounts = map[string]int{"BATCH_WORKFLOW_FAILED": report.Requests}
					report.Diagnostics = []string{fmt.Sprintf("batch workflow failed: %s", errorType(err))}
					// A terminal child failure must still resolve its accepted
					// requests. A recipient that already completed is harmless.
					for _, entry := range manifest.Entries {
						outcome := model.Outcome{RequestID: entry.Request.RequestID, Fingerprint: entry.Fingerprint, BatchID: manifest.BatchID,
							Failure: &model.Failure{Code: "BATCH_WORKFLOW_FAILED", Message: "batch workflow failed before completing delivery"}}
						var delivery DeliveryResult
						if completionErr := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, safeOptions()), serviceActivities.CompleteResult, entry.Target, outcome).Get(ctx, &delivery); completionErr == nil && delivery.Delivered {
							report.Delivered++
						} else {
							report.Skipped++
						}
					}
				}
				status.Batches[index] = report
				batchFinished[manifest.BatchID] = true
				pending--
				version++
			})
		}
		if status.Closed && len(buffer) == 0 && pending == 0 {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return currentStatus(), err
			}
			return currentStatus(), nil
		}
		observed := version
		if len(buffer) > 0 {
			left := bufferSince.Add(input.Config.Window).Sub(workflow.Now(ctx))
			if left <= 0 {
				seal()
				continue
			}
			changed, err := workflow.AwaitWithTimeout(ctx, left, func() bool { return version != observed })
			if err != nil {
				return currentStatus(), err
			}
			if !changed {
				seal()
			}
		} else if err := workflow.Await(ctx, func() bool { return version != observed }); err != nil {
			return currentStatus(), err
		}
	}
}

func sameTarget(a, b model.CompletionTarget) bool {
	return a.Namespace == b.Namespace && a.ActivityID == b.ActivityID && a.ActivityRunID == b.ActivityRunID && a.Attempt == b.Attempt && bytes.Equal(a.TaskToken, b.TaskToken)
}
