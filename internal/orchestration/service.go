// Package orchestration owns durable batching and the public Nexus operation.
package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/provider"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// RequestActivityID preserves Nexus start idempotency while keeping independent
// operations distinct, so duplicate logical request IDs can be rejected.
func RequestActivityID(r model.Request, nexusRequestID string) string {
	h := sha256.Sum256([]byte(r.RunID + "\x00" + r.RequestID + "\x00" + nexusRequestID))
	return "request-activity/" + r.RunID + "/" + hex.EncodeToString(h[:])
}

func RegisterService(w worker.Worker, c client.Client, p provider.Provider) {
	w.RegisterWorkflow(CoordinatorWorkflow)
	w.RegisterWorkflow(BatchWorkflow)
	w.RegisterActivity(&ServiceActivities{Client: c, Provider: p})
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[model.Request, model.Result]{
		Name: model.Operation,
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input model.Request, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[model.Result], error) {
			if err := model.ValidateRequest(input); err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "%s", err)
			}
			value, err := nc.GetWorkflowClient().QueryWorkflow(ctx, model.CoordinatorID(input.RunID), "", model.ConfigQuery)
			if err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, configurationQueryError(err)
			}
			var config model.Config
			if err := value.Get(&config); err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, err
			}
			if err := config.Validate(); err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, err
			}
			activityID := RequestActivityID(input, opts.RequestID)
			result, err := temporalnexus.StartActivity(ctx, nc, client.StartActivityOptions{
				ID:                     activityID,
				StartToCloseTimeout:    config.OperationTimeout,
				ScheduleToCloseTimeout: config.OperationTimeout,
				RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
				ActivityIDReusePolicy:  enums.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
			}, serviceActivities.SubmitRequest, model.SAAInput{Request: input, Config: config})
			if err == nil {
				// SDK 1.49 propagates callbacks into StartActivity but does not add the
				// reverse link to NexusOperationStarted. Add the pinned Activity link.
				if description, describeErr := nc.GetWorkflowClient().GetActivityHandle(client.GetActivityHandleOptions{ActivityID: activityID}).Describe(ctx, client.DescribeActivityOptions{}); describeErr == nil {
					nexus.AddHandlerLinks(ctx, temporalnexus.ConvertLinkActivityToNexusLink(&commonpb.Link_Activity{
						Namespace: temporalnexus.GetOperationInfo(ctx).Namespace, ActivityId: activityID, RunId: description.ActivityRunID,
					}))
				}
			}
			return result, err
		},
	})
	svc := nexus.NewService(model.Service)
	if err := svc.Register(op); err != nil {
		panic(err)
	}
	w.RegisterNexusService(svc)
}

func configurationQueryError(err error) *nexus.HandlerError {
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "batching run is not open")
	}
	// A transport or query-task failure occurs before an Activity is created.
	// Permit Nexus to retry the start without exposing raw query diagnostics.
	return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUnavailable,
		Message:       "batching run configuration is temporarily unavailable",
		RetryBehavior: nexus.HandlerErrorRetryBehaviorRetryable}
}

func RegisterCaller(w worker.Worker) { w.RegisterWorkflow(CallerWorkflow) }

// CallerWorkflow performs one logical operation. All provider files, batches,
// polling and result fan-out remain behind the Nexus boundary.
func CallerWorkflow(ctx workflow.Context, input model.CallerInput) (model.Result, error) {
	if err := model.ValidateRequest(input.Request); err != nil {
		return model.Result{}, typedFailure(err)
	}
	if err := input.Config.Validate(); err != nil {
		return model.Result{}, failure("INVALID_INPUT", err.Error())
	}
	f := workflow.NewNexusClient(input.Config.Endpoint, model.Service).ExecuteOperation(ctx,
		model.Operation, input.Request, workflow.NexusOperationOptions{ScheduleToCloseTimeout: input.Config.OperationTimeout,
			CancellationType: workflow.NexusOperationCancellationTypeWaitRequested})
	var result model.Result
	err := f.Get(ctx, &result)
	return result, err
}

func failure(code, message string, details ...any) error {
	return temporal.NewNonRetryableApplicationError(message, code, nil, details...)
}
func typedFailure(err error) error {
	var f *model.Failure
	if errors.As(err, &f) {
		return failure(f.Code, f.Message)
	}
	return err
}
func errorType(err error) string {
	var e *temporal.ApplicationError
	if errors.As(err, &e) {
		return e.Type()
	}
	return ""
}

// ServiceActivities contains the only network calls made by service workflows.
var serviceActivities *ServiceActivities

type ServiceActivities struct {
	Client   client.Client
	Provider provider.Provider
}

func (a *ServiceActivities) Enroll(ctx context.Context, e model.Enrollment) (model.Admission, error) {
	h, err := a.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: model.CoordinatorID(e.Request.RunID), UpdateName: model.EnrollUpdate,
		UpdateID: enrollmentUpdateID(e), Args: []any{e}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return model.Admission{}, failure("RUN_NOT_STARTED", "batching run is not open")
		}
		return model.Admission{}, err
	}
	var admission model.Admission
	err = h.Get(ctx, &admission)
	return admission, err
}

func providerError(err error) error {
	if provider.IsPermanent(err) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "PROVIDER_ERROR", err)
	}
	return err
}

func (a *ServiceActivities) Upload(ctx context.Context, m model.Manifest) (string, error) {
	b, err := model.JSONL(m)
	if err != nil {
		return "", failure("INVALID_BATCH", err.Error())
	}
	id, err := a.Provider.Upload(ctx, m.BatchID, b)
	return id, providerError(err)
}

type CreateResult struct {
	Job       provider.Job
	Failure   string
	Ambiguous bool
}

// Create's Activity has one attempt and the provider makes one HTTP attempt.
// An Activity transport failure is also ambiguous and is reconciled by Find.
func (a *ServiceActivities) Create(ctx context.Context, fileID string, metadata map[string]string) (CreateResult, error) {
	job, err := a.Provider.Create(ctx, fileID, metadata)
	if err != nil {
		return CreateResult{Failure: err.Error(), Ambiguous: provider.IsAmbiguous(err)}, nil
	}
	return CreateResult{Job: job}, nil
}

func (a *ServiceActivities) Find(ctx context.Context, metadata map[string]string) ([]provider.Job, error) {
	jobs, err := a.Provider.Find(ctx, metadata)
	return jobs, providerError(err)
}
func (a *ServiceActivities) Retrieve(ctx context.Context, id string) (provider.Job, error) {
	job, err := a.Provider.Retrieve(ctx, id)
	return job, providerError(err)
}

type ReconciledResults struct {
	Outcomes    []model.Outcome
	Diagnostics []string
}

func (a *ServiceActivities) Results(ctx context.Context, m model.Manifest, job provider.Job) (ReconciledResults, error) {
	output, errorFile, err := a.Provider.Results(ctx, job)
	if err != nil {
		return ReconciledResults{}, providerError(err)
	}
	outcomes, diagnostics := provider.Reconcile(m, job, output, errorFile)
	return ReconciledResults{Outcomes: outcomes, Diagnostics: diagnostics}, nil
}

func batchMetadata(m model.Manifest) map[string]string {
	return map[string]string{"run_id": m.RunID, "logical_batch_id": m.BatchID}
}

func batchName(runID string, sequence int) string { return fmt.Sprintf("%s-%04d", runID, sequence) }
