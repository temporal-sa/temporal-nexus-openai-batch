// Package orchestration owns durable batching and the public Nexus operation.
package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/provider"
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

func RegisterBatch(w worker.Worker, c client.Client, p provider.Provider, repo Repository) {
	w.RegisterWorkflow(BatchWorkflow)
	activities := &ServiceActivities{Client: c, Provider: p, Store: repo}
	// Enrollment and exact-token completion are plain helpers inside these
	// Activities. They are not registered as per-request Workflow Activities.
	for _, implementation := range []any{activities.Upload, activities.Create, activities.Find,
		activities.Poll, activities.Results, activities.FailBatch, activities.SaveReport, activities.Deliver} {
		w.RegisterActivity(implementation)
	}
}

func RegisterService(w worker.Worker, c client.Client, p provider.Provider, repo Repository) {
	activities := &ServiceActivities{Client: c, Provider: p, Store: repo}
	w.RegisterActivity(activities.SubmitRequest)
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[model.Request, model.Result]{
		Name: model.Operation,
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input model.Request, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[model.Result], error) {
			if err := model.ValidateRequest(input); err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "%s", err)
			}
			run, err := repo.GetRun(ctx, input.RunID)
			if err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, configurationLookupError(err)
			}
			config := run.Config
			if err := validateIngressEndpoint(config.Endpoint, temporalnexus.GetOperationInfo(ctx).Endpoint); err != nil {
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
				HeartbeatTimeout:       model.EnrollmentHeartbeat,
				RetryPolicy:            &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 10 * time.Second},
				ActivityIDReusePolicy:  enums.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
			}, serviceActivities.SubmitRequest, model.SAAInput{Request: input, Config: config})
			if err == nil {
				// Link the caller's Nexus operation to its exact standalone
				// Activity execution for inspection in Workflow history.
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

func validateIngressEndpoint(configured, actual string) error {
	if actual != configured {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "batching run belongs to another Nexus endpoint")
	}
	return nil
}

func configurationLookupError(err error) *nexus.HandlerError {
	var invalid *model.Failure
	if errors.As(err, &invalid) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "%s", invalid.Message)
	}
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "batching run is not open")
	}
	// A storage failure occurs before an Activity is created.
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
	Store    Repository
}

func (a *ServiceActivities) Enroll(ctx context.Context, e model.Enrollment) (model.Admission, error) {
	admission, err := a.Store.Enroll(ctx, e)
	return admission, typedFailure(err)
}

func providerError(err error) error {
	if provider.IsPermanent(err) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "PROVIDER_ERROR", err)
	}
	return err
}

func (a *ServiceActivities) Upload(ctx context.Context, ref model.BatchRef) (string, error) {
	m, err := a.Store.LoadManifest(ctx, ref.BatchID)
	if err != nil {
		return "", err
	}
	data, err := model.JSONL(m)
	if err != nil {
		return "", failure("INVALID_BATCH", err.Error())
	}
	id, err := a.Provider.Upload(ctx, ref.BatchID, data)
	return id, providerError(err)
}

type CreateResult struct {
	Job         provider.Job
	Failure     string
	FailureCode string
	Ambiguous   bool
}

// Create's Activity has one attempt and the provider makes one HTTP attempt.
// An Activity transport failure is also ambiguous and is reconciled by Find.
func (a *ServiceActivities) Create(ctx context.Context, ref model.BatchRef, fileID string) (CreateResult, error) {
	var result CreateResult
	called := false
	err := a.Store.WithSubmission(ctx, ref.BatchID, func(callCtx context.Context) error {
		called = true
		job, providerErr := a.Provider.Create(callCtx, fileID, batchMetadata(ref))
		if providerErr != nil {
			result = CreateResult{Failure: providerErr.Error(), Ambiguous: provider.IsAmbiguous(providerErr)}
		} else {
			result.Job = controlJob(job)
		}
		return nil
	})
	if err != nil {
		var known *model.Failure
		if !called && errors.As(err, &known) {
			return CreateResult{Failure: known.Message, FailureCode: known.Code}, nil
		}
		// A gate acknowledgment may be lost after the provider accepted work.
		// Preserve the one-attempt rule and reconcile instead of creating again.
		return CreateResult{Failure: "submission gate could not be confirmed", Ambiguous: true}, nil
	}
	return result, nil
}

func (a *ServiceActivities) Find(ctx context.Context, ref model.BatchRef, fileID string) (CreateResult, error) {
	jobs, err := a.Provider.Find(ctx, batchMetadata(ref))
	if err != nil {
		return CreateResult{}, providerError(err)
	}
	var match provider.Job
	count := 0
	for _, candidate := range jobs {
		if candidate.InputFileID == fileID && metadataEqual(candidate.Metadata, batchMetadata(ref)) {
			match = candidate
			count++
		}
	}
	if count != 1 {
		return CreateResult{Ambiguous: true, Failure: "provider submission could not be uniquely reconciled"}, nil
	}
	return CreateResult{Job: controlJob(match)}, nil
}

func controlJob(job provider.Job) provider.Job {
	// Arbitrary upstream metadata stays outside Workflow histories.
	job.Metadata = nil
	return job
}

// Results keeps raw output, outcomes and task tokens inside this Activity and
// Postgres. The Workflow receives only a bounded aggregate.
func (a *ServiceActivities) Results(ctx context.Context, ref model.BatchRef, job provider.Job) (model.DeliverySummary, error) {
	manifest, err := a.Store.LoadManifest(ctx, ref.BatchID)
	if err != nil {
		return model.DeliverySummary{}, err
	}
	output, errorFile, err := a.Provider.Results(ctx, job)
	if err != nil {
		return model.DeliverySummary{}, providerError(err)
	}
	outcomes, diagnostics := provider.Reconcile(manifest, job, output, errorFile)
	if err := a.Store.SaveOutcomes(ctx, ref.BatchID, outcomes, diagnostics); err != nil {
		return model.DeliverySummary{}, err
	}
	return a.Store.DeliverySummary(ctx, ref.BatchID)
}

func (a *ServiceActivities) FailBatch(ctx context.Context, ref model.BatchRef, code, message string) (model.DeliverySummary, error) {
	if err := a.Store.FailBatch(ctx, ref.BatchID, code, message); err != nil {
		return model.DeliverySummary{}, err
	}
	return a.Store.DeliverySummary(ctx, ref.BatchID)
}

func (a *ServiceActivities) SaveReport(ctx context.Context, report model.BatchReport) error {
	return a.Store.SaveReport(ctx, report)
}

func batchMetadata(ref model.BatchRef) map[string]string {
	return map[string]string{"run_id": ref.RunID, "logical_batch_id": ref.BatchID}
}
