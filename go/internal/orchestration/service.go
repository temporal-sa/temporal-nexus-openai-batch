// Package orchestration connects a per-request Nexus operation to shared provider
// batches. PostgreSQL owns admission, batch membership, completion credentials and
// outcomes; BatchWorkflow sequences the provider work using small references.
//
// Workflow code uses Temporal's deterministic APIs so its decisions can replay.
// HTTP, SQL and Temporal client RPCs live in Activities or the dispatcher/keeper,
// which run as ordinary worker-process code and recover through durable records.
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
// The Nexus request ID identifies retries of one operation start. A second
// operation using the same logical request_id gets a different Activity ID and
// must pass the store's ownership checks instead of silently sharing a result.
func RequestActivityID(r model.Request, nexusRequestID string) string {
	h := sha256.Sum256([]byte(r.RunID + "\x00" + r.RequestID + "\x00" + nexusRequestID))
	return "request-activity/" + r.RunID + "/" + hex.EncodeToString(h[:])
}

// RegisterBatch separates bulk work from short admission Activities. Each batch
// runs a fixed sequence of coarse Activities rather than one Activity per item.
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

// RegisterService exposes respond as a Temporal-backed asynchronous Nexus
// operation. Its backing execution is a standalone Activity (SAA), not a Workflow
// that waits for a later signal. The SDK carries the Nexus callback onto that
// Activity, so external completion ultimately resolves the caller's operation.
func RegisterService(w worker.Worker, c client.Client, p provider.Provider, repo Repository) {
	activities := &ServiceActivities{Client: c, Provider: p, Store: repo}
	w.RegisterActivity(activities.SubmitRequest)
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[model.Request, model.Result]{
		Name: model.Operation,
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input model.Request, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[model.Result], error) {
			if err := model.ValidateRequest(input); err != nil {
				return temporalnexus.TemporalOperationResult[model.Result]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "%s", err)
			}
			// Read the authoritative run configuration at ingress. The endpoint
			// check keeps a fake/live operation from entering a run owned by a
			// different service endpoint even if its run ID is known.
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
			// Starting the SAA returns an asynchronous operation handle promptly.
			// The long operation timeout includes collection and provider waiting;
			// the shorter heartbeat timeout detects lost enrollment/keeper work.
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
// It is an example consumer: a real business Workflow can make this same Nexus
// call directly. Waiting on the future persists the wait in Temporal; it does
// not reserve a worker thread for the provider's processing window. This
// per-request history still contains its own bounded request and result.
func CallerWorkflow(ctx workflow.Context, input model.CallerInput) (model.Result, error) {
	if err := model.ValidateRequest(input.Request); err != nil {
		return model.Result{}, typedFailure(err)
	}
	if err := input.Config.Validate(); err != nil {
		return model.Result{}, failure("INVALID_INPUT", err.Error())
	}
	// WaitRequested waits for the cancellation request to be acknowledged. It
	// does not mean the caller waits for the shared upstream batch to terminate.
	f := workflow.NewNexusClient(input.Config.Endpoint, model.Service).ExecuteOperation(ctx,
		model.Operation, input.Request, workflow.NexusOperationOptions{ScheduleToCloseTimeout: input.Config.OperationTimeout,
			CancellationType: workflow.NexusOperationCancellationTypeWaitRequested})
	var result model.Result
	err := f.Get(ctx, &result)
	return result, err
}

// failure marks a known business/validation failure as non-retryable. Retrying
// unchanged invalid input cannot repair it; transient infrastructure errors are
// left as ordinary errors so the surrounding Temporal policy can retry them.
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

// serviceActivities supplies method references when scheduling Activities. The
// Workflow does not dereference this nil pointer or call its methods locally;
// RegisterBatch registers methods on a real instance with I/O dependencies.
var serviceActivities *ServiceActivities

// ServiceActivities is the I/O boundary used by BatchWorkflow and the SAA.
// Dependencies are worker-local objects and are never serialized into history.
type ServiceActivities struct {
	Client   client.Client
	Provider provider.Provider
	Store    Repository
}

// Enroll is a direct repository call inside SubmitRequest, not a separately
// scheduled Workflow Activity. Admission and its exact completion target commit
// together before SubmitRequest gives up its execution slot.
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

// Upload materializes the sealed manifest only inside this Activity. The
// Workflow receives a file ID, so batch-sized JSONL never becomes its payload.
// Repeating an upload can leave another input file, but does not execute a batch.
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

// CreateResult distinguishes a confirmed provider rejection from an ambiguous
// send. Ambiguity means a paid job may exist even though its ID was not received.
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
	// The store's run lock orders submission against explicit run cleanup. It
	// cannot make an HTTP request atomic with SQL: a lost acknowledgment still
	// requires lookup, rather than assuming the provider did not accept work.
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

// Find reconciles an uncertain Create by both logical-batch metadata and input
// file ID. Only one exact match is safe to adopt; zero or multiple matches leave
// the submission unknown and must not trigger another paid Create.
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
	// Commit outcomes before any external completion. Delivery can then retry
	// from PostgreSQL after a crash without depending on downloading files again.
	if err := a.Store.SaveOutcomes(ctx, ref.BatchID, outcomes, diagnostics); err != nil {
		return model.DeliverySummary{}, err
	}
	return a.Store.DeliverySummary(ctx, ref.BatchID)
}

// FailBatch turns a batch-level failure into stored per-request outcomes. Those
// failures use the same delivery path as successful provider results, allowing
// each original Nexus caller to receive its own terminal failure.
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
