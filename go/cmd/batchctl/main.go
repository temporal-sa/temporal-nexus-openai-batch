// Command batchctl runs the prototype's workers and example callers. The CLI is
// a launcher/inspector: PostgreSQL and Temporal retain the durable work after a
// demo command stops waiting or its terminal is closed.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/orchestration"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/provider"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/store"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type connection struct{ address, namespace string }

func (c *connection) flags(f *flag.FlagSet) {
	f.StringVar(&c.address, "address", env("TEMPORAL_ADDRESS", "127.0.0.1:7234"), "Temporal gRPC address")
	f.StringVar(&c.namespace, "namespace", env("TEMPORAL_NAMESPACE", "default"), "Temporal namespace")
}
func (c connection) dial() (client.Client, error) {
	return client.Dial(client.Options{HostPort: c.address, Namespace: c.namespace, Logger: tlog.NewStructuredLogger(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))})
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Keep fake and live service work on separate queues and Nexus endpoints. A live
// worker must not accidentally consume a fake run's provider work, or vice versa.
func serviceQueue(mode string) string {
	if mode == "live" {
		return model.ServiceTaskQueue + "-live"
	}
	return model.ServiceTaskQueue
}
func endpoint(mode string) string {
	if mode == "live" {
		return model.NexusEndpoint + "-live"
	}
	return model.NexusEndpoint
}
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func openStore(ctx context.Context, namespace string) (*store.Store, error) {
	db, err := store.Open(ctx, env("DATABASE_URL", store.LocalDSN), namespace)
	if err != nil {
		return nil, fmt.Errorf("open Postgres store: %w", err)
	}
	return db, nil
}

func initializeDatabase(args []string) error {
	f := flag.NewFlagSet("init-db", flag.ContinueOnError)
	var conn connection
	conn.flags(f)
	if err := f.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := openStore(ctx, conn.namespace)
	if err != nil {
		return err
	}
	defer db.Close()
	return printJSON(map[string]any{"store": "postgres", "namespace": conn.namespace, "schema": "ready"})
}

// capabilities inspects the server support required by the real SAA-backed
// Nexus path; it does not infer support from whether an SDK symbol compiles.
func capabilities(args []string) error {
	f := flag.NewFlagSet("capabilities", flag.ContinueOnError)
	var conn connection
	conn.flags(f)
	if err := f.Parse(args); err != nil {
		return err
	}
	c, err := conn.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ns, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: conn.namespace})
	if err != nil {
		return err
	}
	system, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return err
	}
	report := map[string]any{"server_version": system.GetServerVersion(), "namespace": conn.namespace,
		"standalone_activities": ns.GetNamespaceInfo().GetCapabilities().GetStandaloneActivities(),
		"nexus_backing":         "standalone_activity", "completion_mode": "external_async_completion"}
	return printJSON(report)
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: batchctl <worker|demo|inspect|finish|cancel|cleanup|capabilities|init-db> [flags]; see README.md")
	}
	switch args[0] {
	case "capabilities":
		return capabilities(args[1:])
	case "init-db":
		return initializeDatabase(args[1:])
	case "worker":
		return runWorker(args[1:])
	case "demo":
		return runDemo(args[1:])
	case "inspect", "finish", "cancel", "cleanup":
		return control(args[0], args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// runWorker hosts three Temporal workers plus the store-backed dispatcher/keeper.
// They share a process for this prototype, but their distinct queues make the
// admission and bulk-work capacity choices explicit.
func runWorker(args []string) error {
	f := flag.NewFlagSet("worker", flag.ContinueOnError)
	var conn connection
	conn.flags(f)
	mode := f.String("mode", "fake", "fake or live")
	state := f.String("state-dir", ".state/fake", "persistent fake-provider state directory")
	scenario := f.String("scenario", "success", "fake scenario: success, mixed, expired, missing, duplicate, unknown, malformed, submission-unknown, submission-unreconciled")
	delay := f.Duration("delay", 2*time.Second, "fake processing duration")
	serviceSlots := f.Int("service-concurrency", 32, "maximum concurrent enrollment Activities")
	batchSlots := f.Int("batch-concurrency", 32, "maximum concurrent batch Activities; size for batch memory and provider capacity")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *serviceSlots < 1 || *batchSlots < 1 {
		return fmt.Errorf("worker concurrency must be positive")
	}
	var p provider.Provider
	var err error
	switch *mode {
	case "fake":
		p, err = provider.NewFake(provider.FakeConfig{StateDir: *state, Scenario: *scenario, ProcessingDelay: *delay})
	case "live":
		p, err = provider.NewOpenAI(provider.OpenAIConfig{APIKey: os.Getenv("OPENAI_API_KEY")})
	default:
		return fmt.Errorf("mode must be fake or live")
	}
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dbCtx, dbCancel := context.WithTimeout(ctx, 30*time.Second)
	db, err := openStore(dbCtx, conn.namespace)
	dbCancel()
	if err != nil {
		return err
	}
	defer db.Close()
	c, err := conn.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	// Enrollment is short: the SAA writes its request/target to PostgreSQL and
	// returns ErrResultPending. Bulk upload, polling and delivery have separate
	// Activity slots so large batches cannot consume all admission capacity.
	sw := worker.New(c, serviceQueue(*mode), worker.Options{MaxConcurrentActivityExecutionSize: *serviceSlots, WorkerStopTimeout: 5 * time.Second})
	orchestration.RegisterService(sw, c, p, db)
	batchQueue := serviceQueue(*mode) + "-batches"
	bw := worker.New(c, batchQueue, worker.Options{MaxConcurrentActivityExecutionSize: *batchSlots, WorkerStopTimeout: 5 * time.Second})
	orchestration.RegisterBatch(bw, c, p, db)
	// This worker runs the demo's per-document business consumer. Applications
	// can invoke the same Nexus operation from their own existing Workflows.
	cw := worker.New(c, model.CallerTaskQueue, worker.Options{})
	orchestration.RegisterCaller(cw)
	if err = sw.Start(); err != nil {
		return err
	}
	defer sw.Stop()
	if err = bw.Start(); err != nil {
		return err
	}
	defer bw.Stop()
	if err = cw.Start(); err != nil {
		return err
	}
	defer cw.Stop()
	fmt.Fprintf(os.Stderr, "Workers ready: mode=%s service_queue=%s endpoint=%s store=postgres\n", *mode, serviceQueue(*mode), endpoint(*mode))
	// The dispatcher seals due batches and starts their stable Workflow IDs;
	// its independent keeper heartbeats pending SAAs and repairs completions.
	// Durable store rows, rather than this channel, survive process failure.
	dispatchErr := make(chan error, 1)
	go func() {
		dispatchErr <- orchestration.Dispatcher(ctx, c, db, *mode, batchQueue, endpoint(*mode))
	}()
	defer stop()
	select {
	case <-ctx.Done():
		return nil
	case err := <-dispatchErr:
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

// itemReport describes what this launcher confirmed. "pending" means the CLI
// wait expired without a confirmed terminal outcome; it is not a failed request.
type itemReport struct {
	RequestID  string        `json:"request_id"`
	WorkflowID string        `json:"workflow_id"`
	Status     string        `json:"status"`
	Result     *model.Result `json:"result,omitempty"`
	Error      string        `json:"error,omitempty"`
}
type demoReport struct {
	RunID       string          `json:"run_id"`
	Mode        string          `json:"mode"`
	Simulated   bool            `json:"simulated"`
	Elapsed     time.Duration   `json:"elapsed_ns"`
	Succeeded   int             `json:"succeeded"`
	Failed      int             `json:"failed"`
	Pending     int             `json:"pending"`
	Status      model.RunStatus `json:"status"`
	StatusError string          `json:"status_error,omitempty"`
	Items       []itemReport    `json:"items"`
}

// runDemo creates a run and starts one CallerWorkflow per document. It does not
// call OpenAI directly: the callers cross Nexus and the service combines their
// requests into externally stored batches.
func runDemo(args []string) error {
	f := flag.NewFlagSet("demo", flag.ContinueOnError)
	var conn connection
	conn.flags(f)
	mode := f.String("mode", "fake", "fake or live; live submits paid provider requests")
	id := f.String("run-id", "demo-"+time.Now().UTC().Format("20060102-150405"), "unique run identifier")
	n := f.Int("count", 0, "number of synthetic documents (default 100 fake or 5 live)")
	input := f.String("input", "", "JSONL file containing request_id and input")
	window := f.Duration("window", 5*time.Second, "collection window")
	maxBatch := f.Int("batch-size", 100, "maximum requests per batch, at most 50000")
	maxBytes := f.Int("batch-bytes", 512*1024, "maximum JSONL bytes per batch, at most 200000000")
	maxAdmissions := f.Int("max-admissions", 0, "optional maximum unique run admissions; 0 is unlimited")
	modelName := f.String("model", os.Getenv("OPENAI_MODEL"), "live model; alternatively OPENAI_MODEL")
	wait := f.Duration("wait", 0, "maximum CLI wait, default operation timeout plus 1 minute")
	if err := f.Parse(args); err != nil {
		return err
	}
	cfg := model.DefaultConfig(*mode)
	cfg.Endpoint = endpoint(*mode)
	cfg.Window = *window
	cfg.MaxBatchRequests = *maxBatch
	cfg.MaxBatchBytes = *maxBytes
	cfg.MaxAdmissions = *maxAdmissions
	if *mode == "live" {
		cfg.Model = *modelName
		if os.Getenv("OPENAI_API_KEY") == "" {
			return fmt.Errorf("live mode requires OPENAI_API_KEY")
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if *n == 0 {
		*n = 100
		if *mode == "live" {
			*n = 5
		}
	}
	reqs, err := documents(*id, *input, *n)
	if err != nil {
		return err
	}
	if cfg.MaxAdmissions > 0 && len(reqs) > cfg.MaxAdmissions {
		return fmt.Errorf("%d documents exceeds run admission limit %d", len(reqs), cfg.MaxAdmissions)
	}
	if *wait == 0 {
		*wait = cfg.OperationTimeout + time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()
	c, err := conn.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	db, err := openStore(ctx, conn.namespace)
	if err != nil {
		return err
	}
	defer db.Close()
	start := time.Now()
	// Run configuration must exist before ingress can authorize enrollment.
	// A new ID makes each demo's admission scope and cleanup ownership explicit.
	if err = db.CreateRun(ctx, model.RunInput{RunID: *id, Config: cfg}); err != nil {
		return fmt.Errorf("create run (choose a new --run-id for each demo): %w", err)
	}
	report := demoReport{RunID: *id, Mode: *mode, Simulated: *mode == "fake", Items: make([]itemReport, len(reqs))}
	var wg sync.WaitGroup
	// Limit simultaneous start RPCs, then release the slot before waiting for a
	// result. Holding it through Get would admit only 32 requests at a time and
	// could prevent large batches from filling while the provider is processing.
	sem := make(chan struct{}, 32)
	for i, r := range reqs {
		wg.Add(1)
		go func(i int, r model.Request) {
			defer wg.Done()
			sem <- struct{}{}
			wr, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: model.CallerID(r.RunID, r.RequestID), TaskQueue: model.CallerTaskQueue, WorkflowExecutionTimeout: cfg.OperationTimeout + time.Minute, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, orchestration.CallerWorkflow, model.CallerInput{Request: r, Config: cfg})
			<-sem
			item := itemReport{RequestID: r.RequestID, WorkflowID: model.CallerID(r.RunID, r.RequestID)}
			if e == nil {
				var result model.Result
				e = wr.Get(ctx, &result)
				if e == nil {
					item.Result = &result
					item.Status = "completed"
				}
			}
			if e != nil {
				recordItemError(&item, e)
			}
			report.Items[i] = item
		}(i, r)
	}
	wg.Wait()
	for _, item := range report.Items {
		if item.Status == "pending" {
			report.Pending++
		} else if item.Error != "" {
			report.Failed++
		} else {
			report.Succeeded++
		}
	}
	if ctx.Err() != nil || report.Pending > 0 {
		// The launcher deadline only stops observation. It does not cancel the
		// caller Workflows, their SAAs, or a submitted shared provider batch.
		statusCtx, statusCancel := context.WithTimeout(context.Background(), 3*time.Second)
		refreshReportStatus(statusCtx, db, &report)
		statusCancel()
		report.Elapsed = time.Since(start)
		_ = printJSON(report)
		return fmt.Errorf("CLI wait ended with %d unconfirmed outcome(s); Workflows continue durably; inspect run %s or cancel explicitly", report.Pending, *id)
	}
	// Closing admission also seals any remaining collection. It does not mean
	// all batch bookkeeping has finished, so read stored reports until terminal.
	if err = db.FinishRun(ctx, *id); err != nil {
		report.Elapsed = time.Since(start)
		_ = printJSON(report)
		return fmt.Errorf("close admission: %w", err)
	}
	for {
		refreshReportStatus(ctx, db, &report)
		if report.StatusError != "" || runComplete(report.Status) {
			break
		}
		select {
		case <-ctx.Done():
			report.StatusError = "launcher stopped waiting for the stored batch report"
		case <-time.After(100 * time.Millisecond):
			continue
		}
		break
	}
	report.Elapsed = time.Since(start)
	if err = printJSON(report); err != nil {
		return err
	}
	if report.StatusError != "" {
		return fmt.Errorf("stored report unavailable: %s", report.StatusError)
	}
	if report.Failed > 0 {
		return fmt.Errorf("%d request(s) failed; see per-request report", report.Failed)
	}
	return nil
}

func refreshReportStatus(ctx context.Context, db *store.Store, report *demoReport) {
	status, err := db.Status(ctx, report.RunID)
	if err != nil {
		report.StatusError = "could not read Postgres batch progress; inspect the run to retry"
		return
	}
	report.Status, report.StatusError = status, ""
}

func runComplete(status model.RunStatus) bool {
	if !status.Closed || status.Buffered != 0 {
		return false
	}
	for _, batch := range status.Batches {
		if batch.Status != "completed" && batch.Status != "completed_with_errors" && batch.Status != "workflow_failed" {
			return false
		}
	}
	return true
}

func recordItemError(item *itemReport, err error) {
	item.Status = "failed"
	item.Error = err.Error()
	// An RPC can exhaust its deadline just before the parent Context's timer
	// fires. Its outcome remains unconfirmed even when ctx.Err() is still nil.
	rpcCode := serviceerror.ToStatus(err).Code()
	if rpcCode == codes.DeadlineExceeded || rpcCode == codes.Canceled {
		item.Status = "pending"
		item.Error = "launcher stopped waiting; durable terminal outcome is unconfirmed"
	}
}

func documents(runID, path string, n int) ([]model.Request, error) {
	var out []model.Request
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 8192), model.MaxRequestBytes+1)
		for s.Scan() {
			if strings.TrimSpace(s.Text()) == "" {
				continue
			}
			var r model.Request
			if err = json.Unmarshal(s.Bytes(), &r); err != nil {
				return nil, fmt.Errorf("input line %d: %w", len(out)+1, err)
			}
			r.RunID = runID
			out = append(out, r)
		}
		if err = s.Err(); err != nil {
			return nil, err
		}
	} else {
		if n < 1 {
			return nil, fmt.Errorf("count must be positive")
		}
		for i := 0; i < n; i++ {
			out = append(out, model.Request{RunID: runID, RequestID: fmt.Sprintf("document-%04d", i+1), Input: fmt.Sprintf("Summarize in two sentences. Document %d: A small library expanded its opening hours after residents requested evening access. Volunteers staffed the additional sessions, and the library will review attendance next month.", i+1)})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no documents")
	}
	seen := map[string]bool{}
	for _, r := range out {
		if err := model.ValidateRequest(r); err != nil {
			return nil, err
		}
		if seen[r.RequestID] {
			return nil, fmt.Errorf("duplicate input request_id %q", r.RequestID)
		}
		seen[r.RequestID] = true
	}
	return out, nil
}

// control separates closing admission (finish), observing progress (inspect),
// canceling one consumer (cancel), and explicitly stopping a run (cleanup).
// Canceling a consumer does not automatically cancel a shared upstream batch.
func control(command string, args []string) error {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	var conn connection
	conn.flags(f)
	id := f.String("run-id", "", "demo run identifier")
	rid := f.String("request-id", "", "request identifier for cancel")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--run-id is required")
	}
	if err := model.ValidateRequest(model.Request{RunID: *id, RequestID: "validate", Input: "validate"}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var trackedBatches []string
	var trackedTargets []model.CompletionTarget
	if command == "inspect" || command == "finish" || command == "cleanup" {
		db, err := openStore(ctx, conn.namespace)
		if err != nil {
			return err
		}
		defer db.Close()
		if command == "finish" {
			if err = db.FinishRun(ctx, *id); err != nil {
				return err
			}
		}
		if command != "cleanup" {
			status, err := db.Status(ctx, *id)
			if err != nil {
				return err
			}
			return printJSON(status)
		}
		// Stop admission/submission before reading owned resources. The store's
		// submission fence orders this stop against a concurrent provider Create;
		// visibility listing alone could miss not-yet-visible Temporal resources.
		if err = db.StopRun(ctx, *id); err != nil {
			var failure *model.Failure
			if !errors.As(err, &failure) || failure.Code != "RUN_NOT_STARTED" {
				return err
			}
		} else {
			trackedBatches, trackedTargets, err = db.RunResources(ctx, *id)
			if err != nil {
				return err
			}
		}
	}
	c, err := conn.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	switch command {
	case "cleanup":
		return cleanup(ctx, c, *id, trackedBatches, trackedTargets)
	case "cancel":
		if *rid == "" {
			return fmt.Errorf("--request-id is required")
		}
		return c.CancelWorkflow(ctx, model.CallerID(*id, *rid), "")
	}
	return nil
}

// Select only exact prototype IDs. The extra local check keeps run "demo" from
// touching another run named "demo-other" when listing batch prefixes.
func ownedWorkflow(id, runID string) bool {
	if strings.HasPrefix(id, "caller/"+runID+"/") {
		return true
	}
	prefix := "batch/" + runID + "-"
	if !strings.HasPrefix(id, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(id, prefix)
	if len(suffix) < 4 {
		return false
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// cleanup uses stored exact identities first, then visibility as a supplemental
// search. It closes this run's Temporal resources; it cannot undo provider work
// already accepted, and it deliberately leaves upstream jobs/files in place.
func cleanup(ctx context.Context, c client.Client, runID string, trackedBatches []string, trackedTargets []model.CompletionTarget) error {
	q := fmt.Sprintf("ExecutionStatus = 'Running' AND (WorkflowId STARTS_WITH 'caller/%s/' OR WorkflowId STARTS_WITH 'batch/%s-')", runID, runID)
	var ids []string
	seenIDs := map[string]bool{}
	for _, batchID := range trackedBatches {
		id := model.BatchWorkflowID(batchID)
		ids = append(ids, id)
		seenIDs[id] = true
	}
	var cleanupErrors []error
	var token []byte
	for {
		page, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: q, PageSize: 1000, NextPageToken: token})
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("supplemental workflow visibility: %w", err))
			break
		}
		for _, e := range page.Executions {
			if ownedWorkflow(e.Execution.WorkflowId, runID) && !seenIDs[e.Execution.WorkflowId] {
				ids = append(ids, e.Execution.WorkflowId)
				seenIDs[e.Execution.WorkflowId] = true
			}
		}
		if len(page.NextPageToken) == 0 {
			break
		}
		token = page.NextPageToken
	}
	for _, id := range ids {
		if err := c.CancelWorkflow(ctx, id, ""); err != nil {
			var missing *serviceerror.NotFound
			if !errors.As(err, &missing) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("cancel %s: %w", id, err))
			}
		}
	}
	var terminated []string
	seenTargets := map[string]bool{}
	terminate := func(target model.CompletionTarget) {
		key := target.ActivityID + "/" + target.ActivityRunID
		if seenTargets[key] {
			return
		}
		seenTargets[key] = true
		h := c.GetActivityHandle(client.GetActivityHandleOptions{ActivityID: target.ActivityID, RunID: target.ActivityRunID})
		if err := h.Terminate(ctx, client.TerminateActivityOptions{Reason: "explicit prototype run cleanup"}); err != nil {
			var missing *serviceerror.NotFound
			if !errors.As(err, &missing) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("terminate %s: %w", target.ActivityID, err))
			}
			return
		}
		terminated = append(terminated, target.ActivityID)
	}
	for _, target := range trackedTargets {
		terminate(target)
	}
	activities, err := c.ListActivities(ctx, client.ListActivitiesOptions{Query: fmt.Sprintf("ActivityId STARTS_WITH 'request-activity/%s/'", runID)})
	if err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("supplemental activity visibility: %w", err))
	} else {
		for info, e := range activities.Results {
			if e != nil {
				cleanupErrors = append(cleanupErrors, e)
				break
			}
			if !strings.HasPrefix(info.ActivityID, "request-activity/"+runID+"/") {
				continue
			}
			if info.Status != enums.ACTIVITY_EXECUTION_STATUS_RUNNING && info.Status != enums.ACTIVITY_EXECUTION_STATUS_PAUSED {
				continue
			}
			terminate(model.CompletionTarget{ActivityID: info.ActivityID, ActivityRunID: info.ActivityRunID})
		}
	}
	if err = printJSON(map[string]any{"run_id": runID, "cancellation_requested": ids, "standalone_activities_terminated": terminated, "note": "Cleanup closes only this run's Temporal resources. Upstream OpenAI jobs and uploaded files are retained; shared provider jobs are never canceled automatically."}); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}
	return errors.Join(cleanupErrors...)
}
