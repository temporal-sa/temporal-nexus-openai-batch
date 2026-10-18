// Package model defines the values crossing the CLI, Nexus, PostgreSQL and
// provider boundaries. Keeping these values explicit makes the storage/history
// boundary visible: BatchRef and aggregate reports may enter BatchWorkflow;
// manifests, per-item outcomes and completion credentials remain in the store.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

const (
	Service          = "llm-batch"
	Operation        = "respond"
	CallerTaskQueue  = "nexus-batch-callers"
	ServiceTaskQueue = "nexus-batch-service"
	NexusEndpoint    = "llm-batch"
	StatusQuery      = "status"
	// Per-request envelopes are deliberately small even when the aggregate
	// batch is large. The provider hard limits apply to serialized JSONL bytes,
	// not the size of an in-memory manifest containing completion credentials.
	MaxRequestBytes      = 8 * 1024
	MaxResultBytes       = 4 * 1024
	HardMaxBatchRequests = 50_000
	HardMaxBatchBytes    = 200_000_000
	DefaultBatchBytes    = 512 * 1024
	EnrollmentHeartbeat  = 30 * time.Second
)

// Request is one logical unit of work. RequestID is unique within RunID and
// becomes the provider's custom_id, which correlates unordered output lines
// back to their original recipients.
type Request struct {
	RunID     string `json:"run_id"`
	RequestID string `json:"request_id"`
	Input     string `json:"input"`
}

type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Result is the normalized, bounded success returned through Nexus. Raw provider
// response envelopes remain behind the service boundary.
type Result struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	Usage     *Usage `json:"usage"`
}

// Failure separates a stable application code from an optional upstream code.
// It is persisted as an outcome and delivered as a structured Temporal failure.
type Failure struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	ProviderCode string `json:"provider_code,omitempty"`
}

func (f *Failure) Error() string { return f.Code + ": " + f.Message }

// Outcome binds a reconciled success or failure to the admitted logical request
// and its fingerprint. The store persists it before any completion RPC occurs.
type Outcome struct {
	RequestID   string   `json:"request_id"`
	Fingerprint string   `json:"fingerprint"`
	BatchID     string   `json:"batch_id"`
	Result      *Result  `json:"result,omitempty"`
	Failure     *Failure `json:"failure,omitempty"`
}

// Config is fixed when a run is created so enrollment, JSONL sizing and execution
// agree on the same model and limits. Go time.Duration values serialize as
// nanoseconds in JSON; the Python copy has its own configuration representation.
type Config struct {
	Model            string        `json:"model"`
	Mode             string        `json:"mode"`
	Window           time.Duration `json:"window"`
	MaxBatchRequests int           `json:"max_batch_requests"`
	MaxBatchBytes    int           `json:"max_batch_bytes"`
	MaxAdmissions    int           `json:"max_admissions"`
	PollInterval     time.Duration `json:"poll_interval"`
	// ResultDeadline budgets provider work after collection; OperationTimeout
	// also covers collection and leaves time to complete the original recipient.
	ResultDeadline   time.Duration `json:"result_deadline"`
	OperationTimeout time.Duration `json:"operation_timeout"`
	MaxOutputTokens  int           `json:"max_output_tokens"`
	Endpoint         string        `json:"nexus_endpoint"`
}

// DefaultConfig gives fake runs a short feedback loop and live runs a longer
// provider budget. Live mode requires the caller to supply a real model name.
func DefaultConfig(mode string) Config {
	c := Config{Model: "fake-model", Mode: mode, Window: 5 * time.Second, MaxBatchRequests: 100,
		MaxBatchBytes: DefaultBatchBytes, MaxAdmissions: 0, PollInterval: 250 * time.Millisecond,
		ResultDeadline: time.Minute, OperationTimeout: 2 * time.Minute, MaxOutputTokens: 128, Endpoint: NexusEndpoint}
	if mode == "live" {
		c.Model = ""
		c.PollInterval = 30 * time.Second
		c.ResultDeadline = 25 * time.Hour
		c.OperationTimeout = 26 * time.Hour
	}
	return c
}

// Validate checks relationships as well as individual limits. A collection
// window and provider wait must fit inside the recipient's operation timeout;
// otherwise a valid provider result could consistently arrive too late.
func (c Config) Validate() error {
	if c.Mode != "fake" && c.Mode != "live" {
		return fmt.Errorf("mode must be fake or live")
	}
	if c.Model == "" || c.Endpoint == "" {
		return fmt.Errorf("model and Nexus endpoint are required")
	}
	if c.Window <= 0 || c.PollInterval <= 0 || c.ResultDeadline <= c.Window || c.OperationTimeout <= c.ResultDeadline {
		return fmt.Errorf("durations must be positive, with window < result deadline < operation timeout")
	}
	// Subtract after the positive ordering check, so even maximum-duration
	// inputs cannot overflow while checking the complete collection budget.
	if c.Window >= c.OperationTimeout-c.ResultDeadline {
		return fmt.Errorf("collection window plus result deadline must be less than operation timeout")
	}
	if c.MaxBatchRequests < 1 || c.MaxBatchRequests > HardMaxBatchRequests || c.MaxBatchBytes < 1 || c.MaxBatchBytes > HardMaxBatchBytes || c.MaxAdmissions < 0 {
		return fmt.Errorf("batch limits must stay within 50,000 requests and 200 MB, and max admissions must be nonnegative (0 is unlimited)")
	}
	if c.MaxOutputTokens < 16 || c.MaxOutputTokens > 512 {
		return fmt.Errorf("max output tokens must be between 16 and 512")
	}
	return nil
}

// RunInput creates the external-store admission scope. The run is a PostgreSQL
// record, not a Workflow holding every incoming request in memory/history.
type RunInput struct {
	RunID  string `json:"run_id"`
	Config Config `json:"config"`
}

type CallerInput struct {
	Request Request `json:"request"`
	Config  Config  `json:"config"`
}

// Enrollment is the durable handoff from the SAA to the store. Its deadline is
// captured from the executing Activity rather than invented when a batch seals.
type Enrollment struct {
	Request     Request          `json:"request"`
	Target      CompletionTarget `json:"completion_target"`
	Fingerprint string           `json:"fingerprint"`
	DeadlineAt  time.Time        `json:"deadline_at"`
}

// CompletionTarget fences external completion to the exact standalone Activity
// execution and task attempt. TaskToken must never appear in user-facing logs.
// A retry can keep ActivityID/ActivityRunID while changing Attempt and TaskToken;
// retaining only an Activity ID would lose that distinction.
type CompletionTarget struct {
	Namespace     string `json:"namespace"`
	ActivityID    string `json:"activity_id"`
	ActivityRunID string `json:"activity_run_id"`
	TaskToken     []byte `json:"task_token"`
	Attempt       int32  `json:"attempt"`
}

// SAAInput carries one bounded request into the Nexus-backed standalone Activity.
// It does not contain other batch members or their completion targets.
type SAAInput struct {
	Request Request `json:"request"`
	Config  Config  `json:"config"`
}

// Admission confirms membership without returning the full manifest. Repeated
// enrollment of the same exact request/target returns the same membership.
type Admission struct {
	BatchID     string `json:"batch_id"`
	Fingerprint string `json:"fingerprint"`
	Config      Config `json:"config"`
}

// Entry couples immutable logical membership with its current completion target.
// A permitted SAA retry can refresh that target without adding another member.
type Entry struct {
	Request     Request          `json:"request"`
	Target      CompletionTarget `json:"completion_target"`
	Fingerprint string           `json:"fingerprint"`
	AdmittedAt  time.Time        `json:"admitted_at"`
}

// Manifest materializes one sealed batch for Activity-local work. It can be much
// larger than a Temporal payload because it contains every request and target;
// it must never be passed to BatchWorkflow as an argument or Activity result.
type Manifest struct {
	RunID    string    `json:"run_id"`
	BatchID  string    `json:"batch_id"`
	Config   Config    `json:"config"`
	Entries  []Entry   `json:"entries"`
	SealedAt time.Time `json:"sealed_at"`
	Bytes    int       `json:"bytes"`
}

// BatchRef is the entire orchestration payload. It contains no request bodies,
// results, completion targets, or task tokens; those remain in external storage.
type BatchRef struct {
	RunID           string    `json:"run_id"`
	BatchID         string    `json:"batch_id"`
	Config          Config    `json:"config"`
	Requests        int       `json:"requests"`
	Bytes           int       `json:"bytes"`
	SealedAt        time.Time `json:"sealed_at"`
	FirstAdmittedAt time.Time `json:"first_admitted_at"`
	// DeadlineAt is the earliest admitted recipient deadline in this batch.
	// Batch orchestration must honor it even if the dispatcher starts late.
	DeadlineAt time.Time `json:"deadline_at"`
}

// StoredDelivery is loaded and consumed within an Activity, never a Workflow.
type StoredDelivery struct {
	RequestID string           `json:"request_id"`
	Target    CompletionTarget `json:"completion_target"`
	Outcome   Outcome          `json:"outcome"`
}

// BatchDelivery is consumed by the completion keeper outside Workflow history.
type BatchDelivery struct {
	BatchID string `json:"batch_id"`
	StoredDelivery
}

// DeliverySummary separates business outcomes (Succeeded/Failed) from transport
// outcomes (Delivered/Skipped). A failed model request can be delivered correctly,
// while a successful model result may target a recipient that already timed out.
type DeliverySummary struct {
	Delivered     int            `json:"delivered"`
	Skipped       int            `json:"skipped"`
	Succeeded     int            `json:"succeeded"`
	Failed        int            `json:"failed"`
	FailureCounts map[string]int `json:"failure_counts,omitempty"`
	Diagnostics   []string       `json:"diagnostics,omitempty"`
}

// BatchReport is coarse progress safe to return to a Workflow or CLI. Provider
// status is not updated on every HTTP poll; phase transitions are persisted.
type BatchReport struct {
	BatchID          string         `json:"batch_id"`
	ProviderBatchID  string         `json:"provider_batch_id,omitempty"`
	InputFileID      string         `json:"input_file_id,omitempty"`
	Status           string         `json:"status"`
	ProviderStatus   string         `json:"provider_status,omitempty"`
	FailureCounts    map[string]int `json:"failure_counts,omitempty"`
	Requests         int            `json:"requests"`
	Bytes            int            `json:"bytes"`
	Succeeded        int            `json:"succeeded"`
	Failed           int            `json:"failed"`
	Delivered        int            `json:"delivered"`
	Skipped          int            `json:"skipped"`
	Diagnostics      []string       `json:"diagnostics,omitempty"`
	CollectionWait   time.Duration  `json:"collection_wait"`
	ProviderDuration time.Duration  `json:"provider_duration"`
	DeliveryDuration time.Duration  `json:"delivery_duration"`
}

type RunStatus struct {
	RunID          string        `json:"run_id"`
	Backing        string        `json:"backing"`
	Storage        string        `json:"storage"`
	CompletionMode string        `json:"completion_mode"`
	Admitted       int           `json:"admitted"`
	Buffered       int           `json:"buffered"`
	Closed         bool          `json:"closed"`
	Batches        []BatchReport `json:"batches"`
}

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$`)

// ValidateRequest bounds the full JSON envelope, including IDs and escaping,
// rather than only the number of characters in the document text.
func ValidateRequest(r Request) error {
	if !validID.MatchString(r.RunID) || !validID.MatchString(r.RequestID) {
		return &Failure{Code: "INVALID_INPUT", Message: "run_id and request_id must be 1-96 letters, digits, dots, underscores or hyphens"}
	}
	if r.Input == "" {
		return &Failure{Code: "INVALID_INPUT", Message: "input is required"}
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > MaxRequestBytes {
		return &Failure{Code: "INVALID_INPUT", Message: "request envelope exceeds 8 KiB"}
	}
	return nil
}

// Fingerprint detects conflicting content for the same admitted logical request.
// It is an ownership/integrity check, not a provider-side idempotency key.
func Fingerprint(r Request) string {
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func BatchWorkflowID(batchID string) string   { return "batch/" + batchID }
func CallerID(runID, requestID string) string { return "caller/" + runID + "/" + requestID }

// JSONLLine defines the current provider request contract: one POST to Responses
// per document, with RequestID as custom_id. It is shared by admission's byte
// accounting and final upload so limits use the bytes actually sent upstream.
func JSONLLine(r Request, c Config) ([]byte, error) {
	line := struct {
		CustomID string `json:"custom_id"`
		Method   string `json:"method"`
		URL      string `json:"url"`
		Body     any    `json:"body"`
	}{
		CustomID: r.RequestID, Method: "POST", URL: "/v1/responses", Body: struct {
			Model           string `json:"model"`
			Input           string `json:"input"`
			MaxOutputTokens int    `json:"max_output_tokens"`
			Store           bool   `json:"store"`
		}{c.Model, r.Input, c.MaxOutputTokens, false},
	}
	b, err := json.Marshal(line)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// JSONL builds an upload inside an Activity and checks the sealed batch bounds
// again. Request ordering is preserved here, but output reconciliation must use
// custom_id because the provider need not return results in the same order.
func JSONL(m Manifest) ([]byte, error) {
	var out []byte
	for _, e := range m.Entries {
		b, err := JSONLLine(e.Request, m.Config)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	if len(m.Entries) == 0 || len(m.Entries) > m.Config.MaxBatchRequests || len(out) > m.Config.MaxBatchBytes {
		return nil, fmt.Errorf("invalid sealed batch size")
	}
	return out, nil
}
