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
	Service              = "llm-batch"
	Operation            = "respond"
	CallerTaskQueue      = "nexus-batch-callers"
	ServiceTaskQueue     = "nexus-batch-service"
	NexusEndpoint        = "llm-batch"
	StatusQuery          = "status"
	MaxRequestBytes      = 8 * 1024
	MaxResultBytes       = 4 * 1024
	HardMaxBatchRequests = 50_000
	HardMaxBatchBytes    = 200_000_000
	DefaultBatchBytes    = 512 * 1024
	EnrollmentHeartbeat  = 30 * time.Second
)

type Request struct {
	RunID     string `json:"run_id"`
	RequestID string `json:"request_id"`
	Input     string `json:"input"`
}

type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type Result struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	Usage     *Usage `json:"usage"`
}

type Failure struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	ProviderCode string `json:"provider_code,omitempty"`
}

func (f *Failure) Error() string { return f.Code + ": " + f.Message }

type Outcome struct {
	RequestID   string   `json:"request_id"`
	Fingerprint string   `json:"fingerprint"`
	BatchID     string   `json:"batch_id"`
	Result      *Result  `json:"result,omitempty"`
	Failure     *Failure `json:"failure,omitempty"`
}

type Config struct {
	Model            string        `json:"model"`
	Mode             string        `json:"mode"`
	Window           time.Duration `json:"window"`
	MaxBatchRequests int           `json:"max_batch_requests"`
	MaxBatchBytes    int           `json:"max_batch_bytes"`
	MaxAdmissions    int           `json:"max_admissions"`
	PollInterval     time.Duration `json:"poll_interval"`
	ResultDeadline   time.Duration `json:"result_deadline"`
	OperationTimeout time.Duration `json:"operation_timeout"`
	MaxOutputTokens  int           `json:"max_output_tokens"`
	Endpoint         string        `json:"nexus_endpoint"`
}

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

type RunInput struct {
	RunID  string `json:"run_id"`
	Config Config `json:"config"`
}

type CallerInput struct {
	Request Request `json:"request"`
	Config  Config  `json:"config"`
}

type Enrollment struct {
	Request     Request          `json:"request"`
	Target      CompletionTarget `json:"completion_target"`
	Fingerprint string           `json:"fingerprint"`
	DeadlineAt  time.Time        `json:"deadline_at"`
}

// CompletionTarget fences external completion to the exact standalone Activity
// execution and task attempt. TaskToken must never appear in user-facing logs.
type CompletionTarget struct {
	Namespace     string `json:"namespace"`
	ActivityID    string `json:"activity_id"`
	ActivityRunID string `json:"activity_run_id"`
	TaskToken     []byte `json:"task_token"`
	Attempt       int32  `json:"attempt"`
}

type SAAInput struct {
	Request Request `json:"request"`
	Config  Config  `json:"config"`
}

type Admission struct {
	BatchID     string `json:"batch_id"`
	Fingerprint string `json:"fingerprint"`
	Config      Config `json:"config"`
}

type Entry struct {
	Request     Request          `json:"request"`
	Target      CompletionTarget `json:"completion_target"`
	Fingerprint string           `json:"fingerprint"`
	AdmittedAt  time.Time        `json:"admitted_at"`
}

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
	DeadlineAt      time.Time `json:"deadline_at"`
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

type DeliverySummary struct {
	Delivered     int            `json:"delivered"`
	Skipped       int            `json:"skipped"`
	Succeeded     int            `json:"succeeded"`
	Failed        int            `json:"failed"`
	FailureCounts map[string]int `json:"failure_counts,omitempty"`
	Diagnostics   []string       `json:"diagnostics,omitempty"`
}

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

func Fingerprint(r Request) string {
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func BatchWorkflowID(batchID string) string   { return "batch/" + batchID }
func CallerID(runID, requestID string) string { return "caller/" + runID + "/" + requestID }

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
