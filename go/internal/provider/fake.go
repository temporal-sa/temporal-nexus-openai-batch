package provider

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FakeConfig controls local provider behavior without replacing the real
// PostgreSQL admission, Temporal execution, or Nexus callback path.
type FakeConfig struct {
	StateDir        string
	Scenario        string
	ProcessingDelay time.Duration
}

// Fake replaces only the OpenAI provider, not Temporal, Nexus, or workflows.
// State and files are persisted behind a process-shared advisory lock so that
// killing and restarting a worker preserves submitted jobs.
type Fake struct {
	dir      string
	scenario string
	delay    time.Duration
	mu       sync.Mutex
}

type fakeRecord struct {
	Job      Job       `json:"job"`
	ReadyAt  time.Time `json:"ready_at"`
	Terminal string    `json:"terminal"`
	Hidden   bool      `json:"hidden,omitempty"`
}

type fakeState struct {
	Jobs map[string]fakeRecord `json:"jobs"`
}

func NewFake(cfg FakeConfig) (*Fake, error) {
	if cfg.StateDir == "" {
		cfg.StateDir = ".state/fake-provider"
	}
	if cfg.Scenario == "" {
		cfg.Scenario = "success"
	}
	if cfg.Scenario == "unreconciled-unknown" {
		cfg.Scenario = "submission-unreconciled"
	}
	switch cfg.Scenario {
	case "success", "mixed", "expired", "missing", "duplicate", "unknown", "malformed", "submission-unknown", "submission-unreconciled":
	default:
		return nil, fmt.Errorf("unknown fake provider scenario %q", cfg.Scenario)
	}
	if cfg.ProcessingDelay < 0 {
		return nil, fmt.Errorf("fake processing delay cannot be negative")
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, "files"), 0700); err != nil {
		return nil, err
	}
	return &Fake{dir: cfg.StateDir, scenario: cfg.Scenario, delay: cfg.ProcessingDelay}, nil
}

func (p *Fake) Upload(ctx context.Context, logicalBatchID string, data []byte) (id string, err error) {
	if len(data) == 0 || int64(len(data)) > defaultMaxFileBytes {
		return "", &Error{Op: "upload", Code: "input_size", Message: "input file is empty or exceeds configured size limit", Permanent: true}
	}
	// Content plus logical batch identity makes repeated uploads stable while
	// keeping identical documents in different batches independently addressable.
	hash := sha256.New()
	hash.Write([]byte(logicalBatchID))
	hash.Write([]byte{0})
	hash.Write(data)
	id = "file-input-" + hex.EncodeToString(hash.Sum(nil))[:32]
	err = p.locked(ctx, func(*fakeState) error { return p.writeFile(id, data) })
	return id, err
}

// Create persists a new, randomly identified job on every call, just as a blind
// resubmission could create duplicate paid work with the live provider. Recovery
// must find the original metadata match instead of relying on fake deduplication.
func (p *Fake) Create(ctx context.Context, fileID string, metadata map[string]string) (job Job, err error) {
	err = p.locked(ctx, func(state *fakeState) error {
		data, err := p.readFile(fileID)
		if err != nil {
			return err
		}
		output, errorFile, err := fakeResults(data, p.scenario)
		if err != nil {
			return err
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		job = Job{ID: "batch-fake-" + hex.EncodeToString(random[:]), Status: "in_progress", InputFileID: fileID, Metadata: copyMetadata(metadata)}
		if len(output) > 0 {
			job.OutputFileID = "file-output-" + job.ID
			if err := p.writeFile(job.OutputFileID, output); err != nil {
				return err
			}
		}
		if len(errorFile) > 0 {
			job.ErrorFileID = "file-error-" + job.ID
			if err := p.writeFile(job.ErrorFileID, errorFile); err != nil {
				return err
			}
		}
		terminal := "completed"
		if p.scenario == "expired" {
			terminal = "expired"
		}
		state.Jobs[job.ID] = fakeRecord{Job: job, ReadyAt: time.Now().Add(p.delay), Terminal: terminal, Hidden: p.scenario == "submission-unreconciled"}
		return nil
	})
	if err != nil {
		return Job{}, err
	}
	if p.scenario == "submission-unknown" || p.scenario == "submission-unreconciled" {
		// Persist first, then lose the acknowledgment. The unreconciled variant
		// also hides the job from Find, exercising the no-blind-resubmit rule.
		return Job{}, &Error{Op: "create", Code: "submission_unknown", Message: "simulated connection loss after submission", Ambiguous: true}
	}
	return job, nil
}

func (p *Fake) Retrieve(ctx context.Context, batchID string) (job Job, err error) {
	err = p.locked(ctx, func(state *fakeState) error {
		record, exists := state.Jobs[batchID]
		if !exists {
			return &Error{Op: "retrieve", Code: "not_found", Message: "fake batch does not exist", Permanent: true, Status: 404}
		}
		job = currentFakeJob(record)
		return nil
	})
	return job, err
}

func (p *Fake) Find(ctx context.Context, metadata map[string]string) (jobs []Job, err error) {
	err = p.locked(ctx, func(state *fakeState) error {
		for _, record := range state.Jobs {
			if !record.Hidden && metadataMatches(record.Job.Metadata, metadata) {
				jobs = append(jobs, currentFakeJob(record))
			}
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
		return nil
	})
	return jobs, err
}

func (p *Fake) Results(ctx context.Context, job Job) (output, errorFile []byte, err error) {
	err = p.locked(ctx, func(state *fakeState) error {
		record, exists := state.Jobs[job.ID]
		if !exists {
			return &Error{Op: "results", Code: "not_found", Message: "fake batch does not exist", Permanent: true, Status: 404}
		}
		job = currentFakeJob(record)
		if !job.Terminal() {
			return &Error{Op: "results", Code: "not_ready", Message: "fake batch is still processing"}
		}
		if job.OutputFileID != "" {
			output, err = p.readFile(job.OutputFileID)
			if err != nil {
				return err
			}
		}
		if job.ErrorFileID != "" {
			errorFile, err = p.readFile(job.ErrorFileID)
		}
		return err
	})
	return output, errorFile, err
}

// currentFakeJob derives progress from persisted wall-clock time, so completing
// a processing delay does not require the original worker process to stay alive.
func currentFakeJob(record fakeRecord) Job {
	job := record.Job
	job.Metadata = copyMetadata(job.Metadata)
	if !time.Now().Before(record.ReadyAt) {
		job.Status = record.Terminal
	} else {
		job.OutputFileID, job.ErrorFileID = "", ""
	}
	return job
}

// locked serializes the complete read/modify/write cycle. The mutex coordinates
// goroutines sharing this instance; flock coordinates separate worker processes
// sharing StateDir. Loading state only after both locks prevents stale writers
// from overwriting each other's submitted jobs during restart tests.
func (p *Fake) locked(ctx context.Context, fn func(*fakeState) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	lock, err := os.OpenFile(filepath.Join(p.dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Nonblocking flock plus a short timer lets cancellation interrupt lock
	// contention instead of leaving a worker blocked in a system call.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	state := fakeState{Jobs: map[string]fakeRecord{}}
	data, err := os.ReadFile(filepath.Join(p.dir, "state.json"))
	if err == nil {
		if err := json.Unmarshal(data, &state); err != nil || state.Jobs == nil {
			return fmt.Errorf("invalid persisted fake provider state")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := fn(&state); err != nil {
		return err
	}
	data, err = json.Marshal(state)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(p.dir, "state.json"), data)
}

func (p *Fake) readFile(id string) ([]byte, error) {
	if id == "" || strings.ContainsAny(id, "/\\") || id == "." || id == ".." {
		return nil, &Error{Op: "file", Code: "invalid_file_id", Message: "invalid fake file ID", Permanent: true}
	}
	data, err := os.ReadFile(filepath.Join(p.dir, "files", id))
	if os.IsNotExist(err) {
		return nil, &Error{Op: "file", Code: "not_found", Message: "fake input or result file does not exist", Permanent: true, Status: 404}
	}
	return data, err
}

func (p *Fake) writeFile(id string, data []byte) error {
	return atomicWrite(filepath.Join(p.dir, "files", id), data)
}

// atomicWrite publishes a complete file via a same-directory rename. Readers
// observe the old or new file rather than a partially written JSON document.
// Sync flushes the temporary file before it becomes the visible destination.
func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// fakeResults produces the same JSONL envelope shape consumed by Reconcile.
// Scenarios deliberately include missing, duplicate, unknown, and malformed
// records, ensuring the manifest remains the source of recipient identity.
func fakeResults(input []byte, scenario string) ([]byte, []byte, error) {
	var outputs, errors [][]byte
	var ids []string
	for _, line := range bytes.Split(input, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var request struct {
			CustomID string `json:"custom_id"`
			Body     struct {
				Input string `json:"input"`
			} `json:"body"`
		}
		if err := json.Unmarshal(line, &request); err != nil || request.CustomID == "" {
			return nil, nil, &Error{Op: "create", Code: "invalid_jsonl", Message: "fake provider input is invalid", Permanent: true}
		}
		index := len(ids)
		ids = append(ids, request.CustomID)
		if scenario == "missing" && index == 0 {
			continue
		}
		if (scenario == "mixed" && index%3 == 1) || (scenario == "expired" && index%2 == 0) {
			code := "simulated_request_error"
			if scenario == "expired" {
				code = "batch_expired"
			}
			line, _ := json.Marshal(map[string]any{"custom_id": request.CustomID, "response": nil, "error": map[string]any{"code": code, "message": "simulated request failure"}})
			errors = append(errors, line)
			continue
		}
		text := request.Body.Input
		if len(text) > 256 {
			text = string([]rune(text)[:min(128, len([]rune(text)))])
		}
		line, _ := json.Marshal(map[string]any{"custom_id": request.CustomID, "error": nil, "response": map[string]any{"status_code": 200, "request_id": "req-fake-" + request.CustomID, "body": map[string]any{"id": "resp-fake-" + request.CustomID, "status": "completed", "object": "response", "output": []any{map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "Fake response: " + text}}}}, "usage": map[string]int{"input_tokens": 12, "output_tokens": 20}}}})
		outputs = append(outputs, line)
	}
	if len(ids) == 0 {
		return nil, nil, &Error{Op: "create", Code: "empty_batch", Message: "fake provider input is empty", Permanent: true}
	}
	if scenario == "duplicate" && len(outputs) > 0 {
		outputs = append(outputs, bytes.Clone(outputs[0]))
	}
	if scenario == "unknown" {
		line, _ := json.Marshal(map[string]any{"custom_id": "unknown-provider-request", "response": nil, "error": map[string]string{"code": "unknown", "message": "not in manifest"}})
		outputs = append(outputs, line)
	}
	if scenario == "malformed" && len(outputs) > 0 {
		outputs[0], _ = json.Marshal(map[string]any{"custom_id": ids[0], "response": map[string]any{"status_code": 200, "body": map[string]string{"status": "completed"}}})
		outputs = append(outputs, []byte("{malformed JSON"))
	}
	// Reverse ordering deliberately prevents positional result correlation.
	for i, j := 0, len(outputs)-1; i < j; i, j = i+1, j-1 {
		outputs[i], outputs[j] = outputs[j], outputs[i]
	}
	join := func(lines [][]byte) []byte {
		if len(lines) == 0 {
			return nil
		}
		return append(bytes.Join(lines, []byte{'\n'}), '\n')
	}
	return join(outputs), join(errors), nil
}
