package provider

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
)

func testManifest(t *testing.T, n int) (model.Manifest, []byte) {
	t.Helper()
	m := model.Manifest{RunID: "test-run", BatchID: "test-batch", Config: model.DefaultConfig("fake")}
	for i := 0; i < n; i++ {
		r := model.Request{RunID: m.RunID, RequestID: string(rune('a' + i)), Input: "summarize document " + string(rune('a'+i))}
		m.Entries = append(m.Entries, model.Entry{Request: r, Fingerprint: model.Fingerprint(r)})
	}
	data, err := model.JSONL(m)
	if err != nil {
		t.Fatal(err)
	}
	return m, data
}

func TestFakeScenariosReconcileEveryManifestEntry(t *testing.T) {
	for _, test := range []struct {
		scenario string
		failed   int
		diag     bool
	}{
		{"success", 0, false}, {"mixed", 1, false}, {"expired", 2, false},
		{"missing", 1, true}, {"duplicate", 1, true}, {"unknown", 0, true}, {"malformed", 1, true},
	} {
		t.Run(test.scenario, func(t *testing.T) {
			ctx := context.Background()
			p, err := NewFake(FakeConfig{StateDir: t.TempDir(), Scenario: test.scenario})
			if err != nil {
				t.Fatal(err)
			}
			manifest, input := testManifest(t, 3)
			id, err := p.Upload(ctx, manifest.BatchID, input)
			if err != nil {
				t.Fatal(err)
			}
			job, err := p.Create(ctx, id, map[string]string{"logical_batch": manifest.BatchID})
			if err != nil {
				t.Fatal(err)
			}
			job, err = p.Retrieve(ctx, job.ID)
			if err != nil || !job.Terminal() {
				t.Fatalf("job = %+v, err = %v", job, err)
			}
			output, errorFile, err := p.Results(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, diagnostics := Reconcile(manifest, job, output, errorFile)
			if len(outcomes) != 3 {
				t.Fatalf("got %d outcomes", len(outcomes))
			}
			failed := 0
			for i, outcome := range outcomes {
				if outcome.RequestID != manifest.Entries[i].Request.RequestID || outcome.Fingerprint != manifest.Entries[i].Fingerprint || outcome.BatchID != manifest.BatchID {
					t.Fatalf("correlation lost: %+v", outcome)
				}
				if (outcome.Failure == nil) == (outcome.Result == nil) {
					t.Fatalf("outcome is not exclusively success or failure: %+v", outcome)
				}
				if outcome.Failure != nil {
					failed++
				}
			}
			if failed != test.failed {
				t.Fatalf("got %d failed, want %d: %+v", failed, test.failed, outcomes)
			}
			if (len(diagnostics) > 0) != test.diag {
				t.Fatalf("unexpected diagnostics: %v", diagnostics)
			}
			if test.scenario == "success" && !strings.Contains(string(output[:min(len(output), 100)]), `"custom_id":"c"`) {
				t.Fatalf("fake output did not shuffle IDs: %s", output)
			}
		})
	}
}

func TestFakePersistsAmbiguousSubmissionAndProcessingDeadline(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p, err := NewFake(FakeConfig{StateDir: dir, Scenario: "submission-unknown", ProcessingDelay: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	manifest, input := testManifest(t, 2)
	id, err := p.Upload(ctx, manifest.BatchID, input)
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{"logical_batch_id": manifest.BatchID, "manifest_hash": "abc123"}
	_, err = p.Create(ctx, id, metadata)
	if !IsAmbiguous(err) || IsPermanent(err) {
		t.Fatalf("expected ambiguous submission, got %v", err)
	}
	metadata["manifest_hash"] = "mutated"
	// A fresh adapter models a restarted worker and sees the original durable job.
	restarted, err := NewFake(FakeConfig{StateDir: dir, Scenario: "success"})
	if err != nil {
		t.Fatal(err)
	}
	found, err := restarted.Find(ctx, map[string]string{"logical_batch_id": manifest.BatchID, "manifest_hash": "abc123"})
	if err != nil || len(found) != 1 {
		t.Fatalf("found = %v, err = %v", found, err)
	}
	if found[0].Terminal() {
		t.Fatalf("job completed before fake deadline: %+v", found[0])
	}
	if _, _, err := restarted.Results(ctx, found[0]); err == nil {
		t.Fatal("results returned before processing finished")
	}
	time.Sleep(50 * time.Millisecond)
	job, err := restarted.Retrieve(ctx, found[0].ID)
	if err != nil || !job.Terminal() {
		t.Fatalf("job = %+v, err = %v", job, err)
	}
	output, errors, err := restarted.Results(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, _ := Reconcile(manifest, job, output, errors)
	if outcomes[0].Result == nil || outcomes[1].Result == nil {
		t.Fatalf("restarted job lost results: %v", outcomes)
	}
}

func TestFakeUnreconciledSubmissionDoesNotAppearInFind(t *testing.T) {
	p, err := NewFake(FakeConfig{StateDir: t.TempDir(), Scenario: "submission-unreconciled"})
	if err != nil {
		t.Fatal(err)
	}
	m, data := testManifest(t, 1)
	id, err := p.Upload(context.Background(), m.BatchID, data)
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{"batch": m.BatchID}
	if _, err := p.Create(context.Background(), id, metadata); !IsAmbiguous(err) {
		t.Fatalf("expected ambiguity, got %v", err)
	}
	found, err := p.Find(context.Background(), metadata)
	if err != nil || len(found) != 0 {
		t.Fatalf("found = %v, err = %v", found, err)
	}
}

func TestFakeMultipleInstancesDoNotLoseConcurrentSubmissions(t *testing.T) {
	const count = 12
	dir := t.TempDir()
	_, data := testManifest(t, 1)
	var wg sync.WaitGroup
	errors := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := NewFake(FakeConfig{StateDir: dir})
			if err == nil {
				var id string
				id, err = p.Upload(context.Background(), "batch", data)
				if err == nil {
					_, err = p.Create(context.Background(), id, map[string]string{"batch": "shared"})
				}
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	p, _ := NewFake(FakeConfig{StateDir: dir})
	found, err := p.Find(context.Background(), map[string]string{"batch": "shared"})
	if err != nil || len(found) != count {
		t.Fatalf("got %d jobs, want %d, err %v", len(found), count, err)
	}
}

func TestReconcileErrorsAndBounds(t *testing.T) {
	manifest, _ := testManifest(t, 5)
	good := func(id, text string) string {
		data, _ := json.Marshal(map[string]any{"custom_id": id, "response": map[string]any{"status_code": 200, "body": map[string]any{"status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]string{"type": "output_text", "text": text}}}}}}})
		return string(data)
	}
	output := strings.Join([]string{
		good("b", "second request"),
		good("a", "first request"),
		good("c", strings.Repeat("x", model.MaxResultBytes)),
		`{"custom_id":"d","response":{"status_code":200,"body":{"status":"incomplete"}}}`,
		`{broken`,
		`{"custom_id":"unknown","response":null,"error":{"code":"test"}}`,
	}, "\n")
	errorFile := `{"custom_id":"a","response":null,"error":{"code":"batch_expired","message":"error must override ambiguous duplicate"}}`
	outcomes, diagnostics := Reconcile(manifest, Job{Status: "expired"}, []byte(output), []byte(errorFile))
	want := []string{"DUPLICATE_RESULT", "", "RESULT_TOO_LARGE", "INCOMPLETE_RESULT", "BATCH_EXPIRED"}
	for i, outcome := range outcomes {
		if want[i] == "" {
			if outcome.Result == nil || outcome.Result.Text != "second request" {
				t.Fatalf("wrong correlated text: %+v", outcome)
			}
		} else if outcome.Failure == nil || outcome.Failure.Code != want[i] {
			t.Fatalf("request %d = %+v, want %s", i, outcome, want[i])
		}
	}
	if len(diagnostics) != 4 {
		t.Fatalf("got diagnostics %v", diagnostics)
	}
}

func TestReconcileHugeLineAndDiagnosticLimit(t *testing.T) {
	manifest, _ := testManifest(t, 1)
	input := strings.Repeat("x", int(maxControlBytes)+1) + "\n" + strings.Repeat("{bad\n", 200)
	outcomes, diagnostics := Reconcile(manifest, Job{Status: strings.Repeat("secret", 5000)}, []byte(input), nil)
	if len(outcomes) != 1 || outcomes[0].Failure.Code != "MISSING_RESULT" {
		t.Fatalf("missing terminal outcome: %+v", outcomes)
	}
	if len(diagnostics) != 101 || diagnostics[100] != "additional diagnostics omitted" {
		t.Fatalf("unbounded diagnostics: %d", len(diagnostics))
	}
	if len(outcomes[0].Failure.Message) > 200 {
		t.Fatal("unbounded failure message")
	}
}

func TestReconcilePreservesOnlyBoundedProviderMachineCodes(t *testing.T) {
	manifest, _ := testManifest(t, 1)
	for _, test := range []struct {
		name, code, want string
		status           int
		bodyError        bool
	}{
		{"item error", "rate_limit_exceeded", "rate_limit_exceeded", 0, false},
		{"expired item", "batch_expired", "batch_expired", 0, false},
		{"body HTTP error", "invalid_prompt", "invalid_prompt", 400, true},
		{"body response error", "server_error", "server_error", 200, true},
		{"maximum code", strings.Repeat("x", 64), strings.Repeat("x", 64), 400, true},
		{"overlong code", strings.Repeat("x", 65), "", 0, false},
		{"control character", "rate_limit\nsecret", "", 0, false},
		{"message as code", "failed for confidential document", "", 400, true},
		{"non-ASCII code", "error_é", "", 200, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			providerError := map[string]string{"code": test.code, "message": "confidential-provider-message"}
			record := map[string]any{"custom_id": "a"}
			if test.bodyError {
				record["response"] = map[string]any{"status_code": test.status, "body": map[string]any{"status": "failed", "error": providerError}}
			} else {
				record["error"] = providerError
			}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, _ := Reconcile(manifest, Job{Status: "completed"}, data, nil)
			failure := outcomes[0].Failure
			if failure == nil || failure.ProviderCode != test.want {
				t.Fatalf("got failure %+v, want provider code %q", failure, test.want)
			}
			if test.code == "batch_expired" && failure.Code != "BATCH_EXPIRED" {
				t.Fatalf("lost typed expiration: %+v", failure)
			}
			if strings.Contains(failure.Message, "confidential-provider-message") || strings.Contains(failure.Message, "\n") {
				t.Fatalf("unsafe provider content in failure: %+v", failure)
			}
			if test.want != "" && !strings.Contains(failure.Message, test.want) {
				t.Fatalf("provider code is not visible in message: %+v", failure)
			}
		})
	}
}

func TestReconcileTerminalBatchesTypeMissingRequestsAndRetainPartialSuccess(t *testing.T) {
	manifest, _ := testManifest(t, 2)
	partial := []byte(`{"custom_id":"a","response":{"status_code":200,"body":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"completed before terminal batch state"}]}]}}}`)
	for _, test := range []struct {
		status, wantCode string
	}{
		{"failed", "BATCH_FAILED"},
		{"cancelled", "BATCH_CANCELLED"},
		{"expired", "BATCH_EXPIRED"},
		{"completed", "MISSING_RESULT"},
	} {
		t.Run(test.status, func(t *testing.T) {
			empty, diagnostics := Reconcile(manifest, Job{Status: test.status}, nil, nil)
			if len(empty) != 2 || len(diagnostics) != 2 {
				t.Fatalf("empty terminal batch lost outcomes or missing diagnostics: %+v, %v", empty, diagnostics)
			}
			for _, outcome := range empty {
				if outcome.Result != nil || outcome.Failure == nil || outcome.Failure.Code != test.wantCode {
					t.Fatalf("terminal missing outcome = %+v, want %s", outcome, test.wantCode)
				}
			}
			outcomes, diagnostics := Reconcile(manifest, Job{Status: test.status}, partial, nil)
			if len(outcomes) != 2 || outcomes[0].Failure != nil || outcomes[0].Result == nil || outcomes[0].Result.Text != "completed before terminal batch state" {
				t.Fatalf("validated partial success lost: %+v", outcomes)
			}
			if outcomes[1].Result != nil || outcomes[1].Failure == nil || outcomes[1].Failure.Code != test.wantCode || len(diagnostics) != 1 {
				t.Fatalf("partial terminal missing outcome = %+v, diagnostics %v; want %s", outcomes[1], diagnostics, test.wantCode)
			}
		})
	}
}
