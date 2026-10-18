package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRequestAndConfigBounds(t *testing.T) {
	r := Request{RunID: "test-run", RequestID: "document-1", Input: "short document"}
	if err := ValidateRequest(r); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Request){func(r *Request) { r.RunID = "../escape" }, func(r *Request) { r.RequestID = "" }, func(r *Request) { r.Input = "" }, func(r *Request) { r.Input = strings.Repeat("x", MaxRequestBytes) }} {
		x := r
		mutate(&x)
		if err := ValidateRequest(x); err == nil {
			t.Fatalf("accepted invalid request: id=%q bytes=%d", x.RequestID, len(x.Input))
		}
	}
	c := DefaultConfig("fake")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.MaxBatchRequests = HardMaxBatchRequests + 1 }, func(c *Config) { c.MaxAdmissions = -1 }, func(c *Config) { c.MaxBatchBytes = HardMaxBatchBytes + 1 }, func(c *Config) { c.OperationTimeout = c.ResultDeadline }, func(c *Config) { c.PollInterval = 0 }} {
		x := c
		mutate(&x)
		if err := x.Validate(); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}

func TestOperationTimeoutIncludesCollectionAndProviderBudget(t *testing.T) {
	c := DefaultConfig("live")
	c.Model = "test-model"
	c.Window = 24 * time.Hour
	if err := c.Validate(); err == nil {
		t.Fatal("accepted 49-hour collection/provider budget behind a 26-hour Activity timeout")
	}
	c = DefaultConfig("fake")
	c.OperationTimeout = c.Window + c.ResultDeadline
	if err := c.Validate(); err == nil {
		t.Fatal("accepted exact budget with no completion margin")
	}
	c.OperationTimeout++
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	// Addition would overflow int64; subtraction must still reject the budget.
	c.Window = time.Duration(4_000_000_000_000_000_000)
	c.ResultDeadline = time.Duration(6_000_000_000_000_000_000)
	c.OperationTimeout = time.Duration(9_000_000_000_000_000_000)
	if err := c.Validate(); err == nil {
		t.Fatal("overflowing combined budget was accepted")
	}
}

func TestJSONLCountsEncodedBytesAndPreservesInput(t *testing.T) {
	c := DefaultConfig("fake")
	r := Request{RunID: "run", RequestID: "doc", Input: "Line one\n\"Quoted text\" and 日本語"}
	line, err := JSONLLine(r, c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(line), "\n") != 1 {
		t.Fatal("input newline escaped incorrectly")
	}
	var decoded struct {
		CustomID string `json:"custom_id"`
		Method   string `json:"method"`
		URL      string `json:"url"`
		Body     struct {
			Input string `json:"input"`
			Model string `json:"model"`
		} `json:"body"`
	}
	if err = json.Unmarshal(line, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Body.Input != r.Input || decoded.CustomID != r.RequestID || decoded.URL != "/v1/responses" || decoded.Method != "POST" || decoded.Body.Model != c.Model {
		t.Fatal("wrong provider contract")
	}
	m := Manifest{Config: c, Entries: []Entry{{Request: r}}}
	m.Config.MaxBatchBytes = len(line)
	if _, err = JSONL(m); err != nil {
		t.Fatal("exact byte boundary rejected", err)
	}
	m.Config.MaxBatchBytes--
	if _, err = JSONL(m); err == nil {
		t.Fatal("over-limit serialized JSONL accepted")
	}
	if Fingerprint(r) != Fingerprint(r) {
		t.Fatal("unstable fingerprint")
	}
	r.Input += "."
	if Fingerprint(r) == Fingerprint(m.Entries[0].Request) {
		t.Fatal("changed input shares fingerprint")
	}
}

func TestExternalStoreLimitsAllowLargeRunsAndBatches(t *testing.T) {
	c := DefaultConfig("fake")
	if c.MaxAdmissions != 0 {
		t.Fatal("default admission must be unlimited")
	}
	c.MaxAdmissions = 25_000
	c.MaxBatchRequests = 50_000
	c.MaxBatchBytes = 200_000_000
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	ref := BatchRef{RunID: "run", BatchID: "run-0001", Config: c, Requests: 20_000, Bytes: 42_000_000}
	data, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 2048 {
		t.Fatalf("batch reference unexpectedly large: %d", len(data))
	}
}
