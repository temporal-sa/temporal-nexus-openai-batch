package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOpenAIHTTPContractAndPaginatedRecovery(t *testing.T) {
	var creates, pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Errorf("missing auth header")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/files":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if r.FormValue("purpose") != "batch" {
				t.Errorf("wrong upload purpose %q", r.FormValue("purpose"))
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			if string(data) != "{input}\n" || header.Filename != "batch.jsonl" {
				t.Errorf("wrong multipart input")
			}
			fmt.Fprint(w, `{"id":"file-input"}`)
		case "POST /v1/batches":
			creates.Add(1)
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["endpoint"] != "/v1/responses" || body["completion_window"] != "24h" || body["input_file_id"] != "file-input" {
				t.Errorf("wrong batch payload: %v", body)
			}
			if body["metadata"].(map[string]any)["batch"] != "logical-1" {
				t.Errorf("missing correlation metadata")
			}
			fmt.Fprint(w, `{"id":"batch-provider","status":"validating","input_file_id":"file-input","metadata":{"batch":"logical-1"}}`)
		case "GET /v1/batches/batch-provider":
			fmt.Fprint(w, `{"id":"batch-provider","status":"completed","output_file_id":"file-output","error_file_id":"file-error"}`)
		case "GET /v1/batches":
			pages.Add(1)
			if r.URL.Query().Get("limit") != "100" {
				t.Error("list page was not bounded at 100")
			}
			if r.URL.Query().Get("after") == "" {
				fmt.Fprint(w, `{"data":[{"id":"unrelated","status":"completed","metadata":{"batch":"other"}}],"has_more":true,"last_id":"unrelated"}`)
			} else if r.URL.Query().Get("after") == "unrelated" {
				fmt.Fprint(w, `{"data":[{"id":"batch-provider","status":"completed","metadata":{"batch":"logical-1"}}],"has_more":false}`)
			} else {
				t.Error("wrong recovery cursor")
				w.WriteHeader(400)
			}
		case "GET /v1/files/file-output/content":
			fmt.Fprint(w, "{output}\n")
		case "GET /v1/files/file-error/content":
			fmt.Fprint(w, "{error}\n")
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	p, err := NewOpenAI(OpenAIConfig{APIKey: "test-secret", BaseURL: server.URL + "/v1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := p.Upload(ctx, "logical-1", []byte("{input}\n"))
	if err != nil || id != "file-input" {
		t.Fatalf("upload = %s, %v", id, err)
	}
	job, err := p.Create(ctx, id, map[string]string{"batch": "logical-1"})
	if err != nil || job.ID != "batch-provider" {
		t.Fatalf("create = %+v, %v", job, err)
	}
	job, err = p.Retrieve(ctx, job.ID)
	if err != nil || !job.Terminal() {
		t.Fatalf("retrieve = %+v, %v", job, err)
	}
	output, errorFile, err := p.Results(ctx, job)
	if err != nil || string(output) != "{output}\n" || string(errorFile) != "{error}\n" {
		t.Fatalf("results = %s %s %v", output, errorFile, err)
	}
	found, err := p.Find(ctx, map[string]string{"batch": "logical-1"})
	if err != nil || len(found) != 1 || found[0].ID != job.ID || pages.Load() != 2 {
		t.Fatalf("find = %v, err %v, pages %d", found, err, pages.Load())
	}
	if creates.Load() != 1 {
		t.Fatalf("retried create %d times", creates.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestOpenAICreateNeverRetriesAndClassifiesUncertainty(t *testing.T) {
	for _, test := range []struct {
		name                 string
		status               int
		body                 string
		transportErr         bool
		permanent, ambiguous bool
	}{
		{"network", 0, "", true, false, true},
		{"server", 503, `{"error":"test-secret"}`, false, false, true},
		{"rate_limit", 429, "", false, false, true},
		{"invalid_input", 400, `{"error":"test-secret"}`, false, true, false},
		{"authentication", 401, `{"error":"test-secret"}`, false, true, false},
		{"invalid_success", 200, `{}`, false, false, true},
		{"unreadable_success", 200, strings.Repeat("x", int(maxControlBytes)+1), false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempts int
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempts++
				if test.transportErr {
					return nil, fmt.Errorf("do not expose test-secret")
				}
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body)), Header: http.Header{}}, nil
			})}
			p, err := NewOpenAI(OpenAIConfig{APIKey: "test-secret", HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Create(context.Background(), "file-input", nil)
			if err == nil || IsPermanent(err) != test.permanent || IsAmbiguous(err) != test.ambiguous {
				t.Fatalf("got %v, permanent %v ambiguous %v", err, IsPermanent(err), IsAmbiguous(err))
			}
			if attempts != 1 {
				t.Fatalf("create made %d attempts", attempts)
			}
			if strings.Contains(err.Error(), "test-secret") {
				t.Fatal("provider error exposed secret or response body")
			}
		})
	}
}

func TestOpenAIResponseLimitsAndNonadvancingPagination(t *testing.T) {
	t.Run("bounded result download", func(t *testing.T) {
		p, err := NewOpenAI(OpenAIConfig{APIKey: "secret", MaxFileBytes: 8, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("0123456789"))}, nil
		})}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := p.Results(context.Background(), Job{OutputFileID: "file"}); !IsPermanent(err) {
			t.Fatalf("oversized output accepted: %v", err)
		}
		if _, err := p.Upload(context.Background(), "batch", []byte("0123456789")); !IsPermanent(err) {
			t.Fatalf("oversized input accepted: %v", err)
		}
	})
	t.Run("stalled page", func(t *testing.T) {
		var pages int
		p, _ := NewOpenAI(OpenAIConfig{APIKey: "secret", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			pages++
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"id":"same","status":"completed"}],"has_more":true,"last_id":"same"}`))}, nil
		})}})
		if _, err := p.Find(context.Background(), map[string]string{"batch": "target"}); err == nil || pages != 2 {
			t.Fatalf("pagination did not stop: pages=%d err=%v", pages, err)
		}
	})
}
