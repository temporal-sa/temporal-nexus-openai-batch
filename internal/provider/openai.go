package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultMaxFileBytes int64 = 16 << 20
const maxControlBytes int64 = 1 << 20
const maxListPages = 1000

type OpenAIConfig struct {
	APIKey       string
	BaseURL      string
	HTTPClient   *http.Client
	MaxFileBytes int64
}

type OpenAI struct {
	key, baseURL string
	client       *http.Client
	maxFileBytes int64
}

func NewOpenAI(cfg OpenAIConfig) (*OpenAI, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY is required for live provider")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.openai.com/v1"
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid OpenAI base URL")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	// Never allow redirects to forward credentials or transparently replay a POST.
	client := *cfg.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = defaultMaxFileBytes
	}
	return &OpenAI{key: cfg.APIKey, baseURL: strings.TrimRight(cfg.BaseURL, "/"), client: &client, maxFileBytes: cfg.MaxFileBytes}, nil
}

func (p *OpenAI) Upload(ctx context.Context, logicalBatchID string, data []byte) (string, error) {
	if len(data) == 0 || int64(len(data)) > p.maxFileBytes {
		return "", &Error{Op: "upload", Code: "input_size", Message: "input file is empty or exceeds configured size limit", Permanent: true}
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("purpose", "batch"); err != nil {
		return "", err
	}
	// The file name is deliberately constant; logical identifiers live in metadata.
	part, err := w.CreateFormFile("file", "batch.jsonl")
	if err != nil {
		return "", err
	}
	if _, err = part.Write(data); err != nil {
		return "", err
	}
	if err = w.Close(); err != nil {
		return "", err
	}
	response, err := p.request(ctx, "upload", http.MethodPost, "/files", w.FormDataContentType(), &body, maxControlBytes, false)
	if err != nil {
		return "", err
	}
	var file struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response, &file); err != nil || file.ID == "" {
		return "", invalidResponse("upload", false)
	}
	return file.ID, nil
}

func (p *OpenAI) Create(ctx context.Context, fileID string, metadata map[string]string) (Job, error) {
	if fileID == "" {
		return Job{}, &Error{Op: "create", Code: "input_file_missing", Message: "input file ID is required", Permanent: true}
	}
	body, err := json.Marshal(struct {
		InputFileID      string            `json:"input_file_id"`
		Endpoint         string            `json:"endpoint"`
		CompletionWindow string            `json:"completion_window"`
		Metadata         map[string]string `json:"metadata"`
	}{fileID, "/v1/responses", "24h", metadata})
	if err != nil {
		return Job{}, err
	}
	response, err := p.request(ctx, "create", http.MethodPost, "/batches", "application/json", bytes.NewReader(body), maxControlBytes, true)
	if err != nil {
		return Job{}, err
	}
	return decodeJob(response, "create", true)
}

func (p *OpenAI) Retrieve(ctx context.Context, batchID string) (Job, error) {
	response, err := p.request(ctx, "retrieve", http.MethodGet, "/batches/"+url.PathEscape(batchID), "", nil, maxControlBytes, false)
	if err != nil {
		return Job{}, err
	}
	return decodeJob(response, "retrieve", false)
}

// Find scans every list page. OpenAI's list operation has no server-side metadata
// filter, so stopping after a page could incorrectly declare a submission absent.
func (p *OpenAI) Find(ctx context.Context, metadata map[string]string) ([]Job, error) {
	var found []Job
	seen := map[string]bool{}
	after := ""
	for pages := 0; pages < maxListPages; pages++ {
		query := url.Values{"limit": {"100"}}
		if after != "" {
			query.Set("after", after)
		}
		response, err := p.request(ctx, "find", http.MethodGet, "/batches?"+query.Encode(), "", nil, maxControlBytes, false)
		if err != nil {
			return nil, err
		}
		var page struct {
			Data    []Job  `json:"data"`
			HasMore bool   `json:"has_more"`
			LastID  string `json:"last_id"`
		}
		if err := json.Unmarshal(response, &page); err != nil || page.Data == nil {
			return nil, invalidResponse("find", false)
		}
		for _, job := range page.Data {
			if job.ID == "" {
				return nil, invalidResponse("find", false)
			}
			if metadataMatches(job.Metadata, metadata) {
				found = append(found, job)
			}
		}
		if !page.HasMore {
			return found, nil
		}
		after = page.LastID
		if after == "" && len(page.Data) > 0 {
			after = page.Data[len(page.Data)-1].ID
		}
		if after == "" || seen[after] {
			return nil, invalidResponse("find", false)
		}
		seen[after] = true
	}
	return nil, &Error{Op: "find", Code: "pagination_limit", Message: "provider batch listing exceeded 1000 pages; submission absence cannot be established", Permanent: true}
}

func (p *OpenAI) Results(ctx context.Context, job Job) ([]byte, []byte, error) {
	output, err := p.fileContent(ctx, job.OutputFileID)
	if err != nil {
		return nil, nil, err
	}
	errors, err := p.fileContent(ctx, job.ErrorFileID)
	if err != nil {
		return nil, nil, err
	}
	return output, errors, nil
}

func (p *OpenAI) fileContent(ctx context.Context, id string) ([]byte, error) {
	if id == "" {
		return nil, nil
	}
	return p.request(ctx, "results", http.MethodGet, "/files/"+url.PathEscape(id)+"/content", "", nil, p.maxFileBytes, false)
}

func (p *OpenAI) request(ctx context.Context, op, method, path, contentType string, body io.Reader, maxBytes int64, submission bool) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return nil, &Error{Op: op, Code: "invalid_request", Message: "could not construct provider request", Permanent: true}
	}
	req.Header.Set("Authorization", "Bearer "+p.key)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, &Error{Op: op, Code: "transport_error", Message: "provider request did not return a response", Ambiguous: submission}
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		permanent := resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 && resp.StatusCode != 409 && resp.StatusCode != 429
		// A gateway/server error cannot prove the POST was not accepted.
		return nil, &Error{Op: op, Code: "http_error", Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode), Permanent: permanent, Ambiguous: submission && !permanent}
	}
	if readErr != nil || int64(len(data)) > maxBytes {
		return nil, &Error{Op: op, Code: "response_size_or_read", Message: "provider response was unreadable or exceeded configured size limit", Permanent: !submission, Ambiguous: submission}
	}
	return data, nil
}

func decodeJob(data []byte, op string, ambiguous bool) (Job, error) {
	var job Job
	if err := json.Unmarshal(data, &job); err != nil || job.ID == "" || job.Status == "" {
		return Job{}, invalidResponse(op, ambiguous)
	}
	return job, nil
}

func invalidResponse(op string, ambiguous bool) error {
	return &Error{Op: op, Code: "invalid_response", Message: "provider returned an invalid response", Permanent: !ambiguous, Ambiguous: ambiguous}
}
