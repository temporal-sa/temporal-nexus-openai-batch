package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
)

// Reconcile is pure: exactly one bounded terminal outcome is produced for every
// manifest entry, including requests absent from malformed or partial files.
// Results are matched exclusively by custom_id, never by file ordering.
// Starting with failures ensures that a missing or unparseable provider record
// cannot silently strand a recipient. Neither downloading nor completion occurs
// here, so the same files and manifest always produce the same reconciliation.
func Reconcile(manifest model.Manifest, job Job, output, errorFile []byte) ([]model.Outcome, []string) {
	outcomes := make([]model.Outcome, len(manifest.Entries))
	expected := make(map[string]int, len(manifest.Entries))
	seen := make(map[string]int, len(manifest.Entries))
	var diagnostics []string
	diagnose := func(message string) {
		if len(diagnostics) < 100 {
			diagnostics = append(diagnostics, message)
		} else if len(diagnostics) == 100 {
			diagnostics = append(diagnostics, "additional diagnostics omitted")
		}
	}
	for i, entry := range manifest.Entries {
		id := entry.Request.RequestID
		expected[id] = i
		missingCode := "MISSING_RESULT"
		switch job.Status {
		case "failed":
			missingCode = "BATCH_FAILED"
		case "cancelled":
			missingCode = "BATCH_CANCELLED"
		case "expired":
			missingCode = "BATCH_EXPIRED"
		}
		outcomes[i] = model.Outcome{RequestID: id, Fingerprint: entry.Fingerprint, BatchID: manifest.BatchID,
			Failure: &model.Failure{Code: missingCode, Message: "provider did not return a result for this request (batch status: " + safeStatus(job.Status) + ")"}}
	}
	consume := func(file string, data []byte) {
		for lineNum, line := range bytes.Split(data, []byte{'\n'}) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			if int64(len(line)) > maxControlBytes {
				diagnose(fmt.Sprintf("%s line %d exceeds JSONL line limit", file, lineNum+1))
				continue
			}
			var record batchRecord
			if err := json.Unmarshal(line, &record); err != nil {
				diagnose(fmt.Sprintf("%s line %d is malformed JSON", file, lineNum+1))
				continue
			}
			index, known := expected[record.CustomID]
			if !known {
				diagnose(fmt.Sprintf("%s line %d has an unknown or missing custom_id", file, lineNum+1))
				continue
			}
			seen[record.CustomID]++
			if seen[record.CustomID] > 1 {
				// Two records for one custom_id are conflicting evidence, even
				// across the output and error files. Do not choose one arbitrarily.
				outcomes[index].Result = nil
				outcomes[index].Failure = &model.Failure{Code: "DUPLICATE_RESULT", Message: "provider returned multiple results for this request"}
				diagnose(fmt.Sprintf("%s line %d duplicates a manifest request", file, lineNum+1))
				continue
			}
			result, failure := normalize(record)
			outcomes[index].Result, outcomes[index].Failure = result, failure
		}
	}
	consume("output", output)
	consume("error", errorFile)
	for _, outcome := range outcomes {
		if seen[outcome.RequestID] == 0 {
			diagnose("manifest request missing from provider result files")
		}
	}
	return outcomes, diagnostics
}

type batchRecord struct {
	CustomID string `json:"custom_id"`
	Response *struct {
		StatusCode int             `json:"status_code"`
		Body       json.RawMessage `json:"body"`
	} `json:"response"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// normalize reduces a Responses record to the prototype's small text contract.
// HTTP success is insufficient: the response must itself be completed, contain
// output text, and fit the completion payload bound. Raw provider error text and
// non-message output are intentionally excluded from caller-facing results.
func normalize(record batchRecord) (*model.Result, *model.Failure) {
	fail := func(code, message string) (*model.Result, *model.Failure) {
		return nil, &model.Failure{Code: code, Message: message}
	}
	if record.Error != nil {
		code := "PROVIDER_REQUEST_ERROR"
		if record.Error.Code == "batch_expired" {
			code = "BATCH_EXPIRED"
		}
		return providerFailure(code, "provider could not complete this request", record.Error.Code)
	}
	if record.Response == nil {
		return fail("INVALID_RESULT", "provider returned neither a response nor an error")
	}
	var response struct {
		Status string          `json:"status"`
		Error  json.RawMessage `json:"error"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Usage *model.Usage `json:"usage"`
	}
	decodeErr := json.Unmarshal(record.Response.Body, &response)
	if record.Response.StatusCode < 200 || record.Response.StatusCode >= 300 {
		return providerFailure("PROVIDER_REQUEST_ERROR", fmt.Sprintf("provider returned HTTP %d for this request", record.Response.StatusCode), errorObjectCode(response.Error))
	}
	if decodeErr != nil {
		return fail("INVALID_RESULT", "provider response body is malformed")
	}
	if len(response.Error) > 0 && !bytes.Equal(bytes.TrimSpace(response.Error), []byte("null")) {
		return providerFailure("PROVIDER_REQUEST_ERROR", "provider response contains an error", errorObjectCode(response.Error))
	}
	if response.Status != "completed" {
		return fail("INCOMPLETE_RESULT", "provider response did not complete")
	}
	var text strings.Builder
	for _, item := range response.Output {
		if item.Type != "message" {
			continue
		}
		for _, content := range item.Content {
			switch content.Type {
			case "output_text":
				text.WriteString(content.Text)
			case "refusal":
				return fail("MODEL_REFUSAL", "model declined this request")
			}
		}
	}
	if text.Len() == 0 {
		return fail("INVALID_RESULT", "provider response contains no output text")
	}
	if response.Usage != nil && (response.Usage.InputTokens < 0 || response.Usage.OutputTokens < 0) {
		return fail("INVALID_RESULT", "provider response contains invalid token usage")
	}
	result := &model.Result{RequestID: record.CustomID, Text: text.String(), Usage: response.Usage}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > model.MaxResultBytes {
		return fail("RESULT_TOO_LARGE", "normalized provider result exceeds 4 KiB")
	}
	return result, nil
}

func providerFailure(code, message, rawProviderCode string) (*model.Result, *model.Failure) {
	providerCode := safeMachineCode(rawProviderCode)
	if providerCode != "" {
		message += " (provider code: " + providerCode + ")"
	}
	return nil, &model.Failure{Code: code, Message: message, ProviderCode: providerCode}
}

func errorObjectCode(data []byte) string {
	var providerError struct {
		Code json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal(data, &providerError); err != nil {
		return ""
	}
	var code string
	if err := json.Unmarshal(providerError.Code, &code); err != nil {
		return ""
	}
	return code
}

// Provider codes are identifiers, never arbitrary error messages. Discard codes
// with controls, whitespace, non-ASCII text or excessive length rather than
// copying untrusted provider messages into workflow histories and CLI output.
func safeMachineCode(code string) string {
	if len(code) == 0 || len(code) > 64 {
		return ""
	}
	for i := range len(code) {
		c := code[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		if i > 0 && (c == '_' || c == '-' || c == '.') {
			continue
		}
		return ""
	}
	return code
}

func safeStatus(status string) string {
	switch status {
	case "completed", "failed", "expired", "cancelled", "validating", "in_progress", "finalizing", "cancelling":
		return status
	default:
		return "unknown"
	}
}
