// Package provider isolates OpenAI Batch API I/O from durable orchestration.
// Live and fake implementations share the same upload/create/poll/download
// contract; Temporal Activities decide when those calls run and PostgreSQL
// preserves their manifests and normalized outcomes.
package provider

import (
	"context"
	"errors"
	"fmt"
)

// Provider methods make one attempt. In particular Create must never retry an
// uncertain submission: the caller must reconcile it using Find first. Metadata
// helps locate an accepted job after a lost response; it is not a provider-side
// idempotency guarantee. Even an empty search must not trigger a blind resubmit.
type Provider interface {
	Upload(context.Context, string, []byte) (string, error)
	Create(context.Context, string, map[string]string) (Job, error)
	Retrieve(context.Context, string) (Job, error)
	Find(context.Context, map[string]string) ([]Job, error)
	Results(context.Context, Job) ([]byte, []byte, error)
}

// Job contains only the control information needed to resume processing. Input
// documents and result-file contents are passed separately and stay out of the
// batch Workflow's control state.
type Job struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	InputFileID  string            `json:"input_file_id"`
	OutputFileID string            `json:"output_file_id,omitempty"`
	ErrorFileID  string            `json:"error_file_id,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// Terminal includes failed, expired, and cancelled jobs because their available
// files may still resolve some requests. Reconciliation supplies a failure for
// every manifest request those files leave unresolved.
func (j Job) Terminal() bool {
	switch j.Status {
	case "completed", "failed", "expired", "cancelled":
		return true
	default:
		return false
	}
}

// Error contains safe diagnostic information without response bodies, request
// inputs or credentials. Ambiguous means Create may have reached the provider.
// Permanent describes retryability of the reported error; it does not turn an
// ambiguous paid submission into a safe operation to repeat.
type Error struct {
	Op        string
	Code      string
	Message   string
	Status    int
	Permanent bool
	Ambiguous bool
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("provider %s: %s (HTTP %d): %s", e.Op, e.Code, e.Status, e.Message)
	}
	return fmt.Sprintf("provider %s: %s: %s", e.Op, e.Code, e.Message)
}

func IsPermanent(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Permanent
}

func IsAmbiguous(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Ambiguous
}

func metadataMatches(actual, expected map[string]string) bool {
	for k, v := range expected {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func copyMetadata(metadata map[string]string) map[string]string {
	copy := make(map[string]string, len(metadata))
	for k, v := range metadata {
		copy[k] = v
	}
	return copy
}
