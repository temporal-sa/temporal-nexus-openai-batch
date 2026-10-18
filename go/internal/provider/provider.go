// Package provider isolates OpenAI Batch API I/O from durable orchestration.
package provider

import (
	"context"
	"errors"
	"fmt"
)

// Provider methods make one attempt. In particular Create must never retry an
// uncertain submission: the caller must reconcile it using Find first.
type Provider interface {
	Upload(context.Context, string, []byte) (string, error)
	Create(context.Context, string, map[string]string) (Job, error)
	Retrieve(context.Context, string) (Job, error)
	Find(context.Context, map[string]string) ([]Job, error)
	Results(context.Context, Job) ([]byte, []byte, error)
}

type Job struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	InputFileID  string            `json:"input_file_id"`
	OutputFileID string            `json:"output_file_id,omitempty"`
	ErrorFileID  string            `json:"error_file_id,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

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
