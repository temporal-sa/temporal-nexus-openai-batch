package main

import (
	"context"
	"errors"
	"go.temporal.io/api/serviceerror"
	"os"
	"path/filepath"
	"testing"
)

func TestDocumentsRejectDuplicateAndOverrideRun(t *testing.T) {
	p := filepath.Join(t.TempDir(), "documents.jsonl")
	if err := os.WriteFile(p, []byte("{\"run_id\":\"old\",\"request_id\":\"doc\",\"input\":\"hello\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	docs, err := documents("new-run", p, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].RunID != "new-run" {
		t.Fatal("file run_id was not scoped to launcher run")
	}
	if err = os.WriteFile(p, []byte("{\"request_id\":\"doc\",\"input\":\"one\"}\n{\"request_id\":\"doc\",\"input\":\"two\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = documents("run", p, 1); err == nil {
		t.Fatal("duplicate document identity accepted")
	}
	if _, err = documents("run", "", 1001); err == nil {
		t.Fatal("unbounded count accepted")
	}
}

func TestCleanupOwnsOnlyTheNamedRun(t *testing.T) {
	for _, id := range []string{"batch-coordinator/demo", "caller/demo/doc", "batch-request/demo/hash", "batch/demo-0001"} {
		if !ownedWorkflow(id, "demo") {
			t.Errorf("expected own id %s", id)
		}
	}
	for _, id := range []string{"batch-coordinator/demo-other", "caller/demo-other/doc", "batch-request/demo-other/hash", "batch/demo-other-0001", "batch/demo-0001-other", "unrelated/demo"} {
		if ownedWorkflow(id, "demo") {
			t.Errorf("cleanup would touch unrelated id %s", id)
		}
	}
}

func TestLocalWaitExpiryIsPendingNotWorkflowFailure(t *testing.T) {
	var item itemReport
	recordItemError(&item, context.Canceled)
	if item.Status != "pending" {
		t.Fatalf("unfinished durable operation counted as %s", item.Status)
	}
	recordItemError(&item, context.DeadlineExceeded)
	if item.Status != "pending" {
		t.Fatal("RPC deadline before parent cancellation must remain unconfirmed")
	}
	recordItemError(&item, serviceerror.NewDeadlineExceeded("context deadline exceeded"))
	if item.Status != "pending" {
		t.Fatal("Temporal service deadline must remain unconfirmed")
	}
	recordItemError(&item, errors.New("actual provider failure"))
	if item.Status != "failed" {
		t.Fatal("known provider failure must remain a failure")
	}
}
