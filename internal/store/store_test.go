package store

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
)

func TestInvalidConnectionDoesNotExposeCredentials(t *testing.T) {
	_, err := Open(context.Background(), "postgres://user:confidential-password@host/%zz", "default")
	if err == nil || strings.Contains(err.Error(), "confidential-password") || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("unsafe connection error: %v", err)
	}
	pgErr := &pgconn.PgError{Code: "23505", Message: "confidential document", Detail: "task_token=secret"}
	err = dbError("enroll", pgErr)
	if !strings.Contains(err.Error(), "23505") || strings.Contains(err.Error(), "confidential") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe SQL error: %v", err)
	}
}

func TestCompletionTargetIsNamespaceAndAttemptFenced(t *testing.T) {
	target := model.CompletionTarget{Namespace: "default", ActivityID: "activity", ActivityRunID: "execution", Attempt: 1, TaskToken: []byte("token")}
	if !validTarget(target, "default") {
		t.Fatal("valid exact target rejected")
	}
	if validTarget(target, "another") {
		t.Fatal("cross-namespace target accepted")
	}
	target.Attempt = 0
	if validTarget(target, "default") {
		t.Fatal("unfenced attempt accepted")
	}
	target.Attempt = 1
	target.TaskToken = make([]byte, 16*1024+1)
	if validTarget(target, "default") {
		t.Fatal("oversized token accepted")
	}
}
