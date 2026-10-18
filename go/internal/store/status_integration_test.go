package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
)

func TestPostgresStatusIdentifiesBackingCompletionAndStorage(t *testing.T) {
	if os.Getenv("POSTGRES_TEST") != "1" {
		t.Skip("set POSTGRES_TEST=1 to test the real PostgreSQL store")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = LocalDSN
	}
	namespace := fmt.Sprintf("status-metadata-%d", time.Now().UnixNano())
	s, err := Open(ctx, dsn, namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = s.pool.Exec(cleanCtx, `DELETE FROM nexus_batch_runs WHERE namespace=$1`, namespace)
	}()
	if err = s.CreateRun(ctx, model.RunInput{RunID: "metadata", Config: model.DefaultConfig("fake")}); err != nil {
		t.Fatal(err)
	}
	status, err := s.Status(ctx, "metadata")
	if err != nil {
		t.Fatal(err)
	}
	if status.Backing != "nexus_standalone_activity" || status.CompletionMode != "external_async_completion" || status.Storage != "postgres" {
		t.Fatalf("ambiguous runtime metadata: %+v", status)
	}
}
