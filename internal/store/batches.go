package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
)

func boundedLimit(limit int) int {
	if limit < 1 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (s *Store) SealDue(ctx context.Context, mode, endpoint string, limit int) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, dbError("seal due", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT r.run_id,b.batch_id FROM nexus_batch_runs r JOIN nexus_batch_batches b ON b.namespace=r.namespace AND b.batch_id=r.open_batch_id WHERE r.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped AND b.state='open' AND b.seal_at<=clock_timestamp() ORDER BY b.seal_at LIMIT $4 FOR UPDATE OF r SKIP LOCKED`, s.namespace, mode, endpoint, boundedLimit(limit))
	if err != nil {
		return 0, dbError("select due", err)
	}
	type pair struct{ runID, batchID string }
	var due []pair
	for rows.Next() {
		var p pair
		if err = rows.Scan(&p.runID, &p.batchID); err != nil {
			rows.Close()
			return 0, dbError("read due", err)
		}
		due = append(due, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, dbError("read due", err)
	}
	for _, p := range due {
		// Lock order matches Enroll: run row was locked above, batch comes next.
		var batchID string
		if err = tx.QueryRow(ctx, `SELECT batch_id FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2 AND state='open' FOR UPDATE`, s.namespace, p.batchID).Scan(&batchID); err != nil {
			return 0, dbError("lock due", err)
		}
		if err = s.seal(ctx, tx, p.runID, p.batchID, time.Now().UTC()); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, dbError("commit due", err)
	}
	return len(due), nil
}

// ReadyBatches claims the durable dispatch outbox briefly. A crash before
// starting Temporal work leaves the row available again after the lease; a
// crash after starting is repaired by the deterministic workflow ID.
func (s *Store) ReadyBatches(ctx context.Context, mode, endpoint string, limit int) ([]model.BatchRef, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, dbError("claim dispatch", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT b.run_id,b.batch_id,r.config,b.requests,b.bytes,b.sealed_at,b.first_admitted_at FROM nexus_batch_batches b JOIN nexus_batch_runs r ON r.namespace=b.namespace AND r.run_id=b.run_id WHERE b.namespace=$1 AND b.mode=$2 AND b.endpoint=$3 AND b.state='sealed' AND NOT b.dispatched AND NOT r.stopped AND (b.dispatch_claim_until IS NULL OR b.dispatch_claim_until<clock_timestamp()) ORDER BY b.sealed_at,b.batch_id LIMIT $4 FOR UPDATE OF b SKIP LOCKED`, s.namespace, mode, endpoint, boundedLimit(limit))
	if err != nil {
		return nil, dbError("select dispatch", err)
	}
	refs := []model.BatchRef{}
	ids := []string{}
	for rows.Next() {
		var ref model.BatchRef
		var raw []byte
		if err = rows.Scan(&ref.RunID, &ref.BatchID, &raw, &ref.Requests, &ref.Bytes, &ref.SealedAt, &ref.FirstAdmittedAt); err != nil {
			rows.Close()
			return nil, dbError("read dispatch", err)
		}
		if json.Unmarshal(raw, &ref.Config) != nil {
			rows.Close()
			return nil, fmt.Errorf("stored run configuration is invalid")
		}
		refs = append(refs, ref)
		ids = append(ids, ref.BatchID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError("read dispatch", err)
	}
	if len(ids) > 0 {
		_, err = tx.Exec(ctx, `UPDATE nexus_batch_batches SET dispatch_claim_until=$3 WHERE namespace=$1 AND batch_id=ANY($2::text[])`, s.namespace, ids, time.Now().UTC().Add(dispatchLease))
		if err != nil {
			return nil, dbError("lease dispatch", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, dbError("commit dispatch", err)
	}
	return refs, nil
}

func (s *Store) MarkDispatched(ctx context.Context, batchID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE nexus_batch_batches SET dispatched=true,dispatch_claim_until=NULL WHERE namespace=$1 AND batch_id=$2`, s.namespace, batchID)
	if err != nil {
		return dbError("mark dispatch", err)
	}
	if tag.RowsAffected() != 1 {
		return fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	return nil
}

func (s *Store) LoadManifest(ctx context.Context, batchID string) (model.Manifest, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return model.Manifest{}, dbError("load manifest", err)
	}
	defer tx.Rollback(ctx)
	m := model.Manifest{BatchID: batchID}
	var raw []byte
	var state string
	var sealedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT b.run_id,r.config,b.sealed_at,b.bytes,b.state FROM nexus_batch_batches b JOIN nexus_batch_runs r ON r.namespace=b.namespace AND r.run_id=b.run_id WHERE b.namespace=$1 AND b.batch_id=$2`, s.namespace, batchID).Scan(&m.RunID, &raw, &sealedAt, &m.Bytes, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	if err != nil {
		return m, dbError("load manifest", err)
	}
	if state == "open" || sealedAt == nil {
		return m, fail("BATCH_NOT_SEALED", "batch manifest is not sealed")
	}
	m.SealedAt = *sealedAt
	if json.Unmarshal(raw, &m.Config) != nil {
		return m, fmt.Errorf("stored run configuration is invalid")
	}
	rows, err := tx.Query(ctx, `SELECT request,target,fingerprint,admitted_at FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2 ORDER BY position`, s.namespace, batchID)
	if err != nil {
		return m, dbError("load entries", err)
	}
	for rows.Next() {
		var entry model.Entry
		var request, target []byte
		if err = rows.Scan(&request, &target, &entry.Fingerprint, &entry.AdmittedAt); err != nil {
			rows.Close()
			return m, dbError("read entries", err)
		}
		if json.Unmarshal(request, &entry.Request) != nil || json.Unmarshal(target, &entry.Target) != nil {
			rows.Close()
			return m, fmt.Errorf("stored manifest entry is invalid")
		}
		m.Entries = append(m.Entries, entry)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return m, dbError("read entries", err)
	}
	if _, err = model.JSONL(m); err != nil {
		return m, fail("INVALID_BATCH", "stored batch violates configured bounds")
	}
	if err = tx.Commit(ctx); err != nil {
		return m, dbError("read manifest", err)
	}
	return m, nil
}

func (s *Store) SaveReport(ctx context.Context, report model.BatchReport) error {
	report.Diagnostics = boundedDiagnostics(report.Diagnostics)
	raw, err := json.Marshal(report)
	if err != nil {
		return fail("INVALID_REPORT", "batch report cannot be serialized")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE nexus_batch_batches SET report=$3,state=CASE WHEN $4 IN ('completed','completed_with_errors') THEN 'completed' ELSE state END WHERE namespace=$1 AND batch_id=$2`, s.namespace, report.BatchID, raw, report.Status)
	if err != nil {
		return dbError("save report", err)
	}
	if tag.RowsAffected() != 1 {
		return fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	return nil
}

func (s *Store) Status(ctx context.Context, runID string) (model.RunStatus, error) {
	status := model.RunStatus{RunID: runID, Backing: "nexus_standalone_activity", CompletionMode: "external_async_completion_legacy", Storage: "postgres", Batches: []model.BatchReport{}}
	err := s.pool.QueryRow(ctx, `SELECT admitted,closed FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2`, s.namespace, runID).Scan(&status.Admitted, &status.Closed)
	if errors.Is(err, pgx.ErrNoRows) {
		return status, fail("RUN_NOT_STARTED", "batching run does not exist")
	}
	if err != nil {
		return status, dbError("run status", err)
	}
	rows, err := s.pool.Query(ctx, `SELECT batch_id,state,requests,bytes,report,dispatched,first_admitted_at,sealed_at FROM nexus_batch_batches WHERE namespace=$1 AND run_id=$2 ORDER BY batch_id`, s.namespace, runID)
	if err != nil {
		return status, dbError("batch status", err)
	}
	defer rows.Close()
	for rows.Next() {
		var report model.BatchReport
		var raw []byte
		var state string
		var dispatched bool
		var first time.Time
		var sealed *time.Time
		if err = rows.Scan(&report.BatchID, &state, &report.Requests, &report.Bytes, &raw, &dispatched, &first, &sealed); err != nil {
			return status, dbError("read status", err)
		}
		if state == "open" {
			status.Buffered += report.Requests
			continue
		}
		if len(raw) > 0 {
			if json.Unmarshal(raw, &report) != nil {
				return status, fmt.Errorf("stored batch report is invalid")
			}
		} else {
			report.Status = "sealed"
			if dispatched {
				report.Status = "starting"
			}
			if sealed != nil {
				report.CollectionWait = sealed.Sub(first)
			}
		}
		status.Batches = append(status.Batches, report)
	}
	return status, dbError("read status", rows.Err())
}

func boundedDiagnostics(values []string) []string {
	out := make([]string, 0, min(len(values), 100))
	for _, value := range values {
		if len(out) >= 100 {
			break
		}
		if len(value) > 512 {
			value = value[:512]
		}
		out = append(out, value)
	}
	return out
}
