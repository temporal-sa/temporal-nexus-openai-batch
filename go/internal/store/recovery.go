package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/temporal-sa/temporal-nexus-openai-batch/go/internal/model"
)

// WithSubmission serializes the start of a provider submission with StopRun.
// StopRun cannot commit while a submission owns the run lock; once it commits,
// no later submission callback for that run is allowed to start. The callback
// must honor its bounded context and must not call back into the store.
func (s *Store) WithSubmission(ctx context.Context, batchID string, create func(context.Context) error) error {
	boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(boundedCtx)
	if err != nil {
		return dbError("begin submission", err)
	}
	defer tx.Rollback(boundedCtx)
	var stopped bool
	var state string
	var deadline time.Time
	err = tx.QueryRow(boundedCtx, `SELECT r.stopped,b.state,b.deadline_at FROM nexus_batch_runs r JOIN nexus_batch_batches b ON b.namespace=r.namespace AND b.run_id=r.run_id WHERE b.namespace=$1 AND b.batch_id=$2 FOR UPDATE OF r`, s.namespace, batchID).Scan(&stopped, &state, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	if err != nil {
		return dbError("fence submission", err)
	}
	if stopped {
		return fail("RUN_STOPPED", "run explicitly stopped before provider submission")
	}
	if state == "open" {
		return fail("BATCH_NOT_SEALED", "batch manifest is not sealed")
	}
	if !time.Now().Before(deadline) {
		return fail("DEADLINE_EXCEEDED", "request completion deadline has passed")
	}
	submitCtx, submitCancel := context.WithDeadline(boundedCtx, deadline)
	defer submitCancel()
	if err = create(submitCtx); err != nil {
		return err
	}
	return dbError("finish submission", tx.Commit(boundedCtx))
}

// RunResources reads the exact resources owned by durable admission records.
// Calling this after StopRun avoids relying on eventually consistent Temporal
// visibility when canceling already claimed or not-yet-visible batch Workflows.
func (s *Store) RunResources(ctx context.Context, runID string) ([]string, []model.CompletionTarget, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, dbError("run resources", err)
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2)`, s.namespace, runID).Scan(&exists); err != nil {
		return nil, nil, dbError("run resources", err)
	}
	if !exists {
		return nil, nil, fail("RUN_NOT_STARTED", "batching run does not exist")
	}
	batchIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT batch_id FROM nexus_batch_batches WHERE namespace=$1 AND run_id=$2 ORDER BY batch_id`, s.namespace, runID)
	if err != nil {
		return nil, nil, dbError("run batch resources", err)
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, nil, dbError("read batch resource", err)
		}
		batchIDs = append(batchIDs, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, dbError("read batch resources", err)
	}
	targets := []model.CompletionTarget{}
	rows, err = tx.Query(ctx, `SELECT target FROM nexus_batch_requests WHERE namespace=$1 AND run_id=$2 ORDER BY request_id`, s.namespace, runID)
	if err != nil {
		return nil, nil, dbError("run completion resources", err)
	}
	for rows.Next() {
		var raw []byte
		var target model.CompletionTarget
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, nil, dbError("read completion resource", err)
		}
		if json.Unmarshal(raw, &target) != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("stored completion target is invalid")
		}
		targets = append(targets, target)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, dbError("read completion resources", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, nil, dbError("run resources", err)
	}
	return batchIDs, targets, nil
}

// HeartbeatTargets claims a small page of exact pending SAA targets. Worker
// processes keep these asynchronous attempts alive after SubmitRequest has
// released its slot; a pre-enrollment worker death instead permits a retry.
func (s *Store) HeartbeatTargets(ctx context.Context, mode, endpoint string, limit int) ([]model.CompletionTarget, error) {
	rows, err := s.pool.Query(ctx, `WITH due AS (
		SELECT q.namespace,q.run_id,q.request_id
		FROM nexus_batch_requests q JOIN nexus_batch_runs r ON r.namespace=q.namespace AND r.run_id=q.run_id
		WHERE q.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
		AND q.delivery_status='pending' AND q.deadline_at>clock_timestamp() AND q.next_heartbeat_at<=clock_timestamp()
		ORDER BY q.next_heartbeat_at,q.run_id,q.request_id LIMIT $4 FOR UPDATE OF q SKIP LOCKED
	)
	UPDATE nexus_batch_requests q SET next_heartbeat_at=clock_timestamp()+interval '10 seconds'
	FROM due WHERE q.namespace=due.namespace AND q.run_id=due.run_id AND q.request_id=due.request_id
	RETURNING q.target`, s.namespace, mode, endpoint, boundedLimit(limit))
	if err != nil {
		return nil, dbError("claim heartbeat targets", err)
	}
	defer rows.Close()
	targets := []model.CompletionTarget{}
	for rows.Next() {
		var raw []byte
		var target model.CompletionTarget
		if err = rows.Scan(&raw); err != nil {
			return nil, dbError("read heartbeat target", err)
		}
		if json.Unmarshal(raw, &target) != nil {
			return nil, fmt.Errorf("stored completion target is invalid")
		}
		targets = append(targets, target)
	}
	return targets, dbError("read heartbeat targets", rows.Err())
}

// PendingCompletions lets the independent keeper repair delivery after a batch
// Workflow has already closed, including when a later SAA attempt reattaches.
// Completion is exact-token idempotent; MarkDelivered fences its checkpoint.
func (s *Store) PendingCompletions(ctx context.Context, mode, endpoint string, limit int) ([]model.BatchDelivery, error) {
	rows, err := s.pool.Query(ctx, `SELECT q.batch_id,q.request_id,q.target,q.outcome
		FROM nexus_batch_requests q JOIN nexus_batch_runs r ON r.namespace=q.namespace AND r.run_id=q.run_id
		JOIN nexus_batch_batches b ON b.namespace=q.namespace AND b.batch_id=q.batch_id
		WHERE q.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
		AND b.state='completed' AND q.delivery_status='pending' AND q.outcome IS NOT NULL
		ORDER BY q.admitted_at,q.batch_id,q.position LIMIT $4`, s.namespace, mode, endpoint, boundedLimit(limit))
	if err != nil {
		return nil, dbError("pending completions", err)
	}
	defer rows.Close()
	deliveries := []model.BatchDelivery{}
	for rows.Next() {
		var delivery model.BatchDelivery
		var rawTarget, rawOutcome []byte
		if err = rows.Scan(&delivery.BatchID, &delivery.RequestID, &rawTarget, &rawOutcome); err != nil {
			return nil, dbError("read pending completion", err)
		}
		if json.Unmarshal(rawTarget, &delivery.Target) != nil || json.Unmarshal(rawOutcome, &delivery.Outcome) != nil {
			return nil, fmt.Errorf("stored delivery is invalid")
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, dbError("read pending completions", rows.Err())
}
