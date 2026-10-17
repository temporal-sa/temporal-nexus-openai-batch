package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
)

func (s *Store) SaveOutcomes(ctx context.Context, batchID string, outcomes []model.Outcome, diagnostics []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbError("save outcomes", err)
	}
	defer tx.Rollback(ctx)
	var saved bool
	var state string
	err = tx.QueryRow(ctx, `SELECT outcomes_saved,state FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2 FOR UPDATE`, s.namespace, batchID).Scan(&saved, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	if err != nil {
		return dbError("save outcomes", err)
	}
	if state == "open" {
		return fail("BATCH_NOT_SEALED", "batch manifest is not sealed")
	}
	// The first committed reconciliation owns the outcomes. A retried download
	// must never replace results that may already have reached recipients.
	if saved {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT request_id,fingerprint FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2 ORDER BY position`, s.namespace, batchID)
	if err != nil {
		return dbError("outcome manifest", err)
	}
	identity := map[string]string{}
	for rows.Next() {
		var id, fp string
		if err = rows.Scan(&id, &fp); err != nil {
			rows.Close()
			return dbError("outcome identity", err)
		}
		identity[id] = fp
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError("outcome identity", err)
	}
	if len(identity) != len(outcomes) {
		return fail("INVALID_RESULT", "reconciliation must resolve every manifest request exactly once")
	}
	seen := map[string]bool{}
	for _, o := range outcomes {
		fp, exists := identity[o.RequestID]
		if !exists || seen[o.RequestID] || o.BatchID != batchID || o.Fingerprint != fp || (o.Result == nil) == (o.Failure == nil) {
			return fail("INVALID_RESULT", "outcome identity or terminal result is invalid")
		}
		seen[o.RequestID] = true
		if o.Result != nil {
			raw, _ := json.Marshal(o.Result)
			if o.Result.RequestID != o.RequestID || len(raw) > model.MaxResultBytes {
				return fail("INVALID_RESULT", "normalized result exceeds its request or size bound")
			}
		}
		if o.Failure != nil && (o.Failure.Code == "" || len(o.Failure.Code) > 96 || len(o.Failure.Message) > 1024 || len(o.Failure.ProviderCode) > 64) {
			return fail("INVALID_RESULT", "failure exceeds its size bound")
		}
		raw, err := json.Marshal(o)
		if err != nil {
			return fail("INVALID_RESULT", "outcome cannot be serialized")
		}
		_, err = tx.Exec(ctx, `UPDATE nexus_batch_requests SET outcome=$4 WHERE namespace=$1 AND batch_id=$2 AND request_id=$3`, s.namespace, batchID, o.RequestID, raw)
		if err != nil {
			return dbError("write outcome", err)
		}
	}
	rawDiagnostics, _ := json.Marshal(boundedDiagnostics(diagnostics))
	_, err = tx.Exec(ctx, `UPDATE nexus_batch_batches SET outcomes_saved=true,state='delivering',diagnostics=$3,dispatched=true WHERE namespace=$1 AND batch_id=$2`, s.namespace, batchID, rawDiagnostics)
	if err != nil {
		return dbError("commit outcomes", err)
	}
	return dbError("commit outcomes", tx.Commit(ctx))
}

func (s *Store) FailBatch(ctx context.Context, batchID, code, message string) error {
	// Only Activity code loads the manifest. Neither it nor the completion
	// targets are returned to the Batch Workflow.
	m, err := s.LoadManifest(ctx, batchID)
	if err != nil {
		return err
	}
	outcomes := make([]model.Outcome, 0, len(m.Entries))
	for _, entry := range m.Entries {
		outcomes = append(outcomes, model.Outcome{RequestID: entry.Request.RequestID, Fingerprint: entry.Fingerprint, BatchID: batchID, Failure: &model.Failure{Code: code, Message: message}})
	}
	return s.SaveOutcomes(ctx, batchID, outcomes, []string{code})
}

func (s *Store) PendingDeliveries(ctx context.Context, batchID string, limit int) ([]model.StoredDelivery, error) {
	rows, err := s.pool.Query(ctx, `SELECT request_id,target,outcome FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2 AND delivery_status='pending' AND outcome IS NOT NULL ORDER BY position LIMIT $3`, s.namespace, batchID, boundedLimit(limit))
	if err != nil {
		return nil, dbError("pending deliveries", err)
	}
	defer rows.Close()
	deliveries := []model.StoredDelivery{}
	for rows.Next() {
		var delivery model.StoredDelivery
		var rawTarget, rawOutcome []byte
		if err = rows.Scan(&delivery.RequestID, &rawTarget, &rawOutcome); err != nil {
			return nil, dbError("read delivery", err)
		}
		if json.Unmarshal(rawTarget, &delivery.Target) != nil || json.Unmarshal(rawOutcome, &delivery.Outcome) != nil {
			return nil, fmt.Errorf("stored delivery is invalid")
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, dbError("read delivery", rows.Err())
}

func (s *Store) MarkDelivered(ctx context.Context, batchID, requestID string, expected model.CompletionTarget, delivered bool, diagnostic string) error {
	status := "skipped"
	if delivered {
		status = "delivered"
	}
	if len(diagnostic) > 512 {
		diagnostic = diagnostic[:512]
	}
	// Terminal progress is monotonic and retry-idempotent, including after a lost
	// completion acknowledgment has been reconciled by the delivery Activity.
	rawTarget, err := json.Marshal(expected)
	if err != nil {
		return fail("INVALID_ACTIVITY_TARGET", "completion target cannot be serialized")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE nexus_batch_requests SET delivery_status=CASE WHEN delivery_status='pending' THEN $4 ELSE delivery_status END,delivery_diagnostic=CASE WHEN delivery_status='pending' THEN $5 ELSE delivery_diagnostic END WHERE namespace=$1 AND batch_id=$2 AND request_id=$3 AND outcome IS NOT NULL AND target=$6::jsonb`, s.namespace, batchID, requestID, status, diagnostic, rawTarget)
	if err != nil {
		return dbError("mark delivery", err)
	}
	if tag.RowsAffected() != 1 {
		// A concurrently retried SAA may have reattached its newer exact target.
		// The old delivery must not mark that recipient skipped or completed.
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2 AND request_id=$3 AND outcome IS NOT NULL)`, s.namespace, batchID, requestID).Scan(&exists); err != nil {
			return dbError("check delivery", err)
		}
		if !exists {
			return fail("DELIVERY_NOT_FOUND", "stored outcome does not exist")
		}
	}
	return nil
}

func (s *Store) DeliverySummary(ctx context.Context, batchID string) (model.DeliverySummary, error) {
	summary := model.DeliverySummary{FailureCounts: map[string]int{}, Diagnostics: []string{}}
	var rawDiagnostics []byte
	err := s.pool.QueryRow(ctx, `SELECT diagnostics FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2`, s.namespace, batchID).Scan(&rawDiagnostics)
	if errors.Is(err, pgx.ErrNoRows) {
		return summary, fail("BATCH_NOT_FOUND", "batch does not exist")
	}
	if err != nil {
		return summary, dbError("delivery summary", err)
	}
	if json.Unmarshal(rawDiagnostics, &summary.Diagnostics) != nil {
		return summary, fmt.Errorf("stored diagnostics are invalid")
	}
	rows, err := s.pool.Query(ctx, `SELECT outcome,delivery_status,delivery_diagnostic FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2 ORDER BY position`, s.namespace, batchID)
	if err != nil {
		return summary, dbError("delivery summary", err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var status, diagnostic string
		if err = rows.Scan(&raw, &status, &diagnostic); err != nil {
			return summary, dbError("read summary", err)
		}
		if len(raw) > 0 {
			var outcome model.Outcome
			if json.Unmarshal(raw, &outcome) != nil {
				return summary, fmt.Errorf("stored outcome is invalid")
			}
			if outcome.Failure != nil {
				summary.Failed++
				summary.FailureCounts[outcome.Failure.Code]++
			} else if outcome.Result != nil {
				summary.Succeeded++
			}
		}
		if status == "delivered" {
			summary.Delivered++
		} else if status == "skipped" {
			summary.Skipped++
		}
		if diagnostic != "" {
			exists := false
			for _, previous := range summary.Diagnostics {
				if previous == diagnostic {
					exists = true
					break
				}
			}
			if !exists && len(summary.Diagnostics) < 100 {
				summary.Diagnostics = append(summary.Diagnostics, diagnostic)
			}
		}
	}
	return summary, dbError("read summary", rows.Err())
}
