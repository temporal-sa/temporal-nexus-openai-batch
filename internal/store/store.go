// Package store owns transactional enrollment, sealed manifests and completion
// delivery state. Large batches and exact Activity attempt tokens stay here,
// outside Temporal workflow histories.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
)

const LocalDSN = "postgres://nexus_batch:nexus_batch@127.0.0.1:54329/nexus_batch?sslmode=disable"
const schemaVersion = 2
const dispatchLease = 10 * time.Second

//go:embed schema.sql
var schemaSQL string

//go:embed migration_v2.sql
var migrationV2SQL string

type Store struct {
	pool      *pgxpool.Pool
	namespace string
}

// Open checks connectivity and applies the versioned schema. Errors deliberately
// exclude the connection string and underlying parser/connection diagnostics.
func Open(ctx context.Context, dsn, namespace string) (*Store, error) {
	if dsn == "" || namespace == "" || len(namespace) > 255 {
		return nil, fmt.Errorf("DATABASE_URL and Temporal namespace are required")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DATABASE_URL")
	}
	if config.MaxConns > 32 {
		config.MaxConns = 32
	}
	config.ConnConfig.ConnectTimeout = 10 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("could not initialize PostgreSQL connection pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("could not connect to PostgreSQL; check DATABASE_URL and database availability")
	}
	s := &Store{pool: pool, namespace: namespace}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close()            { s.pool.Close() }
func (s *Store) Namespace() string { return s.namespace }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbError("migrate", err)
	}
	defer tx.Rollback(ctx)
	// One database-wide transaction lock serializes concurrent worker migrations.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(819482729640031)"); err != nil {
		return dbError("migrate", err)
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS nexus_batch_schema_versions(version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return dbError("migrate", err)
	}
	var version int
	if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM nexus_batch_schema_versions").Scan(&version); err != nil {
		return dbError("migrate", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("PostgreSQL batch schema is newer than this binary")
	}
	if version == 0 {
		if _, err = tx.Exec(ctx, schemaSQL); err != nil {
			return dbError("migrate", err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO nexus_batch_schema_versions(version) VALUES(1)"); err != nil {
			return dbError("migrate", err)
		}
		version = 1
	}
	if version < 2 {
		if _, err = tx.Exec(ctx, migrationV2SQL); err != nil {
			return dbError("migrate", err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO nexus_batch_schema_versions(version) VALUES(2)"); err != nil {
			return dbError("migrate", err)
		}
	}
	return dbError("migrate", tx.Commit(ctx))
}

func (s *Store) CreateRun(ctx context.Context, input model.RunInput) error {
	if err := input.Config.Validate(); err != nil {
		return fail("INVALID_INPUT", err.Error())
	}
	if err := model.ValidateRequest(model.Request{RunID: input.RunID, RequestID: "run", Input: "run"}); err != nil {
		return err
	}
	config, err := json.Marshal(input.Config)
	if err != nil {
		return fail("INVALID_INPUT", "run configuration cannot be serialized")
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO nexus_batch_runs(namespace,run_id,config,mode,endpoint) VALUES($1,$2,$3,$4,$5)`, s.namespace, input.RunID, config, input.Config.Mode, input.Config.Endpoint)
	if uniqueViolation(err) {
		return fail("RUN_EXISTS", "run_id already exists; choose a new run ID")
	}
	return dbError("create run", err)
}

func (s *Store) GetRun(ctx context.Context, runID string) (model.RunInput, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT config FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2`, s.namespace, runID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.RunInput{}, fail("RUN_NOT_STARTED", "batching run does not exist")
	}
	if err != nil {
		return model.RunInput{}, dbError("get run", err)
	}
	input := model.RunInput{RunID: runID}
	if err = json.Unmarshal(raw, &input.Config); err != nil {
		return input, fmt.Errorf("stored run configuration is invalid")
	}
	return input, nil
}

func (s *Store) Enroll(ctx context.Context, e model.Enrollment) (model.Admission, error) {
	if err := model.ValidateRequest(e.Request); err != nil {
		return model.Admission{}, err
	}
	if e.Fingerprint != model.Fingerprint(e.Request) || !validTarget(e.Target, s.namespace) {
		return model.Admission{}, fail("INVALID_INPUT", "enrollment fingerprint or completion target is invalid")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Admission{}, dbError("enroll", err)
	}
	defer tx.Rollback(ctx)
	var rawConfig []byte
	var closed, stopped bool
	var admitted, sequence int64
	var openBatch *string
	err = tx.QueryRow(ctx, `SELECT config,closed,stopped,admitted,sequence,open_batch_id FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE`, s.namespace, e.Request.RunID).Scan(&rawConfig, &closed, &stopped, &admitted, &sequence, &openBatch)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Admission{}, fail("RUN_NOT_STARTED", "batching run does not exist")
	}
	if err != nil {
		return model.Admission{}, dbError("enroll", err)
	}
	var cfg model.Config
	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		return model.Admission{}, fmt.Errorf("stored run configuration is invalid")
	}
	var previousTarget []byte
	var previousFingerprint, previousBatch, previousDelivery string
	err = tx.QueryRow(ctx, `SELECT target,fingerprint,batch_id,delivery_status FROM nexus_batch_requests WHERE namespace=$1 AND run_id=$2 AND request_id=$3 FOR UPDATE`, s.namespace, e.Request.RunID, e.Request.RequestID).Scan(&previousTarget, &previousFingerprint, &previousBatch, &previousDelivery)
	if err == nil {
		var target model.CompletionTarget
		if json.Unmarshal(previousTarget, &target) != nil {
			return model.Admission{}, fmt.Errorf("stored completion target is invalid")
		}
		if previousFingerprint == e.Fingerprint && reflect.DeepEqual(target, e.Target) {
			return model.Admission{BatchID: previousBatch, Fingerprint: e.Fingerprint, Config: cfg}, nil
		}
		if target.ActivityID == e.Target.ActivityID && target.ActivityRunID == e.Target.ActivityRunID {
			if previousFingerprint == e.Fingerprint && e.Target.Attempt > target.Attempt && previousDelivery != "delivered" && !stopped {
				// A retried submission attempt reattaches to its original immutable
				// membership and result. Only its exact completion target changes.
				rawTarget, _ := json.Marshal(e.Target)
				_, err = tx.Exec(ctx, `UPDATE nexus_batch_requests SET target=$4,delivery_status='pending',delivery_diagnostic='',next_heartbeat_at=clock_timestamp() WHERE namespace=$1 AND run_id=$2 AND request_id=$3`, s.namespace, e.Request.RunID, e.Request.RequestID, rawTarget)
				if err != nil {
					return model.Admission{}, dbError("reattach enrollment", err)
				}
				if err = tx.Commit(ctx); err != nil {
					return model.Admission{}, dbError("commit reattachment", err)
				}
				return model.Admission{BatchID: previousBatch, Fingerprint: e.Fingerprint, Config: cfg}, nil
			}
			return model.Admission{}, fail("STALE_ACTIVITY_ATTEMPT", "request is already fenced to a different Activity attempt")
		}
		return model.Admission{}, fail("DUPLICATE_REQUEST", "request_id is already owned by another operation")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.Admission{}, dbError("enroll", err)
	}
	if closed || stopped {
		return model.Admission{}, fail("RUN_CLOSED", "batching run is closed to new requests")
	}
	if cfg.MaxAdmissions > 0 && admitted >= int64(cfg.MaxAdmissions) {
		return model.Admission{}, fail("RUN_FULL", "batching run admission limit reached")
	}
	line, err := model.JSONLLine(e.Request, cfg)
	if err != nil {
		return model.Admission{}, fail("INVALID_INPUT", "request cannot be serialized")
	}
	if len(line) > cfg.MaxBatchBytes {
		return model.Admission{}, fail("INVALID_INPUT", "request exceeds configured batch byte limit")
	}
	now := time.Now().UTC()
	deadline := e.DeadlineAt
	if deadline.IsZero() {
		// Older callers do not carry the SAA's actual absolute deadline. Preserve
		// their admission behavior, while runtime callers supply DeadlineAt.
		deadline = now.Add(cfg.OperationTimeout)
	}
	if !now.Before(deadline) {
		return model.Admission{}, fail("DEADLINE_EXCEEDED", "request completion deadline has passed")
	}
	batchID := ""
	count, byteCount := 0, 0
	if openBatch != nil {
		batchID = *openBatch
		var sealAt time.Time
		err = tx.QueryRow(ctx, `SELECT requests,bytes,seal_at FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2 AND state='open' FOR UPDATE`, s.namespace, batchID).Scan(&count, &byteCount, &sealAt)
		if err != nil {
			return model.Admission{}, dbError("enroll open batch", err)
		}
		if byteCount+len(line) > cfg.MaxBatchBytes || !now.Before(sealAt) {
			if err = s.seal(ctx, tx, e.Request.RunID, batchID, now); err != nil {
				return model.Admission{}, err
			}
			sequence++
			batchID = ""
			count, byteCount = 0, 0
		}
	}
	if batchID == "" {
		batchID = fmt.Sprintf("%s-%08d", e.Request.RunID, sequence)
		_, err = tx.Exec(ctx, `INSERT INTO nexus_batch_batches(namespace,batch_id,run_id,mode,endpoint,first_admitted_at,seal_at,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, s.namespace, batchID, e.Request.RunID, cfg.Mode, cfg.Endpoint, now, now.Add(cfg.Window), deadline)
		if err != nil {
			return model.Admission{}, dbError("create batch", err)
		}
		if _, err = tx.Exec(ctx, `UPDATE nexus_batch_runs SET open_batch_id=$3,sequence=$4 WHERE namespace=$1 AND run_id=$2`, s.namespace, e.Request.RunID, batchID, sequence); err != nil {
			return model.Admission{}, dbError("open batch", err)
		}
	}
	rawRequest, _ := json.Marshal(e.Request)
	rawTarget, _ := json.Marshal(e.Target)
	_, err = tx.Exec(ctx, `INSERT INTO nexus_batch_requests(namespace,run_id,request_id,batch_id,request,target,activity_id,activity_run_id,fingerprint,admitted_at,position,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, s.namespace, e.Request.RunID, e.Request.RequestID, batchID, rawRequest, rawTarget, e.Target.ActivityID, e.Target.ActivityRunID, e.Fingerprint, now, count, deadline)
	if uniqueViolation(err) {
		return model.Admission{}, fail("DUPLICATE_REQUEST", "completion target is already owned by another request")
	}
	if err != nil {
		return model.Admission{}, dbError("enroll request", err)
	}
	count++
	byteCount += len(line)
	if _, err = tx.Exec(ctx, `UPDATE nexus_batch_batches SET requests=$3,bytes=$4,deadline_at=LEAST(deadline_at,$5) WHERE namespace=$1 AND batch_id=$2`, s.namespace, batchID, count, byteCount, deadline); err != nil {
		return model.Admission{}, dbError("size batch", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE nexus_batch_runs SET admitted=admitted+1 WHERE namespace=$1 AND run_id=$2`, s.namespace, e.Request.RunID); err != nil {
		return model.Admission{}, dbError("count admission", err)
	}
	if count >= cfg.MaxBatchRequests || byteCount >= cfg.MaxBatchBytes {
		if err = s.seal(ctx, tx, e.Request.RunID, batchID, now); err != nil {
			return model.Admission{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return model.Admission{}, dbError("commit enrollment", err)
	}
	return model.Admission{BatchID: batchID, Fingerprint: e.Fingerprint, Config: cfg}, nil
}

// Every operation that touches an open batch acquires the run row first, then
// its batch row. This fixed lock order prevents timer/admission deadlocks.
func (s *Store) seal(ctx context.Context, tx pgx.Tx, runID, batchID string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE nexus_batch_batches SET state='sealed',sealed_at=$3 WHERE namespace=$1 AND batch_id=$2 AND state='open' AND requests>0`, s.namespace, batchID, now)
	if err != nil {
		return dbError("seal batch", err)
	}
	_, err = tx.Exec(ctx, `UPDATE nexus_batch_runs SET open_batch_id=NULL,sequence=sequence+1 WHERE namespace=$1 AND run_id=$2 AND open_batch_id=$3`, s.namespace, runID, batchID)
	return dbError("seal run", err)
}

func (s *Store) FinishRun(ctx context.Context, runID string) error {
	return s.closeRun(ctx, runID, false)
}
func (s *Store) CloseRun(ctx context.Context, runID string) error { return s.FinishRun(ctx, runID) }
func (s *Store) StopRun(ctx context.Context, runID string) error  { return s.closeRun(ctx, runID, true) }
func (s *Store) closeRun(ctx context.Context, runID string, stop bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbError("close run", err)
	}
	defer tx.Rollback(ctx)
	var openBatch *string
	err = tx.QueryRow(ctx, `SELECT open_batch_id FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE`, s.namespace, runID).Scan(&openBatch)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail("RUN_NOT_STARTED", "batching run does not exist")
	}
	if err != nil {
		return dbError("close run", err)
	}
	if openBatch != nil {
		if err = s.seal(ctx, tx, runID, *openBatch, time.Now().UTC()); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE nexus_batch_runs SET closed=true,stopped=stopped OR $3 WHERE namespace=$1 AND run_id=$2`, s.namespace, runID, stop)
	if err != nil {
		return dbError("close run", err)
	}
	if stop {
		_, err = tx.Exec(ctx, `UPDATE nexus_batch_requests SET delivery_status='skipped',delivery_diagnostic='run explicitly stopped' WHERE namespace=$1 AND run_id=$2 AND delivery_status='pending'`, s.namespace, runID)
		if err != nil {
			return dbError("stop delivery", err)
		}
	}
	return dbError("close run", tx.Commit(ctx))
}

func validTarget(t model.CompletionTarget, namespace string) bool {
	return t.Namespace == namespace && t.ActivityID != "" && len(t.ActivityID) <= 1024 && t.ActivityRunID != "" && len(t.ActivityRunID) <= 1024 && t.Attempt > 0 && len(t.TaskToken) > 0 && len(t.TaskToken) <= 16*1024
}
func fail(code, message string) error { return &model.Failure{Code: code, Message: message} }
func uniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}
func dbError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return fmt.Errorf("PostgreSQL %s failed (SQLSTATE %s)", op, pg.Code)
	}
	return fmt.Errorf("PostgreSQL %s failed", op)
}
