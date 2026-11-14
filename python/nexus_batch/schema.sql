-- The Python service owns a separate database; this is its complete schema.
-- PostgreSQL is the external buffer and recovery ledger. Large manifests and
-- per-request completion capabilities never become batch Workflow payloads.
-- Namespace is part of each identity so one database can separate Temporal
-- namespaces. Mode and endpoint route background work to the matching workers.

-- One configuration snapshot per run. Admission takes this row's lock before
-- deciding membership, which serializes that decision within the run.
-- closed rejects new requests and flushes a partial batch; stopped additionally
-- fences future dispatch/submission and stops the keeper for this run.
CREATE TABLE IF NOT EXISTS nexus_batch_runs (
    namespace text NOT NULL,
    run_id text NOT NULL,
    config jsonb NOT NULL,
    mode text NOT NULL,
    endpoint text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    stopped boolean NOT NULL DEFAULT false,
    admitted bigint NOT NULL DEFAULT 0,
    sequence bigint NOT NULL DEFAULT 1,
    open_batch_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, run_id)
);

-- Collection progresses open -> sealed -> delivering -> completed. Sealing
-- freezes membership and timestamps; only a small BatchRef crosses into Temporal.
-- requests/bytes describe the uploaded JSONL, not the size of stored JSON.
-- deadline_at is the earliest enrolled recipient deadline and may only shorten.
-- dispatched + dispatch_claim_until form the leased Workflow-start outbox.
-- outcomes_saved makes a retried results Activity keep its first committed set.
CREATE TABLE IF NOT EXISTS nexus_batch_batches (
    namespace text NOT NULL,
    batch_id text NOT NULL,
    run_id text NOT NULL,
    mode text NOT NULL,
    endpoint text NOT NULL,
    state text NOT NULL DEFAULT 'open',
    requests integer NOT NULL DEFAULT 0,
    bytes integer NOT NULL DEFAULT 0,
    first_admitted_at timestamptz NOT NULL,
    seal_at timestamptz NOT NULL,
    sealed_at timestamptz,
    deadline_at timestamptz NOT NULL,
    dispatched boolean NOT NULL DEFAULT false,
    dispatch_claim_until timestamptz,
    outcomes_saved boolean NOT NULL DEFAULT false,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    report jsonb,
    PRIMARY KEY (namespace, batch_id),
    FOREIGN KEY (namespace, run_id) REFERENCES nexus_batch_runs(namespace, run_id)
);
-- Partial indexes keep window closing and dispatch scans focused on pending work.
CREATE INDEX IF NOT EXISTS nexus_batch_due ON nexus_batch_batches(namespace, mode, endpoint, seal_at)
    WHERE state = 'open';
CREATE INDEX IF NOT EXISTS nexus_batch_outbox ON nexus_batch_batches(namespace, mode, endpoint, sealed_at)
    WHERE state = 'sealed' AND NOT dispatched;
CREATE INDEX IF NOT EXISTS nexus_batch_run_batches ON nexus_batch_batches(namespace, run_id, batch_id);

-- One row per admitted logical request. request/fingerprint/batch/position and
-- deadline retain the original admission; target can advance to a newer attempt
-- of that same SAA execution without adding another manifest member.
-- target contains an opaque task token and must be treated as a capability.
-- outcome + delivery_status form the delivery outbox. A completion checkpoint
-- compares the entire expected target to fence workers holding an older token.
-- The two identity constraints prevent request-ID reuse and SAA-target reuse.
CREATE TABLE IF NOT EXISTS nexus_batch_requests (
    namespace text NOT NULL,
    run_id text NOT NULL,
    request_id text NOT NULL,
    batch_id text NOT NULL,
    request jsonb NOT NULL,
    target jsonb NOT NULL,
    activity_id text NOT NULL,
    activity_run_id text NOT NULL,
    fingerprint text NOT NULL,
    admitted_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    next_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    position integer NOT NULL,
    outcome jsonb,
    delivery_status text NOT NULL DEFAULT 'pending',
    delivery_diagnostic text NOT NULL DEFAULT '',
    PRIMARY KEY (namespace, run_id, request_id),
    UNIQUE (namespace, activity_id, activity_run_id),
    FOREIGN KEY (namespace, batch_id) REFERENCES nexus_batch_batches(namespace, batch_id)
);
-- Manifest order is stable for upload, but provider output is joined by custom_id.
CREATE INDEX IF NOT EXISTS nexus_batch_request_manifest ON nexus_batch_requests(namespace, batch_id, position);
CREATE INDEX IF NOT EXISTS nexus_batch_pending_delivery ON nexus_batch_requests(namespace, batch_id, position)
    WHERE delivery_status = 'pending' AND outcome IS NOT NULL;
-- The keeper claims due rows with SKIP LOCKED and postpones their next heartbeat;
-- this index includes requests whose provider outcome is not available yet.
CREATE INDEX IF NOT EXISTS nexus_batch_pending_heartbeat ON nexus_batch_requests(namespace, next_heartbeat_at)
    WHERE delivery_status = 'pending';

-- Bulk manifests are immutable external artifacts. Their contents and portable
-- output schemas never cross the Nexus/Workflow boundary.
CREATE TABLE IF NOT EXISTS nexus_batch_bulk_inputs (
    namespace text NOT NULL,
    manifest_id text NOT NULL,
    digest text NOT NULL DEFAULT '',
    item_count integer NOT NULL DEFAULT 0,
    bytes bigint NOT NULL DEFAULT 0,
    output_schema jsonb,
    sealed boolean NOT NULL DEFAULT false,
    PRIMARY KEY(namespace, manifest_id)
);
CREATE TABLE IF NOT EXISTS nexus_batch_bulk_input_items (
    namespace text NOT NULL,
    manifest_id text NOT NULL,
    position integer NOT NULL,
    custom_id text NOT NULL,
    body jsonb NOT NULL,
    bytes integer NOT NULL,
    PRIMARY KEY(namespace, manifest_id, position),
    UNIQUE(namespace, manifest_id, custom_id),
    FOREIGN KEY(namespace, manifest_id) REFERENCES nexus_batch_bulk_inputs(namespace, manifest_id)
        ON DELETE CASCADE
);

-- One exact asynchronous completion recipient for an entire bulk submission.
CREATE TABLE IF NOT EXISTS nexus_batch_bulk_submissions (
    namespace text NOT NULL,
    submission_id text NOT NULL,
    run_id text NOT NULL,
    input_ref jsonb NOT NULL,
    target jsonb NOT NULL,
    activity_id text NOT NULL,
    activity_run_id text NOT NULL,
    deadline_at timestamptz NOT NULL,
    admitted integer NOT NULL DEFAULT 0,
    admission_complete boolean NOT NULL DEFAULT false,
    result_ref jsonb,
    delivery_status text NOT NULL DEFAULT 'pending',
    delivery_diagnostic text NOT NULL DEFAULT '',
    next_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(namespace, submission_id),
    UNIQUE(namespace, activity_id, activity_run_id),
    FOREIGN KEY(namespace, run_id) REFERENCES nexus_batch_runs(namespace, run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS nexus_batch_bulk_pending ON nexus_batch_bulk_submissions(namespace,run_id)
    WHERE delivery_status='pending';

-- Snapshots retain metadata and visible item positions, not copies of raw
-- outcomes. An earlier partial reference remains stable as more results arrive.
CREATE TABLE IF NOT EXISTS nexus_batch_bulk_snapshots (
    namespace text NOT NULL,
    submission_id text NOT NULL,
    digest text NOT NULL,
    as_of timestamptz NOT NULL,
    reference jsonb NOT NULL,
    PRIMARY KEY(namespace,submission_id,digest),
    FOREIGN KEY(namespace,submission_id)
        REFERENCES nexus_batch_bulk_submissions(namespace,submission_id) ON DELETE CASCADE
);

-- These additive migrations also upgrade an existing single-request database.
ALTER TABLE nexus_batch_requests ADD COLUMN IF NOT EXISTS bulk_id text;
ALTER TABLE nexus_batch_requests ADD COLUMN IF NOT EXISTS bulk_position integer;
ALTER TABLE nexus_batch_requests ADD COLUMN IF NOT EXISTS outcome_saved_at timestamptz;
ALTER TABLE nexus_batch_requests ALTER COLUMN target DROP NOT NULL;
ALTER TABLE nexus_batch_requests ALTER COLUMN activity_id DROP NOT NULL;
ALTER TABLE nexus_batch_requests ALTER COLUMN activity_run_id DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS nexus_batch_bulk_positions
    ON nexus_batch_requests(namespace,bulk_id,bulk_position) WHERE bulk_id IS NOT NULL;

ALTER TABLE nexus_batch_runs ADD COLUMN IF NOT EXISTS reserved bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS nexus_batch_bulk_snapshot_items (
    namespace text NOT NULL,
    submission_id text NOT NULL,
    digest text NOT NULL,
    position integer NOT NULL,
    PRIMARY KEY(namespace,submission_id,digest,position),
    FOREIGN KEY(namespace,submission_id,digest)
        REFERENCES nexus_batch_bulk_snapshots(namespace,submission_id,digest) ON DELETE CASCADE
);
ALTER TABLE nexus_batch_bulk_submissions
    ADD COLUMN IF NOT EXISTS admission_aborted boolean NOT NULL DEFAULT false;
ALTER TABLE nexus_batch_bulk_input_items
    ADD COLUMN IF NOT EXISTS content_digest text NOT NULL DEFAULT '';
