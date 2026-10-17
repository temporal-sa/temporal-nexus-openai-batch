-- The Python service owns a separate database; this is its complete schema.
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
CREATE INDEX IF NOT EXISTS nexus_batch_due ON nexus_batch_batches(namespace, mode, endpoint, seal_at)
    WHERE state = 'open';
CREATE INDEX IF NOT EXISTS nexus_batch_outbox ON nexus_batch_batches(namespace, mode, endpoint, sealed_at)
    WHERE state = 'sealed' AND NOT dispatched;
CREATE INDEX IF NOT EXISTS nexus_batch_run_batches ON nexus_batch_batches(namespace, run_id, batch_id);
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
CREATE INDEX IF NOT EXISTS nexus_batch_request_manifest ON nexus_batch_requests(namespace, batch_id, position);
CREATE INDEX IF NOT EXISTS nexus_batch_pending_delivery ON nexus_batch_requests(namespace, batch_id, position)
    WHERE delivery_status = 'pending' AND outcome IS NOT NULL;
CREATE INDEX IF NOT EXISTS nexus_batch_pending_heartbeat ON nexus_batch_requests(namespace, next_heartbeat_at)
    WHERE delivery_status = 'pending';
