ALTER TABLE nexus_batch_requests ADD COLUMN deadline_at timestamptz;
ALTER TABLE nexus_batch_requests ADD COLUMN next_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE nexus_batch_batches ADD COLUMN deadline_at timestamptz;

-- Version 1 did not retain the recipient's original SAA deadline. Its existing
-- admission-based deadline is the only recoverable approximation for old rows.
UPDATE nexus_batch_requests q
SET deadline_at = q.admitted_at + ((r.config->>'operation_timeout')::double precision / 1000000000) * interval '1 second'
FROM nexus_batch_runs r
WHERE r.namespace=q.namespace AND r.run_id=q.run_id;

UPDATE nexus_batch_batches b
SET deadline_at = COALESCE(
    (SELECT MIN(q.deadline_at) FROM nexus_batch_requests q WHERE q.namespace=b.namespace AND q.batch_id=b.batch_id),
    b.first_admitted_at + ((r.config->>'operation_timeout')::double precision / 1000000000) * interval '1 second')
FROM nexus_batch_runs r
WHERE r.namespace=b.namespace AND r.run_id=b.run_id;

ALTER TABLE nexus_batch_requests ALTER COLUMN deadline_at SET NOT NULL;
ALTER TABLE nexus_batch_batches ALTER COLUMN deadline_at SET NOT NULL;

CREATE INDEX nexus_batch_pending_heartbeat ON nexus_batch_requests(namespace,next_heartbeat_at)
    WHERE delivery_status='pending';
