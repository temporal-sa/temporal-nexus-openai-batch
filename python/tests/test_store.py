"""Store tests use an isolated namespace in the separate Python database."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from nexus_batch.models import (
    BatchReport,
    CompletionTarget,
    Config,
    Enrollment,
    Failure,
    Outcome,
    Request,
    Result,
    RunInput,
    fingerprint,
    jsonl_line,
)
from nexus_batch.store import Store


def utcnow():
    return datetime.now(timezone.utc)


def enrollment(store, run_id="run", request_id="request", *, attempt=1, token=None, deadline=None):
    request = Request(run_id, request_id, "hello")
    target = CompletionTarget(
        store.namespace,
        f"activity-{run_id}-{request_id}",
        "execution",
        token or f"token-{attempt}".encode(),
        attempt,
    )
    return Enrollment(request, target, fingerprint(request), deadline or utcnow() + timedelta(seconds=120))


@pytest.fixture
async def store():
    dsn = os.environ.get("PYTHON_DATABASE_URL")
    if not dsn:
        pytest.skip("set PYTHON_DATABASE_URL to test the real PostgreSQL store")
    result = await Store.connect(dsn, f"python-test-{uuid4().hex}")
    try:
        yield result
    finally:
        async with result.pool.acquire() as conn:
            async with conn.transaction():
                for table in ("nexus_batch_requests", "nexus_batch_batches", "nexus_batch_runs"):
                    await conn.execute(f"DELETE FROM {table} WHERE namespace=$1", result.namespace)
        await result.close()


async def test_connection_errors_do_not_expose_credentials(monkeypatch):
    async def fail(*args, **kwargs):
        raise ValueError("postgres://user:confidential-password@host invalid connection string")

    monkeypatch.setattr("nexus_batch.store.asyncpg.create_pool", fail)
    with pytest.raises(RuntimeError) as error:
        await Store.connect("postgres://user:confidential-password@host/%zz")
    assert "confidential" not in str(error.value)
    assert "postgres://" not in str(error.value)


async def test_concurrent_admission_is_per_run_idempotent_and_bounded(store):
    config = replace(Config.defaults(), max_batch_requests=3)
    await store.start_run(RunInput("run", config))
    requests = [enrollment(store, request_id=f"r{i:02d}") for i in range(25)]
    admissions = await asyncio.gather(*(store.enroll(item) for item in requests))
    duplicate = await store.enroll(requests[0])
    assert duplicate == admissions[0]
    status = await store.status("run")
    assert (status.admitted, status.buffered, len(status.batches)) == (25, 1, 8)
    assert (status.backing, status.storage, status.completion_mode) == (
        "nexus_standalone_activity",
        "postgres",
        "external_async_completion",
    )
    await store.close_run("run")
    refs = await store.ready_batches("fake", config.nexus_endpoint)
    assert len(refs) == 9
    assert sum(ref.requests for ref in refs) == 25
    assert all(0 < ref.requests <= 3 for ref in refs)
    seen = set()
    for ref in refs:
        manifest = await store.load_manifest(ref.batch_id)
        assert manifest.bytes == sum(len(jsonl_line(entry.request, config)) for entry in manifest.entries)
        seen.update(entry.request.request_id for entry in manifest.entries)
    assert seen == {item.request.request_id for item in requests}
    assert await store.get_run("run") == RunInput("run", config)
    assert (await store.status("run")).closed
    with pytest.raises(Failure, match="RUN_CLOSED"):
        await store.enroll(enrollment(store, request_id="new"))
    assert await store.enroll(requests[0]) == duplicate
    await store.start_run(RunInput("other", config))
    other = await store.enroll(enrollment(store, run_id="other"))
    assert other.batch_id not in {ref.batch_id for ref in refs}


async def test_byte_overflow_fixed_window_and_admission_limit(store):
    template = enrollment(store, request_id="r1")
    line_bytes = len(jsonl_line(template.request, Config.defaults()))
    config = replace(Config.defaults(), max_batch_bytes=line_bytes * 2 - 1, max_admissions=3)
    await store.start_run(RunInput("run", config))
    first = await store.enroll(template)
    second = await store.enroll(enrollment(store, request_id="r2"))
    assert first.batch_id != second.batch_id
    assert len((await store.load_manifest(first.batch_id)).entries) == 1
    async with store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE nexus_batch_batches SET seal_at=clock_timestamp()-interval '1 second' "
            "WHERE namespace=$1 AND batch_id=$2",
            store.namespace,
            second.batch_id,
        )
    third = await store.enroll(enrollment(store, request_id="r3"))
    assert third.batch_id != second.batch_id
    assert (await store.status("run")).buffered == 1
    with pytest.raises(Failure, match="RUN_FULL"):
        await store.enroll(enrollment(store, request_id="r4"))
    # Admission limits alone leave the last partial batch open.
    with pytest.raises(Failure, match="BATCH_NOT_SEALED"):
        await store.load_manifest(third.batch_id)
    async with store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE nexus_batch_batches SET seal_at=clock_timestamp()-interval '1 second' "
            "WHERE namespace=$1 AND batch_id=$2",
            store.namespace,
            third.batch_id,
        )
    assert await store.seal_due("fake", "other-endpoint") == 0
    assert await store.seal_due("live", config.nexus_endpoint) == 0
    assert await store.seal_due("fake", config.nexus_endpoint) == 1
    assert (await store.load_manifest(third.batch_id)).entries[0].request.request_id == "r3"
    # Hitting the byte cap exactly seals after that admission.
    exact = replace(Config.defaults(), max_batch_bytes=line_bytes)
    await store.start_run(RunInput("exact", exact))
    admitted = await store.enroll(enrollment(store, run_id="exact", request_id="r1"))
    assert len((await store.load_manifest(admitted.batch_id)).entries) == 1


async def test_deadlines_validation_and_namespace_fencing(store):
    await store.start_run(RunInput("run", Config.defaults()))
    with pytest.raises(Failure, match="RUN_EXISTS"):
        await store.start_run(RunInput("run", Config.defaults()))
    expired = enrollment(store, deadline=utcnow() - timedelta(seconds=1))
    with pytest.raises(Failure, match="DEADLINE_EXCEEDED"):
        await store.enroll(expired)
    cross_namespace = replace(
        expired,
        deadline_at=utcnow() + timedelta(seconds=100),
        completion_target=replace(expired.completion_target, namespace="other"),
    )
    with pytest.raises(Failure, match="INVALID_INPUT"):
        await store.enroll(cross_namespace)
    with pytest.raises(Failure, match="INVALID_INPUT"):
        await store.enroll(
            replace(cross_namespace, completion_target=replace(expired.completion_target, task_token=b""))
        )
    early = utcnow() + timedelta(seconds=60)
    e1 = enrollment(store, request_id="one", deadline=early)
    e2 = enrollment(store, request_id="two", deadline=utcnow() + timedelta(seconds=90))
    one, two = await store.enroll(e1), await store.enroll(e2)
    assert one.batch_id == two.batch_id
    await store.close_run("run")
    refs = await store.ready_batches("fake", Config.defaults().nexus_endpoint)
    assert refs[0].deadline_at == early
    other_namespace = Store(store.pool, "other-namespace")
    assert await other_namespace.ready_batches("fake", Config.defaults().nexus_endpoint) == []
    with pytest.raises(Failure, match="BATCH_NOT_FOUND"):
        await other_namespace.load_manifest(one.batch_id)
    with pytest.raises(Failure, match="RUN_NOT_STARTED"):
        await other_namespace.get_run("run")


async def test_outcomes_are_atomic_first_writer_and_attempt_fenced(store):
    config = replace(Config.defaults(), max_batch_requests=2)
    await store.start_run(RunInput("run", config))
    first, second = enrollment(store, request_id="one"), enrollment(store, request_id="two")
    admission = await store.enroll(first)
    await store.enroll(second)
    outcomes = [
        Outcome(
            item.request.request_id,
            item.fingerprint,
            admission.batch_id,
            result=Result(item.request.request_id, "original"),
        )
        for item in (first, second)
    ]
    with pytest.raises(Failure, match="INVALID_RESULT"):
        await store.save_outcomes(admission.batch_id, outcomes[:1], [])
    with pytest.raises(Failure, match="INVALID_RESULT"):
        await store.save_outcomes(
            admission.batch_id, [outcomes[0], replace(outcomes[1], fingerprint="invalid")], []
        )
    assert await store.pending_deliveries(admission.batch_id) == []
    await store.save_outcomes(admission.batch_id, outcomes, ["downloaded"])
    await store.fail_batch(admission.batch_id, "RETRY", "must not replace committed outcomes")
    assert all(
        row.outcome.result.text == "original" for row in await store.pending_deliveries(admission.batch_id)
    )
    await store.mark_delivered(
        admission.batch_id, "one", first.completion_target, False, "recipient unavailable"
    )
    # A later SAA attempt keeps its old membership, outcome and deadline, including after closing.
    await store.close_run("run")
    newer = replace(
        first,
        completion_target=replace(first.completion_target, attempt=2, task_token=b"new-token"),
        deadline_at=utcnow() + timedelta(seconds=300),
    )
    assert (await store.enroll(newer)).batch_id == admission.batch_id
    assert (await store.status("run")).admitted == 2
    await store.mark_delivered(admission.batch_id, "one", first.completion_target, False, "stale completion")
    pending = await store.pending_deliveries(admission.batch_id)
    assert len(pending) == 2
    assert pending[0].completion_target == newer.completion_target
    assert pending[0].outcome == outcomes[0]
    with pytest.raises(Failure, match="STALE_ACTIVITY_ATTEMPT"):
        await store.enroll(first)
    assert await store.pending_completions("fake", config.nexus_endpoint) == []
    await store.save_report(BatchReport(batch_id=admission.batch_id, status="completed", delivered=99))
    repair = await store.pending_completions("fake", config.nexus_endpoint)
    assert len(repair) == 2
    assert repair[0].batch_id == admission.batch_id
    await store.mark_delivered(admission.batch_id, "one", newer.completion_target, True, "")
    await store.mark_delivered(
        admission.batch_id, "one", newer.completion_target, False, "must remain delivered"
    )
    await store.mark_delivered(admission.batch_id, "two", second.completion_target, False, "gone")
    summary = await store.delivery_summary(admission.batch_id)
    assert (summary.succeeded, summary.failed, summary.delivered, summary.skipped) == (2, 0, 1, 1)
    assert summary.diagnostics == ["downloaded", "gone"]
    status = await store.status("run")
    assert (status.batches[0].delivered, status.batches[0].skipped) == (1, 1)
    with pytest.raises(Failure, match="STALE_ACTIVITY_ATTEMPT"):
        await store.enroll(replace(newer, completion_target=replace(newer.completion_target, attempt=3)))
    assert await store.pending_completions("fake", config.nexus_endpoint) == []


async def test_dispatch_and_heartbeat_claims_have_leases_and_ownership(store):
    config = replace(Config.defaults(), max_batch_requests=1)
    await store.start_run(RunInput("run", config))
    original = enrollment(store)
    admitted = await store.enroll(original)
    assert await store.ready_batches("fake", "other-endpoint") == []
    assert await store.ready_batches("live", config.nexus_endpoint) == []
    a, b = await asyncio.gather(
        store.ready_batches("fake", config.nexus_endpoint), store.ready_batches("fake", config.nexus_endpoint)
    )
    assert len(a) + len(b) == 1
    assert await store.ready_batches("fake", config.nexus_endpoint) == []
    async with store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE nexus_batch_batches SET dispatch_claim_until=clock_timestamp()-interval '1 second' "
            "WHERE namespace=$1 AND batch_id=$2",
            store.namespace,
            admitted.batch_id,
        )
    assert len(await store.ready_batches("fake", config.nexus_endpoint)) == 1
    await store.mark_dispatched(admitted.batch_id)
    assert await store.ready_batches("fake", config.nexus_endpoint) == []
    assert await store.pending_heartbeats("fake", "other-endpoint") == []
    a, b = await asyncio.gather(
        store.pending_heartbeats("fake", config.nexus_endpoint),
        store.pending_heartbeats("fake", config.nexus_endpoint),
    )
    assert a + b == [original.completion_target]
    assert await store.pending_heartbeats("fake", config.nexus_endpoint) == []
    newer = replace(
        original, completion_target=replace(original.completion_target, attempt=2, task_token=b"new")
    )
    await store.enroll(newer)
    assert await store.pending_heartbeats("fake", config.nexus_endpoint) == [newer.completion_target]
    await store.stop_run("run")
    assert await store.pending_heartbeats("fake", config.nexus_endpoint) == []
    batches, targets = await store.run_resources("run")
    assert batches == [admitted.batch_id]
    assert targets == [newer.completion_target]
    assert (await store.delivery_summary(admitted.batch_id)).skipped == 1
    with pytest.raises(Failure, match="STALE_ACTIVITY_ATTEMPT"):
        await store.enroll(replace(newer, completion_target=replace(newer.completion_target, attempt=3)))


async def test_submission_serializes_stop_and_releases_lock_after_error(store):
    config = replace(Config.defaults(), max_batch_requests=1)
    await store.start_run(RunInput("run", config))
    admitted = await store.enroll(enrollment(store))
    stop = None
    async with store.submission(admitted.batch_id):
        stop = asyncio.create_task(store.stop_run("run"))
        # Stop cannot commit until this provider-create critical section exits.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(stop), timeout=0.05)
    await asyncio.wait_for(stop, timeout=2)
    with pytest.raises(Failure, match="RUN_STOPPED"):
        async with store.submission(admitted.batch_id):
            pytest.fail("stopped run reached provider creation")
    await store.start_run(RunInput("error", config))
    other = await store.enroll(enrollment(store, run_id="error"))
    with pytest.raises(ValueError, match="provider failure"):
        async with store.submission(other.batch_id):
            raise ValueError("provider failure")
    await asyncio.wait_for(store.stop_run("error"), timeout=2)
    await store.start_run(RunInput("expired", config))
    expired = await store.enroll(enrollment(store, run_id="expired"))
    async with store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE nexus_batch_batches SET deadline_at=clock_timestamp()-interval '1 second' "
            "WHERE namespace=$1 AND batch_id=$2",
            store.namespace,
            expired.batch_id,
        )
    with pytest.raises(Failure, match="DEADLINE_EXCEEDED"):
        async with store.submission(expired.batch_id):
            pytest.fail("expired batch reached provider creation")
