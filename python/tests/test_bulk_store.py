"""Real-PostgreSQL checks for immutable artifacts and bulk recipient ownership."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from nexus_batch.bulk_models import BulkRequest, BulkSubmission, provider_id
from nexus_batch.bulk_store import BulkStore
from nexus_batch.models import (
    CompletionTarget,
    Config,
    Enrollment,
    Failure,
    Outcome,
    Request,
    Result,
    RunInput,
    dumps,
    fingerprint,
)
from nexus_batch.store import Store


@pytest.fixture
async def bulk():
    dsn = os.environ.get("PYTHON_DATABASE_URL")
    if not dsn:
        pytest.skip("set PYTHON_DATABASE_URL to test the real PostgreSQL store")
    store = await Store.connect(dsn, "bulk-store-" + uuid4().hex)
    try:
        yield BulkStore(store)
    finally:
        async with store.pool.acquire() as conn:
            async with conn.transaction():
                for table in (
                    "nexus_batch_requests",
                    "nexus_batch_batches",
                    "nexus_batch_runs",
                    "nexus_batch_bulk_inputs",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE namespace=$1", store.namespace)
        await store.close()


def deadline():
    return datetime.now(timezone.utc) + timedelta(minutes=5)


def target(bulk, name="bulk", attempt=1):
    return CompletionTarget(bulk.namespace, name, "execution", f"{name}-{attempt}".encode(), attempt)


async def attach(bulk, submission_id, items, *, run_id="run", schema=None):
    ref = await bulk.write_input(items, schema)
    submission = BulkSubmission(run_id, submission_id, ref)
    await bulk.attach(submission, target(bulk, submission_id), deadline())
    return submission


async def finish_admission(bulk, submission_id, limit=100):
    while not await bulk.admit_page(submission_id, limit):
        pass


async def save_successes(bulk, batch_id, text="stored"):
    manifest = await bulk.store.load_manifest(batch_id)
    outcomes = [
        Outcome(
            entry.request.request_id,
            entry.fingerprint,
            batch_id,
            result=Result(entry.request.request_id, "" if entry.request.bulk_id else text),
            bulk={"status": "succeeded", "response": {"output_text": text}, "output": {"value": text}}
            if entry.request.bulk_id
            else None,
        )
        for entry in manifest.entries
    ]
    await bulk.store.save_outcomes(batch_id, outcomes, [])


async def test_manifests_reject_duplicates_and_tampering_before_admission(bulk):
    await bulk.store.start_run(RunInput("run", Config.defaults()))
    with pytest.raises(Failure, match="DUPLICATE_REQUEST"):
        await bulk.write_input([BulkRequest("same", {"input": "one"}), BulkRequest("same", {"input": "two"})])
    ref = await bulk.write_input([BulkRequest("one", {"input": "original"})])
    with pytest.raises(Failure, match="INVALID_INPUT"):
        await bulk.read_input(replace(ref, digest="0" * 64))
    async with bulk.store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE nexus_batch_bulk_input_items SET body=$3 WHERE namespace=$1 AND manifest_id=$2",
            bulk.namespace,
            ref.manifest_id,
            json.dumps({"input": "changed"}),
        )
    with pytest.raises(Failure, match="digest"):
        await bulk.attach(BulkSubmission("run", "tampered", ref), target(bulk), deadline())
    assert (await bulk.store.status("run")).admitted == 0
    invalid = await bulk.write_input(
        [
            BulkRequest("valid", {"input": "first"}),
            BulkRequest("bad", {"input": "last", "model": "wrong"}),
        ]
    )
    with pytest.raises(Failure, match="model"):
        await bulk.attach(BulkSubmission("run", "invalid", invalid), target(bulk), deadline())
    assert (await bulk.store.status("run")).admitted == 0


async def test_bulk_and_single_requests_share_batches_and_keep_ownership(bulk):
    config = replace(Config.defaults(), max_batch_requests=3)
    await bulk.store.start_run(RunInput("run", config))
    one = await attach(bulk, "first", [BulkRequest("same", {"input": "one"})])
    two = await attach(bulk, "second", [BulkRequest("same", {"input": "two"})])
    await finish_admission(bulk, one.submission_id)
    await finish_admission(bulk, two.submission_id)
    request = Request("run", "ordinary", "three")
    admission = await bulk.store.enroll(
        Enrollment(request, target(bulk, "ordinary"), fingerprint(request), deadline())
    )
    manifest = await bulk.store.load_manifest(admission.batch_id)
    assert len(manifest.entries) == 3
    assert len({entry.request.request_id for entry in manifest.entries}) == 3
    assert sum(entry.completion_target is None for entry in manifest.entries) == 2
    await save_successes(bulk, admission.batch_id)
    deliveries = await bulk.store.pending_deliveries(admission.batch_id)
    assert len(deliveries) == 1 and deliveries[0].request_id == "ordinary"
    ready = await bulk.pending_completions("fake", config.nexus_endpoint)
    assert {row.submission_id for row in ready} == {"first", "second"}
    for row in ready:
        items = await bulk.read_results(row.result)
        assert len(items) == 1 and items[0].custom_id == "same" and items[0].ok
    assert await bulk.pending_completions("live", config.nexus_endpoint) == []


async def test_bulk_retry_reattachment_preserves_deadline_and_fences_completion(bulk):
    config = replace(Config.defaults(), max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    submission = await attach(
        bulk, "attempts", [BulkRequest("a", {"input": "one"}), BulkRequest("b", {"input": "two"})]
    )
    await bulk.admit_page("attempts", 1)
    original = target(bulk, "attempts")
    async with bulk.store.pool.acquire() as conn:
        old_deadline = await conn.fetchval(
            "SELECT deadline_at FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id='attempts'",
            bulk.namespace,
        )
    await bulk.attach(submission, original, deadline())
    newer = target(bulk, "attempts", 2)
    await bulk.attach(submission, newer, deadline() + timedelta(hours=1))
    await finish_admission(bulk, "attempts", 1)
    assert await bulk.admit_page("attempts", 1)
    assert (await bulk.store.status("run")).admitted == 2
    with pytest.raises(Failure, match="STALE_ACTIVITY_ATTEMPT"):
        await bulk.attach(submission, original, deadline())
    for batch in await bulk.store.ready_batches("fake", config.nexus_endpoint):
        await save_successes(bulk, batch.batch_id)
    ready = await bulk.pending_completions("fake", config.nexus_endpoint)
    assert len(ready) == 1 and ready[0].completion_target == newer
    await bulk.mark_delivered("attempts", original, True, "stale")
    assert len(await bulk.pending_completions("fake", config.nexus_endpoint)) == 1
    await bulk.mark_delivered("attempts", newer, True, "")
    assert await bulk.pending_completions("fake", config.nexus_endpoint) == []
    async with bulk.store.pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT deadline_at FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id='attempts'",
                bulk.namespace,
            )
            == old_deadline
        )


async def test_partial_reference_ignores_outcome_committed_after_snapshot(bulk):
    config = replace(Config.defaults(), max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    await attach(bulk, "partial", [BulkRequest("a", {"input": "document"})])
    await finish_admission(bulk, "partial")
    batch = (await bulk.store.ready_batches("fake", config.nexus_endpoint))[0]
    manifest = await bulk.store.load_manifest(batch.batch_id)
    entry = manifest.entries[0]
    outcome = Outcome(
        entry.request.request_id,
        entry.fingerprint,
        batch.batch_id,
        result=Result(entry.request.request_id, ""),
        bulk={"status": "succeeded", "response": {"text": "later"}, "output": "later"},
    )
    async with bulk.store.pool.acquire() as conn:
        async with conn.transaction():
            # Timestamp exists before the snapshot, but this update is uncommitted
            # and therefore must remain absent from the immutable partial result.
            await conn.execute(
                """UPDATE nexus_batch_requests SET outcome=$3,outcome_saved_at=clock_timestamp()
                WHERE namespace=$1 AND bulk_id=$2""",
                bulk.namespace,
                "partial",
                dumps(outcome),
            )
            partial = await bulk.get_result("partial", partial=True)
            assert (await bulk.read_results(partial))[0].status == "pending"
        assert (await bulk.read_results(partial))[0].status == "pending"
    complete = await bulk.get_result("partial")
    assert complete.digest != partial.digest
    assert (await bulk.read_results(complete))[0].output == "later"
    assert (await bulk.read_results(partial))[0].status == "pending"


async def test_bulk_manifest_pages_are_byte_bounded_and_reserve_admission(bulk):
    config = replace(Config.defaults(), max_admissions=6, max_batch_bytes=2_000_000)
    await bulk.store.start_run(RunInput("run", config))
    requests = [BulkRequest(str(i), {"input": "x" * 700_000}) for i in range(6)]
    submission = await attach(bulk, "large", requests)
    page = await bulk.read_input(submission.requests, limit=1000)
    assert 1 <= len(page) < 6
    assert len(dumps(page).encode()) < 4 * 1024 * 1024
    ordinary = Request("run", "ordinary", "capacity already reserved")
    with pytest.raises(Failure, match="RUN_FULL"):
        await bulk.store.enroll(Enrollment(ordinary, target(bulk), fingerprint(ordinary), deadline()))
    await finish_admission(bulk, "large")
    assert (await bulk.store.status("run")).admitted == 6
    await bulk.store.stop_run("run")
    assert await bulk.pending_heartbeats("fake", config.nexus_endpoint) == []
    _, targets = await bulk.store.run_resources("run")
    assert targets == [target(bulk, "large")]
    with pytest.raises(Failure, match="INVALID_INPUT"):
        await bulk.read_input(submission.requests, limit=0)
    assert provider_id("large", 0) != provider_id("different", 0)


async def test_aborted_and_expired_admission_release_reserved_capacity_once(bulk):
    config = replace(Config.defaults(), max_admissions=4, max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    submission = await attach(bulk, "abort", [BulkRequest(str(i), {"input": "body"}) for i in range(4)])
    await bulk.admit_page("abort", 1)
    original = target(bulk, "abort")
    newer = target(bulk, "abort", 2)
    await bulk.attach(submission, newer, deadline())
    await bulk.abort_admission("abort", original, "stale cancellation")
    async with bulk.store.pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT reserved FROM nexus_batch_runs WHERE namespace=$1 AND run_id='run'",
                bulk.namespace,
            )
            == 3
        )
    await bulk.abort_admission("abort", newer, "cancelled")
    await bulk.abort_admission("abort", newer, "duplicate cancellation")
    with pytest.raises(Failure, match="ADMISSION_ABORTED"):
        await bulk.admit_page("abort")
    request = Request("run", "ordinary", "capacity was released")
    await bulk.store.enroll(Enrollment(request, target(bulk), fingerprint(request), deadline()))
    await attach(bulk, "expiry", [BulkRequest("remaining", {"input": "later"})])
    async with bulk.store.pool.acquire() as conn:
        await conn.execute(
            """UPDATE nexus_batch_bulk_submissions SET deadline_at=clock_timestamp()-interval '1 second'
            WHERE namespace=$1 AND submission_id='expiry'""",
            bulk.namespace,
        )
    await bulk.pending_heartbeats("fake", config.nexus_endpoint)
    async with bulk.store.pool.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT reserved FROM nexus_batch_runs WHERE namespace=$1 AND run_id='run'",
                bulk.namespace,
            )
            == 0
        )


async def test_keeper_does_not_mask_an_incomplete_admission_worker_loss(bulk):
    config = replace(Config.defaults(), max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    await attach(
        bulk, "handoff", [BulkRequest("one", {"input": "one"}), BulkRequest("two", {"input": "two"})]
    )
    # The executing Activity owns its heartbeat until the entire manifest has
    # been admitted. The keeper must permit a crashed worker to time out/retry.
    assert await bulk.pending_heartbeats("fake", config.nexus_endpoint) == []
    assert not await bulk.admit_page("handoff", 1)
    assert await bulk.pending_heartbeats("fake", config.nexus_endpoint) == []
    assert await bulk.admit_page("handoff", 1)
    assert await bulk.pending_heartbeats("fake", config.nexus_endpoint) == [target(bulk, "handoff")]
    assert await bulk.pending_heartbeats("fake", config.nexus_endpoint) == []


async def test_finite_json_numbers_keep_manifest_and_result_identity_through_jsonb(bulk):
    from nexus_batch.bulk_models import canonical_json

    numbers = [1e20, 3.3e30, 1.0000000000000002e20, 1.0, -0.0, 1e-8]
    body = {"input": "numbers are externally stored", "extension": {"values": numbers}}
    schema = {"type": "object", "properties": {"value": {"type": "number", "examples": numbers}}}
    config = replace(Config.defaults(), max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    submission = await attach(bulk, "numeric", [BulkRequest("number", body)], schema=schema)
    loaded = await bulk.read_input(submission.requests)
    assert canonical_json(loaded[0].body) == canonical_json(body)
    assert canonical_json(await bulk.input_schema(submission.requests.manifest_id)) == canonical_json(schema)
    await finish_admission(bulk, "numeric")
    batch = (await bulk.store.ready_batches("fake", config.nexus_endpoint))[0]
    entry = (await bulk.store.load_manifest(batch.batch_id)).entries[0]
    await bulk.store.save_outcomes(
        batch.batch_id,
        [
            Outcome(
                entry.request.request_id,
                entry.fingerprint,
                batch.batch_id,
                result=Result(entry.request.request_id, ""),
                bulk={
                    "status": "succeeded",
                    "response": {"numbers": numbers},
                    "output": {"numbers": numbers},
                },
            )
        ],
        [],
    )
    ref = await bulk.get_result("numeric")
    item = (await bulk.read_results(ref))[0]
    assert item.ok
    assert canonical_json(item.output) == canonical_json({"numbers": numbers})
    assert await bulk.get_result("numeric") == ref
