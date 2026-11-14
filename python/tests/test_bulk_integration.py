"""Exercise the combined API through real Nexus callbacks and PostgreSQL.

OpenAI alone is simulated. Run with the same NEXUS_INTEGRATION settings as the
existing integration suite; each test owns its application worker queue.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal
from uuid import uuid4

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    import pytest
    from pydantic import BaseModel, field_validator
    from temporalio.client import ActivityExecutionStatus, WorkflowFailureError
    from temporalio.testing import ActivityEnvironment
    from temporalio.worker import Worker

    from test_integration import (
        payloads,
        pytestmark as integration_markers,
        rig as base_rig,
        stop_process,
        wait_for,
        worker_process,
    )
    from nexus_batch.bulk import iter_results, responses, write_requests
    from nexus_batch.bulk_models import BatchInputRef, BatchResultRef, BulkRequest
    from nexus_batch.bulk_store import BulkStore
    from nexus_batch.models import Config, workflow_id
    from nexus_batch.provider import FakeProvider
    from nexus_batch.runtime import running_workers

pytestmark = integration_markers
rig = base_rig


@dataclass
class BulkCall:
    run_id: str
    submission_id: str
    requests: BatchInputRef
    config: Config


@workflow.defn
class BulkCaller:
    @workflow.run
    async def run(self, input: BulkCall) -> BatchResultRef:
        return await responses(
            run_id=input.run_id,
            submission_id=input.submission_id,
            requests=input.requests,
            endpoint=input.config.nexus_endpoint,
            operation_timeout=timedelta(seconds=input.config.operation_timeout),
        )


class Label(BaseModel):
    label: Literal["keep"]


class LocalRule(Label):
    @field_validator("label")
    @classmethod
    def application_rule(cls, value):
        raise ValueError("application-specific rule rejected this result")


async def prepare(store, items, output_type=None):
    return await ActivityEnvironment().run(write_requests, BulkStore(store), items, output_type=output_type)


async def read(store, ref, output_type=None):
    async def consume():
        return [item async for item in iter_results(BulkStore(store), ref, output_type=output_type)]

    return await ActivityEnvironment().run(consume)


async def start_bulk(rig, queue, run_id, config, submission_id, ref):
    return await rig.client.start_workflow(
        BulkCaller.run,
        BulkCall(run_id, submission_id, ref, config),
        id=f"bulk-caller/{run_id}/{submission_id}",
        task_queue=queue,
    )


async def resolved_batches(rig, run_id, count):
    async def ready():
        status = await rig.store.status(run_id)
        if (
            status.admitted == count
            and status.batches
            and all(batch.status in {"completed", "completed_with_errors"} for batch in status.batches)
        ):
            return status

    return await wait_for(ready)


async def test_bulk_and_single_callers_share_batches_with_independent_completion(rig):
    queue = f"bulk-application-{uuid4().hex}"
    first = await prepare(rig.store, [BulkRequest("same-id", {"input": "first"})], Label)
    second = await prepare(rig.store, [BulkRequest("same-id", {"input": "second"})], Label)
    run_id, config = await rig.start("bulk-shared", 3, window=10.0)
    provider = FakeProvider(rig.directory, processing_delay=0.1)
    async with running_workers(rig.client, rig.store, provider):
        async with Worker(rig.client, task_queue=queue, workflows=[BulkCaller]):
            a, b = await asyncio.gather(
                start_bulk(rig, queue, run_id, config, f"{run_id}-one", first),
                start_bulk(rig, queue, run_id, config, f"{run_id}-two", second),
            )
            single = (await rig.callers(run_id, config, ["ordinary request"]))[0]
            async with asyncio.timeout(60):
                one, two, ordinary = await asyncio.gather(a.result(), b.result(), single.result())
            assert ordinary.text == "Fake response: ordinary request"
            assert one.counts.succeeded == two.counts.succeeded == 1
            first_items = await read(rig.store, one, Label)
            second_items = await read(rig.store, two, Label)
            assert first_items[0].custom_id == second_items[0].custom_id == "same-id"
            assert first_items[0].request["input"] == "first"
            assert second_items[0].request["input"] == "second"
            assert first_items[0].output == Label(label="keep")
            assert first_items[0].response
            rejected = await read(rig.store, one, LocalRule)
            assert rejected[0].parse_error and not rejected[0].ok
            assert one.counts.succeeded == 1
            status = await resolved_batches(rig, run_id, 3)
            assert len(status.batches) == 1
            assert len(await provider.find({"run_id": run_id})) == 1


async def test_large_bulk_has_one_operation_one_recipient_and_small_histories(rig):
    count = 120
    marker = "BULK_PRIVATE_CONTENT_" + uuid4().hex
    text = marker + " input data" * 1000
    ref = await prepare(rig.store, [BulkRequest(f"item-{index}", {"input": text}) for index in range(count)])
    run_id, config = await rig.start("bulk-large", count, max_batch_bytes=2_000_000, window=10.0)
    queue = f"bulk-application-{uuid4().hex}"
    provider = FakeProvider(rig.directory, processing_delay=0.1)
    async with running_workers(rig.client, rig.store, provider):
        async with Worker(rig.client, task_queue=queue, workflows=[BulkCaller]):
            caller = await start_bulk(rig, queue, run_id, config, f"{run_id}-bulk", ref)
            async with asyncio.timeout(90):
                result = await caller.result()
            assert result.counts.total == result.counts.succeeded == count
            items = await read(rig.store, result)
            assert len(items) == count
            assert marker in items[0].response["output"][0]["content"][0]["text"]
            status = await resolved_batches(rig, run_id, count)
            histories = [await caller.fetch_history()]
            histories.extend(
                [
                    await rig.client.get_workflow_handle(workflow_id(batch.batch_id)).fetch_history()
                    for batch in status.batches
                ]
            )
            for history in histories:
                recorded = [payload for event in history.events for payload in payloads(event)]
                assert recorded and max(len(payload.data) for payload in recorded) <= 8192
                assert marker.encode() not in b"\n".join(payload.data for payload in recorded)
            assert (
                sum(
                    event.HasField("nexus_operation_started_event_attributes")
                    for event in histories[0].events
                )
                == 1
            )
            async with rig.store.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT target FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND run_id=$2",
                    rig.store.namespace,
                    run_id,
                )
            assert len(rows) == 1
            from nexus_batch.models import CompletionTarget, loads

            target = loads(CompletionTarget, rows[0]["target"])
            description = await rig.client.get_activity_handle(
                target.activity_id, run_id=target.activity_run_id
            ).describe()
            assert description.status == ActivityExecutionStatus.COMPLETED
            assert len(await provider.find({"run_id": run_id})) == len(status.batches)


@pytest.mark.parametrize(
    "scenario,successes,failures,expired",
    [
        ("mixed", 4, 2, 0),
        ("expired", 3, 0, 3),
        ("missing", 5, 1, 0),
        ("duplicate", 5, 1, 0),
        ("unknown", 6, 0, 0),
        ("malformed", 5, 1, 0),
        ("submission-unknown", 6, 0, 0),
        ("submission-unreconciled", 0, 6, 0),
    ],
)
async def test_provider_failures_are_individual_bulk_outcomes(rig, scenario, successes, failures, expired):
    ref = await prepare(rig.store, [BulkRequest(f"item-{index}", {"input": "body"}) for index in range(6)])
    run_id, config = await rig.start("bulk-outcomes", 6)
    queue = f"bulk-application-{uuid4().hex}"
    provider = FakeProvider(rig.directory, processing_delay=0.05, scenario=scenario)
    async with running_workers(rig.client, rig.store, provider):
        async with Worker(rig.client, task_queue=queue, workflows=[BulkCaller]):
            caller = await start_bulk(rig, queue, run_id, config, f"{run_id}-bulk", ref)
            async with asyncio.timeout(60):
                result = await caller.result()
            assert result.counts.total == 6
            assert (result.counts.succeeded, result.counts.failed, result.counts.expired) == (
                successes,
                failures,
                expired,
            )
            items = await read(rig.store, result)
            assert len({item.custom_id for item in items}) == 6
            assert sum(item.ok for item in items) == successes
            # Even an unconfirmed accepted submission must not produce a second
            # paid creation call. The persistent fake ledger exposes all jobs.
            import json

            assert len(json.loads((rig.directory / "state.json").read_text())["jobs"]) == 1


CRASH_DURING_BULK_ADMISSION = """
import os
import signal
from pathlib import Path
from nexus_batch.bulk_store import BulkStore
from nexus_batch.cli import main

original = BulkStore.admit_page
async def admit(self, submission_id, *args, **kwargs):
    done = await original(self, submission_id, *args, **kwargs)
    if submission_id == os.environ['BULK_TEST_SUBMISSION'] and not done:
        Path(os.environ['BULK_TEST_MARKER']).write_text('committed page')
        os.kill(os.getpid(), signal.SIGKILL)
    return done
BulkStore.admit_page = admit
main()
"""


async def test_bulk_admission_resumes_after_process_loss_without_repeating_membership(rig):
    from nexus_batch.models import CompletionTarget, loads

    ref = await prepare(
        rig.store, [BulkRequest(f"item-{index}", {"input": "recover this item"}) for index in range(120)]
    )
    run_id, config = await rig.start(
        "bulk-recovery", 40, window=0.2, result_deadline=120.0, operation_timeout=180.0
    )
    submission_id = f"{run_id}-bulk"
    queue = f"bulk-application-{uuid4().hex}"
    marker = rig.directory / "bulk-worker-crashed"
    process = None

    async def recipient():
        async with rig.store.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT target,deadline_at,admitted FROM nexus_batch_bulk_submissions "
                "WHERE namespace=$1 AND submission_id=$2",
                rig.store.namespace,
                submission_id,
            )

    try:
        async with Worker(rig.client, task_queue=queue, workflows=[BulkCaller]):
            process = await worker_process(
                rig,
                code=CRASH_DURING_BULK_ADMISSION,
                environment={"BULK_TEST_SUBMISSION": submission_id, "BULK_TEST_MARKER": str(marker)},
            )
            caller = await start_bulk(rig, queue, run_id, config, submission_id, ref)

            async def crashed():
                return marker.exists()

            await wait_for(crashed, timeout=20)
            await process.wait()
            original = await recipient()
            assert 0 < original["admitted"] < ref.count
            process = await worker_process(rig)
            async with asyncio.timeout(100):
                result = await caller.result()
            rebound = await recipient()
            assert result.counts.total == result.counts.succeeded == 120
            assert rebound["deadline_at"] == original["deadline_at"]
            before = loads(CompletionTarget, original["target"])
            after = loads(CompletionTarget, rebound["target"])
            assert after.activity_id == before.activity_id
            assert after.activity_run_id == before.activity_run_id
            assert after.attempt > before.attempt
            assert len(await read(rig.store, result)) == 120
            assert (await rig.store.status(run_id)).admitted == 120
    finally:
        await stop_process(process)


async def test_bulk_cancellation_preserves_shared_work_and_external_results(rig):
    ref = await prepare(
        rig.store,
        [BulkRequest("one", {"input": "first"}), BulkRequest("two", {"input": "second"})],
    )
    run_id, config = await rig.start("bulk-cancellation", 3, window=10.0)
    submission_id = f"{run_id}-bulk"
    queue = f"bulk-application-{uuid4().hex}"
    provider = FakeProvider(rig.directory, processing_delay=0.5)
    async with running_workers(rig.client, rig.store, provider):
        async with Worker(rig.client, task_queue=queue, workflows=[BulkCaller]):
            caller = await start_bulk(rig, queue, run_id, config, submission_id, ref)
            ordinary = (await rig.callers(run_id, config, ["independent caller"]))[0]

            async def admitted():
                return (await rig.store.status(run_id)).admitted == 3

            await wait_for(admitted, timeout=20)
            await caller.cancel()
            with pytest.raises(WorkflowFailureError):
                await caller.result()
            async with asyncio.timeout(30):
                single_result = await ordinary.result()
            assert single_result.text == "Fake response: independent caller"
            stored = await BulkStore(rig.store).get_result(submission_id, partial=True)
            assert stored.counts.succeeded == 2
            assert len(await read(rig.store, stored)) == 2
            assert len(await provider.find({"run_id": run_id})) == 1
