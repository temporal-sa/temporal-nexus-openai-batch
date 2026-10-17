"""Real PostgreSQL, Nexus callbacks and Standalone Activities; only OpenAI is fake.

Run with NEXUS_INTEGRATION=1 against a development server started with
``--dynamic-config-value activity.enableCallbacks=true``. Python task queues
must not have another worker running while these tests own them.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from uuid import uuid4

import pytest
from google.protobuf.message import Message
from temporalio.api.common.v1 import Payload
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest, DeleteNexusEndpointRequest
from temporalio.client import ActivityExecutionStatus, Client, WorkflowFailureError
from temporalio.exceptions import ApplicationError

from nexus_batch.activities import Activities
from nexus_batch.cli import cleanup, setup
from nexus_batch.models import (
    CallerInput,
    CompletionTarget,
    Config,
    Outcome,
    Request,
    Result,
    RunInput,
    caller_id,
    loads,
    workflow_id,
)
from nexus_batch.provider import FakeProvider
from nexus_batch.runtime import queues, running_workers
from nexus_batch.store import LOCAL_DSN, Store
from nexus_batch.workflows import CallerWorkflow

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("NEXUS_INTEGRATION") != "1", reason="set NEXUS_INTEGRATION=1 for real services"
    ),
]


@dataclass
class Rig:
    client: Client
    store: Store
    directory: Path
    runs: list[str] = field(default_factory=list)

    async def start(self, name: str, count: int = 3, **changes):
        run_id = f"py-integration-{name}-{uuid4().hex[:12]}"
        options = {
            "window": 2.0,
            "max_batch_requests": count,
            "poll_interval": 0.05,
            "result_deadline": 45.0,
            "operation_timeout": 90.0,
            **changes,
        }
        config = replace(Config.defaults(), **options)
        await self.store.start_run(RunInput(run_id, config))
        self.runs.append(run_id)
        return run_id, config

    async def callers(self, run_id: str, config: Config, documents: list[str]):
        async def start(index, text):
            request = Request(run_id, f"document-{index:04d}", text)
            return await self.client.start_workflow(
                CallerWorkflow.run,
                CallerInput(request, config),
                id=caller_id(run_id, request.request_id),
                task_queue=queues("fake")[0],
            )

        return await asyncio.gather(*(start(index, text) for index, text in enumerate(documents)))


@pytest.fixture
async def rig(tmp_path):
    address = os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7234")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    client = await Client.connect(address, namespace=namespace)
    await setup(client, "fake")
    store = await Store.connect(os.getenv("PYTHON_DATABASE_URL", LOCAL_DSN), namespace)
    result = Rig(client, store, tmp_path)
    try:
        yield result
    finally:
        # Stop through the same submission fence used by the CLI. Keep rows and
        # Temporal histories available for inspection after an integration run.
        for run_id in result.runs:
            with suppress(Exception):
                async with asyncio.timeout(10):
                    await cleanup(client, store, run_id)
        await store.close()


async def wait_for(check, timeout=45.0):
    async with asyncio.timeout(timeout):
        while True:
            result = await check()
            if result:
                return result
            await asyncio.sleep(0.05)


async def completed(rig: Rig, run_id: str, count: int):
    async def ready():
        status = await rig.store.status(run_id)
        if (
            status.admitted == count
            and status.batches
            and all(batch.status in {"completed", "completed_with_errors"} for batch in status.batches)
            and sum(batch.delivered for batch in status.batches) == count
        ):
            return status

    return await wait_for(ready)


async def stored_rows(rig: Rig, run_id: str):
    async with rig.store.pool.acquire() as connection:
        return await connection.fetch(
            "SELECT request_id,target,outcome,delivery_status,batch_id FROM nexus_batch_requests "
            "WHERE namespace=$1 AND run_id=$2 ORDER BY request_id",
            rig.store.namespace,
            run_id,
        )


def failure_type(error: Exception) -> str:
    current = error
    while current is not None:
        if isinstance(current, ApplicationError):
            return current.type or ""
        current = getattr(current, "cause", None)
    return ""


async def worker_process(
    rig: Rig,
    *,
    code: str | None = None,
    environment: dict[str, str] | None = None,
    processing_delay: float = 0.1,
):
    """Launch the actual CLI; test-only fault injection stays in the child."""
    command = [sys.executable, "-c", code] if code else [sys.executable, "-m", "nexus_batch"]
    log = rig.directory / f"worker-{uuid4().hex[:8]}.log"
    with log.open("wb") as errors:
        child = await asyncio.create_subprocess_exec(
            *command,
            "worker",
            "--address",
            os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7234"),
            "--namespace",
            rig.client.namespace,
            "--database-url",
            os.getenv("PYTHON_DATABASE_URL", LOCAL_DSN),
            "--state-dir",
            str(rig.directory),
            "--processing-delay",
            str(processing_delay),
            stdout=asyncio.subprocess.PIPE,
            stderr=errors,
            env={**os.environ, **(environment or {})},
        )
    try:
        async with asyncio.timeout(25):
            ready = await child.stdout.readline()
        assert ready.strip() == b"Python workers ready", log.read_text()[-2000:]
        return child
    except BaseException:
        if child.returncode is None:
            child.kill()
            await child.wait()
        raise


async def stop_process(process):
    if process is not None and process.returncode is None:
        process.terminate()
        try:
            async with asyncio.timeout(10):
                await process.wait()
        except TimeoutError:
            process.kill()
            await process.wait()


@pytest.mark.parametrize(
    "scenario,expected_failures",
    [
        ("success", {}),
        ("mixed", {1: "PROVIDER_REQUEST_ERROR"}),
        ("missing", {0: "MISSING_RESULT"}),
        ("duplicate", {0: "DUPLICATE_RESULT"}),
        ("unknown", {}),
        ("malformed", {0: "INVALID_RESULT"}),
        ("expired", {0: "BATCH_EXPIRED", 2: "BATCH_EXPIRED"}),
        ("submission-unknown", {}),
        (
            "submission-unreconciled",
            {0: "SUBMISSION_UNKNOWN", 1: "SUBMISSION_UNKNOWN", 2: "SUBMISSION_UNKNOWN"},
        ),
    ],
)
async def test_provider_scenarios_complete_original_nexus_activities(rig, scenario, expected_failures):
    provider = FakeProvider(rig.directory, scenario, processing_delay=0.1)
    async with running_workers(rig.client, rig.store, provider):
        run_id, config = await rig.start(scenario)
        callers = await rig.callers(run_id, config, ["first document", "second document", "third document"])
        async with asyncio.timeout(45):
            results = await asyncio.gather(*(caller.result() for caller in callers), return_exceptions=True)
        status = await completed(rig, run_id, 3)
        assert (status.backing, status.storage, status.completion_mode) == (
            "nexus_standalone_activity",
            "postgres",
            "external_async_completion",
        )
        assert len(status.batches) == 1
        assert status.batches[0].succeeded == 3 - len(expected_failures)
        assert status.batches[0].failed == len(expected_failures)
        manifest = await rig.store.load_manifest(status.batches[0].batch_id)
        # Concurrent caller starts do not promise a PostgreSQL admission order.
        failed_ids = {
            manifest.entries[index].request.request_id: code for index, code in expected_failures.items()
        }
        for index, result in enumerate(results):
            request_id = f"document-{index:04d}"
            if request_id in failed_ids:
                assert isinstance(result, WorkflowFailureError)
                assert failure_type(result) == failed_ids[request_id]
            else:
                assert isinstance(result, Result)
                assert result.request_id == f"document-{index:04d}"
                assert (
                    result.text
                    == "Fake response: " + ["first document", "second document", "third document"][index]
                )

        rows = await stored_rows(rig, run_id)
        assert len(rows) == 3
        activities = Activities(rig.client, rig.store, provider)
        for row in rows:
            target = loads(CompletionTarget, row["target"])
            outcome = loads(Outcome, row["outcome"])
            # This describe API addresses a true Standalone Activity execution,
            # which also has the Nexus completion callback registered on it.
            description = await rig.client.get_activity_handle(
                target.activity_id,
                run_id=target.activity_run_id,
            ).describe(include_outcome=True)
            assert description.activity_type == "submit_request"
            assert description.activity_run_id == target.activity_run_id
            assert description.raw_callbacks
            assert description.status == (
                ActivityExecutionStatus.FAILED if outcome.failure else ActivityExecutionStatus.COMPLETED
            )
            assert row["delivery_status"] == "delivered"
            # Model a completion RPC whose acknowledgement was lost: the
            # already-closed execution must be reconciled to this exact outcome.
            retry = await activities.complete(target, outcome)
            assert retry.delivered
            different = (
                replace(outcome, failure=replace(outcome.failure, message="different outcome"))
                if outcome.failure
                else replace(outcome, result=replace(outcome.result, text="different outcome"))
            )
            assert not (await activities.complete(target, different)).delivered
        state = json.loads((rig.directory / "state.json").read_text())
        assert len(state["jobs"]) == 1  # Includes deliberately hidden uncertain submissions.


def payloads(message: Message):
    if isinstance(message, Payload):
        yield message
        return
    for descriptor, value in message.ListFields():
        if descriptor.message_type is None:
            continue
        if descriptor.is_repeated:
            for item in value:
                if isinstance(item, Message):
                    yield from payloads(item)
        else:
            yield from payloads(value)


async def test_large_manifest_stays_out_of_batch_workflow_history(rig):
    count = 120
    marker = "PRIVATE_DOCUMENT_CONTENT_" + uuid4().hex
    text = marker + " lorem ipsum" * 260
    provider = FakeProvider(rig.directory, processing_delay=0.1)
    async with running_workers(rig.client, rig.store, provider, service_concurrency=64):
        run_id, config = await rig.start("large", count, max_batch_bytes=1_000_000, window=10.0)
        callers = await rig.callers(run_id, config, [text] * count)
        async with asyncio.timeout(90):
            results = await asyncio.gather(*(caller.result() for caller in callers))
        assert len(results) == count
        status = await completed(rig, run_id, count)
        assert len(status.batches) == 1
        batch_id = status.batches[0].batch_id
        manifest = await rig.store.load_manifest(batch_id)
        assert len(manifest.entries) == count
        assert manifest.bytes > 300_000
        history = await rig.client.get_workflow_handle(workflow_id(batch_id)).fetch_history()
        recorded = [payload for event in history.events for payload in payloads(event)]
        assert recorded
        assert max(len(payload.data) for payload in recorded) <= 4096
        encoded = b"\n".join(payload.data for payload in recorded)
        assert marker.encode() not in encoded
        assert b'"completion_target"' not in encoded
        assert b'"entries"' not in encoded
        for entry in manifest.entries:
            assert base64.b64encode(entry.completion_target.task_token) not in encoded
        poll_schedules = [
            event
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
            and event.activity_task_scheduled_event_attributes.activity_type.name == "poll"
        ]
        assert len(poll_schedules) == 1
        assert len(json.loads((rig.directory / "state.json").read_text())["jobs"]) == 1


async def test_wrong_ingress_endpoint_is_rejected_before_admission(rig):
    _, service_queue, _, _ = queues("fake")
    alias = f"llm-batch-python-alias-{uuid4().hex[:12]}"
    created = await rig.client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=alias,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(namespace=rig.client.namespace, task_queue=service_queue)
                ),
            ),
        )
    )
    try:
        provider = FakeProvider(rig.directory)
        async with running_workers(rig.client, rig.store, provider):
            run_id, config = await rig.start("wrong-endpoint", 1)
            callers = await rig.callers(run_id, replace(config, nexus_endpoint=alias), ["document"])
            with pytest.raises(WorkflowFailureError):
                async with asyncio.timeout(15):
                    await callers[0].result()
            assert (await rig.store.status(run_id)).admitted == 0
            assert await stored_rows(rig, run_id) == []
            assert await provider.find({}) == []
    finally:
        await rig.client.operator_service.delete_nexus_endpoint(
            DeleteNexusEndpointRequest(
                id=created.endpoint.id,
                version=created.endpoint.version,
            )
        )


async def test_worker_process_loss_keeps_original_provider_job(rig):
    """SIGKILL all workers, then recover pending polling on a fresh process."""
    provider = FakeProvider(rig.directory)
    process = None

    try:
        process = await worker_process(rig, processing_delay=8)
        run_id, config = await rig.start("process-loss", 3)
        callers = await rig.callers(run_id, config, ["first", "second", "third"])

        async def submitted():
            jobs = await provider.find({"run_id": run_id})
            return jobs[0] if jobs else None

        original_job = await wait_for(submitted)
        assert not original_job.terminal()
        process.kill()
        assert await process.wait() < 0
        process = None
        # A newly constructed adapter and OS process share only durable state.
        provider = FakeProvider(rig.directory)
        process = await worker_process(rig, processing_delay=8)
        async with asyncio.timeout(75):
            results = await asyncio.gather(*(caller.result() for caller in callers))
        assert len(results) == 3
        status = await completed(rig, run_id, 3)
        assert status.batches[0].provider_batch_id == original_job.id
        jobs = await provider.find({"run_id": run_id})
        assert len(jobs) == 1
        assert jobs[0].id == original_job.id
        assert len(json.loads((rig.directory / "state.json").read_text())["jobs"]) == 1
    finally:
        await stop_process(process)


# These scripts run in separate OS processes. Production code has no crash
# flags, mock clients, shorter heartbeat timeouts, or alternate enrollment path.
CRASH_AT_ENROLLMENT = """
import json
import os
import signal
from pathlib import Path
from nexus_batch.cli import main
from nexus_batch.store import Store

original = Store.enroll
async def enroll(self, enrollment):
    selected = enrollment.request.run_id == os.environ['NEXUS_TEST_RUN_ID']
    phase = os.environ['NEXUS_TEST_CRASH_PHASE']
    def crash():
        Path(os.environ['NEXUS_TEST_MARKER']).write_text(json.dumps({'attempt': enrollment.completion_target.attempt, 'phase': phase}))
        os.kill(os.getpid(), signal.SIGKILL)
    if selected and phase == 'before':
        crash()
    result = await original(self, enrollment)
    if selected and phase == 'after':
        crash()
    return result
Store.enroll = enroll
main()
"""


WAIT_FOR_NEW_ATTEMPT = """
import os
from nexus_batch.activities import Activities
from nexus_batch.cli import main
from nexus_batch.store import Store

prefix = 'request-activity/' + os.environ['NEXUS_TEST_RUN_ID'] + '/'
pending = Store.pending_heartbeats
async def heartbeats(self, *args, **kwargs):
    targets = await pending(self, *args, **kwargs)
    return [target for target in targets if not (target.activity_id.startswith(prefix) and target.attempt == 1)]
Store.pending_heartbeats = heartbeats

complete = Activities.complete
async def complete_after_retry(self, target, outcome):
    if target.activity_id.startswith(prefix) and target.attempt == 1:
        raise RuntimeError('test waits for original attempt heartbeat timeout')
    return await complete(self, target, outcome)
Activities.complete = complete_after_retry
main()
"""


@pytest.mark.parametrize("phase", ["before", "after"])
async def test_enrollment_worker_loss_retries_original_activity(rig, phase):
    run_id, config = await rig.start(f"enrollment-{phase}", 1, result_deadline=90.0, operation_timeout=150.0)
    marker = rig.directory / "enrollment-crash.json"
    environment = {
        "NEXUS_TEST_RUN_ID": run_id,
        "NEXUS_TEST_CRASH_PHASE": phase,
        "NEXUS_TEST_MARKER": str(marker),
    }
    process = None
    original_target = None
    original_job = None
    try:
        process = await worker_process(rig, code=CRASH_AT_ENROLLMENT, environment=environment)
        callers = await rig.callers(run_id, config, ["original document"])

        async def crashed():
            return marker.exists()

        await wait_for(crashed)
        async with asyncio.timeout(10):
            assert await process.wait() < 0
        process = None
        assert json.loads(marker.read_text()) == {"attempt": 1, "phase": phase}
        rows = await stored_rows(rig, run_id)
        assert len(rows) == (1 if phase == "after" else 0)
        if rows:
            original_target = loads(CompletionTarget, rows[0]["target"])
            assert original_target.attempt == 1

        process = await worker_process(rig, code=WAIT_FOR_NEW_ATTEMPT, environment=environment)
        provider = FakeProvider(rig.directory)
        if phase == "after":
            # The committed admission can finish provider work while its first
            # SAA attempt is deliberately left without heartbeat or completion.
            async def submitted():
                jobs = await provider.find({"run_id": run_id})
                return jobs[0] if jobs else None

            original_job = await wait_for(submitted, timeout=15)
            assert loads(CompletionTarget, (await stored_rows(rig, run_id))[0]["target"]).attempt == 1
        async with asyncio.timeout(120):
            result = await callers[0].result()
        assert result.text == "Fake response: original document"
        status = await completed(rig, run_id, 1)
        assert status.admitted == 1
        assert len(status.batches) == 1
        rows = await stored_rows(rig, run_id)
        assert len(rows) == 1
        rebound = loads(CompletionTarget, rows[0]["target"])
        assert rebound.attempt >= 2
        if original_target:
            assert rebound.activity_id == original_target.activity_id
            assert rebound.activity_run_id == original_target.activity_run_id
            assert rebound.task_token != original_target.task_token
        description = await rig.client.get_activity_handle(
            rebound.activity_id,
            run_id=rebound.activity_run_id,
        ).describe()
        assert description.status == ActivityExecutionStatus.COMPLETED
        assert description.attempt >= 2
        jobs = await provider.find({"run_id": run_id})
        assert len(jobs) == 1
        if original_job:
            assert jobs[0].id == original_job.id == status.batches[0].provider_batch_id
        history = await callers[0].fetch_history()
        assert (
            sum(event.HasField("nexus_operation_started_event_attributes") for event in history.events) == 1
        )
        assert len(json.loads((rig.directory / "state.json").read_text())["jobs"]) == 1
    finally:
        await stop_process(process)
