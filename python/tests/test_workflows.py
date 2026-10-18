"""Control-flow regression tests use Temporal's time-skipping test server."""

import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from nexus_batch.activities import CreateResult
from nexus_batch.models import BatchRef, Config, DeliverySummary
from nexus_batch.provider import Job
from nexus_batch.workflows import BatchWorkflow, provider_deadline


class Stubs:
    def __init__(self, *, ambiguous=False, unknown=False, initial_save_failures=0, final_save_failures=0):
        self.ambiguous, self.unknown = ambiguous, unknown
        self.initial_save_failures, self.final_save_failures = initial_save_failures, final_save_failures
        self.calls = []
        self.failure = ""

    @activity.defn(name="save_report")
    async def save_report(self, report: dict) -> None:
        if report["status"] == "uploading" and self.initial_save_failures:
            self.initial_save_failures -= 1
            raise RuntimeError("temporary store outage")
        if report["status"] == "completed" and self.final_save_failures:
            self.final_save_failures -= 1
            raise RuntimeError("temporary final report outage")

    @activity.defn(name="upload")
    async def upload(self, ref: BatchRef) -> str:
        self.calls.append("upload")
        return "file-1"

    @activity.defn(name="create")
    async def create(self, ref: BatchRef, file_id: str) -> CreateResult:
        self.calls.append("create")
        if self.ambiguous:
            raise RuntimeError("provider accepted; acknowledgement lost")
        return CreateResult(job=Job("job-1", "in_progress", input_file_id=file_id))

    @activity.defn(name="find")
    async def find(self, ref: BatchRef, file_id: str) -> CreateResult:
        self.calls.append("find")
        if self.unknown:
            return CreateResult(ambiguous=True)
        return CreateResult(job=Job("job-1", "in_progress", input_file_id=file_id))

    @activity.defn(name="poll")
    async def poll(self, job: Job, deadline: datetime, interval: float) -> Job:
        self.calls.append("poll")
        return Job(job.id, "completed", input_file_id=job.input_file_id)

    @activity.defn(name="results")
    async def results(self, ref: BatchRef, job: Job) -> DeliverySummary:
        self.calls.append("results")
        return DeliverySummary(succeeded=ref.requests)

    @activity.defn(name="fail_batch")
    async def fail_batch(self, ref: BatchRef, code: str, message: str) -> DeliverySummary:
        self.failure = code
        return DeliverySummary(failed=ref.requests, failure_counts={code: ref.requests})

    @activity.defn(name="deliver")
    async def deliver(self, ref: BatchRef) -> DeliverySummary:
        self.calls.append("deliver")
        if self.failure:
            return DeliverySummary(
                delivered=ref.requests, failed=ref.requests, failure_counts={self.failure: ref.requests}
            )
        return DeliverySummary(delivered=ref.requests, succeeded=ref.requests)

    def activities(self):
        return [
            self.save_report,
            self.upload,
            self.create,
            self.find,
            self.poll,
            self.results,
            self.fail_batch,
            self.deliver,
        ]


def reference(now):
    config = Config(window=0.1, result_deadline=3, operation_timeout=5)
    return BatchRef("run-1", "batch-1", config, 1, 200, now, now, now + timedelta(seconds=5))


def test_provider_deadline_reserves_delivery_after_delayed_sealing():
    now = datetime.now(timezone.utc)
    ref = reference(now)
    ref.sealed_at += timedelta(seconds=2)
    # A delayed seal cannot extend the original recipient's processing budget.
    assert provider_deadline(ref) == now + timedelta(seconds=3.1)


@pytest.fixture
async def temporal():
    if not os.getenv("PYTHON_WORKFLOW_TESTS"):
        pytest.skip("set PYTHON_WORKFLOW_TESTS=1 for time-skipping Workflow tests")
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        yield environment


async def execute(client: Client, stub: Stubs, ref: BatchRef):
    queue = "python-workflow-test-" + uuid4().hex
    async with Worker(client, task_queue=queue, workflows=[BatchWorkflow], activities=stub.activities()):
        return await client.execute_workflow(BatchWorkflow.run, ref, id=queue, task_queue=queue)


async def test_late_batch_does_not_upload_or_create(temporal):
    now = await temporal.get_current_time()
    stub = Stubs()
    ref = reference(now - timedelta(minutes=5))
    report = await execute(temporal.client, stub, ref)
    assert stub.failure == "DEADLINE_EXCEEDED"
    assert stub.calls == ["deliver"]
    assert report.status == "completed_with_errors"


async def test_storage_outage_crossing_deadline_prevents_provider_submission(temporal):
    stub = Stubs(initial_save_failures=3)
    report = await execute(temporal.client, stub, reference(await temporal.get_current_time()))
    assert stub.failure == "DEADLINE_EXCEEDED"
    assert "upload" not in stub.calls and "create" not in stub.calls
    assert report.delivered == 1


@pytest.mark.parametrize("unknown", [False, True])
async def test_lost_create_acknowledgement_never_resubmits(temporal, unknown):
    stub = Stubs(ambiguous=True, unknown=unknown)
    report = await execute(temporal.client, stub, reference(await temporal.get_current_time()))
    assert stub.calls.count("create") == 1
    assert stub.calls.count("find") == 1
    assert stub.failure == ("SUBMISSION_UNKNOWN" if unknown else "")
    assert report.failed == (1 if unknown else 0)


async def test_final_bookkeeping_retries_beyond_recipient_deadline(temporal):
    stub = Stubs(final_save_failures=4)
    ref = reference(await temporal.get_current_time())
    report = await execute(temporal.client, stub, ref)
    assert (await temporal.get_current_time()) > ref.deadline_at
    assert stub.calls.count("poll") == 1
    assert stub.calls.count("create") == 1
    assert report.delivered == 1 and report.status == "completed"
