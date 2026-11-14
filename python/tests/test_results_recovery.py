"""A lost collection acknowledgement must not require provider files again."""

from unittest.mock import AsyncMock

from test_bulk_store import (
    attach,
    bulk as database_fixture,
    finish_admission,
    save_successes,
)
from nexus_batch.activities import Activities
from nexus_batch.bulk_models import BulkRequest
from nexus_batch.models import Config, RunInput
from nexus_batch.provider import Job

bulk = database_fixture


async def test_committed_results_survive_missing_provider_files(bulk):
    config = Config(max_batch_requests=1)
    await bulk.store.start_run(RunInput("run", config))
    await attach(bulk, "submission", [BulkRequest("item", {"input": "original document"})])
    await finish_admission(bulk, "submission")
    ref = (await bulk.store.ready_batches("fake", config.nexus_endpoint))[0]
    assert not await bulk.store.outcomes_saved(ref.batch_id)
    await save_successes(bulk, ref.batch_id)
    before = await bulk.get_result("submission")
    provider = AsyncMock()
    provider.download.side_effect = FileNotFoundError("provider files have expired")
    activities = Activities(AsyncMock(), bulk.store, provider)
    report = await activities.results(ref, Job(id="accepted-job", status="completed", output_file_id="gone"))
    assert report.succeeded == 1
    provider.download.assert_not_awaited()
    assert await bulk.get_result("submission") == before
