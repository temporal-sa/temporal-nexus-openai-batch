"""Client contracts that protect Workflow payloads and typed result consumption."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, field_validator

from nexus_batch import bulk
from nexus_batch.bulk_models import BatchCounts, BatchInputRef, BatchResultRef, BulkItem, BulkRequest
from nexus_batch.models import dumps


class Label(BaseModel):
    text: str

    @field_validator("text")
    @classmethod
    def require_short_text(cls, value):
        if len(value) > 10:
            raise ValueError("text is too long")
        return value


class MemoryArtifacts:
    def __init__(self, items=(), *, page_cap=100):
        self.items = list(items)
        self.page_cap = page_cap
        self.offsets = []
        self.written = []
        self.schema = None
        self.original_schema = {"type": "object", "properties": {"text": {"type": "string"}}}

    async def write_input(self, requests, schema=None):
        self.schema = schema
        if hasattr(requests, "__aiter__"):
            self.written = [item async for item in requests]
        else:
            self.written = list(requests)
        return BatchInputRef("manifest", "d" * 64, len(self.written))

    async def read_results(self, ref, offset=0, limit=100):
        self.offsets.append(offset)
        return self.items[offset : offset + min(limit, self.page_cap)]

    async def input_schema(self, manifest_id):
        assert manifest_id == "manifest"
        return self.original_schema


def result_ref(count):
    return BatchResultRef("submission", "manifest", "d" * 64, count, BatchCounts(total=count))


@pytest.fixture
def in_activity(monkeypatch):
    monkeypatch.setattr(bulk.activity, "in_activity", lambda: True)


async def test_data_helpers_reject_workflow_or_ordinary_process_io():
    store = MemoryArtifacts()
    with pytest.raises(RuntimeError, match="inside an Activity"):
        await bulk.write_requests(store, [BulkRequest("one", {"input": "one"})])
    with pytest.raises(RuntimeError, match="inside an Activity"):
        await bulk.load_page(store, result_ref(1))
    with pytest.raises(RuntimeError, match="inside an Activity"):
        await anext(bulk.iter_results(store, result_ref(1)))
    with pytest.raises(RuntimeError, match="inside an Activity"):
        await bulk.unfinished_requests(store, result_ref(1))
    assert not store.written and not store.offsets


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_write_seals_portable_strict_schema_and_returns_small_ref(in_activity, asynchronous):
    store = MemoryArtifacts()
    requests = [BulkRequest("one", {"instructions": "classify", "input": [{"role": "user", "content": "x"}]})]

    async def source():
        for item in requests:
            yield item

    ref = await bulk.write_requests(store, source() if asynchronous else requests, output_type=Label)
    assert ref.count == 1 and store.written == requests
    assert store.schema["additionalProperties"] is False
    assert store.schema["required"] == ["text"]
    assert "test_bulk_client" not in dumps(store.schema)
    assert "schema" not in dumps(ref) and "instructions" not in dumps(ref)
    assert len(dumps(ref)) < 256


async def test_schema_generation_rejects_non_model_output_type(in_activity):
    with pytest.raises(TypeError, match="BaseModel"):
        await bulk.write_requests(MemoryArtifacts(), [], output_type=dict)


async def test_local_validation_preserves_raw_data_and_service_counts(in_activity):
    response = {"output": [{"type": "message", "content": [{"text": "raw output"}]}]}
    items = [
        BulkItem("ok", {"input": "first"}, "succeeded", response, {"text": "short"}),
        BulkItem(
            "custom-failure",
            {"input": "second"},
            "succeeded",
            response,
            {"text": "far too long for validator"},
        ),
        BulkItem(
            "schema-failure",
            {"input": "third"},
            "parse_failed",
            response,
            {"text": 1},
            parse_error="schema error",
        ),
    ]
    ref = result_ref(3)
    ref.counts.succeeded, ref.counts.parse_failed = 2, 1
    page = await bulk.load_page(MemoryArtifacts(items), ref, output_type=Label)
    assert isinstance(page.items[0].output, Label)
    failed = page.items[1]
    assert not failed.ok and failed.status == "parse_failed"
    assert failed.parse_error.startswith("local validation:")
    assert failed.response == response and failed.output == items[1].output
    assert items[1].status == "succeeded" and not items[1].parse_error
    assert page.items[2].parse_error == "schema error"
    assert ref.counts.succeeded == 2 and ref.counts.parse_failed == 1


async def test_byte_shortened_pages_do_not_end_iteration_early(in_activity):
    items = [BulkItem(str(i), {"input": str(i)}, "succeeded", output={"text": str(i)}) for i in range(3)]
    store = MemoryArtifacts(items, page_cap=1)
    loaded = [item async for item in bulk.iter_results(store, result_ref(3), page_size=100)]
    assert [item.custom_id for item in loaded] == ["0", "1", "2"]
    assert store.offsets == [0, 1, 2]


async def test_empty_page_before_expected_end_is_a_data_error(in_activity):
    with pytest.raises(RuntimeError, match="no progress"):
        await bulk.load_page(MemoryArtifacts(), result_ref(1))


@pytest.mark.parametrize("offset,limit", [(-1, 1), (2, 1), (0, 0), (0, 1001)])
async def test_page_bounds_rejected_before_store_io(in_activity, offset, limit):
    store = MemoryArtifacts()
    with pytest.raises(ValueError):
        await bulk.load_page(store, result_ref(1), offset=offset, limit=limit)
    assert store.offsets == []


async def test_unfinished_selection_is_an_explicit_new_manifest(in_activity):
    items = [
        BulkItem("ok", {"input": "ok"}, "succeeded"),
        BulkItem("expired", {"input": "retry expired", "instructions": "preserve me"}, "expired"),
        BulkItem("cancelled", {"input": "retry cancelled"}, "cancelled"),
        BulkItem("failure", {"input": "do not retry"}, "failed"),
    ]
    store = MemoryArtifacts(items, page_cap=1)
    ref = await bulk.unfinished_requests(store, result_ref(4), output_type=Label)
    assert ref.count == 2
    assert store.written == [BulkRequest(item.custom_id, item.request) for item in items[1:3]]
    assert store.schema["required"] == ["text"]


async def test_no_unfinished_results_does_not_write_empty_manifest(in_activity):
    store = MemoryArtifacts([BulkItem("ok", {"input": "ok"}, "succeeded")])
    assert await bulk.unfinished_requests(store, result_ref(1)) is None
    assert not store.written


async def test_unfinished_selection_preserves_original_portable_schema_by_default(in_activity):
    store = MemoryArtifacts([BulkItem("expired", {"input": "retry"}, "expired")])
    ref = await bulk.unfinished_requests(store, result_ref(1))
    assert ref.count == 1 and store.schema == store.original_schema


async def test_large_manifest_uses_one_nexus_call_with_only_ref(monkeypatch):
    result = result_ref(50_000)
    nexus_client = SimpleNamespace(execute_operation=AsyncMock(return_value=result))
    monkeypatch.setattr(bulk.workflow, "in_workflow", lambda: True)
    monkeypatch.setattr(bulk.workflow, "create_nexus_client", lambda **kwargs: nexus_client)
    ref = BatchInputRef("manifest", "d" * 64, 50_000)
    assert (
        await bulk.responses(
            "run", "submission", ref, endpoint="example", operation_timeout=timedelta(seconds=90)
        )
        == result
    )
    nexus_client.execute_operation.assert_awaited_once()
    call = nexus_client.execute_operation.call_args
    assert call.args[0] == "responses" and call.args[1].requests == ref
    assert len(dumps(call.args[1]).encode()) < 512
    assert call.kwargs["schedule_to_close_timeout"] == timedelta(seconds=90)


async def test_workflow_helper_rejects_outside_workflow():
    with pytest.raises(RuntimeError, match="inside a Workflow"):
        await bulk.responses("run", "submission", BatchInputRef("m", "d" * 64, 1), endpoint="example")
