"""Bulk schemas and provider outcomes stay independent of caller Python models."""

import json
import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import get_type_hints
from unittest.mock import AsyncMock

import pytest
import nexusrpc

from nexus_batch.bulk_models import (
    MAX_BULK_ITEM_BYTES,
    BatchCounts,
    BatchInputRef,
    BatchResultRef,
    BulkRequest,
    BulkSAAInput,
    BulkSubmission,
    canonical_json,
    prepare_body,
    provider_id,
    schema_validator,
    validate_bulk_request,
    validate_schema,
    validate_submission,
)
from nexus_batch.models import (
    Config,
    Entry,
    Failure,
    Manifest,
    Request,
    dumps,
    fingerprint,
    jsonl,
    loads,
    validate_request,
    RunInput,
)
from nexus_batch.nexus_service import BatchService
from nexus_batch.provider import FakeProvider, Job, reconcile
from temporalio.converter import DataConverter


SCHEMA = {
    "type": "object",
    "properties": {"label": {"type": "string", "enum": ["negative", "positive"]}},
    "required": ["label"],
    "additionalProperties": False,
}


def make_manifest(*bodies):
    now = datetime.now(timezone.utc)
    entries = []
    for index, body in enumerate(bodies):
        request = Request("run", provider_id("submission", index), "", body, "submission")
        entries.append(Entry(request, None, fingerprint(request), now))
    return Manifest("run", "batch", Config(), entries, now, 0)


def response_line(request_id, text="{}", **body):
    return dumps(
        {
            "custom_id": request_id,
            "response": {
                "status_code": 200,
                "body": {
                    "status": "completed",
                    "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
                    **body,
                },
            },
        }
    ).encode()


def test_bulk_control_refs_stay_small_and_round_trip_generic_types():
    reference = BatchInputRef("manifest", "a" * 64, 10_000_000)
    submission = BulkSubmission("run", "submission", reference)
    validate_submission(submission)
    assert len(dumps(submission).encode()) < 512
    assert loads(BulkSubmission, dumps(submission)) == submission
    assert loads(BatchInputRef[str], dumps(reference)) == reference
    result = BatchResultRef(
        "submission", "manifest", "b" * 64, reference.count, BatchCounts(total=reference.count)
    )
    assert loads(BatchResultRef[str], dumps(result)) == result
    assert result.page(10, 100).ref is result
    for offset, count in [(-1, 1), (0, 1001), (0, True)]:
        with pytest.raises(Failure):
            result.page(offset, count)
    with pytest.raises(Failure):
        validate_submission(replace(submission, requests=replace(reference, digest="wrong")))


def test_original_text_request_fingerprint_survives_added_bulk_defaults():
    legacy = {"run_id": "run", "request_id": "request", "input": "document"}
    request = loads(Request, legacy)
    assert fingerprint(request) == hashlib.sha256(dumps(legacy).encode()).hexdigest()


async def test_wire_dataclasses_round_trip_through_temporal_default_converter():
    # The SDK converter handles bare dataclasses but does not decode nested
    # parameterized dataclass aliases. Keep runtime wire annotations concrete.
    reference = BatchInputRef("manifest", "a" * 64, 10)
    submission = BulkSubmission("run", "submission", reference)
    result = BatchResultRef("submission", "manifest", "b" * 64, 10, BatchCounts(total=10))
    values = [submission, BulkSAAInput(submission, Config()), result, result.page()]
    converter = DataConverter.default
    for value in values:
        [decoded] = await converter.decode(await converter.encode([value]), [type(value)])
        assert decoded == value
    assert get_type_hints(BatchResultRef, globalns={})["counts"] is BatchCounts


@pytest.mark.parametrize("field_name", ["model", "nexus_endpoint"])
@pytest.mark.parametrize("invalid", [None, 1, True, [], {}, " " * 2, "x" * 257, "é" * 129])
async def test_oversized_or_non_string_run_config_is_rejected_before_activity_start(field_name, invalid):
    config = replace(Config(), **{field_name: invalid})
    with pytest.raises(ValueError, match="256 UTF-8 bytes"):
        config.validate()
    store = SimpleNamespace(get_run=AsyncMock(return_value=RunInput("run", config)))
    service = BatchService(store, "endpoint")
    client = SimpleNamespace(start_activity=AsyncMock())
    submission = BulkSubmission("run", "submission", BatchInputRef("manifest", "a" * 64, 1))
    with pytest.raises(nexusrpc.HandlerError):
        await service.responses(SimpleNamespace(request_id="operation"), client, submission)
    client.start_activity.assert_not_awaited()


def test_config_name_limits_measure_utf8_bytes_and_keep_control_payload_small():
    config = Config(model="é" * 128, nexus_endpoint="x" * 256)
    config.validate()
    submission = BulkSubmission("run", "submission", BatchInputRef("manifest", "a" * 64, 100_000))
    assert len(dumps(BulkSAAInput(submission, config)).encode()) < 2048


@pytest.mark.parametrize(
    "floating,integer",
    [
        (1.0, 1),
        (-0.0, 0),
        (1e20, 100_000_000_000_000_000_000),
        (1.0000000000000002e20, 100_000_000_000_000_020_000),
    ],
)
def test_canonical_json_survives_jsonb_integral_number_normalization(floating, integer):
    assert canonical_json({"nested": [floating]}) == canonical_json({"nested": [integer]})
    if floating:
        assert json.loads(canonical_json(floating)) == integer
    with pytest.raises(ValueError):
        canonical_json({"value": float("nan")})


def test_request_preparation_preserves_params_and_rejects_conflicts():
    original = {"instructions": "Classify", "input": [{"role": "user", "content": "bad"}], "temperature": 0.2}
    body = prepare_body(original, "model", SCHEMA)
    assert body["instructions"] == original["instructions"]
    assert body["input"] == original["input"]
    assert body["temperature"] == 0.2
    assert body["model"] == "model"
    assert body["text"]["format"]["schema"] == SCHEMA
    assert "model" not in original
    request = Request("run", "provider-id", "", body, "submission")
    value = json.loads(jsonl(replace(make_manifest(body), config=Config(model="model"))))
    assert value["body"] == {**body, "max_output_tokens": 128}
    with pytest.raises(Failure):
        validate_request(request)
    for invalid in [
        {"input": "x", "model": "other"},
        {"input": "x", "stream": True},
        {"input": float("nan")},
        {"input": "x", "text": {"format": {"type": "text"}}},
    ]:
        with pytest.raises(Failure):
            prepare_body(invalid, "model", SCHEMA)
    with pytest.raises(Failure):
        validate_bulk_request(BulkRequest("id", {"input": "é" * MAX_BULK_ITEM_BYTES}))


def test_portable_schema_rejects_network_references_and_invalid_schema():
    assert validate_schema(SCHEMA) == SCHEMA
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    for schema in [
        {"$ref": "https://example.com/schema"},
        {"$dynamicRef": "https://example.com/schema"},
        {"$recursiveRef": "https://example.com/schema"},
        {"properties": {"nested": {"$dynamicRef": "other.json"}}},
        {"$id": "https://example.com/schema", "type": "object"},
        {"type": "invalid-type"},
    ]:
        with pytest.raises(Failure, match="INVALID_SCHEMA"):
            validate_schema(schema)


def test_external_schema_registry_denies_unvalidated_external_resources():
    # Exercise the resolver independently of admission validation: even a
    # mistakenly admitted schema must never trigger an external retrieval.
    with pytest.raises(Exception, match="Unresolvable"):
        schema_validator({"$ref": "https://example.invalid/must-not-be-retrieved"}).validate({})


@pytest.mark.parametrize("value", [None, [], "", 0, False, "bad"])
def test_raw_body_output_configuration_is_validated_without_manifest_schema(value):
    with pytest.raises(Failure, match="INVALID_INPUT"):
        prepare_body({"input": "request", "text": value}, "fake-model")
    with pytest.raises(Failure, match="INVALID_INPUT"):
        prepare_body({"input": "request", "text": {"format": value}}, "fake-model")


def test_embedded_schema_is_validated_without_manifest_schema():
    for keyword in ("$ref", "$dynamicRef", "$recursiveRef"):
        body = {
            "input": "request",
            "text": {
                "format": {"type": "json_schema", "schema": {keyword: "https://example.invalid/schema"}}
            },
        }
        with pytest.raises(Failure, match="INVALID_SCHEMA"):
            prepare_body(body, "fake-model")


def test_local_recursive_schema_references_are_supported_and_self_recursion_is_bounded():
    recursive = {
        "type": "object",
        "properties": {"children": {"type": "array", "items": {"$ref": "#"}}},
    }
    schema_validator(validate_schema(recursive)).validate({"children": [{"children": []}]})
    body = prepare_body({"input": "recursive"}, "fake-model", {"$ref": "#"})
    manifest = make_manifest(body)
    output = response_line(manifest.entries[0].request.request_id, "{}")
    outcomes, _ = reconcile(manifest, Job("job", "completed"), [output])
    assert outcomes[0].bulk["status"] == "parse_failed"
    assert len(outcomes[0].bulk["parse_error"].encode()) <= 2048
    assert outcomes[0].bulk["response"] is not None


def test_corrupt_stored_output_format_becomes_terminal_item_failure():
    manifest = make_manifest({"input": "request", "text": {"format": "corrupt"}})
    output = response_line(manifest.entries[0].request.request_id, "{}")
    outcomes, _ = reconcile(manifest, Job("job", "completed"), [output])
    assert outcomes[0].failure.code == "INVALID_RESULT"
    assert outcomes[0].bulk["status"] == "failed"


def test_bulk_schema_failures_preserve_raw_response_and_do_not_hide_other_successes():
    body = prepare_body({"input": "classify"}, "fake-model", SCHEMA)
    manifest = make_manifest(body, body)
    one, two = [entry.request.request_id for entry in manifest.entries]
    output = b"\n".join([response_line(two, '{"label": "positive"}'), response_line(one, '{"label": 7}')])
    outcomes, diagnostics = reconcile(manifest, Job("job", "completed"), [output])
    assert not diagnostics
    assert outcomes[0].result is not None and outcomes[0].failure is None
    assert outcomes[0].bulk["status"] == "parse_failed"
    assert outcomes[0].bulk["parse_error"]
    assert outcomes[0].bulk["response"]["output"][0]["content"][0]["text"] == '{"label": 7}'
    assert outcomes[1].bulk["status"] == "succeeded"
    assert outcomes[1].bulk["output"] == {"label": "positive"}
    assert outcomes[1].result.text == ""


def test_bulk_partial_duplicate_refusal_and_raw_mode_outcomes():
    manifest = make_manifest({"input": "raw"}, {"input": "missing"})
    one = manifest.entries[0].request.request_id
    outcomes, _ = reconcile(manifest, Job("job", "expired"), [response_line(one, "hello")])
    assert outcomes[0].bulk["status"] == "succeeded"
    assert outcomes[0].bulk["output"] == outcomes[0].bulk["response"]
    assert outcomes[1].bulk["status"] == "expired"
    assert outcomes[1].failure.code == "BATCH_EXPIRED"
    line = response_line(one)
    duplicate, _ = reconcile(manifest, Job("job", "completed"), [line + b"\n" + line])
    assert duplicate[0].failure.code == "DUPLICATE_RESULT"
    assert duplicate[0].bulk["status"] == "failed"
    refusal = response_line(one, output=[{"type": "message", "content": [{"type": "refusal"}]}])
    failed, _ = reconcile(manifest, Job("job", "completed"), [refusal])
    assert failed[0].failure.code == "MODEL_REFUSAL"
    assert failed[0].bulk["response"]["output"][0]["content"][0]["type"] == "refusal"
    cancelled = dumps({"custom_id": one, "error": {"code": "batch_cancelled"}}).encode()
    outcomes, _ = reconcile(manifest, Job("job", "cancelled"), [cancelled])
    assert outcomes[0].failure.code == "BATCH_CANCELLED"
    assert outcomes[0].bulk["status"] == "cancelled"


def test_bulk_raw_tool_calls_and_provider_error_material_are_preserved():
    manifest = make_manifest({"input": "call a tool"}, {"input": "error"})
    one, two = [entry.request.request_id for entry in manifest.entries]
    tool = {"type": "function_call", "name": "lookup", "arguments": '{"key":"x"}', "call_id": "call"}
    output = response_line(one, output=[tool])
    error = dumps(
        {"custom_id": two, "error": {"code": "bad_request", "message": "full provider detail"}}
    ).encode()
    outcomes, _ = reconcile(manifest, Job("job", "completed"), [output, error])
    assert outcomes[0].bulk["status"] == "succeeded"
    assert outcomes[0].bulk["response"]["output"] == [tool]
    assert outcomes[1].bulk["provider_error"]["message"] == "full provider detail"
    assert "full provider detail" not in outcomes[1].failure.message


def test_nonfinite_structured_output_becomes_parse_failure_not_unpersistable_data():
    schema = {"type": "object", "properties": {"value": {"type": "number"}}}
    body = prepare_body({"input": "number"}, "fake-model", schema)
    manifest = make_manifest(body)
    output = response_line(manifest.entries[0].request.request_id, '{"value": NaN}')
    outcomes, _ = reconcile(manifest, Job("job", "completed"), [output])
    assert outcomes[0].bulk["status"] == "parse_failed"
    dumps(outcomes[0])  # Every returned outcome must be safe for the finite store codec.


async def test_fake_provider_supports_portable_schema_without_caller_model(tmp_path):
    manifest = make_manifest(
        prepare_body({"input": "bad broken item"}, "fake-model", SCHEMA),
        prepare_body({"input": "good item"}, "fake-model", SCHEMA),
    )
    provider = FakeProvider(tmp_path)
    file_id = await provider.upload(manifest.batch_id, jsonl(manifest))
    created = await provider.create(file_id, {})
    job = await provider.retrieve(created.id)
    outcomes, _ = reconcile(manifest, job, list(await provider.results(job)))
    assert [item.bulk["output"]["label"] for item in outcomes] == ["negative", "positive"]
