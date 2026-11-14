"""Portable bulk references; large values belong only in activity-side storage.

Python model classes never cross the Nexus boundary. The producer stores a JSON
Schema with the immutable manifest; the consumer may apply stricter local model
validation after loading an item.
"""

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Generic, TypeVar

from .models import CompletionTarget, Config, Failure, MAX_REQUEST_BYTES, dumps

BULK_OPERATION = "responses"
MAX_BULK_ITEM_BYTES = 1024 * 1024
MAX_BULK_OUTCOME_BYTES = 3 * 1024 * 1024
MAX_BULK_PAGE_BYTES = 4 * 1024 * 1024
MAX_BULK_PAGE_ITEMS = 1000
MAX_SCHEMA_BYTES = 64 * 1024
T = TypeVar("T")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def canonical_json(value: Any) -> bytes:
    """Stable bytes for immutable content identities and byte accounting."""

    def normalize(item: Any) -> Any:
        # JSONB stores JSON numbers as decimal numeric values. For example,
        # 1e20 may reload as an integer while the producer supplied a float.
        # Normalize equivalent integral forms before hashing. Decimal(repr())
        # preserves the value emitted by JSON, avoiding int(float)'s binary
        # rounding artefact for large values such as 1.0000000000000002e20.
        if isinstance(item, float) and math.isfinite(item) and item.is_integer():
            return int(Decimal(repr(item)))
        if isinstance(item, dict):
            return {key: normalize(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalize(child) for child in item]
        return item

    return json.dumps(
        normalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


@dataclass(frozen=True)
class BulkRequest:
    custom_id: str
    body: dict[str, Any]


@dataclass(frozen=True)
class BatchInputRef(Generic[T]):
    manifest_id: str
    digest: str
    count: int


@dataclass(frozen=True)
class BulkSubmission:
    run_id: str
    submission_id: str
    requests: BatchInputRef


@dataclass(frozen=True)
class BulkSAAInput:
    submission: BulkSubmission
    config: Config


@dataclass
class BatchCounts:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    expired: int = 0
    cancelled: int = 0
    parse_failed: int = 0

    @property
    def has_failures(self) -> bool:
        return bool(self.failed or self.expired or self.cancelled or self.parse_failed)


@dataclass(frozen=True)
class BatchResultRef(Generic[T]):
    submission_id: str
    manifest_id: str
    digest: str
    count: int
    counts: BatchCounts = field(default_factory=BatchCounts)

    def page(self, offset: int = 0, limit: int = 100) -> "BatchChunkRef[T]":
        validate_page(offset, limit)
        return BatchChunkRef(self, offset, limit)


@dataclass(frozen=True)
class BatchChunkRef(Generic[T]):
    ref: BatchResultRef
    offset: int
    limit: int


@dataclass
class BulkItem(Generic[T]):
    custom_id: str
    request: dict[str, Any]
    status: str
    response: dict[str, Any] | None = None
    output: T | None = None
    error: Failure | None = None
    parse_error: str = ""
    provider_error: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.status == "succeeded" and not self.parse_error


@dataclass(frozen=True)
class BulkDelivery:
    submission_id: str
    completion_target: CompletionTarget
    result: BatchResultRef


def validate_page(offset: int, limit: int) -> None:
    if (
        not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
        or not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_BULK_PAGE_ITEMS
    ):
        raise Failure("INVALID_INPUT", "page offset must be nonnegative and limit must be 1-1000")


def validate_input_ref(ref: BatchInputRef[Any]) -> None:
    if (
        not isinstance(ref, BatchInputRef)
        or not isinstance(ref.manifest_id, str)
        or not _ID.fullmatch(ref.manifest_id)
        or not isinstance(ref.digest, str)
        or not _DIGEST.fullmatch(ref.digest)
        or not isinstance(ref.count, int)
        or isinstance(ref.count, bool)
        or not 1 <= ref.count <= 2**63 - 1
    ):
        raise Failure("INVALID_INPUT", "invalid immutable input manifest reference")


def validate_submission(submission: BulkSubmission) -> None:
    if (
        not isinstance(submission, BulkSubmission)
        or not isinstance(submission.run_id, str)
        or not _ID.fullmatch(submission.run_id)
        or not isinstance(submission.submission_id, str)
        or not _ID.fullmatch(submission.submission_id)
    ):
        raise Failure("INVALID_INPUT", "run_id and submission_id must be valid 1-96 character IDs")
    validate_input_ref(submission.requests)
    if len(dumps(submission).encode()) > MAX_REQUEST_BYTES:
        raise Failure("INVALID_INPUT", "bulk control envelope exceeds 8 KiB")


def validate_bulk_request(request: BulkRequest) -> None:
    if (
        not isinstance(request, BulkRequest)
        or not isinstance(request.custom_id, str)
        or not 1 <= len(request.custom_id.encode()) <= 256
        or not isinstance(request.body, dict)
        or not request.body
    ):
        raise Failure("INVALID_INPUT", "bulk requests require a custom ID and a nonempty request body")
    try:
        encoded = canonical_json({"custom_id": request.custom_id, "body": request.body})
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise Failure("INVALID_INPUT", "request body must contain finite JSON values") from None
    if len(encoded) > MAX_BULK_ITEM_BYTES:
        raise Failure("INVALID_INPUT", "bulk request exceeds 1 MiB")
    if request.body.get("stream") is not None and request.body["stream"] is not False:
        raise Failure("INVALID_INPUT", "batch requests cannot stream")


def validate_schema(schema: dict[str, Any] | None) -> dict[str, Any] | None:
    if schema is None:
        return None
    if not isinstance(schema, dict):
        raise Failure("INVALID_SCHEMA", "output schema must be a JSON Schema object")
    try:
        encoded = canonical_json(schema)
        if len(encoded) > MAX_SCHEMA_BYTES:
            raise ValueError()
        normalized = json.loads(encoded)
        nodes = 0

        def check(value: Any, depth: int = 0) -> None:
            nonlocal nodes
            nodes += 1
            if nodes > 10_000 or depth > 64:
                raise ValueError()
            if isinstance(value, dict):
                if "$id" in value:
                    raise ValueError()
                for key in ("$ref", "$dynamicRef", "$recursiveRef"):
                    reference = value.get(key)
                    if reference is not None and (
                        not isinstance(reference, str) or not (reference == "#" or reference.startswith("#/"))
                    ):
                        raise ValueError()
                for child in value.values():
                    check(child, depth + 1)
            elif isinstance(value, list):
                for child in value:
                    check(child, depth + 1)

        check(normalized)
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(normalized)
    except Exception:
        raise Failure(
            "INVALID_SCHEMA", "schema must be valid, bounded JSON Schema using only local references"
        ) from None
    return normalized


def schema_validator(schema: dict[str, Any]):
    """Build an evaluator whose reference resolver cannot access the network.

    Admission separately checks every reference keyword. The explicit registry
    also denies external resources if a future keyword or stored-schema bug
    bypasses that check. Fragment references continue to resolve in this schema.
    """
    from jsonschema import Draft202012Validator
    from referencing import Registry
    from referencing.exceptions import NoSuchResource

    def deny_external_resource(uri: str):
        raise NoSuchResource(ref=uri)

    return Draft202012Validator(schema, registry=Registry(retrieve=deny_external_resource))


def schema_digest(schema: dict[str, Any] | None) -> str:
    return hashlib.sha256(canonical_json(schema)).hexdigest()


def prepare_body(body: dict[str, Any], model: str, schema: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate a stored body and apply the run model and optional portable schema."""
    validate_bulk_request(BulkRequest("validation", body))
    if not isinstance(model, str) or not model or body.get("model", model) != model:
        raise Failure("INVALID_INPUT", "request model must equal the configured run model")
    result = json.loads(canonical_json(body))
    result["model"] = model
    result.setdefault("store", False)
    text = result.get("text", {})
    if not isinstance(text, dict):
        raise Failure("INVALID_INPUT", "request text configuration must be an object")
    existing = text.get("format")
    if "format" in text and not isinstance(existing, dict):
        raise Failure("INVALID_INPUT", "request text.format must be an object")
    if isinstance(existing, dict) and existing.get("type") == "json_schema":
        if validate_schema(existing.get("schema")) is None:
            raise Failure("INVALID_SCHEMA", "structured output schema is missing")
    if schema is not None:
        schema = validate_schema(schema)
        text_format = {
            "type": "json_schema",
            "name": "bulk_output",
            "schema": schema,
            "strict": True,
        }
        if existing is not None and canonical_json(existing) != canonical_json(text_format):
            raise Failure("INVALID_INPUT", "request text.format conflicts with the manifest output schema")
        result["text"] = {**text, "format": text_format}
    validate_bulk_request(BulkRequest("validation", result))
    return result


def provider_id(submission_id: str, position: int) -> str:
    return "bulk-" + hashlib.sha256(f"{submission_id}\0{position}".encode()).hexdigest()
