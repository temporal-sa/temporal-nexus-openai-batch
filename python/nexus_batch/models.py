"""Small orchestration references and the data kept in PostgreSQL.

All duration fields are seconds, including serialized configuration and report
timings. Documents, completion tokens, and per-request outcomes belong in the
store; batch Workflow history carries references and aggregate control records.
The individual caller Workflow still records its own request and bounded result.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import types
from dataclasses import dataclass, fields, is_dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints

SERVICE = "llm-batch"
OPERATION = "respond"
CALLER_TASK_QUEUE = "nexus-batch-python-callers"
SERVICE_TASK_QUEUE = "nexus-batch-python-service"
NEXUS_ENDPOINT = "llm-batch-python"
STATUS_QUERY = "status"
# The per-operation envelopes stay deliberately small even though the external
# manifest can grow to the provider's batch limit. Byte limits count UTF-8 bytes.
MAX_REQUEST_BYTES = 8 * 1024
MAX_RESULT_BYTES = 4 * 1024
HARD_MAX_BATCH_REQUESTS = 50_000
HARD_MAX_BATCH_BYTES = 200_000_000
DEFAULT_BATCH_BYTES = 512 * 1024
ENROLLMENT_HEARTBEAT = 30.0


@dataclass
class Request:
    """Application input; request_id becomes the provider JSONL custom_id."""

    run_id: str
    request_id: str
    input: str


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class Result:
    request_id: str
    text: str
    usage: Usage | None = None


@dataclass
class Failure(Exception):
    code: str
    message: str
    provider_code: str = ""

    def __post_init__(self) -> None:
        Exception.__init__(self, self.code, self.message)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


@dataclass
class Outcome:
    """One terminal result or failure tied to a particular admitted request body."""

    request_id: str
    fingerprint: str
    batch_id: str
    result: Result | None = None
    failure: Failure | None = None


@dataclass
class Config:
    """An immutable run snapshot once written to PostgreSQL.

    ``window`` controls collection, ``result_deadline`` bounds provider waiting,
    and ``operation_timeout`` bounds the end-to-end Nexus/SAA execution. Extra
    operation time leaves room to deliver a deadline failure back to the caller.
    """

    model: str = "fake-model"
    mode: str = "fake"
    window: float = 5.0
    max_batch_requests: int = 100
    max_batch_bytes: int = DEFAULT_BATCH_BYTES
    max_admissions: int = 0
    poll_interval: float = 0.25
    result_deadline: float = 60.0
    operation_timeout: float = 120.0
    max_output_tokens: int = 128
    nexus_endpoint: str = NEXUS_ENDPOINT

    @classmethod
    def defaults(cls, mode: str = "fake") -> Config:
        # Live requests need a deliberate model selection and enough time for
        # OpenAI's 24-hour processing window plus collection and result delivery.
        if mode == "live":
            return cls(
                mode=mode,
                model="",
                poll_interval=30.0,
                result_deadline=25 * 3600.0,
                operation_timeout=26 * 3600.0,
            )
        return cls(mode=mode)

    def validate(self) -> None:
        if self.mode not in {"fake", "live"}:
            raise ValueError("mode must be fake or live")
        if not self.model or not self.nexus_endpoint:
            raise ValueError("model and Nexus endpoint are required")
        durations = (self.window, self.poll_interval, self.result_deadline, self.operation_timeout)
        if not all(math.isfinite(value) and value > 0 for value in durations):
            raise ValueError("durations must be finite and positive")
        if not self.window < self.result_deadline < self.operation_timeout:
            raise ValueError("durations must satisfy window < result deadline < operation timeout")
        if self.window + self.result_deadline >= self.operation_timeout:
            raise ValueError("collection window plus result deadline must be less than operation timeout")
        if not (
            1 <= self.max_batch_requests <= HARD_MAX_BATCH_REQUESTS
            and 1 <= self.max_batch_bytes <= HARD_MAX_BATCH_BYTES
            and self.max_admissions >= 0
        ):
            raise ValueError(
                "batch limits must stay within 50,000 requests and 200 MB; max admissions is nonnegative"
            )
        if not 16 <= self.max_output_tokens <= 512:
            raise ValueError("max output tokens must be between 16 and 512")


@dataclass
class CompletionTarget:
    """Exact Temporal SAA execution and attempt that owns external completion.

    A task token is an opaque completion capability, not a request identifier.
    Keep it in PostgreSQL and out of batch Workflow payloads and log messages.
    """

    namespace: str
    activity_id: str
    activity_run_id: str
    task_token: bytes
    attempt: int


@dataclass
class Enrollment:
    """Admission record with the original execution's absolute completion deadline."""

    request: Request
    completion_target: CompletionTarget
    fingerprint: str
    deadline_at: datetime


@dataclass
class Admission:
    batch_id: str
    fingerprint: str
    config: Config


@dataclass
class Entry:
    request: Request
    completion_target: CompletionTarget
    fingerprint: str
    admitted_at: datetime


@dataclass
class Manifest:
    """Sealed batch membership loaded only in Activities, never as Workflow input."""

    run_id: str
    batch_id: str
    config: Config
    entries: list[Entry]
    sealed_at: datetime
    bytes: int


@dataclass
class BatchRef:
    """Small Workflow input identifying an already sealed external batch.

    Counts, byte size, and timestamps support orchestration without copying the
    manifest into Temporal history. The earliest recipient deadline bounds work.
    """

    run_id: str
    batch_id: str
    config: Config
    requests: int
    bytes: int
    sealed_at: datetime
    first_admitted_at: datetime
    deadline_at: datetime


@dataclass
class StoredDelivery:
    request_id: str
    completion_target: CompletionTarget
    outcome: Outcome


@dataclass
class BatchDelivery:
    batch_id: str
    request_id: str
    completion_target: CompletionTarget
    outcome: Outcome


@dataclass
class DeliverySummary:
    """Bounded aggregate handed back from an Activity to the batch Workflow."""

    delivered: int = 0
    skipped: int = 0
    succeeded: int = 0
    failed: int = 0
    failure_counts: dict[str, int] = field(default_factory=dict)
    diagnostics: list[str] = field(default_factory=list)


@dataclass
class BatchReport:
    """Orchestration milestones; provider_status updates at Activity boundaries.

    A long-running poll Activity can be checking a newer OpenAI state while this
    saved report still shows the state returned by the previous Activity.
    """

    batch_id: str
    provider_batch_id: str = ""
    input_file_id: str = ""
    status: str = ""
    provider_status: str = ""
    failure_counts: dict[str, int] = field(default_factory=dict)
    requests: int = 0
    bytes: int = 0
    succeeded: int = 0
    failed: int = 0
    delivered: int = 0
    skipped: int = 0
    diagnostics: list[str] = field(default_factory=list)
    collection_wait: float = 0.0
    provider_duration: float = 0.0
    delivery_duration: float = 0.0


@dataclass
class RunStatus:
    run_id: str
    backing: str = "nexus_standalone_activity"
    storage: str = "postgres"
    completion_mode: str = "external_async_completion"
    admitted: int = 0
    buffered: int = 0
    closed: bool = False
    batches: list[BatchReport] = field(default_factory=list)


@dataclass
class RunInput:
    run_id: str
    config: Config


@dataclass
class CallerInput:
    request: Request
    config: Config


@dataclass
class SAAInput:
    request: Request
    config: Config


def _json_value(value: Any) -> Any:
    # Store JSON has explicit UTC timestamps and base64 tokens so round trips do
    # not depend on a database driver's implicit dataclass or bytes conversion.
    if is_dataclass(value):
        return {item.name: _json_value(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def dumps(value: Any) -> str:
    """Use a stable compact representation for fingerprints and size accounting."""
    return json.dumps(_json_value(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


T = TypeVar("T")


def _decode(cls: Any, value: Any) -> Any:
    """Restore the shared dataclasses from PostgreSQL's JSON representation."""
    if value is None:
        return None
    origin, args = get_origin(cls), get_args(cls)
    if origin in (Union, types.UnionType):
        for variant in args:
            if variant is not type(None):
                return _decode(variant, value)
    if origin is list:
        return [_decode(args[0], item) for item in value]
    if origin is dict:
        return {key: _decode(args[1], item) for key, item in value.items()}
    if cls is datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("timestamps must include a timezone")
            return value.astimezone(timezone.utc)
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("timestamps must include a timezone")
        return timestamp.astimezone(timezone.utc)
    if cls is bytes:
        return base64.b64decode(value, validate=True) if isinstance(value, str) else bytes(value)
    if is_dataclass(cls):
        if isinstance(value, cls):
            return value
        hints = get_type_hints(cls)
        return cls(
            **{
                item.name: _decode(hints[item.name], value[item.name])
                for item in fields(cls)
                if item.name in value
            }
        )
    return value


def loads(cls: type[T], value: str | bytes | dict[str, Any]) -> T:
    return _decode(cls, json.loads(value) if isinstance(value, (str, bytes)) else value)


_VALID_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")


def validate_request(request: Request) -> None:
    # Validate the entire serialized envelope, rather than just document length;
    # this bounds the payload actually supplied to Nexus and the SAA.
    if not _VALID_ID.fullmatch(request.run_id) or not _VALID_ID.fullmatch(request.request_id):
        raise Failure(
            "INVALID_INPUT",
            "run_id and request_id must be 1-96 letters, digits, dots, underscores or hyphens",
        )
    if not request.input:
        raise Failure("INVALID_INPUT", "input is required")
    if len(dumps(request).encode()) > MAX_REQUEST_BYTES:
        raise Failure("INVALID_INPUT", "request envelope exceeds 8 KiB")


def fingerprint(request: Request) -> str:
    """Detect reuse of a logical request ID with changed request content."""
    return hashlib.sha256(dumps(request).encode()).hexdigest()


def workflow_id(batch_id: str) -> str:
    # Stable IDs let the dispatch outbox retry an uncertain Workflow start.
    return "batch/" + batch_id


def caller_id(run_id: str, request_id: str) -> str:
    return f"caller/{run_id}/{request_id}"


def jsonl_line(request: Request, config: Config) -> bytes:
    """Encode one independent Responses request in OpenAI Batch JSONL format.

    custom_id is the join key because output lines can arrive in another order.
    ``store: False`` is part of the Responses request; the application still
    persists its own admitted document and normalized outcome in PostgreSQL.
    """
    return (
        dumps(
            {
                "custom_id": request.request_id,
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": config.model,
                    "input": request.input,
                    "max_output_tokens": config.max_output_tokens,
                    "store": False,
                },
            }
        )
        + "\n"
    ).encode()


def jsonl(manifest: Manifest) -> bytes:
    # Recheck limits when reading a sealed manifest as well as during admission.
    # This is the actual uploaded JSONL size, not the larger database JSON size.
    data = b"".join(jsonl_line(entry.request, manifest.config) for entry in manifest.entries)
    if (
        not (1 <= len(manifest.entries) <= manifest.config.max_batch_requests)
        or len(data) > manifest.config.max_batch_bytes
    ):
        raise ValueError("invalid sealed batch size")
    return data
