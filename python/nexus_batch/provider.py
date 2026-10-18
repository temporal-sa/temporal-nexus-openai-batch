"""Provider adapters and the conversion from unordered files to caller outcomes.

OpenAI Batch is a file/job protocol: upload JSONL, create one job, poll its state,
then download output and error files. The fake adapter replaces only this I/O
boundary; enrollment, Workflow orchestration, and SAA completion remain real.
Reconciliation joins on custom_id, because file order is not request order.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from urllib.parse import quote, urlsplit

import httpx

from .models import HARD_MAX_BATCH_BYTES, MAX_RESULT_BYTES, Failure, Manifest, Outcome, Result, Usage, dumps

MAX_CONTROL_BYTES = 1024 * 1024
# Listing is deliberately finite. Reaching this bound means lookup is uncertain,
# not evidence that the original create failed and a replacement POST is safe.
MAX_LIST_PAGES = 1000


@dataclass
class Job:
    """Small provider control record; document text stays in separate files."""

    id: str
    status: str
    input_file_id: str = ""
    output_file_id: str = ""
    error_file_id: str = ""
    metadata: dict[str, str] = field(default_factory=dict)

    def terminal(self) -> bool:
        return self.status in {"completed", "failed", "expired", "cancelled"}


@dataclass
class ProviderError(Exception):
    """Tell orchestration whether to retry reads or reconcile an uncertain POST.

    ``permanent`` means retrying this rejected request is not useful.
    ``ambiguous`` means a create may have succeeded despite the missing response.
    These are distinct facts; an HTTP failure is not proof that no job exists.
    """

    op: str
    code: str
    message: str
    status: int = 0
    permanent: bool = False
    ambiguous: bool = False

    def __post_init__(self) -> None:
        Exception.__init__(self, self.op, self.code, self.message)

    def __str__(self) -> str:
        status = f" (HTTP {self.status})" if self.status else ""
        return f"provider {self.op}: {self.code}{status}: {self.message}"


class Provider(Protocol):
    """External I/O seam shared by live HTTP and deterministic failure scenarios."""

    async def upload(self, batch_id: str, data: bytes) -> str: ...
    async def create(self, file_id: str, metadata: dict[str, str]) -> Job: ...
    async def find(self, metadata: dict[str, str]) -> list[Job]: ...
    async def retrieve(self, job_id: str) -> Job: ...
    async def download(self, file_id: str) -> bytes: ...
    async def close(self) -> None: ...


def _invalid_response(op: str, ambiguous: bool = False) -> ProviderError:
    return ProviderError(
        op,
        "invalid_response",
        "provider returned an invalid response",
        permanent=not ambiguous,
        ambiguous=ambiguous,
    )


def _job(value: Any, op: str, ambiguous: bool = False) -> Job:
    # Validate control fields before returning them as Activity results. During
    # create, a malformed response still cannot disprove an accepted submission.
    if not isinstance(value, dict) or not isinstance(value.get("id"), str) or not value["id"]:
        raise _invalid_response(op, ambiguous)
    if not isinstance(value.get("status"), str) or not value["status"]:
        raise _invalid_response(op, ambiguous)
    if any(
        value.get(name) is not None and not isinstance(value[name], str)
        for name in ("input_file_id", "output_file_id", "error_file_id")
    ):
        raise _invalid_response(op, ambiguous)
    metadata = value.get("metadata") or {}
    if not isinstance(metadata, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in metadata.items()
    ):
        raise _invalid_response(op, ambiguous)
    return Job(
        value["id"],
        value["status"],
        value.get("input_file_id") or "",
        value.get("output_file_id") or "",
        value.get("error_file_id") or "",
        dict(metadata),
    )


def _matches(actual: dict[str, str], expected: dict[str, str]) -> bool:
    return all(actual.get(key) == value for key, value in expected.items())


class OpenAIProvider:
    """HTTPX performs one HTTP attempt, including for batch creation.

    An uncertain POST is reported as ambiguous. The Workflow must look up that
    submission before making any decision; this adapter never resubmits it.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        *,
        client: httpx.AsyncClient | None = None,
        max_file_bytes: int = HARD_MAX_BATCH_BYTES,
    ):
        if not api_key.strip():
            raise ValueError("OPENAI_API_KEY is required for live provider")
        url = urlsplit(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.netloc
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError("invalid OpenAI base URL")
        if max_file_bytes < 1:
            raise ValueError("maximum file size must be positive")
        self._key = api_key
        self._base_url = base_url.rstrip("/")
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(timeout=60.0, follow_redirects=False)
        self._max_file_bytes = max_file_bytes

    async def _request(
        self,
        op: str,
        method: str,
        path: str,
        *,
        limit: int = MAX_CONTROL_BYTES,
        submission: bool = False,
        **kwargs: Any,
    ) -> bytes:
        """Make one bounded request without logging secrets or provider bodies.

        Redirects are disabled so Authorization stays on the configured endpoint.
        Streamed size checks bound memory before a whole response is buffered.
        Workflow/Activity policy supplies retries; this helper never hides them.
        """
        try:
            async with self._client.stream(
                method,
                self._base_url + path,
                headers={"Authorization": "Bearer " + self._key},
                follow_redirects=False,
                **kwargs,
            ) as response:
                if not 200 <= response.status_code < 300:
                    # Retryable create failures, including transport/server
                    # failures, are uncertain submissions rather than permission
                    # for another paid create request.
                    permanent = 400 <= response.status_code < 500 and response.status_code not in {
                        408,
                        409,
                        429,
                    }
                    raise ProviderError(
                        op,
                        "http_error",
                        "provider rejected the HTTP request",
                        status=response.status_code,
                        permanent=permanent,
                        ambiguous=submission and not permanent,
                    )
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > limit:
                        raise ProviderError(
                            op,
                            "response_size_or_read",
                            "provider response exceeded configured size limit",
                            permanent=not submission,
                            ambiguous=submission,
                        )
                    data.extend(chunk)
                return bytes(data)
        except httpx.HTTPError:
            raise ProviderError(
                op,
                "transport_error",
                "provider request did not return a complete response",
                ambiguous=submission,
            ) from None

    @staticmethod
    def _json(data: bytes, op: str, ambiguous: bool = False) -> Any:
        try:
            return json.loads(data)
        except (ValueError, UnicodeError):
            raise _invalid_response(op, ambiguous) from None

    async def upload(self, batch_id: str, data: bytes) -> str:
        """Store JSONL as a purpose=batch file before starting model processing."""
        if not data or len(data) > self._max_file_bytes:
            raise ProviderError(
                "upload", "input_size", "input file is empty or exceeds configured size limit", permanent=True
            )
        raw = await self._request(
            "upload",
            "POST",
            "/files",
            data={"purpose": "batch"},
            files={"file": ("batch.jsonl", data, "application/jsonl")},
        )
        value = self._json(raw, "upload")
        if not isinstance(value, dict) or not isinstance(value.get("id"), str) or not value["id"]:
            raise _invalid_response("upload")
        return value["id"]

    async def create(self, file_id: str, metadata: dict[str, str]) -> Job:
        # The endpoint must agree with each JSONL line. Upload alone is separate
        # from submission; this POST is the point that creates the processing job.
        if not file_id:
            raise ProviderError("create", "input_file_missing", "input file ID is required", permanent=True)
        raw = await self._request(
            "create",
            "POST",
            "/batches",
            submission=True,
            json={
                "input_file_id": file_id,
                "endpoint": "/v1/responses",
                "completion_window": "24h",
                "metadata": metadata,
            },
        )
        return _job(self._json(raw, "create", True), "create", True)

    async def retrieve(self, job_id: str) -> Job:
        raw = await self._request("retrieve", "GET", "/batches/" + quote(job_id, safe=""))
        return _job(self._json(raw, "retrieve"), "retrieve")

    async def find(self, metadata: dict[str, str]) -> list[Job]:
        """Search every allowed page; the Activity additionally checks input_file_id.

        This operation only observes jobs. Zero matches or multiple matches stay
        ambiguous, and neither case causes this adapter to create a replacement.
        """
        found, seen, after = [], set(), ""
        for _ in range(MAX_LIST_PAGES):
            params = {"limit": "100"}
            if after:
                params["after"] = after
            raw = await self._request("find", "GET", "/batches", params=params)
            page = self._json(raw, "find")
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise _invalid_response("find")
            jobs = [_job(item, "find") for item in page["data"]]
            found.extend(job for job in jobs if _matches(job.metadata, metadata))
            if not page.get("has_more"):
                return found
            after = page.get("last_id") or (jobs[-1].id if jobs else "")
            if not isinstance(after, str) or not after or after in seen:
                raise _invalid_response("find")
            seen.add(after)
        raise ProviderError(
            "find",
            "pagination_limit",
            "provider batch listing exceeded 1000 pages; submission absence cannot be established",
            permanent=True,
        )

    async def download(self, file_id: str) -> bytes:
        # Output and error files share the same bounded download path. An absent
        # file is allowed because a terminal job may have only one file or neither.
        if not file_id:
            return b""
        return await self._request(
            "results", "GET", "/files/" + quote(file_id, safe="") + "/content", limit=self._max_file_bytes
        )

    async def results(self, job: Job) -> tuple[bytes, bytes]:
        return await self.download(job.output_file_id), await self.download(job.error_file_id)

    async def close(self) -> None:
        if self._owned_client:
            await self._client.aclose()


class FakeProvider:
    """A persistent provider substitute, shared by concurrent worker processes.

    Only provider I/O is replaced. A lock and atomic file replacement preserve
    jobs across worker crashes and serialize processes using the same directory.
    """

    SCENARIOS = {
        "success",
        "mixed",
        "expired",
        "missing",
        "duplicate",
        "unknown",
        "malformed",
        "submission-unknown",
        "submission-unreconciled",
    }

    def __init__(
        self,
        state_dir: str | Path = ".state/python-fake-provider",
        scenario: str = "success",
        processing_delay: float = 0.0,
    ):
        if scenario == "unreconciled-unknown":
            scenario = "submission-unreconciled"
        if scenario not in self.SCENARIOS:
            raise ValueError(f"unknown fake provider scenario {scenario!r}")
        if not math.isfinite(processing_delay) or processing_delay < 0:
            raise ValueError("fake processing delay cannot be negative")
        self._dir = Path(state_dir)
        self._files = self._dir / "files"
        self._files.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._scenario = scenario
        self._delay = processing_delay

    @asynccontextmanager
    async def _locked(self) -> AsyncIterator[dict[str, Any]]:
        # A process-local asyncio lock would not protect two worker processes.
        # The file lock covers read/modify/write of the shared fake job catalog.
        with (self._dir / "lock").open("a+b") as lock:
            os.chmod(lock.name, 0o600)
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.01)
            try:
                path = self._dir / "state.json"
                state = json.loads(path.read_bytes()) if path.exists() else {"jobs": {}}
                if not isinstance(state, dict) or not isinstance(state.get("jobs"), dict):
                    raise ValueError("invalid persisted fake provider state")
                yield state
                _atomic_write(path, dumps(state).encode())
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _file(self, file_id: str) -> Path:
        if not file_id or "/" in file_id or "\\" in file_id or file_id in {".", ".."}:
            raise ProviderError("file", "invalid_file_id", "invalid fake file ID", permanent=True)
        return self._files / file_id

    def _read(self, file_id: str) -> bytes:
        try:
            return self._file(file_id).read_bytes()
        except FileNotFoundError:
            raise ProviderError(
                "file", "not_found", "fake input or result file does not exist", status=404, permanent=True
            ) from None

    async def upload(self, batch_id: str, data: bytes) -> str:
        if not data or len(data) > HARD_MAX_BATCH_BYTES:
            raise ProviderError(
                "upload", "input_size", "input file is empty or exceeds configured size limit", permanent=True
            )
        file_id = "file-input-" + hashlib.sha256(batch_id.encode() + b"\0" + data).hexdigest()[:32]
        async with self._locked():
            _atomic_write(self._file(file_id), data)
        return file_id

    async def create(self, file_id: str, metadata: dict[str, str]) -> Job:
        async with self._locked() as state:
            output, errors = _fake_results(self._read(file_id), self._scenario)
            job = Job("batch-fake-" + uuid.uuid4().hex, "in_progress", file_id, metadata=dict(metadata))
            if output:
                job.output_file_id = "file-output-" + job.id
                _atomic_write(self._file(job.output_file_id), output)
            if errors:
                job.error_file_id = "file-error-" + job.id
                _atomic_write(self._file(job.error_file_id), errors)
            state["jobs"][job.id] = {
                "job": json.loads(dumps(job)),
                "ready_at": time.time() + self._delay,
                "terminal": "expired" if self._scenario == "expired" else "completed",
                "hidden": self._scenario == "submission-unreconciled",
            }
        if self._scenario in {"submission-unknown", "submission-unreconciled"}:
            # Persist the job first, then simulate losing its acknowledgement.
            # This exercises lookup recovery rather than simply rejecting POST.
            raise ProviderError(
                "create", "submission_unknown", "simulated connection loss after submission", ambiguous=True
            )
        return job

    @staticmethod
    def _current(record: dict[str, Any]) -> Job:
        job = _job(record["job"], "fake")
        if time.time() >= record["ready_at"]:
            return replace(job, status=record["terminal"])
        return replace(job, output_file_id="", error_file_id="")

    async def retrieve(self, job_id: str) -> Job:
        async with self._locked() as state:
            if job_id not in state["jobs"]:
                raise ProviderError(
                    "retrieve", "not_found", "fake batch does not exist", status=404, permanent=True
                )
            return self._current(state["jobs"][job_id])

    async def find(self, metadata: dict[str, str]) -> list[Job]:
        async with self._locked() as state:
            return sorted(
                (
                    self._current(record)
                    for record in state["jobs"].values()
                    if not record.get("hidden") and _matches(record["job"].get("metadata", {}), metadata)
                ),
                key=lambda job: job.id,
            )

    async def download(self, file_id: str) -> bytes:
        if not file_id:
            return b""
        async with self._locked():
            return self._read(file_id)

    async def results(self, job: Job) -> tuple[bytes, bytes]:
        current = await self.retrieve(job.id)
        if not current.terminal():
            raise ProviderError("results", "not_ready", "fake batch is still processing")
        return await self.download(current.output_file_id), await self.download(current.error_file_id)

    async def close(self) -> None:
        pass


def _atomic_write(path: Path, data: bytes) -> None:
    """Publish a fully written fake-provider record instead of a partial JSON file."""
    descriptor, name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _fake_results(data: bytes, scenario: str) -> tuple[bytes, bytes]:
    """Produce realistic success/error JSONL, including reconciliation edge cases."""
    outputs, errors, ids = [], [], []
    for line in data.splitlines():
        if not line.strip():
            continue
        try:
            request = json.loads(line)
            request_id, text = request["custom_id"], request["body"]["input"]
            if not isinstance(request_id, str) or not request_id or not isinstance(text, str):
                raise ValueError()
        except (KeyError, TypeError, ValueError, UnicodeError):
            raise ProviderError(
                "create", "invalid_jsonl", "fake provider input is invalid", permanent=True
            ) from None
        index = len(ids)
        ids.append(request_id)
        if scenario == "missing" and index == 0:
            continue
        if (scenario == "mixed" and index % 3 == 1) or (scenario == "expired" and index % 2 == 0):
            errors.append(
                dumps(
                    {
                        "custom_id": request_id,
                        "response": None,
                        "error": {
                            "code": "batch_expired" if scenario == "expired" else "simulated_request_error",
                            "message": "simulated request failure",
                        },
                    }
                ).encode()
            )
            continue
        if len(text.encode()) > 256:
            text = text[:128]
        outputs.append(
            dumps(
                {
                    "custom_id": request_id,
                    "error": None,
                    "response": {
                        "status_code": 200,
                        "request_id": "req-fake-" + request_id,
                        "body": {
                            "id": "resp-fake-" + request_id,
                            "status": "completed",
                            "object": "response",
                            "output": [
                                {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": "Fake response: " + text}],
                                }
                            ],
                            "usage": {"input_tokens": 12, "output_tokens": 20},
                        },
                    },
                }
            ).encode()
        )
    if not ids:
        raise ProviderError("create", "empty_batch", "fake provider input is empty", permanent=True)
    if scenario == "duplicate" and outputs:
        outputs.append(outputs[0])
    if scenario == "unknown":
        outputs.append(
            dumps(
                {"custom_id": "unknown-provider-request", "response": None, "error": {"code": "unknown"}}
            ).encode()
        )
    if scenario == "malformed" and outputs:
        outputs[0] = dumps(
            {"custom_id": ids[0], "response": {"status_code": 200, "body": {"status": "completed"}}}
        ).encode()
        outputs.append(b"{malformed JSON")
    # Reverse success lines intentionally: correct delivery must use custom_id,
    # and must never accidentally rely on the manifest's positional order.
    return b"\n".join(reversed(outputs)), b"\n".join(errors)


_MACHINE_CODE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_STATUSES = {
    "completed",
    "failed",
    "expired",
    "cancelled",
    "validating",
    "in_progress",
    "finalizing",
    "cancelling",
}


def _provider_failure(code: str, message: str, provider_code: Any) -> Failure:
    # Preserve a bounded machine code, not arbitrary provider prose that could
    # contain a document or expand the failure payload recorded by the caller.
    safe_code = (
        provider_code if isinstance(provider_code, str) and _MACHINE_CODE.fullmatch(provider_code) else ""
    )
    if safe_code:
        message += f" (provider code: {safe_code})"
    return Failure(code, message, safe_code)


def _normalize(record: dict[str, Any]) -> tuple[Result | None, Failure | None]:
    """Reduce one provider record to bounded application text or an explicit failure.

    A 2xx record alone is insufficient: the Responses body must be completed,
    contain usable text, and fit the caller result limit. Refusal, incomplete
    generation, malformed output, and provider errors stay distinct outcomes.
    """
    error = record.get("error")
    if error is not None:
        raw_code = error.get("code", "") if isinstance(error, dict) else ""
        code = "BATCH_EXPIRED" if raw_code == "batch_expired" else "PROVIDER_REQUEST_ERROR"
        return None, _provider_failure(code, "provider could not complete this request", raw_code)
    response = record.get("response")
    if not isinstance(response, dict):
        return None, Failure("INVALID_RESULT", "provider returned neither a response nor an error")
    body, status = response.get("body"), response.get("status_code", 0)
    body_error = body.get("error") if isinstance(body, dict) else None
    raw_code = body_error.get("code", "") if isinstance(body_error, dict) else ""
    if not isinstance(status, int) or isinstance(status, bool) or not 200 <= status < 300:
        status = (
            status if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599 else 0
        )
        return None, _provider_failure(
            "PROVIDER_REQUEST_ERROR", f"provider returned HTTP {status} for this request", raw_code
        )
    if not isinstance(body, dict):
        return None, Failure("INVALID_RESULT", "provider response body is malformed")
    if body_error is not None:
        return None, _provider_failure(
            "PROVIDER_REQUEST_ERROR", "provider response contains an error", raw_code
        )
    if body.get("status") != "completed":
        return None, Failure("INCOMPLETE_RESULT", "provider response did not complete")
    text = []
    output = body.get("output") or []
    if not isinstance(output, list):
        return None, Failure("INVALID_RESULT", "provider response body is malformed")
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content") or []
        if not isinstance(content, list):
            return None, Failure("INVALID_RESULT", "provider response body is malformed")
        for part in content:
            if not isinstance(part, dict):
                return None, Failure("INVALID_RESULT", "provider response body is malformed")
            if part.get("type") == "refusal":
                return None, Failure("MODEL_REFUSAL", "model declined this request")
            if part.get("type") == "output_text":
                if not isinstance(part.get("text"), str):
                    return None, Failure("INVALID_RESULT", "provider response body is malformed")
                text.append(part["text"])
    if not text or not "".join(text):
        return None, Failure("INVALID_RESULT", "provider response contains no output text")
    usage = None
    if body.get("usage") is not None:
        raw_usage = body["usage"]
        if not isinstance(raw_usage, dict):
            return None, Failure("INVALID_RESULT", "provider response contains invalid token usage")
        counts = (raw_usage.get("input_tokens", 0), raw_usage.get("output_tokens", 0))
        if any(not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts):
            return None, Failure("INVALID_RESULT", "provider response contains invalid token usage")
        usage = Usage(*counts)
    result = Result(record["custom_id"], "".join(text), usage)
    if len(dumps(result).encode()) > MAX_RESULT_BYTES:
        return None, Failure("RESULT_TOO_LARGE", "normalized provider result exceeds 4 KiB")
    return result, None


def reconcile(manifest: Manifest, job: Job, files: list[bytes]) -> tuple[list[Outcome], list[str]]:
    """Produce exactly one bounded terminal outcome for each manifest request.

    Begin with a failure for every expected member, then replace it only after
    validating a uniquely matching custom_id. This makes partial provider files
    explicit: missing, malformed, unknown, and duplicate rows cannot strand a
    caller or deliver another document's response. The returned set is persisted
    atomically by the results Activity before any external completion happens.
    """
    missing_code = {"failed": "BATCH_FAILED", "cancelled": "BATCH_CANCELLED", "expired": "BATCH_EXPIRED"}.get(
        job.status, "MISSING_RESULT"
    )
    status = job.status if job.status in _STATUSES else "unknown"
    outcomes = [
        Outcome(
            entry.request.request_id,
            entry.fingerprint,
            manifest.batch_id,
            failure=Failure(
                missing_code, f"provider did not return a result for this request (batch status: {status})"
            ),
        )
        for entry in manifest.entries
    ]
    expected = {entry.request.request_id: index for index, entry in enumerate(manifest.entries)}
    seen: dict[str, int] = {}
    diagnostics: list[str] = []

    def diagnose(message: str) -> None:
        if len(diagnostics) < 100:
            diagnostics.append(message)
        elif len(diagnostics) == 100:
            diagnostics.append("additional diagnostics omitted")

    for file_index, data in enumerate(files):
        name = "output" if file_index == 0 else "error"
        for line_number, line in enumerate(data.split(b"\n"), 1):
            if not line.strip():
                continue
            if len(line) > MAX_CONTROL_BYTES:
                diagnose(f"{name} line {line_number} exceeds JSONL line limit")
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict) or not isinstance(record.get("custom_id", ""), str):
                    raise ValueError()
            except (ValueError, UnicodeError):
                diagnose(f"{name} line {line_number} is malformed JSON")
                continue
            request_id = record.get("custom_id", "")
            if request_id not in expected:
                diagnose(f"{name} line {line_number} has an unknown or missing custom_id")
                continue
            seen[request_id] = seen.get(request_id, 0) + 1
            outcome = outcomes[expected[request_id]]
            if seen[request_id] > 1:
                # Do not choose an arbitrary duplicate, even if the text matches.
                # Subsequent duplicates must leave this request failed as well.
                outcome.result = None
                outcome.failure = Failure(
                    "DUPLICATE_RESULT", "provider returned multiple results for this request"
                )
                diagnose(f"{name} line {line_number} duplicates a manifest request")
            else:
                outcome.result, outcome.failure = _normalize(record)
    for outcome in outcomes:
        if not seen.get(outcome.request_id):
            diagnose("manifest request missing from provider result files")
    return outcomes, diagnostics
