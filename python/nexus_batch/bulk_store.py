"""Immutable bulk artifacts and one-recipient admission/completion recovery.

Only activity-side code uses this repository. Workflows carry three-field input
references or bounded result references; documents, schemas and outcome material
remain in namespace-scoped PostgreSQL rows.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterable, Iterable
from datetime import datetime
from typing import Any
from uuid import uuid4

import asyncpg

from .bulk_models import (
    MAX_BULK_PAGE_BYTES,
    BatchCounts,
    BatchInputRef,
    BatchResultRef,
    BulkDelivery,
    BulkItem,
    BulkRequest,
    BulkSubmission,
    canonical_json,
    prepare_body,
    provider_id,
    validate_bulk_request,
    validate_input_ref,
    validate_page,
    validate_schema,
    validate_submission,
)
from .models import CompletionTarget, Config, Failure, Outcome, Request, dumps, fingerprint, jsonl_line
from .store import Store, _decode, _limit, _now, _truncate, valid_target

MAX_MANIFEST_ITEMS = 100_000
MAX_MANIFEST_BYTES = 256 * 1024 * 1024


def _input_bytes(item: BulkRequest) -> bytes:
    return canonical_json({"custom_id": item.custom_id, "body": item.body}) + b"\n"


def _counts(items: Iterable[BulkItem[Any]]) -> BatchCounts:
    result = BatchCounts()
    for item in items:
        result.total += 1
        if item.parse_error:
            result.parse_failed += 1
        elif item.status in {"succeeded", "failed", "expired", "cancelled"}:
            setattr(result, item.status, getattr(result, item.status) + 1)
    return result


class BulkStore:
    def __init__(self, store: Store):
        self.store = store
        self.namespace = store.namespace

    async def write_input(
        self,
        requests: Iterable[BulkRequest] | AsyncIterable[BulkRequest],
        schema: dict[str, Any] | None = None,
    ) -> BatchInputRef[Any]:
        schema = validate_schema(schema)
        manifest_id = uuid4().hex
        digest = hashlib.sha256(canonical_json(schema) + b"\n")
        count = byte_count = 0
        async with self.store._connection("write bulk input", transaction=True) as conn:
            await conn.execute(
                """INSERT INTO nexus_batch_bulk_inputs(namespace,manifest_id,output_schema)
                VALUES($1,$2,$3)""",
                self.namespace,
                manifest_id,
                dumps(schema) if schema is not None else None,
            )

            async def append(item: BulkRequest):
                nonlocal count, byte_count
                validate_bulk_request(item)
                encoded = _input_bytes(item)
                if count >= MAX_MANIFEST_ITEMS or byte_count + len(encoded) > MAX_MANIFEST_BYTES:
                    raise Failure("INVALID_INPUT", "bulk manifest exceeds 100,000 items or 256 MiB")
                try:
                    await conn.execute(
                        """INSERT INTO nexus_batch_bulk_input_items
                        (namespace,manifest_id,position,custom_id,body,bytes,content_digest) VALUES($1,$2,$3,$4,$5,$6,$7)""",
                        self.namespace,
                        manifest_id,
                        count,
                        item.custom_id,
                        dumps(item.body),
                        len(encoded),
                        hashlib.sha256(encoded).hexdigest(),
                    )
                except asyncpg.UniqueViolationError:
                    raise Failure(
                        "DUPLICATE_REQUEST", "custom IDs must be unique within an input manifest"
                    ) from None
                digest.update(encoded)
                count += 1
                byte_count += len(encoded)

            if isinstance(requests, AsyncIterable):
                async for item in requests:
                    await append(item)
            else:
                for item in requests:
                    await append(item)
            if not count:
                raise Failure("INVALID_INPUT", "bulk input cannot be empty")
            value = digest.hexdigest()
            await conn.execute(
                """UPDATE nexus_batch_bulk_inputs SET sealed=true,digest=$3,item_count=$4,bytes=$5
                WHERE namespace=$1 AND manifest_id=$2""",
                self.namespace,
                manifest_id,
                value,
                count,
                byte_count,
            )
        return BatchInputRef(manifest_id, value, count)

    async def input_schema(self, manifest_id: str) -> dict[str, Any] | None:
        """Read the portable schema when selecting requests for another stage."""
        async with self.store._connection("read bulk schema") as conn:
            row = await conn.fetchrow(
                """SELECT output_schema FROM nexus_batch_bulk_inputs
                WHERE namespace=$1 AND manifest_id=$2 AND sealed""",
                self.namespace,
                manifest_id,
            )
            if row is None:
                raise Failure("INVALID_INPUT", "sealed input manifest does not exist")
            return json.loads(row["output_schema"]) if row["output_schema"] is not None else None

    async def _manifest(self, conn, ref: BatchInputRef[Any]):
        validate_input_ref(ref)
        row = await conn.fetchrow(
            "SELECT * FROM nexus_batch_bulk_inputs WHERE namespace=$1 AND manifest_id=$2",
            self.namespace,
            ref.manifest_id,
        )
        if row is None or not row["sealed"] or row["digest"] != ref.digest or row["item_count"] != ref.count:
            raise Failure(
                "INVALID_INPUT", "input manifest is missing, unsealed or differs from its reference"
            )
        return row

    async def _input_page(self, conn, ref: BatchInputRef[Any], offset: int, limit: int):
        rows = await conn.fetch(
            """WITH page AS (
                SELECT position,custom_id,body,content_digest,SUM(bytes) OVER (ORDER BY position) AS page_bytes
                FROM (SELECT position,custom_id,body,bytes,content_digest FROM nexus_batch_bulk_input_items
                    WHERE namespace=$1 AND manifest_id=$2 AND position >= $3 ORDER BY position LIMIT $4) p
            ) SELECT position,custom_id,body,content_digest FROM page WHERE page_bytes <= $5 ORDER BY position""",
            self.namespace,
            ref.manifest_id,
            offset,
            limit,
            MAX_BULK_PAGE_BYTES,
        )
        result = []
        for row in rows:
            item = BulkRequest(row["custom_id"], json.loads(row["body"]))
            if hashlib.sha256(_input_bytes(item)).hexdigest() != row["content_digest"]:
                raise Failure("INVALID_INPUT", "stored input item content digest did not match")
            result.append(item)
        return result

    async def read_input(
        self,
        ref: BatchInputRef[Any],
        offset: int = 0,
        limit: int = 100,
    ) -> list[BulkRequest]:
        validate_page(offset, limit)
        async with self.store._connection("read bulk input", transaction=True, readonly=True) as conn:
            await self._manifest(conn, ref)
            return await self._input_page(conn, ref, offset, limit)

    async def _validate_manifest(self, submission: BulkSubmission, config: Config) -> None:
        """Check every item and the digest before any member can reach dispatch."""
        ref = submission.requests
        async with self.store._connection("validate bulk input", transaction=True, readonly=True) as conn:
            manifest = await self._manifest(conn, ref)
            schema = validate_schema(
                json.loads(manifest["output_schema"]) if manifest["output_schema"] else None
            )
            digest = hashlib.sha256(canonical_json(schema) + b"\n")
            position = byte_count = 0
            while position < ref.count:
                page = await self._input_page(conn, ref, position, 100)
                if not page:
                    raise Failure("INVALID_INPUT", "input manifest is incomplete")
                for item in page:
                    encoded = _input_bytes(item)
                    digest.update(encoded)
                    byte_count += len(encoded)
                    body = prepare_body(item.body, config.model, schema)
                    request = Request(
                        submission.run_id,
                        provider_id(submission.submission_id, position),
                        "",
                        body=body,
                        bulk_id=submission.submission_id,
                    )
                    if len(jsonl_line(request, config)) > config.max_batch_bytes:
                        raise Failure(
                            "INVALID_INPUT", "bulk request cannot fit the configured provider batch"
                        )
                    position += 1
            if digest.hexdigest() != ref.digest or byte_count != manifest["bytes"]:
                raise Failure("INVALID_INPUT", "input manifest content digest did not match")

    async def attach(
        self,
        submission: BulkSubmission,
        target: CompletionTarget,
        deadline: datetime,
    ) -> None:
        validate_submission(submission)
        if (
            not valid_target(target, self.namespace)
            or not isinstance(deadline, datetime)
            or deadline.tzinfo is None
        ):
            raise Failure("INVALID_INPUT", "bulk completion target and absolute deadline are required")
        run = await self.store.get_run(submission.run_id)
        await self._validate_manifest(submission, run.config)
        async with self.store._connection("attach bulk recipient", transaction=True) as conn:
            locked_run = await conn.fetchrow(
                "SELECT * FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE",
                self.namespace,
                submission.run_id,
            )
            previous = await conn.fetchrow(
                "SELECT * FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2 FOR UPDATE",
                self.namespace,
                submission.submission_id,
            )
            if previous:
                if previous["admission_aborted"]:
                    raise Failure("ADMISSION_ABORTED", "bulk admission was already stopped")
                old = _decode(CompletionTarget, previous["target"], "bulk completion target")
                if (
                    previous["run_id"] != submission.run_id
                    or _decode(BatchInputRef, previous["input_ref"], "bulk input reference")
                    != submission.requests
                ):
                    raise Failure(
                        "DUPLICATE_REQUEST", "submission identity is already attached to different input"
                    )
                if old == target:
                    return
                if (
                    old.activity_id == target.activity_id
                    and old.activity_run_id == target.activity_run_id
                    and target.attempt > old.attempt
                    and previous["delivery_status"] != "delivered"
                    and not locked_run["stopped"]
                ):
                    await conn.execute(
                        """UPDATE nexus_batch_bulk_submissions SET target=$3,delivery_status='pending',
                        delivery_diagnostic='',next_heartbeat_at=clock_timestamp()
                        WHERE namespace=$1 AND submission_id=$2""",
                        self.namespace,
                        submission.submission_id,
                        dumps(target),
                    )
                    return
                code = (
                    "STALE_ACTIVITY_ATTEMPT" if old.activity_id == target.activity_id else "DUPLICATE_REQUEST"
                )
                raise Failure(code, "bulk submission belongs to a different operation or newer attempt")
            if locked_run["closed"] or locked_run["stopped"]:
                raise Failure("RUN_CLOSED", "batching run is closed to new submissions")
            if deadline <= _now():
                raise Failure("DEADLINE_EXCEEDED", "bulk recipient deadline has passed")
            config = _decode(Config, locked_run["config"], "run configuration")
            if (
                config.max_admissions > 0
                and locked_run["admitted"] + locked_run["reserved"] + submission.requests.count
                > config.max_admissions
            ):
                raise Failure("RUN_FULL", "bulk submission would exceed the run admission limit")
            try:
                await conn.execute(
                    """INSERT INTO nexus_batch_bulk_submissions(namespace,submission_id,run_id,input_ref,
                    target,activity_id,activity_run_id,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
                    self.namespace,
                    submission.submission_id,
                    submission.run_id,
                    dumps(submission.requests),
                    dumps(target),
                    target.activity_id,
                    target.activity_run_id,
                    deadline,
                )
            except asyncpg.UniqueViolationError:
                raise Failure(
                    "DUPLICATE_REQUEST", "Activity target already owns another bulk submission"
                ) from None
            await conn.execute(
                "UPDATE nexus_batch_runs SET reserved=reserved+$3 WHERE namespace=$1 AND run_id=$2",
                self.namespace,
                submission.run_id,
                submission.requests.count,
            )

    async def admit_page(self, submission_id: str, limit: int = 100) -> bool:
        validate_page(0, limit)
        async with self.store._connection("admit bulk page", transaction=True) as conn:
            run_id = await conn.fetchval(
                "SELECT run_id FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2",
                self.namespace,
                submission_id,
            )
            if run_id is None:
                raise Failure("INVALID_INPUT", "bulk submission is not attached")
            run = await conn.fetchrow(
                "SELECT * FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE",
                self.namespace,
                run_id,
            )
            row = await conn.fetchrow(
                "SELECT * FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2 FOR UPDATE",
                self.namespace,
                submission_id,
            )
            if row["admission_aborted"]:
                raise Failure("ADMISSION_ABORTED", "bulk admission was already stopped")
            if row["admission_complete"]:
                return True
            if run["stopped"]:
                raise Failure("RUN_STOPPED", "bulk admission stopped with its run")
            ref = _decode(BatchInputRef, row["input_ref"], "bulk input reference")
            manifest = await self._manifest(conn, ref)
            schema = json.loads(manifest["output_schema"]) if manifest["output_schema"] else None
            config = _decode(Config, run["config"], "run configuration")
            offset = row["admitted"]
            page = await self._input_page(conn, ref, offset, limit)
            if not page:
                raise Failure("INVALID_INPUT", "bulk manifest lost an admitted page")
            for position, item in enumerate(page, offset):
                request = Request(
                    run_id,
                    provider_id(submission_id, position),
                    "",
                    body=prepare_body(item.body, config.model, schema),
                    bulk_id=submission_id,
                )
                # _admit changes open_batch_id/counts. Refresh under our existing
                # lock so the next member observes those transitions exactly.
                run = await conn.fetchrow(
                    "SELECT * FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE",
                    self.namespace,
                    run_id,
                )
                await self.store._admit(
                    conn,
                    run,
                    request,
                    None,
                    row["deadline_at"],
                    fingerprint(request),
                    bulk_id=submission_id,
                    bulk_position=position,
                )
            admitted = offset + len(page)
            complete = admitted == ref.count
            await conn.execute(
                """UPDATE nexus_batch_bulk_submissions SET admitted=$3,admission_complete=$4
                WHERE namespace=$1 AND submission_id=$2""",
                self.namespace,
                submission_id,
                admitted,
                complete,
            )
            return complete

    async def abort_admission(
        self,
        submission_id: str,
        target: CompletionTarget,
        diagnostic: str,
    ) -> None:
        """Release unconsumed admission capacity once, fencing stale attempts.

        Already admitted provider work remains intact for partial inspection and
        for other recipients. The original recipient is closed by the caller's
        Activity failure/cancellation; this method records the database handoff.
        """
        async with self.store._connection("abort bulk admission", transaction=True) as conn:
            run_id = await conn.fetchval(
                "SELECT run_id FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2",
                self.namespace,
                submission_id,
            )
            if run_id is None:
                return
            await conn.fetchval(
                "SELECT run_id FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE",
                self.namespace,
                run_id,
            )
            row = await conn.fetchrow(
                "SELECT * FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2 FOR UPDATE",
                self.namespace,
                submission_id,
            )
            if row["admission_aborted"] or _decode(CompletionTarget, row["target"], "bulk target") != target:
                return
            ref = _decode(BatchInputRef, row["input_ref"], "bulk input reference")
            await conn.execute(
                "UPDATE nexus_batch_runs SET reserved=reserved-$3 WHERE namespace=$1 AND run_id=$2",
                self.namespace,
                run_id,
                ref.count - row["admitted"],
            )
            await conn.execute(
                """UPDATE nexus_batch_bulk_submissions SET admission_aborted=true,
                delivery_status=CASE WHEN delivery_status='delivered' THEN delivery_status ELSE 'skipped' END,
                delivery_diagnostic=$3 WHERE namespace=$1 AND submission_id=$2""",
                self.namespace,
                submission_id,
                _truncate(diagnostic),
            )

    async def _expire_admissions(self, mode: str, endpoint: str, limit: int) -> None:
        async with self.store._connection("find expired bulk admissions") as conn:
            rows = await conn.fetch(
                """SELECT s.submission_id,s.target FROM nexus_batch_bulk_submissions s
                JOIN nexus_batch_runs r ON r.namespace=s.namespace AND r.run_id=s.run_id
                WHERE s.namespace=$1 AND r.mode=$2 AND r.endpoint=$3
                AND NOT s.admission_complete AND NOT s.admission_aborted
                AND s.deadline_at<=clock_timestamp() LIMIT $4""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
        for row in rows:
            await self.abort_admission(
                row["submission_id"],
                _decode(CompletionTarget, row["target"], "bulk target"),
                "bulk admission deadline expired",
            )

    async def pending_heartbeats(self, mode: str, endpoint: str, limit: int = 100) -> list[CompletionTarget]:
        # The admission Activity owns heartbeats while it is still writing pages.
        # Taking over earlier would mask its crash and prevent Temporal retry
        # from ever resuming the remaining admission work.
        await self._expire_admissions(mode, endpoint, limit)
        async with self.store._connection("claim bulk heartbeats") as conn:
            rows = await conn.fetch(
                """WITH due AS (
                    SELECT s.namespace,s.submission_id FROM nexus_batch_bulk_submissions s
                    JOIN nexus_batch_runs r ON r.namespace=s.namespace AND r.run_id=s.run_id
                    WHERE s.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
                    AND s.delivery_status='pending' AND s.admission_complete AND s.deadline_at>clock_timestamp()
                    AND s.next_heartbeat_at<=clock_timestamp() ORDER BY s.next_heartbeat_at
                    LIMIT $4 FOR UPDATE OF s SKIP LOCKED
                ) UPDATE nexus_batch_bulk_submissions s SET next_heartbeat_at=clock_timestamp()+interval '10 seconds'
                FROM due WHERE s.namespace=due.namespace AND s.submission_id=due.submission_id RETURNING s.target""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
            return [_decode(CompletionTarget, row["target"], "bulk target") for row in rows]

    async def pending_completions(self, mode: str, endpoint: str, limit: int = 100) -> list[BulkDelivery]:
        async with self.store._connection("ready bulk completions") as conn:
            rows = await conn.fetch(
                """SELECT s.submission_id,s.target FROM nexus_batch_bulk_submissions s
                JOIN nexus_batch_runs r ON r.namespace=s.namespace AND r.run_id=s.run_id
                WHERE s.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
                AND s.delivery_status='pending' AND s.admission_complete
                AND s.admitted=(SELECT COUNT(*) FROM nexus_batch_requests q
                    WHERE q.namespace=s.namespace AND q.bulk_id=s.submission_id AND q.outcome IS NOT NULL)
                ORDER BY s.submission_id LIMIT $4""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
        return [
            BulkDelivery(
                row["submission_id"],
                _decode(CompletionTarget, row["target"], "bulk target"),
                await self.get_result(row["submission_id"]),
            )
            for row in rows
        ]

    async def mark_delivered(
        self,
        submission_id: str,
        target: CompletionTarget,
        delivered: bool,
        diagnostic: str,
    ) -> None:
        async with self.store._connection("checkpoint bulk completion") as conn:
            await conn.execute(
                """UPDATE nexus_batch_bulk_submissions SET
                delivery_status=CASE WHEN delivery_status='pending' THEN $4 ELSE delivery_status END,
                delivery_diagnostic=CASE WHEN delivery_status='pending' THEN $5 ELSE delivery_diagnostic END
                WHERE namespace=$1 AND submission_id=$2 AND target=$3::jsonb""",
                self.namespace,
                submission_id,
                dumps(target),
                "delivered" if delivered else "skipped",
                _truncate(diagnostic),
            )

    async def _result_page(
        self,
        conn,
        submission_id: str,
        manifest_id: str,
        snapshot_digest: str | None,
        offset: int,
        limit: int,
    ):
        rows = await conn.fetch(
            """WITH candidates AS (
                SELECT i.position,i.custom_id,i.body,i.content_digest,
                    CASE WHEN $4::text IS NULL OR EXISTS(
                        SELECT 1 FROM nexus_batch_bulk_snapshot_items si WHERE si.namespace=i.namespace
                        AND si.submission_id=$2 AND si.digest=$4 AND si.position=i.position)
                    THEN q.outcome ELSE NULL END AS outcome
                FROM nexus_batch_bulk_input_items i LEFT JOIN nexus_batch_requests q
                    ON q.namespace=i.namespace AND q.bulk_id=$2 AND q.bulk_position=i.position
                WHERE i.namespace=$1 AND i.manifest_id=$3 AND i.position>=$5
                ORDER BY i.position LIMIT $6
            ), page AS (
                SELECT *,SUM(octet_length(body::text)+COALESCE(octet_length(outcome::text),0)+1024)
                    OVER (ORDER BY position) AS page_bytes FROM candidates
            ) SELECT position,custom_id,body,content_digest,outcome FROM page WHERE page_bytes<=$7 OR position=$5 ORDER BY position""",
            self.namespace,
            submission_id,
            manifest_id,
            snapshot_digest,
            offset,
            limit,
            MAX_BULK_PAGE_BYTES,
        )
        items = []
        page_bytes = 0
        for row in rows:
            request = json.loads(row["body"])
            original = BulkRequest(row["custom_id"], request)
            if hashlib.sha256(_input_bytes(original)).hexdigest() != row["content_digest"]:
                raise Failure("INVALID_INPUT", "stored input item content digest did not match")
            if row["outcome"] is None:
                item = BulkItem(row["custom_id"], request, "pending")
            else:
                outcome = _decode(Outcome, row["outcome"], "bulk outcome")
                bulk = outcome.bulk or {}
                if outcome.failure:
                    status = {"BATCH_EXPIRED": "expired", "BATCH_CANCELLED": "cancelled"}.get(
                        outcome.failure.code, "failed"
                    )
                else:
                    status = "succeeded"
                item = BulkItem(
                    row["custom_id"],
                    request,
                    bulk.get("status", status),
                    response=bulk.get("response"),
                    output=bulk.get("output"),
                    error=outcome.failure,
                    parse_error=bulk.get("parse_error", ""),
                    provider_error=bulk.get("provider_error"),
                )
            item_bytes = len(canonical_json(json.loads(dumps(item))))
            if item_bytes > MAX_BULK_PAGE_BYTES:
                raise Failure("INVALID_RESULT", "one externally stored item exceeds the result page bound")
            if page_bytes + item_bytes > MAX_BULK_PAGE_BYTES:
                break
            page_bytes += item_bytes
            items.append(item)
        return items

    async def get_result(self, submission_id: str, partial: bool = False) -> BatchResultRef[Any]:
        async with self.store._connection("snapshot bulk results", transaction=True, consistent=True) as conn:
            # Serialize final reference creation with recipient reattachment. We
            # do not need run locks here: no batch membership changes occur.
            row = await conn.fetchrow(
                "SELECT * FROM nexus_batch_bulk_submissions WHERE namespace=$1 AND submission_id=$2 FOR UPDATE",
                self.namespace,
                submission_id,
            )
            if row is None:
                raise Failure("INVALID_INPUT", "bulk submission does not exist")
            if row["result_ref"]:
                return _decode(BatchResultRef, row["result_ref"], "bulk result reference")
            input_ref = _decode(BatchInputRef, row["input_ref"], "bulk input reference")
            resolved = await conn.fetchval(
                "SELECT COUNT(*) FROM nexus_batch_requests WHERE namespace=$1 AND bulk_id=$2 AND outcome IS NOT NULL",
                self.namespace,
                submission_id,
            )
            complete = row["admission_complete"] and resolved == input_ref.count
            if not complete and not partial:
                raise Failure("RESULTS_PENDING", "bulk admission or outcomes are not complete")
            as_of = await conn.fetchval("SELECT clock_timestamp()")
            digest = hashlib.sha256()
            counts = BatchCounts()
            offset = 0
            while offset < input_ref.count:
                page = await self._result_page(conn, submission_id, input_ref.manifest_id, None, offset, 100)
                if not page:
                    raise Failure("INVALID_RESULT", "bulk result page is missing or exceeds its byte bound")
                for item in page:
                    digest.update(canonical_json(json.loads(dumps(item))) + b"\n")
                current = _counts(page)
                for key in ("total", "succeeded", "failed", "expired", "cancelled", "parse_failed"):
                    setattr(counts, key, getattr(counts, key) + getattr(current, key))
                offset += len(page)
            ref = BatchResultRef(
                submission_id, input_ref.manifest_id, digest.hexdigest(), input_ref.count, counts
            )
            await conn.execute(
                """INSERT INTO nexus_batch_bulk_snapshots(namespace,submission_id,digest,as_of,reference)
                VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""",
                self.namespace,
                submission_id,
                ref.digest,
                as_of,
                dumps(ref),
            )
            await conn.execute(
                """INSERT INTO nexus_batch_bulk_snapshot_items(namespace,submission_id,digest,position)
                SELECT namespace,bulk_id,$3,bulk_position FROM nexus_batch_requests
                WHERE namespace=$1 AND bulk_id=$2 AND outcome IS NOT NULL ON CONFLICT DO NOTHING""",
                self.namespace,
                submission_id,
                ref.digest,
            )
            if complete:
                await conn.execute(
                    "UPDATE nexus_batch_bulk_submissions SET result_ref=$3 WHERE namespace=$1 AND submission_id=$2",
                    self.namespace,
                    submission_id,
                    dumps(ref),
                )
            return ref

    async def read_results(
        self,
        ref: BatchResultRef[Any],
        offset: int = 0,
        limit: int = 100,
    ) -> list[BulkItem[Any]]:
        validate_page(offset, limit)
        async with self.store._connection("read bulk results", transaction=True, readonly=True) as conn:
            snapshot = await conn.fetchrow(
                """SELECT as_of,reference FROM nexus_batch_bulk_snapshots
                WHERE namespace=$1 AND submission_id=$2 AND digest=$3""",
                self.namespace,
                ref.submission_id,
                ref.digest,
            )
            if snapshot is None or _decode(BatchResultRef, snapshot["reference"], "result snapshot") != ref:
                raise Failure("INVALID_INPUT", "result reference is missing or does not match its snapshot")
            return await self._result_page(
                conn, ref.submission_id, ref.manifest_id, ref.digest, offset, limit
            )
