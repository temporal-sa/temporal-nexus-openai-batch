"""Transactional admission, immutable manifests and exact-attempt delivery state."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from typing import AsyncIterator

import asyncpg

from .models import (
    Admission,
    BatchDelivery,
    BatchRef,
    BatchReport,
    CompletionTarget,
    Config,
    DeliverySummary,
    Enrollment,
    Entry,
    Failure,
    Manifest,
    Outcome,
    Request,
    RunInput,
    RunStatus,
    StoredDelivery,
    dumps,
    fingerprint,
    jsonl,
    jsonl_line,
    loads,
    validate_request,
)

LOCAL_DSN = "postgres://nexus_batch:nexus_batch@127.0.0.1:54329/nexus_batch_python?sslmode=disable"
DISPATCH_LEASE = 10
MAX_RESULT_BYTES = 4 * 1024


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _limit(value: int) -> int:
    return min(value, 1000) if value > 0 else 100


def _bytes(value: str) -> int:
    return len(value.encode("utf-8"))


def _truncate(value: str, limit: int = 512) -> str:
    return value.encode("utf-8")[:limit].decode("utf-8", errors="ignore")


def bounded_diagnostics(values: list[str]) -> list[str]:
    return [_truncate(value) for value in values[:100]]


def valid_target(target: CompletionTarget, namespace: str) -> bool:
    return (
        target.namespace == namespace
        and 0 < _bytes(target.activity_id) <= 1024
        and 0 < _bytes(target.activity_run_id) <= 1024
        and target.attempt > 0
        and 0 < len(target.task_token) <= 16 * 1024
    )


def _decode(cls, raw, description: str):
    try:
        return loads(cls, raw)
    except (ValueError, TypeError, KeyError):
        raise RuntimeError(f"stored {description} is invalid") from None


class Store:
    def __init__(self, pool: asyncpg.Pool, namespace: str):
        self.pool = pool
        self.namespace = namespace

    @classmethod
    async def connect(cls, dsn: str = LOCAL_DSN, namespace: str = "default") -> Store:
        if not dsn or not namespace or _bytes(namespace) > 255:
            raise ValueError("DATABASE_URL and Temporal namespace are required")
        pool = None
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=32, timeout=10)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Concurrent workers initialize one fresh schema atomically.
                    await conn.execute("SELECT pg_advisory_xact_lock(819482729640032)")
                    await conn.execute(files(__package__).joinpath("schema.sql").read_text())
        except Exception:
            if pool is not None:
                await pool.close()
            raise RuntimeError(
                "could not initialize PostgreSQL; check DATABASE_URL and database availability"
            ) from None
        return cls(pool, namespace)

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def _connection(
        self, operation: str, *, transaction: bool = False, readonly: bool = False
    ) -> AsyncIterator[asyncpg.Connection]:
        try:
            async with self.pool.acquire() as conn:
                if transaction:
                    async with conn.transaction(
                        isolation="repeatable_read" if readonly else "read_committed", readonly=readonly
                    ):
                        yield conn
                else:
                    yield conn
        except asyncpg.PostgresError as exc:
            raise RuntimeError(f"PostgreSQL {operation} failed (SQLSTATE {exc.sqlstate})") from None
        except (OSError, asyncpg.InterfaceError):
            raise RuntimeError(f"PostgreSQL {operation} failed") from None

    async def start_run(self, run: RunInput) -> None:
        try:
            run.config.validate()
        except (ValueError, Failure) as exc:
            raise Failure("INVALID_INPUT", str(exc)) from None
        validate_request(Request(run_id=run.run_id, request_id="run", input="run"))
        async with self._connection("create run") as conn:
            try:
                await conn.execute(
                    """INSERT INTO nexus_batch_runs(namespace,run_id,config,mode,endpoint)
                    VALUES($1,$2,$3,$4,$5)""",
                    self.namespace,
                    run.run_id,
                    dumps(run.config),
                    run.config.mode,
                    run.config.nexus_endpoint,
                )
            except asyncpg.UniqueViolationError:
                raise Failure("RUN_EXISTS", "run_id already exists; choose a new run ID") from None

    async def get_run(self, run_id: str) -> RunInput:
        async with self._connection("get run") as conn:
            raw = await conn.fetchval(
                "SELECT config FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2", self.namespace, run_id
            )
        if raw is None:
            raise Failure("RUN_NOT_STARTED", "batching run does not exist")
        return RunInput(run_id=run_id, config=_decode(Config, raw, "run configuration"))

    async def enroll(self, enrollment: Enrollment) -> Admission:
        e, target = enrollment, enrollment.completion_target
        validate_request(e.request)
        if e.fingerprint != fingerprint(e.request) or not valid_target(target, self.namespace):
            raise Failure("INVALID_INPUT", "enrollment fingerprint or completion target is invalid")
        async with self._connection("enroll", transaction=True) as conn:
            run = await conn.fetchrow(
                """SELECT config,closed,stopped,admitted,sequence,open_batch_id FROM nexus_batch_runs
                WHERE namespace=$1 AND run_id=$2 FOR UPDATE""",
                self.namespace,
                e.request.run_id,
            )
            if run is None:
                raise Failure("RUN_NOT_STARTED", "batching run does not exist")
            cfg = _decode(Config, run["config"], "run configuration")
            previous = await conn.fetchrow(
                """SELECT target,fingerprint,batch_id,delivery_status FROM nexus_batch_requests
                WHERE namespace=$1 AND run_id=$2 AND request_id=$3 FOR UPDATE""",
                self.namespace,
                e.request.run_id,
                e.request.request_id,
            )
            if previous is not None:
                old = _decode(CompletionTarget, previous["target"], "completion target")
                admission = Admission(batch_id=previous["batch_id"], fingerprint=e.fingerprint, config=cfg)
                if previous["fingerprint"] == e.fingerprint and old == target:
                    return admission
                if old.activity_id == target.activity_id and old.activity_run_id == target.activity_run_id:
                    if (
                        previous["fingerprint"] == e.fingerprint
                        and target.attempt > old.attempt
                        and previous["delivery_status"] != "delivered"
                        and not run["stopped"]
                    ):
                        # Membership, admitted count, deadline and outcome stay immutable.
                        await conn.execute(
                            """UPDATE nexus_batch_requests SET target=$4,delivery_status='pending',
                            delivery_diagnostic='',next_heartbeat_at=clock_timestamp()
                            WHERE namespace=$1 AND run_id=$2 AND request_id=$3""",
                            self.namespace,
                            e.request.run_id,
                            e.request.request_id,
                            dumps(target),
                        )
                        return admission
                    raise Failure(
                        "STALE_ACTIVITY_ATTEMPT", "request is already fenced to a different Activity attempt"
                    )
                raise Failure("DUPLICATE_REQUEST", "request_id is already owned by another operation")
            if run["closed"] or run["stopped"]:
                raise Failure("RUN_CLOSED", "batching run is closed to new requests")
            if cfg.max_admissions > 0 and run["admitted"] >= cfg.max_admissions:
                raise Failure("RUN_FULL", "batching run admission limit reached")
            line = jsonl_line(e.request, cfg)
            if len(line) > cfg.max_batch_bytes:
                raise Failure("INVALID_INPUT", "request exceeds configured batch byte limit")
            now = _now()
            deadline = e.deadline_at
            if not isinstance(deadline, datetime) or deadline.tzinfo is None:
                raise Failure("INVALID_INPUT", "request completion deadline must include a timezone")
            if now >= deadline:
                raise Failure("DEADLINE_EXCEEDED", "request completion deadline has passed")
            batch_id, sequence = run["open_batch_id"], run["sequence"]
            count = byte_count = 0
            if batch_id:
                batch = await conn.fetchrow(
                    """SELECT requests,bytes,seal_at FROM nexus_batch_batches
                    WHERE namespace=$1 AND batch_id=$2 AND state='open' FOR UPDATE""",
                    self.namespace,
                    batch_id,
                )
                if batch is None:
                    raise RuntimeError("stored open batch is invalid")
                count, byte_count = batch["requests"], batch["bytes"]
                if byte_count + len(line) > cfg.max_batch_bytes or now >= batch["seal_at"]:
                    await self._seal(conn, e.request.run_id, batch_id, now)
                    sequence += 1
                    batch_id = None
                    count = byte_count = 0
            if batch_id is None:
                batch_id = f"{e.request.run_id}-{sequence:08d}"
                await conn.execute(
                    """INSERT INTO nexus_batch_batches(namespace,batch_id,run_id,mode,endpoint,
                    first_admitted_at,seal_at,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
                    self.namespace,
                    batch_id,
                    e.request.run_id,
                    cfg.mode,
                    cfg.nexus_endpoint,
                    now,
                    now + timedelta(seconds=cfg.window),
                    deadline,
                )
                await conn.execute(
                    """UPDATE nexus_batch_runs SET open_batch_id=$3,sequence=$4
                    WHERE namespace=$1 AND run_id=$2""",
                    self.namespace,
                    e.request.run_id,
                    batch_id,
                    sequence,
                )
            try:
                await conn.execute(
                    """INSERT INTO nexus_batch_requests(namespace,run_id,request_id,batch_id,request,target,
                    activity_id,activity_run_id,fingerprint,admitted_at,position,deadline_at)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                    self.namespace,
                    e.request.run_id,
                    e.request.request_id,
                    batch_id,
                    dumps(e.request),
                    dumps(target),
                    target.activity_id,
                    target.activity_run_id,
                    e.fingerprint,
                    now,
                    count,
                    deadline,
                )
            except asyncpg.UniqueViolationError:
                raise Failure(
                    "DUPLICATE_REQUEST", "completion target is already owned by another request"
                ) from None
            count += 1
            byte_count += len(line)
            await conn.execute(
                """UPDATE nexus_batch_batches SET requests=$3,bytes=$4,deadline_at=LEAST(deadline_at,$5)
                WHERE namespace=$1 AND batch_id=$2""",
                self.namespace,
                batch_id,
                count,
                byte_count,
                deadline,
            )
            await conn.execute(
                "UPDATE nexus_batch_runs SET admitted=admitted+1 WHERE namespace=$1 AND run_id=$2",
                self.namespace,
                e.request.run_id,
            )
            if count >= cfg.max_batch_requests or byte_count >= cfg.max_batch_bytes:
                await self._seal(conn, e.request.run_id, batch_id, now)
            return Admission(batch_id=batch_id, fingerprint=e.fingerprint, config=cfg)

    async def _seal(self, conn, run_id: str, batch_id: str, now: datetime) -> None:
        # Callers acquire the run row before its batch row.
        await conn.execute(
            """UPDATE nexus_batch_batches SET state='sealed',sealed_at=$3
            WHERE namespace=$1 AND batch_id=$2 AND state='open' AND requests>0""",
            self.namespace,
            batch_id,
            now,
        )
        await conn.execute(
            """UPDATE nexus_batch_runs SET open_batch_id=NULL,sequence=sequence+1
            WHERE namespace=$1 AND run_id=$2 AND open_batch_id=$3""",
            self.namespace,
            run_id,
            batch_id,
        )

    async def close_run(self, run_id: str) -> None:
        await self._close_run(run_id, False)

    async def stop_run(self, run_id: str) -> None:
        await self._close_run(run_id, True)

    async def _close_run(self, run_id: str, stop: bool) -> None:
        async with self._connection("close run", transaction=True) as conn:
            run = await conn.fetchrow(
                "SELECT open_batch_id FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE",
                self.namespace,
                run_id,
            )
            if run is None:
                raise Failure("RUN_NOT_STARTED", "batching run does not exist")
            if run["open_batch_id"]:
                await self._seal(conn, run_id, run["open_batch_id"], _now())
            await conn.execute(
                "UPDATE nexus_batch_runs SET closed=true,stopped=stopped OR $3 WHERE namespace=$1 AND run_id=$2",
                self.namespace,
                run_id,
                stop,
            )
            if stop:
                await conn.execute(
                    """UPDATE nexus_batch_requests SET delivery_status='skipped',
                    delivery_diagnostic='run explicitly stopped'
                    WHERE namespace=$1 AND run_id=$2 AND delivery_status='pending'""",
                    self.namespace,
                    run_id,
                )

    async def run_resources(self, run_id: str) -> tuple[list[str], list[CompletionTarget]]:
        async with self._connection("run resources", transaction=True, readonly=True) as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2)",
                self.namespace,
                run_id,
            )
            if not exists:
                raise Failure("RUN_NOT_STARTED", "batching run does not exist")
            batches = await conn.fetch(
                "SELECT batch_id FROM nexus_batch_batches WHERE namespace=$1 AND run_id=$2 ORDER BY batch_id",
                self.namespace,
                run_id,
            )
            targets = await conn.fetch(
                "SELECT target FROM nexus_batch_requests WHERE namespace=$1 AND run_id=$2 ORDER BY request_id",
                self.namespace,
                run_id,
            )
            return (
                [row["batch_id"] for row in batches],
                [_decode(CompletionTarget, row["target"], "completion target") for row in targets],
            )

    @asynccontextmanager
    async def submission(self, batch_id: str) -> AsyncIterator[None]:
        """Fence creation against stop; do not call other store methods in the body."""
        async with asyncio.timeout(30):
            async with self._connection("submission", transaction=True) as conn:
                row = await conn.fetchrow(
                    """SELECT r.stopped,b.state,b.deadline_at FROM nexus_batch_runs r
                    JOIN nexus_batch_batches b ON b.namespace=r.namespace AND b.run_id=r.run_id
                    WHERE b.namespace=$1 AND b.batch_id=$2 FOR UPDATE OF r""",
                    self.namespace,
                    batch_id,
                )
                if row is None:
                    raise Failure("BATCH_NOT_FOUND", "batch does not exist")
                if row["stopped"]:
                    raise Failure("RUN_STOPPED", "run explicitly stopped before provider submission")
                if row["state"] == "open":
                    raise Failure("BATCH_NOT_SEALED", "batch manifest is not sealed")
                remaining = (row["deadline_at"] - _now()).total_seconds()
                if remaining <= 0:
                    raise Failure("DEADLINE_EXCEEDED", "request completion deadline has passed")
                async with asyncio.timeout(remaining):
                    yield

    async def seal_due(self, mode: str, endpoint: str, limit: int = 100) -> int:
        async with self._connection("seal due", transaction=True) as conn:
            due = await conn.fetch(
                """SELECT r.run_id,b.batch_id FROM nexus_batch_runs r JOIN nexus_batch_batches b
                ON b.namespace=r.namespace AND b.batch_id=r.open_batch_id
                WHERE r.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
                AND b.state='open' AND b.seal_at<=clock_timestamp()
                ORDER BY b.seal_at LIMIT $4 FOR UPDATE OF r SKIP LOCKED""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
            for row in due:
                await conn.fetchval(
                    """SELECT batch_id FROM nexus_batch_batches
                    WHERE namespace=$1 AND batch_id=$2 AND state='open' FOR UPDATE""",
                    self.namespace,
                    row["batch_id"],
                )
                await self._seal(conn, row["run_id"], row["batch_id"], _now())
            return len(due)

    async def ready_batches(self, mode: str, endpoint: str, limit: int = 100) -> list[BatchRef]:
        async with self._connection("claim dispatch", transaction=True) as conn:
            rows = await conn.fetch(
                """SELECT b.run_id,b.batch_id,r.config,b.requests,b.bytes,b.sealed_at,
                b.first_admitted_at,b.deadline_at FROM nexus_batch_batches b
                JOIN nexus_batch_runs r ON r.namespace=b.namespace AND r.run_id=b.run_id
                WHERE b.namespace=$1 AND b.mode=$2 AND b.endpoint=$3 AND b.state='sealed'
                AND NOT b.dispatched AND NOT r.stopped
                AND (b.dispatch_claim_until IS NULL OR b.dispatch_claim_until<clock_timestamp())
                ORDER BY b.sealed_at,b.batch_id LIMIT $4 FOR UPDATE OF b SKIP LOCKED""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
            refs = [
                BatchRef(**{**dict(row), "config": _decode(Config, row["config"], "run configuration")})
                for row in rows
            ]
            if refs:
                await conn.execute(
                    """UPDATE nexus_batch_batches SET dispatch_claim_until=$3
                    WHERE namespace=$1 AND batch_id=ANY($2::text[])""",
                    self.namespace,
                    [ref.batch_id for ref in refs],
                    _now() + timedelta(seconds=DISPATCH_LEASE),
                )
            return refs

    async def mark_dispatched(self, batch_id: str) -> None:
        async with self._connection("mark dispatch") as conn:
            tag = await conn.execute(
                """UPDATE nexus_batch_batches SET dispatched=true,dispatch_claim_until=NULL
                WHERE namespace=$1 AND batch_id=$2""",
                self.namespace,
                batch_id,
            )
            if tag != "UPDATE 1":
                raise Failure("BATCH_NOT_FOUND", "batch does not exist")

    async def load_manifest(self, batch_id: str) -> Manifest:
        async with self._connection("load manifest", transaction=True, readonly=True) as conn:
            row = await conn.fetchrow(
                """SELECT b.run_id,r.config,b.sealed_at,b.bytes,b.state FROM nexus_batch_batches b
                JOIN nexus_batch_runs r ON r.namespace=b.namespace AND r.run_id=b.run_id
                WHERE b.namespace=$1 AND b.batch_id=$2""",
                self.namespace,
                batch_id,
            )
            if row is None:
                raise Failure("BATCH_NOT_FOUND", "batch does not exist")
            if row["state"] == "open" or row["sealed_at"] is None:
                raise Failure("BATCH_NOT_SEALED", "batch manifest is not sealed")
            rows = await conn.fetch(
                """SELECT request,target,fingerprint,admitted_at FROM nexus_batch_requests
                WHERE namespace=$1 AND batch_id=$2 ORDER BY position""",
                self.namespace,
                batch_id,
            )
            entries = [
                Entry(
                    request=_decode(Request, entry["request"], "manifest entry"),
                    completion_target=_decode(CompletionTarget, entry["target"], "manifest entry"),
                    fingerprint=entry["fingerprint"],
                    admitted_at=entry["admitted_at"],
                )
                for entry in rows
            ]
            manifest = Manifest(
                run_id=row["run_id"],
                batch_id=batch_id,
                config=_decode(Config, row["config"], "run configuration"),
                entries=entries,
                sealed_at=row["sealed_at"],
                bytes=row["bytes"],
            )
            try:
                jsonl(manifest)
            except ValueError:
                raise Failure("INVALID_BATCH", "stored batch violates configured bounds") from None
            return manifest

    async def save_report(self, report: BatchReport) -> None:
        report = replace(report, diagnostics=bounded_diagnostics(report.diagnostics))
        async with self._connection("save report") as conn:
            tag = await conn.execute(
                """UPDATE nexus_batch_batches SET report=$3,
                state=CASE WHEN $4 IN ('completed','completed_with_errors') THEN 'completed' ELSE state END
                WHERE namespace=$1 AND batch_id=$2""",
                self.namespace,
                report.batch_id,
                dumps(report),
                report.status,
            )
            if tag != "UPDATE 1":
                raise Failure("BATCH_NOT_FOUND", "batch does not exist")

    async def status(self, run_id: str) -> RunStatus:
        async with self._connection("run status") as conn:
            run = await conn.fetchrow(
                "SELECT admitted,closed FROM nexus_batch_runs WHERE namespace=$1 AND run_id=$2",
                self.namespace,
                run_id,
            )
            if run is None:
                raise Failure("RUN_NOT_STARTED", "batching run does not exist")
            status = RunStatus(
                run_id=run_id,
                backing="nexus_standalone_activity",
                storage="postgres",
                completion_mode="external_async_completion",
                admitted=run["admitted"],
                closed=run["closed"],
                batches=[],
            )
            rows = await conn.fetch(
                """SELECT b.*,COALESCE(d.delivered,0) AS delivered,COALESCE(d.skipped,0) AS skipped
                FROM nexus_batch_batches b LEFT JOIN (
                    SELECT batch_id,COUNT(*) FILTER (WHERE delivery_status='delivered') AS delivered,
                    COUNT(*) FILTER (WHERE delivery_status='skipped') AS skipped
                    FROM nexus_batch_requests WHERE namespace=$1 AND run_id=$2 GROUP BY batch_id
                ) d ON d.batch_id=b.batch_id WHERE b.namespace=$1 AND b.run_id=$2 ORDER BY b.batch_id""",
                self.namespace,
                run_id,
            )
            for row in rows:
                if row["state"] == "open":
                    status.buffered += row["requests"]
                    continue
                if row["report"] is not None:
                    report = _decode(BatchReport, row["report"], "batch report")
                else:
                    report = BatchReport(
                        batch_id=row["batch_id"],
                        requests=row["requests"],
                        bytes=row["bytes"],
                        status="starting" if row["dispatched"] else "sealed",
                    )
                    if row["sealed_at"] is not None:
                        report.collection_wait = (row["sealed_at"] - row["first_admitted_at"]).total_seconds()
                # The keeper may checkpoint recipients after the final report.
                report.delivered, report.skipped = row["delivered"], row["skipped"]
                status.batches.append(report)
            return status

    async def save_outcomes(self, batch_id: str, outcomes: list[Outcome], diagnostics: list[str]) -> None:
        async with self._connection("save outcomes", transaction=True) as conn:
            batch = await conn.fetchrow(
                "SELECT outcomes_saved,state FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2 FOR UPDATE",
                self.namespace,
                batch_id,
            )
            if batch is None:
                raise Failure("BATCH_NOT_FOUND", "batch does not exist")
            if batch["state"] == "open":
                raise Failure("BATCH_NOT_SEALED", "batch manifest is not sealed")
            if batch["outcomes_saved"]:
                return
            rows = await conn.fetch(
                """SELECT request_id,fingerprint FROM nexus_batch_requests
                WHERE namespace=$1 AND batch_id=$2 ORDER BY position""",
                self.namespace,
                batch_id,
            )
            identity = {row["request_id"]: row["fingerprint"] for row in rows}
            if len(identity) != len(outcomes):
                raise Failure(
                    "INVALID_RESULT", "reconciliation must resolve every manifest request exactly once"
                )
            seen = set()
            for outcome in outcomes:
                if (
                    outcome.request_id not in identity
                    or outcome.request_id in seen
                    or outcome.batch_id != batch_id
                    or outcome.fingerprint != identity[outcome.request_id]
                    or (outcome.result is None) == (outcome.failure is None)
                ):
                    raise Failure("INVALID_RESULT", "outcome identity or terminal result is invalid")
                seen.add(outcome.request_id)
                if outcome.result is not None:
                    if (
                        outcome.result.request_id != outcome.request_id
                        or _bytes(dumps(outcome.result)) > MAX_RESULT_BYTES
                    ):
                        raise Failure("INVALID_RESULT", "normalized result exceeds its request or size bound")
                if outcome.failure is not None:
                    failure = outcome.failure
                    if (
                        not failure.code
                        or _bytes(failure.code) > 96
                        or _bytes(failure.message) > 1024
                        or _bytes(failure.provider_code) > 64
                    ):
                        raise Failure("INVALID_RESULT", "failure exceeds its size bound")
                await conn.execute(
                    "UPDATE nexus_batch_requests SET outcome=$4 WHERE namespace=$1 AND batch_id=$2 AND request_id=$3",
                    self.namespace,
                    batch_id,
                    outcome.request_id,
                    dumps(outcome),
                )
            await conn.execute(
                """UPDATE nexus_batch_batches SET outcomes_saved=true,state='delivering',diagnostics=$3,dispatched=true
                WHERE namespace=$1 AND batch_id=$2""",
                self.namespace,
                batch_id,
                json.dumps(bounded_diagnostics(diagnostics)),
            )

    async def fail_batch(self, batch_id: str, code: str, message: str) -> None:
        manifest = await self.load_manifest(batch_id)
        outcomes = [
            Outcome(
                request_id=e.request.request_id,
                fingerprint=e.fingerprint,
                batch_id=batch_id,
                failure=Failure(code, message),
            )
            for e in manifest.entries
        ]
        await self.save_outcomes(batch_id, outcomes, [code])

    async def pending_deliveries(self, batch_id: str, limit: int = 100) -> list[StoredDelivery]:
        async with self._connection("pending deliveries") as conn:
            rows = await conn.fetch(
                """SELECT request_id,target,outcome FROM nexus_batch_requests WHERE namespace=$1 AND batch_id=$2
                AND delivery_status='pending' AND outcome IS NOT NULL ORDER BY position LIMIT $3""",
                self.namespace,
                batch_id,
                _limit(limit),
            )
            return [
                StoredDelivery(
                    request_id=row["request_id"],
                    completion_target=_decode(CompletionTarget, row["target"], "delivery"),
                    outcome=_decode(Outcome, row["outcome"], "delivery"),
                )
                for row in rows
            ]

    async def mark_delivered(
        self,
        batch_id: str,
        request_id: str,
        expected_target: CompletionTarget,
        delivered: bool,
        diagnostic: str,
    ) -> None:
        async with self._connection("mark delivery") as conn:
            tag = await conn.execute(
                """UPDATE nexus_batch_requests SET
                delivery_status=CASE WHEN delivery_status='pending' THEN $4 ELSE delivery_status END,
                delivery_diagnostic=CASE WHEN delivery_status='pending' THEN $5 ELSE delivery_diagnostic END
                WHERE namespace=$1 AND batch_id=$2 AND request_id=$3 AND outcome IS NOT NULL AND target=$6::jsonb""",
                self.namespace,
                batch_id,
                request_id,
                "delivered" if delivered else "skipped",
                _truncate(diagnostic),
                dumps(expected_target),
            )
            if tag != "UPDATE 1":
                exists = await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM nexus_batch_requests
                    WHERE namespace=$1 AND batch_id=$2 AND request_id=$3 AND outcome IS NOT NULL)""",
                    self.namespace,
                    batch_id,
                    request_id,
                )
                if not exists:
                    raise Failure("DELIVERY_NOT_FOUND", "stored outcome does not exist")
                # A newer attempt changed its token; never advance its checkpoint.

    async def delivery_summary(self, batch_id: str) -> DeliverySummary:
        async with self._connection("delivery summary") as conn:
            raw = await conn.fetchval(
                "SELECT diagnostics FROM nexus_batch_batches WHERE namespace=$1 AND batch_id=$2",
                self.namespace,
                batch_id,
            )
            if raw is None:
                raise Failure("BATCH_NOT_FOUND", "batch does not exist")
            try:
                diagnostics = json.loads(raw) if isinstance(raw, str) else raw
            except ValueError:
                raise RuntimeError("stored diagnostics are invalid") from None
            summary = DeliverySummary(failure_counts={}, diagnostics=diagnostics)
            rows = await conn.fetch(
                """SELECT outcome,delivery_status,delivery_diagnostic FROM nexus_batch_requests
                WHERE namespace=$1 AND batch_id=$2 ORDER BY position""",
                self.namespace,
                batch_id,
            )
            for row in rows:
                if row["outcome"] is not None:
                    outcome = _decode(Outcome, row["outcome"], "outcome")
                    if outcome.failure is not None:
                        summary.failed += 1
                        code = outcome.failure.code
                        summary.failure_counts[code] = summary.failure_counts.get(code, 0) + 1
                    elif outcome.result is not None:
                        summary.succeeded += 1
                if row["delivery_status"] == "delivered":
                    summary.delivered += 1
                elif row["delivery_status"] == "skipped":
                    summary.skipped += 1
                diagnostic = row["delivery_diagnostic"]
                if diagnostic and diagnostic not in summary.diagnostics and len(summary.diagnostics) < 100:
                    summary.diagnostics.append(diagnostic)
            return summary

    async def pending_heartbeats(self, mode: str, endpoint: str, limit: int = 100) -> list[CompletionTarget]:
        async with self._connection("claim heartbeat targets") as conn:
            rows = await conn.fetch(
                """WITH due AS (
                    SELECT q.namespace,q.run_id,q.request_id FROM nexus_batch_requests q
                    JOIN nexus_batch_runs r ON r.namespace=q.namespace AND r.run_id=q.run_id
                    WHERE q.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
                    AND q.delivery_status='pending' AND q.deadline_at>clock_timestamp()
                    AND q.next_heartbeat_at<=clock_timestamp()
                    ORDER BY q.next_heartbeat_at,q.run_id,q.request_id LIMIT $4 FOR UPDATE OF q SKIP LOCKED
                ) UPDATE nexus_batch_requests q SET next_heartbeat_at=clock_timestamp()+interval '10 seconds'
                FROM due WHERE q.namespace=due.namespace AND q.run_id=due.run_id AND q.request_id=due.request_id
                RETURNING q.target""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
            return [_decode(CompletionTarget, row["target"], "completion target") for row in rows]

    async def pending_completions(self, mode: str, endpoint: str, limit: int = 100) -> list[BatchDelivery]:
        async with self._connection("pending completions") as conn:
            rows = await conn.fetch(
                """SELECT q.batch_id,q.request_id,q.target,q.outcome FROM nexus_batch_requests q
                JOIN nexus_batch_runs r ON r.namespace=q.namespace AND r.run_id=q.run_id
                JOIN nexus_batch_batches b ON b.namespace=q.namespace AND b.batch_id=q.batch_id
                WHERE q.namespace=$1 AND r.mode=$2 AND r.endpoint=$3 AND NOT r.stopped
                AND b.state='completed' AND q.delivery_status='pending' AND q.outcome IS NOT NULL
                ORDER BY q.admitted_at,q.batch_id,q.position LIMIT $4""",
                self.namespace,
                mode,
                endpoint,
                _limit(limit),
            )
            return [
                BatchDelivery(
                    batch_id=row["batch_id"],
                    request_id=row["request_id"],
                    completion_target=_decode(CompletionTarget, row["target"], "delivery"),
                    outcome=_decode(Outcome, row["outcome"], "delivery"),
                )
                for row in rows
            ]
