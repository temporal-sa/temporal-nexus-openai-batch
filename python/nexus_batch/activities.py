"""Network and database operations. Bulk data stays inside these Activities."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from temporalio import activity
from temporalio.client import ActivityExecutionStatus, Client
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from .models import (
    BatchRef,
    BatchReport,
    CompletionTarget,
    DeliverySummary,
    Enrollment,
    Failure,
    Outcome,
    Result,
    SAAInput,
    dumps,
    fingerprint,
    jsonl,
    loads,
    validate_request,
)
from .provider import Job, Provider, ProviderError, reconcile
from .store import Store


@dataclass
class CreateResult:
    job: Job = field(default_factory=lambda: Job(id="", status=""))
    failure: str = ""
    failure_code: str = ""
    ambiguous: bool = False


@dataclass
class DeliveryResult:
    delivered: bool = False
    diagnostic: str = ""


def application_failure(failure: Failure) -> ApplicationError:
    return ApplicationError(
        failure.message, loads(dict, dumps(failure)), type=failure.code, non_retryable=True
    )


def metadata(ref: BatchRef) -> dict[str, str]:
    return {"run_id": ref.run_id, "logical_batch_id": ref.batch_id}


def control_job(job: Job) -> Job:
    return replace(job, metadata={})


@asynccontextmanager
async def heartbeating(identity: str):
    """Heartbeat independently of slow HTTP calls and PostgreSQL operations."""

    async def beat():
        while True:
            activity.heartbeat(identity)
            await asyncio.sleep(5)

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class Activities:
    def __init__(self, client: Client, store: Store, provider: Provider):
        self.client, self.store, self.provider = client, store, provider

    @activity.defn
    async def submit_request(self, input: SAAInput) -> Result:
        try:
            validate_request(input.request)
            input.config.validate()
        except Failure as exc:
            raise application_failure(exc) from exc
        except ValueError as exc:
            raise ApplicationError(str(exc), type="INVALID_INPUT", non_retryable=True) from exc
        info = activity.info()
        if not info.activity_run_id or not info.task_token:
            raise ApplicationError(
                "standalone Activity required", type="INVALID_ACTIVITY_TARGET", non_retryable=True
            )
        deadlines = []
        if info.schedule_to_close_timeout:
            deadlines.append(info.scheduled_time + info.schedule_to_close_timeout)
        if info.start_to_close_timeout:
            deadlines.append(info.started_time + info.start_to_close_timeout)
        if not deadlines:
            raise ApplicationError(
                "Activity deadline required", type="INVALID_ACTIVITY_TARGET", non_retryable=True
            )
        target = CompletionTarget(
            info.namespace, info.activity_id, info.activity_run_id, info.task_token, info.attempt
        )
        enrollment = Enrollment(input.request, target, fingerprint(input.request), min(deadlines))
        async with asyncio.timeout(20):
            for attempt in range(5):
                activity.heartbeat(input.request.request_id)
                try:
                    await self.store.enroll(enrollment)
                    break
                except Failure as exc:
                    raise application_failure(exc) from exc
                except Exception:
                    if attempt == 4:
                        raise
                    await asyncio.sleep(0.1 * 2**attempt)
        activity.raise_complete_async()

    @activity.defn
    async def upload(self, ref: BatchRef) -> str:
        manifest = await self.store.load_manifest(ref.batch_id)
        try:
            return await self.provider.upload(ref.batch_id, jsonl(manifest))
        except ProviderError as exc:
            self._provider_error(exc)
            raise

    @activity.defn
    async def create(self, ref: BatchRef, file_id: str) -> CreateResult:
        # One Activity attempt and one HTTP attempt. A lost response is reconciled.
        called = False
        try:
            async with self.store.submission(ref.batch_id):
                called = True
                try:
                    job = await self.provider.create(file_id, metadata(ref))
                    return CreateResult(job=control_job(job))
                except ProviderError as exc:
                    return CreateResult(failure=str(exc), ambiguous=exc.ambiguous)
        except Failure as exc:
            if not called:
                return CreateResult(failure=exc.message, failure_code=exc.code)
            return CreateResult(failure="submission gate could not be confirmed", ambiguous=True)
        except Exception:
            return CreateResult(failure="submission gate could not be confirmed", ambiguous=True)

    @activity.defn
    async def find(self, ref: BatchRef, file_id: str) -> CreateResult:
        try:
            jobs = await self.provider.find(metadata(ref))
        except ProviderError as exc:
            self._provider_error(exc)
            raise
        matches = [
            job
            for job in jobs
            if job.input_file_id == file_id
            and all(job.metadata.get(key) == value for key, value in metadata(ref).items())
        ]
        if len(matches) != 1:
            return CreateResult(
                ambiguous=True, failure="provider submission could not be uniquely reconciled"
            )
        return CreateResult(job=control_job(matches[0]))

    @activity.defn
    async def poll(self, job: Job, deadline: datetime, interval: float) -> Job:
        async with heartbeating(job.id):
            while True:
                remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    raise ApplicationError(
                        "provider result deadline reached", type="DEADLINE_EXCEEDED", non_retryable=True
                    )
                if job.terminal():
                    return control_job(job)
                await asyncio.sleep(min(interval, remaining))
                remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    continue
                try:
                    async with asyncio.timeout(remaining):
                        job = await self.provider.retrieve(job.id)
                except ProviderError as exc:
                    self._provider_error(exc)
                except TimeoutError:
                    continue

    @activity.defn
    async def results(self, ref: BatchRef, job: Job) -> DeliverySummary:
        manifest = await self.store.load_manifest(ref.batch_id)
        try:
            files = [
                await self.provider.download(file_id)
                for file_id in (job.output_file_id, job.error_file_id)
                if file_id
            ]
        except ProviderError as exc:
            self._provider_error(exc)
            raise
        outcomes, diagnostics = reconcile(manifest, job, files)
        await self.store.save_outcomes(ref.batch_id, outcomes, diagnostics)
        return await self.store.delivery_summary(ref.batch_id)

    @activity.defn
    async def fail_batch(self, ref: BatchRef, code: str, message: str) -> DeliverySummary:
        await self.store.fail_batch(ref.batch_id, code, message)
        return await self.store.delivery_summary(ref.batch_id)

    @activity.defn
    async def save_report(self, report: BatchReport) -> None:
        await self.store.save_report(report)

    @activity.defn
    async def deliver(self, ref: BatchRef) -> DeliverySummary:
        async with heartbeating(ref.batch_id):
            while True:
                rows = await self.store.pending_deliveries(ref.batch_id)
                if not rows:
                    return await self.store.delivery_summary(ref.batch_id)
                for row in rows:
                    result = await self.complete(row.completion_target, row.outcome)
                    await self.store.mark_delivered(
                        ref.batch_id,
                        row.request_id,
                        row.completion_target,
                        result.delivered,
                        result.diagnostic,
                    )

    async def complete(self, target: CompletionTarget, outcome: Outcome) -> DeliveryResult:
        if (
            target.namespace != self.client.namespace
            or not target.task_token
            or not target.activity_id
            or not target.activity_run_id
            or target.attempt < 1
        ):
            raise ApplicationError(
                "invalid completion target", type="INVALID_ACTIVITY_TARGET", non_retryable=True
            )
        handle = self.client.get_activity_handle(target.activity_id, run_id=target.activity_run_id)
        try:
            description = await handle.describe(include_outcome=True)
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                return DeliveryResult(diagnostic="completion target no longer exists")
            raise
        if (
            description.activity_id != target.activity_id
            or description.activity_run_id != target.activity_run_id
        ):
            return DeliveryResult(diagnostic="completion target identity did not match")
        if description.status not in (ActivityExecutionStatus.RUNNING, ActivityExecutionStatus.PAUSED):
            return await self._reconcile_completion(description, outcome)
        if description.attempt != target.attempt:
            return DeliveryResult(diagnostic="completion target has a superseding attempt")
        async_handle = self.client.get_async_activity_handle(task_token=target.task_token)
        try:
            if outcome.failure:
                await async_handle.fail(application_failure(outcome.failure))
            elif outcome.result:
                await async_handle.complete(outcome.result)
            else:
                raise ApplicationError("outcome is empty", type="INVALID_RESULT", non_retryable=True)
            return DeliveryResult(delivered=True)
        except RPCError as exc:
            if exc.status == RPCStatusCode.INVALID_ARGUMENT:
                return DeliveryResult(diagnostic="completion token was rejected")
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            # The completion may have succeeded before its acknowledgement was lost.
            try:
                description = await handle.describe(include_outcome=True)
            except RPCError as inspect_exc:
                if inspect_exc.status == RPCStatusCode.NOT_FOUND:
                    return DeliveryResult(diagnostic="completion target no longer exists")
                raise
            return await self._reconcile_completion(description, outcome)

    @staticmethod
    async def _reconcile_completion(description, outcome: Outcome) -> DeliveryResult:
        try:
            if description.status == ActivityExecutionStatus.COMPLETED and outcome.result:
                if await description.result(Result) == outcome.result:
                    return DeliveryResult(delivered=True)
            if description.status == ActivityExecutionStatus.FAILED and outcome.failure:
                previous = await description.outcome_failure()
                if (
                    isinstance(previous, ApplicationError)
                    and previous.type == outcome.failure.code
                    and previous.details
                    and loads(Failure, previous.details[0]) == outcome.failure
                ):
                    return DeliveryResult(delivered=True)
        except (TypeError, ValueError, KeyError):
            # Another terminal outcome may not decode as our result or failure.
            pass
        return DeliveryResult(diagnostic="completion target closed without this outcome")

    @staticmethod
    def _provider_error(exc: ProviderError) -> None:
        if exc.permanent:
            raise ApplicationError(str(exc), type="PROVIDER_ERROR", non_retryable=True) from exc
