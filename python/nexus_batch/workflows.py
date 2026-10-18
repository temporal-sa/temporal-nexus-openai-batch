"""Durable control flow. Request bodies, manifests and tokens stay out of batch history."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from .activities import CreateResult
    from .models import BatchRef, BatchReport, CallerInput, DeliverySummary, Result, SERVICE, OPERATION
    from .provider import Job


RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=10))


def finalization_options(bulk: bool = False) -> dict:
    # Bookkeeping still needs to finish after recipients expire during an outage.
    return {"start_to_close_timeout": timedelta(seconds=300 if bulk else 30), "retry_policy": RETRY}


def provider_deadline(ref: BatchRef):
    reserve = ref.config.operation_timeout - ref.config.result_deadline - ref.config.window
    return min(
        ref.sealed_at + timedelta(seconds=ref.config.result_deadline),
        ref.deadline_at - timedelta(seconds=reserve),
    )


def provider_options(deadline, *, create: bool = False, bulk: bool = False) -> dict:
    remaining = max(timedelta(milliseconds=1), deadline - workflow.now())
    return {
        "start_to_close_timeout": min(timedelta(seconds=300 if bulk else 30), remaining),
        "schedule_to_close_timeout": remaining,
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=1 if create else 0,
        ),
    }


@workflow.defn
class CallerWorkflow:
    @workflow.run
    async def run(self, input: CallerInput) -> Result:
        client = workflow.create_nexus_client(service=SERVICE, endpoint=input.config.nexus_endpoint)
        return await client.execute_operation(
            OPERATION,
            input.request,
            output_type=Result,
            schedule_to_close_timeout=timedelta(seconds=input.config.operation_timeout),
            cancellation_type=workflow.NexusOperationCancellationType.WAIT_REQUESTED,
        )


@workflow.defn
class BatchWorkflow:
    def __init__(self):
        self.report = BatchReport(batch_id="", status="starting")

    @workflow.query
    def status(self) -> BatchReport:
        return self.report

    async def save(self) -> None:
        await workflow.execute_activity("save_report", self.report, **finalization_options())

    def apply_summary(self, summary: DeliverySummary) -> None:
        report = self.report
        report.delivered, report.skipped = summary.delivered, summary.skipped
        report.succeeded, report.failed = summary.succeeded, summary.failed
        report.failure_counts = summary.failure_counts
        report.diagnostics = list(dict.fromkeys(report.diagnostics + summary.diagnostics))[:100]

    async def fail(self, ref: BatchRef, code: str, message: str) -> None:
        summary = await workflow.execute_activity(
            "fail_batch", args=[ref, code, message], result_type=DeliverySummary, **finalization_options()
        )
        self.apply_summary(summary)

    @workflow.run
    async def run(self, ref: BatchRef) -> BatchReport:
        try:
            ref.config.validate()
        except ValueError as exc:
            raise ApplicationError(str(exc), type="INVALID_BATCH", non_retryable=True) from exc
        if (
            not ref.batch_id
            or not 1 <= ref.requests <= ref.config.max_batch_requests
            or not 1 <= ref.bytes <= ref.config.max_batch_bytes
            or not ref.deadline_at
        ):
            raise ApplicationError("invalid sealed batch reference", type="INVALID_BATCH", non_retryable=True)
        self.report = BatchReport(
            batch_id=ref.batch_id,
            status="uploading",
            requests=ref.requests,
            bytes=ref.bytes,
            collection_wait=(ref.sealed_at - ref.first_admitted_at).total_seconds(),
        )
        await self.save()
        start = workflow.now()
        deadline = provider_deadline(ref)
        await self.process(ref, deadline)
        self.report.provider_duration = (workflow.now() - start).total_seconds()
        self.report.status = "delivering"
        await self.save()
        start = workflow.now()
        summary = await workflow.execute_activity(
            "deliver",
            ref,
            result_type=DeliverySummary,
            heartbeat_timeout=timedelta(seconds=30),
            **finalization_options(bulk=True),
        )
        self.apply_summary(summary)
        self.report.delivery_duration = (workflow.now() - start).total_seconds()
        self.report.status = "completed_with_errors" if self.report.failed else "completed"
        await self.save()
        return self.report

    async def process(self, ref: BatchRef, deadline) -> None:
        if workflow.now() >= deadline:
            await self.fail(ref, "DEADLINE_EXCEEDED", "provider deadline reached before upload")
            return
        try:
            file_id = await workflow.execute_activity(
                "upload", ref, result_type=str, **provider_options(deadline, bulk=True)
            )
        except ActivityError:
            await self.fail(ref, "UPLOAD_FAILED", "provider input upload failed")
            return
        self.report.input_file_id, self.report.status = file_id, "submitting"
        await self.save()
        if workflow.now() >= deadline:
            await self.fail(ref, "DEADLINE_EXCEEDED", "provider deadline reached before creation")
            return
        try:
            created = await workflow.execute_activity(
                "create",
                args=[ref, file_id],
                result_type=CreateResult,
                **provider_options(deadline, create=True),
            )
        except ActivityError:
            created = CreateResult(ambiguous=True)
        if created.ambiguous:
            self.report.status = "reconciling_submission"
            try:
                created = await workflow.execute_activity(
                    "find",
                    args=[ref, file_id],
                    result_type=CreateResult,
                    **provider_options(workflow.now() + timedelta(minutes=2)),
                )
            except ActivityError:
                created = CreateResult(ambiguous=True)
            if created.ambiguous or not created.job.id:
                await self.fail(
                    ref,
                    "SUBMISSION_UNKNOWN",
                    "provider submission could not be uniquely reconciled; no second submission attempted",
                )
                return
        if created.failure or created.failure_code:
            await self.fail(
                ref, created.failure_code or "SUBMISSION_FAILED", "provider batch submission rejected"
            )
            return
        job = created.job
        if not job.id:
            await self.fail(ref, "SUBMISSION_UNKNOWN", "provider returned no batch identifier")
            return
        self.report.provider_batch_id, self.report.provider_status = job.id, job.status
        self.report.status = "processing_results" if job.terminal() else job.status
        await self.save()
        if not job.terminal():
            if workflow.now() >= deadline:
                await self.fail(ref, "DEADLINE_EXCEEDED", "provider result deadline reached")
                return
            remaining = deadline - workflow.now()
            try:
                job = await workflow.execute_activity(
                    "poll",
                    args=[job, deadline, ref.config.poll_interval],
                    result_type=Job,
                    start_to_close_timeout=remaining,
                    schedule_to_close_timeout=remaining,
                    heartbeat_timeout=min(timedelta(seconds=30), remaining),
                    retry_policy=RETRY,
                )
            except ActivityError as exc:
                code = (
                    "PROVIDER_FAILED"
                    if isinstance(exc.cause, ApplicationError) and exc.cause.type == "PROVIDER_ERROR"
                    else "DEADLINE_EXCEEDED"
                )
                await self.fail(ref, code, "provider polling ended without results")
                return
        self.report.provider_status, self.report.status = job.status, "processing_results"
        await self.save()
        try:
            summary = await workflow.execute_activity(
                "results", args=[ref, job], result_type=DeliverySummary, **finalization_options(bulk=True)
            )
        except ActivityError:
            await self.fail(ref, "RESULT_DOWNLOAD_FAILED", "provider result download failed")
        else:
            self.apply_summary(summary)
