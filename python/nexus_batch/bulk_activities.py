"""One asynchronous recipient per bulk submission, backed by PostgreSQL.

Provider work continues through the existing BatchWorkflow. This Activity only
validates and admits an immutable manifest, then hands its exact completion
target to the independent keeper. Item contents never become Activity results.
"""

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError, is_cancelled_exception

from .activities import Activities, CompletionValue, application_failure, heartbeating
from .bulk_models import BatchResultRef, BulkSAAInput, validate_submission
from .bulk_store import BulkStore
from .models import CompletionTarget, Failure
from .store import Store


class BulkActivities:
    def __init__(self, client: Client, store: Store, completion: Activities):
        self.client = client
        self.store = BulkStore(store)
        self.completion = completion

    @activity.defn
    async def submit_bulk(self, input: BulkSAAInput) -> BatchResultRef:
        try:
            validate_submission(input.submission)
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
        try:
            async with heartbeating(input.submission.submission_id):
                await self.store.attach(input.submission, target, min(deadlines))
                while not await self.store.admit_page(input.submission.submission_id):
                    activity.heartbeat(input.submission.submission_id)
        except Failure as exc:
            await self.store.abort_admission(input.submission.submission_id, target, exc.code)
            raise application_failure(exc) from exc
        except BaseException as exc:
            if is_cancelled_exception(exc):
                # Worker shutdown, timeout, pause and reset also cancel the
                # Activity coroutine. Their replacement attempt must resume the
                # ledger; only an explicit recipient cancellation abandons it.
                details = activity.cancellation_details()
                if details and details.cancel_requested:
                    import asyncio

                    await asyncio.shield(
                        self.store.abort_admission(
                            input.submission.submission_id, target, "recipient cancelled"
                        )
                    )
            raise
        activity.raise_complete_async()

    async def maintain_once(self, mode: str, endpoint: str) -> tuple[int, int]:
        """Heartbeat pending recipients and drain the durable bulk delivery outbox."""
        import asyncio

        async def heartbeat(target):
            async with asyncio.timeout(5):
                await self.client.get_async_activity_handle(task_token=target.task_token).heartbeat()

        async def deliver(row):
            async with asyncio.timeout(5):
                result = await self.completion.complete(
                    row.completion_target, CompletionValue(row.result), BatchResultRef
                )
                await self.store.mark_delivered(
                    row.submission_id, row.completion_target, result.delivered, result.diagnostic
                )

        targets = await self.store.pending_heartbeats(mode, endpoint)
        await asyncio.gather(*(heartbeat(target) for target in targets), return_exceptions=True)
        rows = await self.store.pending_completions(mode, endpoint)
        await asyncio.gather(*(deliver(row) for row in rows), return_exceptions=True)
        return len(targets), len(rows)
