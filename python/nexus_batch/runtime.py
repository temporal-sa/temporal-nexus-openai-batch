"""Workers, the dispatch outbox, and independent external-completion keepers."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Worker

from .activities import Activities
from .models import CALLER_TASK_QUEUE, SERVICE_TASK_QUEUE, NEXUS_ENDPOINT, workflow_id
from .nexus_service import BatchService
from .provider import Provider
from .store import Store
from .workflows import BatchWorkflow, CallerWorkflow

logger = logging.getLogger(__name__)


def queues(mode: str) -> tuple[str, str, str, str]:
    suffix = "-live" if mode == "live" else ""
    service = SERVICE_TASK_QUEUE + suffix
    return CALLER_TASK_QUEUE + suffix, service, service + "-batches", NEXUS_ENDPOINT + suffix


async def dispatch_once(client: Client, store: Store, mode: str, endpoint: str, queue: str) -> None:
    await store.seal_due(mode, endpoint)
    for ref in await store.ready_batches(mode, endpoint):
        try:
            await client.start_workflow(
                BatchWorkflow.run,
                ref,
                id=workflow_id(ref.batch_id),
                task_queue=queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )
        except WorkflowAlreadyStartedError:
            pass
        await store.mark_dispatched(ref.batch_id)


async def dispatcher(client: Client, store: Store, mode: str, endpoint: str, queue: str) -> None:
    delay = 0.1
    while True:
        try:
            await dispatch_once(client, store, mode, endpoint, queue)
        except Exception:
            logger.warning("dispatch unavailable; durable outbox will retry")
            await asyncio.sleep(delay)
            delay = min(5, delay * 2)
        else:
            delay = 0.1
            await asyncio.sleep(delay)


async def maintain_enrollments(activities: Activities, mode: str, endpoint: str) -> None:
    async def heartbeat(target):
        async with asyncio.timeout(5):
            handle = activities.client.get_async_activity_handle(task_token=target.task_token)
            await handle.heartbeat()

    async def complete(row):
        async with asyncio.timeout(5):
            result = await activities.complete(row.completion_target, row.outcome)
            await activities.store.mark_delivered(
                row.batch_id, row.request_id, row.completion_target, result.delivered, result.diagnostic
            )

    while True:
        try:
            targets = await activities.store.pending_heartbeats(mode, endpoint)
            await asyncio.gather(*(heartbeat(target) for target in targets), return_exceptions=True)
            rows = await activities.store.pending_completions(mode, endpoint)
            await asyncio.gather(*(complete(row) for row in rows), return_exceptions=True)
            await asyncio.sleep(0 if len(targets) == 100 or len(rows) == 100 else 0.1)
        except Exception:
            logger.warning("completion keeper unavailable; persisted targets will retry")
            await asyncio.sleep(1)


@asynccontextmanager
async def running_workers(
    client: Client,
    store: Store,
    provider: Provider,
    mode: str = "fake",
    *,
    service_concurrency: int = 32,
    batch_concurrency: int = 32,
):
    caller_queue, service_queue, batch_queue, endpoint = queues(mode)
    activities = Activities(client, store, provider)
    caller = Worker(client, task_queue=caller_queue, workflows=[CallerWorkflow])
    service = Worker(
        client,
        task_queue=service_queue,
        activities=[activities.submit_request],
        nexus_service_handlers=[BatchService(store, endpoint)],
        max_concurrent_activities=service_concurrency,
        graceful_shutdown_timeout=timedelta(seconds=5),
    )
    batch = Worker(
        client,
        task_queue=batch_queue,
        workflows=[BatchWorkflow],
        activities=[
            activities.upload,
            activities.create,
            activities.find,
            activities.poll,
            activities.results,
            activities.fail_batch,
            activities.save_report,
            activities.deliver,
        ],
        max_concurrent_activities=batch_concurrency,
        graceful_shutdown_timeout=timedelta(seconds=5),
    )
    async with caller, service, batch:
        background = [
            asyncio.create_task(dispatcher(client, store, mode, endpoint, batch_queue)),
            asyncio.create_task(maintain_enrollments(activities, mode, endpoint)),
        ]
        try:
            yield
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
