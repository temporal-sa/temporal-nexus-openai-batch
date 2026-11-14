"""Run the two-stage example against real Temporal/PostgreSQL and fake OpenAI."""

import argparse
import asyncio
import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from temporalio.client import Client
from temporalio.worker import Worker

from nexus_batch.bulk_store import BulkStore
from nexus_batch.cli import initialize_database, load_environment, setup
from nexus_batch.models import Config, RunInput, dumps
from nexus_batch.provider import FakeProvider
from nexus_batch.runtime import queues, running_workers
from nexus_batch.store import LOCAL_DSN, Store

from .workflow import ExampleInput, ReviewActivities, ReviewRepliesWorkflow


async def run_example(
    *,
    address: str,
    namespace: str,
    dsn: str,
    run_id: str,
    count: int,
    state_dir: Path,
):
    if not 1 <= count <= 50_000:
        raise ValueError("count must be between 1 and 50,000")
    await initialize_database(dsn)
    client = await Client.connect(address, namespace=namespace)
    await setup(client, "fake")
    store = await Store.connect(dsn, namespace)
    provider = FakeProvider(state_dir, processing_delay=0.05)
    config = replace(
        Config.defaults("fake"),
        window=0.2,
        max_batch_requests=100,
        poll_interval=0.05,
        result_deadline=90.0,
        operation_timeout=120.0,
        max_output_tokens=512,
        nexus_endpoint=queues("fake")[3],
    )
    queue = "bulk-review-replies/" + run_id
    activities = ReviewActivities(BulkStore(store))
    try:
        await store.start_run(RunInput(run_id, config))
        async with (
            running_workers(client, store, provider),
            Worker(
                client,
                task_queue=queue,
                workflows=[ReviewRepliesWorkflow],
                activities=[
                    activities.prepare_reviews,
                    activities.select_negative,
                    activities.consume_replies,
                ],
            ),
        ):
            result = await client.execute_workflow(
                ReviewRepliesWorkflow.run,
                ExampleInput(run_id, count, config.nexus_endpoint, config.operation_timeout),
                id="review-replies/" + run_id,
                task_queue=queue,
            )
            await store.close_run(run_id)
            return result
    finally:
        await provider.close()
        await store.close()


def main():
    load_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7234"))
    parser.add_argument("--namespace", default=os.getenv("TEMPORAL_NAMESPACE", "default"))
    parser.add_argument("--database-url", default=os.getenv("PYTHON_DATABASE_URL", LOCAL_DSN))
    parser.add_argument("--run-id", default="review-replies-" + uuid4().hex[:12])
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--state-dir", type=Path, default=Path(".state/python-bulk-example"))
    args = parser.parse_args()
    result = asyncio.run(
        run_example(
            address=args.address,
            namespace=args.namespace,
            dsn=args.database_url,
            run_id=args.run_id,
            count=args.count,
            state_dir=args.state_dir,
        )
    )
    print(dumps(result))


if __name__ == "__main__":
    main()
