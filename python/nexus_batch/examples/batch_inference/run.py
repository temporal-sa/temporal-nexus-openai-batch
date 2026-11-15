"""Run generic batch inference from generated inputs or a caller-owned JSONL file."""

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
from nexus_batch.provider import FakeProvider, OpenAIProvider
from nexus_batch.runtime import queues, running_workers
from nexus_batch.store import LOCAL_DSN, Store

from .workflow import BatchInferenceWorkflow, ExampleInput, InferenceActivities, InputSource


async def run_example(
    *,
    address: str,
    namespace: str,
    dsn: str,
    run_id: str,
    count: int,
    state_dir: Path,
    input_path: Path | None = None,
    schema_path: Path | None = None,
    mode: str = "fake",
    model: str | None = None,
):
    if not 1 <= count <= 100_000:
        raise ValueError("count must be between 1 and 100,000")
    if mode not in {"fake", "live"}:
        raise ValueError("mode must be fake or live")
    config = Config.defaults(mode)
    if mode == "fake":
        config = replace(
            config, window=0.2, poll_interval=0.05, result_deadline=90.0, operation_timeout=120.0
        )
    config = replace(config, model=model or config.model, nexus_endpoint=queues(mode)[3])
    config.validate()
    provider = (
        FakeProvider(state_dir, processing_delay=0.05)
        if mode == "fake"
        else OpenAIProvider(os.environ.get("OPENAI_API_KEY", ""))
    )
    store = None
    try:
        await initialize_database(dsn)
        client = await Client.connect(address, namespace=namespace)
        await setup(client, mode)
        store = await Store.connect(dsn, namespace)
        await store.start_run(RunInput(run_id, config))
        activities = InferenceActivities(BulkStore(store))
        queue = "batch-inference/" + run_id
        source = InputSource(
            count,
            str(input_path.resolve()) if input_path else None,
            str(schema_path.resolve()) if schema_path else None,
        )
        async with (
            running_workers(client, store, provider, mode=mode),
            Worker(
                client,
                task_queue=queue,
                workflows=[BatchInferenceWorkflow],
                activities=[activities.prepare_requests],
            ),
        ):
            result = await client.execute_workflow(
                BatchInferenceWorkflow.run,
                ExampleInput(run_id, config.nexus_endpoint, source, config.operation_timeout),
                id="batch-inference/" + run_id,
                task_queue=queue,
            )
            await store.close_run(run_id)
            return result
    finally:
        await provider.close()
        if store is not None:
            await store.close()


def main():
    load_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7234"))
    parser.add_argument("--namespace", default=os.getenv("TEMPORAL_NAMESPACE", "default"))
    parser.add_argument("--database-url", default=os.getenv("PYTHON_DATABASE_URL", LOCAL_DSN))
    parser.add_argument("--run-id", default="batch-inference-" + uuid4().hex[:12])
    parser.add_argument("--count", type=int, default=12, help="Generated inputs when --input is omitted")
    parser.add_argument("--input", type=Path, help="JSONL rows with custom_id and Responses body")
    parser.add_argument(
        "--schema", type=Path, help="Optional caller-supplied JSON Schema for structured output"
    )
    parser.add_argument("--mode", choices=["fake", "live"], default="fake")
    parser.add_argument("--model", help="Required in live mode; selects the model for this run")
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
            input_path=args.input,
            schema_path=args.schema,
            mode=args.mode,
            model=args.model,
        )
    )
    print(dumps(result))


if __name__ == "__main__":
    main()
