"""Small local CLI for fake or live batches. Durations and reports use seconds."""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import asyncpg

from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest, ListNexusEndpointsRequest
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.service import RPCError, RPCStatusCode

from .models import CallerInput, Config, Request, RunInput, caller_id, dumps, validate_request, workflow_id
from .provider import FakeProvider, OpenAIProvider
from .runtime import queues, running_workers
from .store import LOCAL_DSN, Store
from .workflows import CallerWorkflow


async def initialize_database(dsn: str) -> None:
    """Create the requested development database when it does not exist yet."""
    try:
        connection = await asyncpg.connect(dsn)
    except asyncpg.InvalidCatalogNameError:
        name = unquote(urlsplit(dsn).path.lstrip("/"))
        if not name:
            raise ValueError("database URL must include a database name") from None
        connection = await asyncpg.connect(dsn, database="postgres")
        try:
            identifier = '"' + name.replace('"', '""') + '"'
            try:
                await connection.execute("CREATE DATABASE " + identifier)
            except asyncpg.DuplicateDatabaseError:
                pass
        finally:
            await connection.close()
    else:
        await connection.close()


async def setup(client: Client, mode: str) -> None:
    _, queue, _, endpoint = queues(mode)
    response = await client.operator_service.list_nexus_endpoints(ListNexusEndpointsRequest(name=endpoint))
    if response.endpoints:
        target = response.endpoints[0].spec.target
        if (
            not target.HasField("worker")
            or target.worker.namespace != client.namespace
            or target.worker.task_queue != queue
        ):
            raise ValueError("existing Nexus endpoint targets a different namespace or task queue")
        return
    await client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=endpoint,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(namespace=client.namespace, task_queue=queue)
                ),
            )
        )
    )


def configuration(args) -> Config:
    config = Config.defaults(args.mode)
    config.nexus_endpoint = queues(args.mode)[3]
    config.model = args.model or (os.environ.get("OPENAI_MODEL", "") if args.mode == "live" else config.model)
    for flag, field in [
        ("window", "window"),
        ("batch_size", "max_batch_requests"),
        ("batch_bytes", "max_batch_bytes"),
        ("max_admissions", "max_admissions"),
        ("poll_interval", "poll_interval"),
        ("result_deadline", "result_deadline"),
        ("operation_timeout", "operation_timeout"),
    ]:
        value = getattr(args, flag, None)
        if value is not None:
            setattr(config, field, value)
    config.validate()
    return config


async def demo(client: Client, store: Store, args) -> int:
    config = configuration(args)
    run_id = args.run_id
    if args.input:
        documents = [json.loads(line) for line in Path(args.input).read_text().splitlines() if line.strip()]
        requests = [Request(run_id, doc["request_id"], doc["input"]) for doc in documents]
    else:
        requests = [
            Request(
                run_id,
                f"document-{i + 1:04d}",
                f"Summarize document {i + 1}: durable batches survive worker restarts.",
            )
            for i in range(args.count)
        ]
    if not requests or len({request.request_id for request in requests}) != len(requests):
        raise ValueError("input must contain at least one request and unique request IDs")
    for request in requests:
        validate_request(request)
    await store.start_run(RunInput(run_id, config))
    queue = queues(args.mode)[0]
    semaphore = asyncio.Semaphore(100)

    async def launch(request):
        async with semaphore:
            return await client.start_workflow(
                CallerWorkflow.run,
                CallerInput(request, config),
                id=caller_id(run_id, request.request_id),
                task_queue=queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )

    handles = await asyncio.gather(*(launch(request) for request in requests))

    async def result(handle):
        try:
            return {"workflow_id": handle.id, "result": await handle.result()}
        except Exception as exc:
            return {"workflow_id": handle.id, "error": str(exc)}

    deadline = asyncio.get_running_loop().time() + args.wait
    pending = asyncio.gather(*(result(handle) for handle in handles))
    try:
        async with asyncio.timeout(args.wait):
            results = await pending
    except TimeoutError:
        print(
            dumps(
                {
                    "run_id": run_id,
                    "simulated": args.mode == "fake",
                    "pending": True,
                    "status": await store.status(run_id),
                }
            )
        )
        return 1
    await store.close_run(run_id)
    status = await store.status(run_id)
    while any(batch.status not in {"completed", "completed_with_errors"} for batch in status.batches):
        if asyncio.get_running_loop().time() >= deadline:
            print(
                dumps({"run_id": run_id, "simulated": args.mode == "fake", "pending": True, "status": status})
            )
            return 1
        await asyncio.sleep(0.1)
        status = await store.status(run_id)
    print(dumps({"run_id": run_id, "simulated": args.mode == "fake", "results": results, "status": status}))
    return 1 if any("error" in item for item in results) else 0


async def cleanup(client: Client, store: Store, run_id: str) -> None:
    await store.stop_run(run_id)
    batches, targets = await store.run_resources(run_id)
    errors = []
    for batch_id in batches:
        try:
            await client.get_workflow_handle(workflow_id(batch_id)).cancel()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                errors.append(exc)
    for target in targets:
        try:
            await client.get_activity_handle(target.activity_id, run_id=target.activity_run_id).terminate(
                reason="run stopped"
            )
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                errors.append(exc)
    try:
        async for execution in client.list_workflows(
            f"WorkflowId STARTS_WITH 'caller/{run_id}/' AND ExecutionStatus = 'Running'"
        ):
            await client.get_workflow_handle(execution.id, run_id=execution.run_id).cancel()
    except Exception as exc:
        errors.append(exc)
    if errors:
        raise RuntimeError(f"run stopped; {len(errors)} Temporal cleanup operations could not be confirmed")


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    commands = command.add_subparsers(dest="command", required=True)
    for name in ("setup", "init-db", "worker", "demo", "inspect", "finish", "cancel", "cleanup"):
        sub = commands.add_parser(name)
        sub.add_argument("--address", default=os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7234"))
        sub.add_argument("--namespace", default=os.getenv("TEMPORAL_NAMESPACE", "default"))
        sub.add_argument("--database-url", default=os.getenv("PYTHON_DATABASE_URL", LOCAL_DSN))
        sub.add_argument("--mode", choices=["fake", "live"], default="fake")
        if name in {"demo", "inspect", "finish", "cancel", "cleanup"}:
            sub.add_argument("--run-id", required=True)
        if name == "cancel":
            sub.add_argument("--request-id", required=True)
        if name == "worker":
            sub.add_argument("--scenario", default="success")
            sub.add_argument("--state-dir", default=".state/python-fake")
            sub.add_argument("--processing-delay", type=float, default=0.5)
            sub.add_argument("--service-concurrency", type=int, default=32)
            sub.add_argument("--batch-concurrency", type=int, default=32)
        if name == "demo":
            sub.add_argument("--count", type=int, default=100)
            sub.add_argument("--input")
            sub.add_argument("--model")
            sub.add_argument("--wait", type=float, default=180)
            sub.add_argument("--batch-size", type=int)
            sub.add_argument("--batch-bytes", type=int)
            sub.add_argument("--max-admissions", type=int)
            for option in ("window", "poll-interval", "result-deadline", "operation-timeout"):
                sub.add_argument("--" + option, type=float, help="seconds")
    return command


async def run(args) -> int:
    if args.command == "setup":
        await setup(await Client.connect(args.address, namespace=args.namespace), args.mode)
        print("Nexus endpoint ready")
        return 0
    if args.command == "init-db":
        await initialize_database(args.database_url)
    store = await Store.connect(args.database_url, args.namespace)
    try:
        if args.command == "init-db":
            print("PostgreSQL schema ready")
        elif args.command == "inspect":
            print(dumps(await store.status(args.run_id)))
        elif args.command == "finish":
            await store.close_run(args.run_id)
        else:
            client = await Client.connect(args.address, namespace=args.namespace)
            if args.command == "worker":
                if args.service_concurrency < 1 or args.batch_concurrency < 1:
                    raise ValueError("worker concurrency must be positive")
                provider = (
                    FakeProvider(args.state_dir, args.scenario, args.processing_delay)
                    if args.mode == "fake"
                    else OpenAIProvider(os.environ.get("OPENAI_API_KEY", ""))
                )
                try:
                    async with running_workers(
                        client,
                        store,
                        provider,
                        args.mode,
                        service_concurrency=args.service_concurrency,
                        batch_concurrency=args.batch_concurrency,
                    ):
                        print("Python workers ready", flush=True)
                        await asyncio.Event().wait()
                finally:
                    await provider.close()
            elif args.command == "demo":
                return await demo(client, store, args)
            elif args.command == "cancel":
                await client.get_workflow_handle(caller_id(args.run_id, args.request_id)).cancel()
            elif args.command == "cleanup":
                # Validate before embedding a run ID in a visibility query.
                validate_request(Request(args.run_id, "cleanup", "cleanup"))
                await cleanup(client, store, args.run_id)
    finally:
        await store.close()
    return 0


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    try:
        status = asyncio.run(run(parser().parse_args()))
    except KeyboardInterrupt:
        status = 130
    except Exception as exc:
        print(f"batchctl-python: {exc}", file=sys.stderr)
        status = 1
    raise SystemExit(status)
