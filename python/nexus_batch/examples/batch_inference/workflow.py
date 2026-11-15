"""Submit caller-owned requests through the generic batch inference service."""

from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
from pathlib import Path

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nexus_batch.bulk import BatchInputRef, BatchResultRef, BulkRequest, responses
    from nexus_batch.bulk_store import BulkStore
    from nexus_batch.models import Failure


@dataclass
class InputSource:
    count: int = 12
    input_path: str | None = None
    schema_path: str | None = None


@dataclass
class ExampleInput:
    run_id: str
    endpoint: str
    source: InputSource
    operation_timeout: float = 120.0


class InferenceActivities:
    def __init__(self, store: BulkStore):
        self.store = store

    @activity.defn
    async def prepare_requests(self, source: InputSource) -> BatchInputRef:
        try:
            return await self._prepare_requests(source)
        except Failure as exc:
            raise ApplicationError(exc.message, type=exc.code, non_retryable=True) from exc
        except (OSError, ValueError) as exc:
            raise ApplicationError(str(exc), type="INVALID_INPUT", non_retryable=True) from exc

    async def _prepare_requests(self, source: InputSource) -> BatchInputRef:
        # Paths and counts cross the Workflow boundary; bodies and schemas are
        # loaded here. A deployed producer can stream from its own data source.
        schema = json.loads(Path(source.schema_path).read_text()) if source.schema_path else None

        def requests():
            if source.input_path:
                with Path(source.input_path).open() as stream:
                    for line_number, line in enumerate(stream, 1):
                        if not line.strip():
                            continue
                        try:
                            value = json.loads(line)
                            yield BulkRequest(value["custom_id"], value["body"])
                        except (ValueError, TypeError, KeyError) as exc:
                            raise ValueError(f"invalid request on JSONL line {line_number}") from exc
            else:
                for index in range(source.count):
                    yield BulkRequest(f"request-{index:06d}", {"input": f"Sample input {index}"})

        return await self.store.write_input(requests(), schema=schema)


@workflow.defn
class BatchInferenceWorkflow:
    @workflow.run
    async def run(self, input: ExampleInput) -> BatchResultRef:
        requests = await workflow.execute_activity(
            "prepare_requests",
            input.source,
            result_type=BatchInputRef,
            start_to_close_timeout=timedelta(minutes=5),
        )
        identity = hashlib.sha256(input.run_id.encode()).hexdigest()[:32]
        return await responses(
            input.run_id,
            identity + "-inference",
            requests,
            endpoint=input.endpoint,
            operation_timeout=timedelta(seconds=input.operation_timeout),
        )
