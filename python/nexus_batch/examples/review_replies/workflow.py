"""Application orchestration and Activity-side selection for the bulk example."""

from dataclasses import dataclass
from datetime import timedelta
import hashlib
from typing import Literal

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel

    from nexus_batch.bulk import (
        BatchInputRef,
        BatchResultRef,
        BulkRequest,
        iter_results,
        responses,
        write_requests,
    )
    from nexus_batch.bulk_store import BulkStore


class Sentiment(BaseModel):
    label: Literal["negative", "positive"]


class Reply(BaseModel):
    text: str


@dataclass
class ExampleInput:
    run_id: str
    count: int
    endpoint: str
    operation_timeout: float = 120.0


@dataclass
class Selection:
    requests: BatchInputRef | None
    local_parse_failed: int = 0


@dataclass
class ReplySummary:
    succeeded: int = 0
    local_parse_failed: int = 0


@dataclass
class ExampleResult:
    classifications: BatchResultRef
    replies: BatchResultRef | None
    selected: int
    consumed_replies: int
    local_parse_failed: int


class ReviewActivities:
    def __init__(self, store: BulkStore):
        self.store = store

    @activity.defn
    async def prepare_reviews(self, count: int) -> BatchInputRef:
        # A real application could stream rows from its own database here. The
        # Workflow passes a count, never a list of review documents.
        def requests():
            for index in range(count):
                negative = index % 3 == 0
                text = "negative: delivery was late" if negative else "positive: delivery was good"
                yield BulkRequest(
                    f"review-{index:06d}",
                    {"instructions": "Classify the review sentiment.", "input": f"{text} (review {index})"},
                )

        return await write_requests(self.store, requests(), output_type=Sentiment)

    @activity.defn
    async def select_negative(self, ref: BatchResultRef) -> Selection:
        local_parse_failed = 0

        async def selected():
            nonlocal local_parse_failed
            async for item in iter_results(self.store, ref, output_type=Sentiment):
                if item.parse_error.startswith("local validation:"):
                    local_parse_failed += 1
                if item.ok and item.output.label == "negative":
                    yield BulkRequest(
                        item.custom_id + "-reply",
                        {
                            "instructions": "Draft a concise, considerate reply.",
                            "input": item.request["input"],
                        },
                    )

        iterator = selected()
        first = await anext(iterator, None)
        if first is None:
            return Selection(None, local_parse_failed)

        async def with_first():
            yield first
            async for item in iterator:
                yield item

        requests = await write_requests(self.store, with_first(), output_type=Reply)
        return Selection(requests, local_parse_failed)

    @activity.defn
    async def consume_replies(self, ref: BatchResultRef) -> ReplySummary:
        summary = ReplySummary()
        async for item in iter_results(self.store, ref, output_type=Reply):
            if item.ok:
                # The reply is already persisted in the shared result store.
                # An application could idempotently upsert it by custom_id here.
                summary.succeeded += 1
            elif item.parse_error.startswith("local validation:"):
                summary.local_parse_failed += 1
        return summary


@workflow.defn
class ReviewRepliesWorkflow:
    @workflow.run
    async def run(self, input: ExampleInput) -> ExampleResult:
        requests = await workflow.execute_activity(
            "prepare_reviews",
            input.count,
            result_type=BatchInputRef,
            start_to_close_timeout=timedelta(minutes=5),
        )
        options = {
            "endpoint": input.endpoint,
            "operation_timeout": timedelta(seconds=input.operation_timeout),
        }
        # Submission IDs are namespace-scoped and must remain unique across runs.
        identity = hashlib.sha256(input.run_id.encode()).hexdigest()[:32]
        classified = await responses(input.run_id, identity + "-classify", requests, **options)
        selection = await workflow.execute_activity(
            "select_negative",
            classified,
            result_type=Selection,
            start_to_close_timeout=timedelta(minutes=5),
        )
        if selection.requests is None:
            return ExampleResult(classified, None, 0, 0, selection.local_parse_failed)
        replies = await responses(input.run_id, identity + "-replies", selection.requests, **options)
        summary = await workflow.execute_activity(
            "consume_replies",
            replies,
            result_type=ReplySummary,
            start_to_close_timeout=timedelta(minutes=5),
        )
        return ExampleResult(
            classified,
            replies,
            selection.requests.count,
            summary.succeeded,
            selection.local_parse_failed + summary.local_parse_failed,
        )
