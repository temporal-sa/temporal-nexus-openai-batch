"""Typed bulk helpers: references in Workflows, data access in Activities.

Every Activity using these helpers receives an explicit ``BulkStore`` backed by
the same namespace-scoped database as the Nexus service. The service exchanges
portable JSON Schema; it never imports an application's Pydantic classes.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from pydantic import BaseModel, ValidationError
from temporalio import activity, workflow

from .bulk_models import (
    BULK_OPERATION,
    MAX_BULK_PAGE_ITEMS,
    BatchCounts,
    BatchInputRef,
    BatchResultRef,
    BulkItem,
    BulkRequest,
    BulkSubmission,
    validate_schema,
)
from .models import SERVICE

if TYPE_CHECKING:
    from .bulk_store import BulkStore

T = TypeVar("T")


def _in_activity() -> None:
    if not activity.in_activity():
        raise RuntimeError("bulk data can only be written or loaded inside an Activity")


def _output_schema(output_type: type[BaseModel] | None) -> dict | None:
    if output_type is None:
        return None
    if not isinstance(output_type, type) or not issubclass(output_type, BaseModel):
        raise TypeError("output_type must be a Pydantic BaseModel class")
    # This public SDK helper applies the same strict JSON Schema conventions as
    # structured outputs, without sending a request or importing private APIs.
    from openai import pydantic_function_tool

    schema = dict(pydantic_function_tool(output_type)["function"]["parameters"])
    validate_schema(schema)
    return schema


async def write_requests(
    store: BulkStore,
    requests: Iterable[BulkRequest] | AsyncIterable[BulkRequest],
    *,
    output_type: type[T] | None = None,
) -> BatchInputRef[T]:
    """Seal a nonempty, immutable input manifest without returning its items.

    A schema is stored with the manifest when ``output_type`` is supplied. The
    reference contains only its opaque ID, digest and count. Empty input and
    duplicate custom IDs are rejected by the store before admission.
    """
    _in_activity()
    schema = _output_schema(output_type)
    return cast(BatchInputRef[T], await store.write_input(requests, schema=schema))


async def responses(
    run_id: str,
    submission_id: str,
    requests: BatchInputRef[T],
    *,
    endpoint: str,
    operation_timeout: timedelta = timedelta(hours=26),
) -> BatchResultRef[T]:
    """Await one bulk Nexus operation, independently of its request count.

    The run's immutable service-side configuration selects the model and
    batching limits. This function performs no database access or bulk loading.
    """
    if not workflow.in_workflow():
        raise RuntimeError("responses must be called inside a Workflow")
    if not endpoint or operation_timeout <= timedelta(0):
        raise ValueError("endpoint and a positive operation_timeout are required")
    if requests.count <= 0:
        raise ValueError("a bulk submission must contain at least one request")
    client = workflow.create_nexus_client(service=SERVICE, endpoint=endpoint)
    return cast(
        BatchResultRef[T],
        await client.execute_operation(
            BULK_OPERATION,
            BulkSubmission(run_id, submission_id, requests),
            output_type=BatchResultRef,
            schedule_to_close_timeout=operation_timeout,
            cancellation_type=workflow.NexusOperationCancellationType.WAIT_REQUESTED,
        ),
    )


@dataclass
class BulkPage(Generic[T]):
    items: list[BulkItem[T]]
    next_offset: int | None


def _typed(item: BulkItem, output_type: type[T] | None) -> BulkItem[T]:
    if output_type is None or item.status != "succeeded":
        return cast(BulkItem[T], item)
    if not isinstance(output_type, type) or not issubclass(output_type, BaseModel):
        raise TypeError("output_type must be a Pydantic BaseModel class")
    try:
        output = output_type.model_validate(item.output)
    except ValidationError as exc:
        # Preserve the raw response and parsed JSON. Python-only validators can
        # add failures beyond service JSON Schema validation; they do not mutate
        # the persisted result set or its service aggregate counts.
        details = str(exc.errors(include_url=False, include_input=False))[:1024]
        return replace(item, status="parse_failed", parse_error="local validation: " + details)
    return replace(item, output=output)


async def load_page(
    store: BulkStore,
    ref: BatchResultRef[T],
    *,
    offset: int = 0,
    limit: int = 100,
    output_type: type[T] | None = None,
) -> BulkPage[T]:
    """Load a byte- and item-bounded page, validating locally when requested.

    The database may shorten a page to its byte limit. Continue with the returned
    offset rather than treating a page shorter than ``limit`` as the end.
    """
    _in_activity()
    if not 0 <= offset <= ref.count or not 1 <= limit <= MAX_BULK_PAGE_ITEMS:
        raise ValueError("offset or page limit is out of range")
    if offset == ref.count:
        return BulkPage([], None)
    items = await store.read_results(ref, offset=offset, limit=limit)
    if not items:
        raise RuntimeError("result page made no progress before the end of the result set")
    next_offset = offset + len(items)
    return BulkPage(
        [_typed(item, output_type) for item in items],
        next_offset if next_offset < ref.count else None,
    )


async def iter_results(
    store: BulkStore,
    ref: BatchResultRef[T],
    *,
    output_type: type[T] | None = None,
    page_size: int = 100,
) -> AsyncIterator[BulkItem[T]]:
    """Iterate results in input order, with bulk I/O confined to the Activity."""
    _in_activity()
    offset = 0
    while offset < ref.count:
        page = await load_page(store, ref, offset=offset, limit=page_size, output_type=output_type)
        for item in page.items:
            yield item
        if page.next_offset is None:
            break
        offset = page.next_offset


async def unfinished_requests(
    store: BulkStore,
    ref: BatchResultRef,
    *,
    statuses: tuple[str, ...] = ("expired", "cancelled"),
    output_type: type[T] | None = None,
) -> BatchInputRef[T] | None:
    """Write a manifest of selected unfinished items; return None if none exist.

    Submitting the returned manifest is an explicit new operation. This helper
    preserves the original portable schema unless ``output_type`` replaces it.
    It does not retry paid work automatically or alter the original outcome set.
    """
    _in_activity()

    async def selected():
        async for item in iter_results(store, ref):
            if item.status in statuses:
                yield BulkRequest(item.custom_id, item.request)

    iterator = selected()
    first = await anext(iterator, None)
    if first is None:
        return None

    async def with_first():
        yield first
        async for item in iterator:
            yield item

    schema = (
        _output_schema(output_type) if output_type is not None else await store.input_schema(ref.manifest_id)
    )
    return cast(BatchInputRef[T], await store.write_input(with_first(), schema=schema))


__all__ = [
    "BatchCounts",
    "BatchInputRef",
    "BatchResultRef",
    "BulkItem",
    "BulkPage",
    "BulkRequest",
    "iter_results",
    "load_page",
    "responses",
    "unfinished_requests",
    "write_requests",
]
