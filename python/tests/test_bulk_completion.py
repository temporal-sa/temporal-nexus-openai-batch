"""Exact-target completion and lost-ack recovery for bulk result references."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from temporalio.client import ActivityExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from nexus_batch.activities import Activities, CompletionValue
from nexus_batch.bulk_models import BatchCounts, BatchResultRef
from nexus_batch.models import CompletionTarget


def bulk_result():
    return BatchResultRef("submission", "manifest", "d" * 64, 2, BatchCounts(total=2, succeeded=2))


def completion_rig(*descriptions):
    target = CompletionTarget("test", "activity", "execution", b"exact-attempt-token", 1)
    handle = SimpleNamespace(describe=AsyncMock(side_effect=list(descriptions)))
    async_handle = SimpleNamespace(complete=AsyncMock(), fail=AsyncMock())
    client = SimpleNamespace(
        namespace="test",
        get_activity_handle=Mock(return_value=handle),
        get_async_activity_handle=Mock(return_value=async_handle),
    )
    return Activities(client, None, None), target, handle, async_handle


def description(status, *, attempt=1, result=None):
    return SimpleNamespace(
        activity_id="activity",
        activity_run_id="execution",
        status=status,
        attempt=attempt,
        result=AsyncMock(return_value=result),
    )


async def test_bulk_completion_uses_exact_target_and_reference():
    result = bulk_result()
    activities, target, _, async_handle = completion_rig(description(ActivityExecutionStatus.RUNNING))
    delivery = await activities.complete(target, CompletionValue(result), BatchResultRef)
    assert delivery.delivered
    activities.client.get_activity_handle.assert_called_once_with("activity", run_id="execution")
    activities.client.get_async_activity_handle.assert_called_once_with(task_token=b"exact-attempt-token")
    async_handle.complete.assert_awaited_once_with(result)
    async_handle.fail.assert_not_awaited()


async def test_bulk_completion_recovers_accepted_result_after_lost_acknowledgement():
    result = bulk_result()
    closed = description(ActivityExecutionStatus.COMPLETED, result=result)
    activities, target, handle, async_handle = completion_rig(
        description(ActivityExecutionStatus.RUNNING),
        closed,
    )
    # Temporal accepted completion but the response was lost. Retrying completion
    # reports NOT_FOUND; describe must decode the closed value as a bulk ref.
    async_handle.complete.side_effect = RPCError("completion already accepted", RPCStatusCode.NOT_FOUND, b"")
    delivery = await activities.complete(target, CompletionValue(result), BatchResultRef)
    assert delivery.delivered and handle.describe.await_count == 2
    closed.result.assert_awaited_once_with(BatchResultRef)


async def test_already_closed_bulk_result_is_decoded_with_explicit_result_type():
    result = bulk_result()
    closed = description(ActivityExecutionStatus.COMPLETED, result=result)
    delivery = await Activities._reconcile_completion(closed, CompletionValue(result), BatchResultRef)
    assert delivery.delivered
    closed.result.assert_awaited_once_with(BatchResultRef)


async def test_stale_running_attempt_cannot_complete_or_decode_new_attempt():
    newer = description(ActivityExecutionStatus.RUNNING, attempt=2)
    activities, target, _, async_handle = completion_rig(newer)
    delivery = await activities.complete(target, CompletionValue(bulk_result()), BatchResultRef)
    assert not delivery.delivered and "superseding attempt" in delivery.diagnostic
    newer.result.assert_not_awaited()
    activities.client.get_async_activity_handle.assert_not_called()
    async_handle.complete.assert_not_awaited()


@pytest.mark.parametrize("invalid_decode", [False, True])
async def test_different_closed_bulk_outcome_does_not_count_as_our_delivery(invalid_decode):
    expected = bulk_result()
    closed = description(ActivityExecutionStatus.COMPLETED, result=replace(expected, digest="e" * 64))
    if invalid_decode:
        closed.result.side_effect = TypeError("unrelated result cannot decode as BatchResultRef")
    activities, target, _, async_handle = completion_rig(closed)
    delivery = await activities.complete(target, CompletionValue(expected), BatchResultRef)
    assert not delivery.delivered and "without this outcome" in delivery.diagnostic
    closed.result.assert_awaited_once_with(BatchResultRef)
    async_handle.complete.assert_not_awaited()


async def test_transient_bulk_completion_error_leaves_delivery_retryable():
    activities, target, _, async_handle = completion_rig(description(ActivityExecutionStatus.RUNNING))
    async_handle.complete.side_effect = RPCError("temporarily unavailable", RPCStatusCode.UNAVAILABLE, b"")
    with pytest.raises(RPCError):
        await activities.complete(target, CompletionValue(bulk_result()), BatchResultRef)
