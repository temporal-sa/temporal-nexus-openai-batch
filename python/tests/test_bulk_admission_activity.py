"""Admission retries survive worker cancellation without releasing ownership."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from nexus_batch.bulk_activities import BulkActivities
from nexus_batch.bulk_models import BatchInputRef, BulkSAAInput, BulkSubmission
from nexus_batch.models import CompletionTarget, Config, Failure


def admission_rig():
    env = ActivityEnvironment()
    now = datetime.now(timezone.utc)
    env.info = replace(
        env.info,
        namespace="test",
        activity_id="bulk-activity",
        activity_run_id="execution",
        task_token=b"attempt-one-token",
        started_time=now,
        scheduled_time=now,
        start_to_close_timeout=timedelta(seconds=120),
        schedule_to_close_timeout=timedelta(seconds=120),
    )
    activities = BulkActivities(None, SimpleNamespace(namespace="test"), None)
    repository = SimpleNamespace(
        attach=AsyncMock(),
        admit_page=AsyncMock(side_effect=asyncio.CancelledError),
        abort_admission=AsyncMock(),
    )
    activities.store = repository
    input = BulkSAAInput(
        BulkSubmission("run", "submission", BatchInputRef("manifest", "a" * 64, 2)), Config()
    )
    target = CompletionTarget("test", "bulk-activity", "execution", b"attempt-one-token", 1)
    return env, activities, repository, input, target


@pytest.mark.parametrize(
    "reason",
    ["worker_shutdown", "timed_out", "paused", "reset", "not_found", None],
)
async def test_infrastructure_cancellation_keeps_admission_retryable(monkeypatch, reason):
    env, activities, repository, input, _ = admission_rig()
    details = activity.ActivityCancellationDetails(**{reason: True}) if reason else None
    monkeypatch.setattr(activity, "cancellation_details", lambda: details)
    with pytest.raises(asyncio.CancelledError):
        await env.run(activities.submit_bulk, input)
    repository.attach.assert_awaited_once()
    repository.admit_page.assert_awaited_once_with("submission")
    repository.abort_admission.assert_not_awaited()


async def test_actual_recipient_cancellation_aborts_exact_admission_target(monkeypatch):
    env, activities, repository, input, target = admission_rig()
    monkeypatch.setattr(
        activity,
        "cancellation_details",
        lambda: activity.ActivityCancellationDetails(cancel_requested=True),
    )
    with pytest.raises(asyncio.CancelledError):
        await env.run(activities.submit_bulk, input)
    repository.abort_admission.assert_awaited_once_with("submission", target, "recipient cancelled")


async def test_permanent_admission_failure_releases_exact_target_and_propagates_code():
    env, activities, repository, input, target = admission_rig()
    repository.admit_page.side_effect = Failure("INVALID_INPUT", "manifest changed")
    with pytest.raises(ApplicationError) as error:
        await env.run(activities.submit_bulk, input)
    assert error.value.type == "INVALID_INPUT" and error.value.non_retryable
    repository.abort_admission.assert_awaited_once_with("submission", target, "INVALID_INPUT")
