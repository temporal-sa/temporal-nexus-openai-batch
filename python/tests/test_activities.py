from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from temporalio.client import ActivityExecutionStatus
from temporalio.exceptions import ApplicationError

from nexus_batch.activities import Activities, application_failure
from nexus_batch.models import Failure, Outcome, Result, dumps, loads


@pytest.mark.parametrize("failure", [False, True])
async def test_closed_completion_acknowledgement_is_idempotent(failure):
    outcome = Outcome("item", "fp", "batch", result=Result("item", "text"))
    description = SimpleNamespace(
        status=ActivityExecutionStatus.COMPLETED, result=AsyncMock(return_value=outcome.result)
    )
    if failure:
        outcome.result, outcome.failure = None, Failure("PROVIDER_FAILED", "failed", "code")
        description.status = ActivityExecutionStatus.FAILED
        description.outcome_failure = AsyncMock(return_value=application_failure(outcome.failure))
    result = await Activities._reconcile_completion(description, outcome)
    assert result.delivered


@pytest.mark.parametrize("failure", [False, True])
async def test_different_terminal_outcome_cannot_stall_delivery(failure):
    outcome = Outcome("item", "fp", "batch", result=Result("item", "text"))
    description = SimpleNamespace(
        status=ActivityExecutionStatus.COMPLETED,
        result=AsyncMock(side_effect=TypeError("wrong result schema")),
    )
    if failure:
        outcome.result, outcome.failure = None, Failure("PROVIDER_FAILED", "failed")
        description.status = ActivityExecutionStatus.FAILED
        description.outcome_failure = AsyncMock(
            return_value=ApplicationError("failed", 42, type="PROVIDER_FAILED")
        )
    result = await Activities._reconcile_completion(description, outcome)
    assert not result.delivered


def test_failure_details_survive_serialization():
    failure = Failure("PROVIDER_FAILED", "safe error", "provider_code")
    assert loads(Failure, dumps(failure)) == failure
