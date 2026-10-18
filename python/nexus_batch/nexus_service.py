"""The public Nexus operation is backed by a real Standalone Activity."""

import hashlib
from datetime import timedelta

import nexusrpc
from nexusrpc import handler
from temporalio import nexus
from temporalio.common import ActivityIDReusePolicy, RetryPolicy

from .activities import Activities
from .models import (
    ENROLLMENT_HEARTBEAT,
    OPERATION,
    SERVICE,
    Failure,
    Request,
    Result,
    SAAInput,
    validate_request,
)
from .store import Store


@handler.service_handler(name=SERVICE)
class BatchService:
    def __init__(self, store: Store, endpoint: str):
        self.store, self.endpoint = store, endpoint

    @nexus.temporal_operation(name=OPERATION)
    async def respond(
        self, ctx: nexus.TemporalStartOperationContext, client: nexus.TemporalNexusClient, input: Request
    ) -> nexus.TemporalOperationResult[Result]:
        try:
            validate_request(input)
            run = await self.store.get_run(input.run_id)
            run.config.validate()
            if run.config.nexus_endpoint != self.endpoint or nexus.info().endpoint not in (
                "",
                run.config.nexus_endpoint,
            ):
                raise Failure("INVALID_INPUT", "batching run belongs to another Nexus endpoint")
        except (Failure, ValueError) as exc:
            raise nexusrpc.HandlerError(str(exc), type=nexusrpc.HandlerErrorType.BAD_REQUEST) from exc
        except Exception as exc:
            raise nexusrpc.HandlerError(
                "batching run configuration temporarily unavailable",
                type=nexusrpc.HandlerErrorType.UNAVAILABLE,
                retryable_override=True,
            ) from exc
        identity = hashlib.sha256(
            f"{input.run_id}\0{input.request_id}\0{ctx.request_id}".encode()
        ).hexdigest()
        return await client.start_activity(
            Activities.submit_request,
            SAAInput(input, run.config),
            id=f"request-activity/{input.run_id}/{identity}",
            schedule_to_close_timeout=timedelta(seconds=run.config.operation_timeout),
            start_to_close_timeout=timedelta(seconds=run.config.operation_timeout),
            heartbeat_timeout=timedelta(seconds=ENROLLMENT_HEARTBEAT),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=10)
            ),
            id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
        )
