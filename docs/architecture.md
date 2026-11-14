# Architecture and code reading guide

The prototype turns independent Nexus requests into shared OpenAI batches. PostgreSQL holds the growing collections of requests and results. Temporal persists each caller's wait, the Nexus-backed Standalone Activity and the batch's control flow. The [system diagram](../README.md#how-batching-works) shows how these parts connect.

Go and Python implement the same single-request design. They run independently, with separate queues, Nexus endpoints and PostgreSQL databases. They share local development services and sample input. Python additionally supports bulk submissions through the `responses` Nexus operation.

## Typed bulk submissions in Python

The bulk interface submits one immutable manifest through one Nexus operation and one backing Standalone Activity. Application Activities write manifests and load results; application Workflows exchange references and counts. The [specification](combined-implementation-spec.md) describes the requirements.

`BulkStore.write_input` stores ordered requests and a portable output schema in PostgreSQL. It seals the manifest with a content digest and rejects duplicate custom IDs. The Nexus input contains only run/submission identity and the manifest ID, digest and count. Submission IDs are unique within a Temporal namespace; use a run-derived or globally unique ID for each stage.

The admission Activity validates every stored request, its digest and encoded provider line before admitting any work. A recipient ledger records its exact Activity attempt, original absolute deadline and admission cursor. Admission pages join the existing run's collection, so a provider batch can contain members of several bulk submissions alongside ordinary callers. Bulk membership rows have no per-item completion token. Their stable internal provider IDs keep identical custom IDs in separate submissions isolated.

The existing `BatchWorkflow` remains the provider orchestration engine. Its results Activity stores bulk raw responses, parsed JSON and explicit parse errors alongside ordinary normalized results. Bulk outcomes stay external. Once admission is complete and every item has an outcome, the keeper creates a stable result-set reference and completes the bulk recipient's exact Activity attempt. It reconciles lost completion acknowledgements with that typed reference.

Batch delivery counters continue to describe ordinary per-request Activity completions. Bulk members use the `stored` delivery state; their recipient's completion checkpoint lives in `nexus_batch_bulk_submissions`. A batch's request count therefore need not equal its ordinary delivery count. A retried results Activity returns committed outcome summaries before accessing provider files again.

Partial snapshots record the exact outcome positions visible when the snapshot was created. Later results do not change an earlier reference. Admission retries preserve their cursor and original deadline. Cancelled or expired admission releases unused capacity reservations; cancelling a recipient leaves shared provider work running. A partial result reference remains available through `BulkStore.get_result(submission_id, partial=True)` in an Activity or administrative client.

The service validates portable JSON Schema without importing caller classes or fetching remote schema references. Consumer Activities may apply a Pydantic model with additional Python validators. Those local failures preserve raw data and do not change the service's persisted aggregate counts.

Bulk control messages retain the existing 8 KiB envelope bound. Each stored input item and raw provider response is limited to 1 MiB; schemas to 64 KiB; manifests to 100,000 items or 256 MiB; pages to 1,000 items and a 4 MiB byte budget. Manifests can span multiple provider batches. Upload/result Activities still materialize bounded provider files in memory. This is explicit PostgreSQL artifact storage, and producers, service workers and consumers need access to that namespace-scoped database. Retention and provider-file deletion remain manual; no files are deleted before durable outcome storage.

## Follow one request

1. **Open a run and invoke Nexus.** The CLI saves a run's configuration in PostgreSQL, then starts a `CallerWorkflow` for each document. Each caller invokes `llm-batch.respond` and waits durably for its result. In a larger application, an existing business Workflow can make that Nexus call directly; the example caller is a minimal consumer.

2. **Start a Standalone Activity and enroll.** The Nexus handler reads the run's configuration and verifies the endpoint. It starts `SubmitRequest` in Go or `submit_request` in Python as the operation's backing Standalone Activity. Enrollment records the document, fingerprint, deadline and exact Activity target in PostgreSQL. The target includes the namespace, Activity ID, Activity run ID, attempt number and task token. Only after that transaction commits does the Activity return `activity.ErrResultPending` or raise `activity.raise_complete_async()`. Its worker slot becomes available while Temporal still considers the Activity pending.

3. **Seal and dispatch a batch.** Enrollment serializes changes to a run by locking its database row. It counts the encoded JSONL bytes, enforces limits and seals batches when needed. The first admitted request fixes the collection deadline; later requests do not reset it. The dispatcher also seals batches whose collection window has elapsed. It claims sealed batches through a leased database outbox and starts a `BatchWorkflow` with a stable ID. If the process loses the start acknowledgement, retrying the same ID finds the existing execution rather than admitting a new batch execution.

4. **Upload, submit and poll.** The batch Workflow passes a small `BatchRef` to Activities. The upload Activity loads the manifest from PostgreSQL and builds a bounded JSONL file in memory. The create Activity posts its uploaded file ID to `/v1/batches` with `endpoint: "/v1/responses"`. A single polling Activity waits for a terminal provider state and heartbeats to Temporal. Polling iterations do not each add Workflow events. Activities perform database and HTTP I/O; Workflow code coordinates them using Temporal's recorded results and Workflow clock.

5. **Store outcomes before delivery.** The results Activity downloads the provider's output and error files. Reconciliation uses `custom_id` to match each line to a manifest member; file order carries no meaning. Missing, duplicate, unknown and malformed lines receive explicit handling. A success or typed failure is recorded for every manifest member before external completion starts. Workflow code receives an aggregate summary, while individual outcomes remain in PostgreSQL.

6. **Complete the original Activity.** Delivery reads pending outcomes in pages. It describes the recorded Activity execution and completes or fails it with its stored attempt token. Temporal then completes the associated Nexus operation and resumes the caller. PostgreSQL records a delivery checkpoint for that exact target. A background keeper heartbeats pending Activities and repairs delivery to newly attached attempts even after the batch Workflow has closed.

## Where state belongs

| State | Owner | Reason |
| --- | --- | --- |
| One caller's request, Nexus wait and returned result | Caller Workflow history | Each caller can resume its own business operation. |
| Activity execution, timeout, attempts and Nexus completion callback | Temporal service | The asynchronous operation survives the enrollment worker releasing its slot. |
| Documents, batch membership, exact targets, outcomes and delivery checkpoints | PostgreSQL | Batch size does not turn into a growing Workflow payload or per-request event loop. |
| Batch reference, provider IDs and aggregate reports | Batch Workflow history | Replay reconstructs orchestration without loading a batch's contents. |
| Input, output and error files; provider job status | OpenAI, or the persistent fake provider | The provider processes the submitted requests independently of workers. |

Keeping a manifest outside Workflow history does not eliminate memory limits. Upload and result Activities materialize bounded files, and long polling consumes Activity worker capacity. The code enforces request, result, count and encoded-byte bounds; production throughput still needs separate measurement.

## Recovery rules worth reading closely

**Batch creation has one Activity attempt and one HTTP attempt.** A request accepted by OpenAI can lose its response before the worker receives a job ID. Retrying creation could submit paid work twice. The Workflow instead searches by run/batch metadata and uploaded file ID. Exactly one match recovers the job. Zero or multiple matches produce `SUBMISSION_UNKNOWN` without a second creation call. This restriction does not promise exactly-once billing.

**Enrollment and completion refer to an exact attempt.** A worker can disappear before enrollment, after commit or while results are pending. Heartbeat timeouts let Temporal retry the Standalone Activity. A newer attempt of the same Activity execution can replace its completion target while retaining batch membership and any stored outcome. A separate Nexus operation with the same logical request ID is rejected. Both completion and its database checkpoint are fenced by the recorded target so stale work cannot complete or mark delivery for a newer attempt.

**A lost completion acknowledgement is recoverable.** Temporal may accept a completion before the worker loses its connection. Retrying the old token can then return an error because the Activity is already closed. Delivery inspects that exact execution and compares its terminal result or failure with the stored outcome before counting it as delivered.

**Deadlines remain absolute.** A restart does not grant a fresh provider budget. The earliest recipient deadline caps provider work and reserves time for completion. Outcome storage and final bookkeeping can continue retrying after a recipient expires; an expired recipient cannot receive a result, but the batch still needs a recorded resolution.

**The submission fence orders cleanup against creation.** The store holds the run lock while checking whether submission is still permitted and calling the provider. Cleanup uses the same lock to stop the run. Work accepted by the provider before cleanup may keep running. PostgreSQL and OpenAI do not share a transaction.

## Suggested reading order

| Topic | Go | Python |
| --- | --- | --- |
| Payloads, limits, deadlines and `BatchRef` | [model.go](../go/internal/model/model.go) | [models.py](../python/nexus_batch/models.py) |
| Caller and batch orchestration | [service.go](../go/internal/orchestration/service.go), [batch.go](../go/internal/orchestration/batch.go) | [workflows.py](../python/nexus_batch/workflows.py) |
| Nexus starts the backing Standalone Activity | [service.go](../go/internal/orchestration/service.go) | [nexus_service.py](../python/nexus_batch/nexus_service.py) |
| Enrollment handoff and exact-token completion | [async.go](../go/internal/orchestration/async.go) | [activities.py](../python/nexus_batch/activities.py) |
| Admission, sealing and immutable outcomes | [store.go](../go/internal/store/store.go), [batches.go](../go/internal/store/batches.go), [deliveries.go](../go/internal/store/deliveries.go) | [store.py](../python/nexus_batch/store.py) |
| Submission fence and attempt reattachment | [recovery.go](../go/internal/store/recovery.go), [store.go](../go/internal/store/store.go) | [store.py](../python/nexus_batch/store.py) |
| Dispatch, keeper and delivery outbox | [repository.go](../go/internal/orchestration/repository.go) | [runtime.py](../python/nexus_batch/runtime.py) |
| Provider polling and result reconciliation | [poll.go](../go/internal/orchestration/poll.go), [openai.go](../go/internal/provider/openai.go), [reconcile.go](../go/internal/provider/reconcile.go) | [activities.py](../python/nexus_batch/activities.py), [provider.py](../python/nexus_batch/provider.py) |
| Worker pools, CLI commands and local configuration | [main.go](../go/cmd/batchctl/main.go) | [cli.py](../python/nexus_batch/cli.py) |

The source comments explain these boundaries where they affect a transaction, timeout or retry decision. The [testing guide](testing.md) maps them to unit, database, Nexus and recovery checks.
