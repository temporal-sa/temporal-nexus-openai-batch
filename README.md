# Temporal Nexus OpenAI Batch prototype

Independent Temporal Workflows call `llm-batch.respond`, which starts a **Standalone Activity through Nexus**. That Activity enrolls the request in **Postgres**, records its exact attempt-scoped completion token, and returns `activity.ErrResultPending`. A dispatcher starts one Workflow per sealed batch. Its Activities load request data from Postgres, submit and poll OpenAI Batch jobs, store the results, and complete the original SAAs externally. Only OpenAI is simulated in fake mode.

Postgres is the source of truth for enrollment and batch data. There is no coordinator Workflow or per-request Workflow Update. Batch Workflow inputs contain a small `BatchRef`; documents, completion tokens, batch manifests, and individual results never enter batch Workflow history. Each caller still has its own request and result in its own history.

## Data and recovery

| Data | Owner |
| --- | --- |
| Run configuration and admission state | Postgres |
| Requests, fingerprints, exact SAA targets and tokens | Postgres |
| Batch membership, encoded byte counts, collection deadlines | Postgres |
| Sealed-batch dispatch outbox | Postgres |
| Per-item outcomes and delivery checkpoints | Postgres |
| Provider IDs, timers and small aggregate batch reports | Batch Workflow history, with reports also stored in Postgres |

Enrollment uses one database transaction to validate ownership, enforce an optional admission limit, append the request, and seal a batch at its count or byte limit. A retry with the same fingerprint and exact Activity attempt target returns its original assignment. Another operation using the same logical request ID is rejected. Transactions lock the run row before its open batch row, so concurrent enrollment, timer sealing and admission closure use one lock order.

SAAs use a 30-second heartbeat timeout so a lost worker can retry before enrollment commits. After enrollment, an independent keeper heartbeats pending targets from Postgres. A strictly newer attempt of the same SAA execution can reattach to its existing request, membership and result. Older attempts remain fenced, including at the delivery checkpoint. The keeper also delivers stored outcomes to late reattachments after their batch Workflow closes.

Dispatchers poll the indexed Postgres outbox by namespace, mode and Nexus endpoint. Short database leases let multiple workers claim work. Each batch has a stable Workflow ID with duplicate execution rejected. If a dispatcher crashes after starting Temporal work but before recording its acknowledgment, retrying the same ID cannot create another execution.

Results are committed before delivery. One heartbeat-enabled delivery Activity pages through pending rows and checkpoints completed recipients in Postgres. If a completion acknowledgment or database checkpoint is lost, retry reconciles the exact closed SAA outcome. Completion always uses the original task token and never falls back to completing a newer attempt by ID.

Uploads and provider batch creation are separate persisted Activities. Creation has **one attempt**, including at the HTTP layer. An uncertain response is reconciled using metadata and the uploaded file ID. Zero or multiple matches produce `SUBMISSION_UNKNOWN`; no replacement job is submitted automatically. The prototype does not promise exactly-once provider execution or billing.

Provider creation holds the run's database lock through its bounded HTTP call. A stop waits for an already-started submission and prevents subsequent creation calls for that run. An upstream job already accepted by OpenAI can continue after cleanup.

The provider deadline is capped by the earliest stored SAA deadline, retaining the configured delivery reserve. Overdue sealing does not restart the full provider budget. Polling runs inside one heartbeat-enabled Activity, so a day-long job does not add thousands of timer and Activity events to Workflow history. Result persistence, delivery and final reports continue retrying after recipient expiry so database recovery can still produce terminal bookkeeping.

## Run locally

The verified stack is Go **1.26.3**, Temporal Go SDK **1.49.0**, pgx **5.11.0**, PostgreSQL **17**, and Temporal CLI **1.9.1** (Server **1.32.0**, UI **2.54.1**). Go 1.26 or later, Python 3, curl, and a SHA-256 utility are needed. Use a Postgres instance plus the local Temporal service.

```sh
git clone https://github.com/temporal-sa/temporal-nexus-openai-batch.git
cd temporal-nexus-openai-batch
make runtime
```

For a native local Postgres cluster, install PostgreSQL 17 first (`brew install postgresql@17` on macOS), then:

```sh
make postgres
```

The script owns only this repository's `.state/postgres-native` cluster and binds it to loopback port **54329**. Alternatively, with a working Docker daemon:

```sh
make postgres-docker
```

Docker Compose uses PostgreSQL 17 with a persistent named volume and the same loopback port. Run one of these database options at a time.

The default development connection is:

```text
postgres://nexus_batch:nexus_batch@127.0.0.1:54329/nexus_batch?sslmode=disable
```

Set `DATABASE_URL` in the environment to use another Postgres instance. Workers and CLI commands must use the same database and Temporal namespace. Use a separate database for each Temporal cluster. The connection string is never passed to a Workflow. `.env.example` lists configuration; the program does not load `.env` automatically. Schema migrations run automatically when a store connection opens, serialized by a database advisory lock. `make migrate` applies them explicitly.

Use three terminals:

```sh
# Terminal 1: persistent local Temporal server
make server
```

```sh
# Terminal 2: build, register the endpoint, start workers and dispatcher
make build
make setup
make worker
```

```sh
# Terminal 3: 100 synthetic documents
make demo

# Or the supplied three-document input
bin/batchctl demo --run-id sample-001 --input samples/documents.jsonl

# A larger run, without the former 1,000-admission Workflow bound
bin/batchctl demo --run-id large-001 --count 2000 --batch-size 1000
```

Each run ID must be new. The CLI writes a JSON report and exits nonzero for failed or unconfirmed outcomes. Durations are nanoseconds. `simulated: true` identifies fake-provider runs; orchestration and database operations are real.

The Temporal service uses **7234**, its [Web UI](http://localhost:8234/namespaces/default/workflows) uses **8234**, and its database is `.state/temporal-saa.db`. `make server` sets `activity.enableCallbacks=true`; no other dynamic-config override is needed. CLI 1.7.2 / Server 1.31.1 lacks the required SAA callback dispatch.

In the UI, find `caller/RUN_ID/document-0001` and follow its Nexus link to the `request-activity/RUN_ID/...` SAA. Shared batch Workflows are named `batch/RUN_ID-SEQUENCE`. No coordinator or per-request handler Workflow is created.

## Inspect and control

```sh
bin/batchctl inspect --run-id sample-001
bin/batchctl finish --run-id sample-001
bin/batchctl cancel --run-id sample-001 --request-id library
bash scripts/cleanup.sh sample-001
```

`inspect` reads aggregate progress from Postgres. `finish` closes admission and seals any partial batch; already accepted work continues. The demo normally finishes its run after its callers return. A short `--wait` limits the launcher only: pending outcomes remain durable, and workers continue processing them.

Caller cancellation reaches the SAA through Nexus. After `ErrResultPending`, no active Activity worker remains to remove the request from a shared batch. The SAA can remain pending until external completion or timeout, and successful completion can still win. Other requests in the same provider job continue.

`cleanup` stops new dispatch for the named Postgres run, cancels its Temporal Workflows, and terminates its remaining SAAs. It retains database records, history, fake-provider state, and provider jobs/files. Stop workers and Temporal with Ctrl-C. `make postgres-stop` stops only the native development cluster; `docker compose down` stops the Compose instance while preserving its named volume.

Cleanup uses Postgres batch/recipient identities and supplements them with Temporal visibility for callers and older resources. A Workflow already claimed by a dispatcher can still start after a stop, but the database submission fence prevents it from creating a new provider job.

Use `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` or CLI connection flags for another local Temporal service. Production authentication and deployment are outside this prototype.

## Fake and live providers

Fake state persists under `.state/fake`. Supported scenarios are `success`, `mixed`, `expired`, `missing`, `duplicate`, `unknown`, `malformed`, `submission-unknown`, and `submission-unreconciled`. Run one scenario per service queue:

```sh
bin/batchctl worker --mode fake --scenario mixed --state-dir .state/fake
bin/batchctl demo --run-id mixed-001 --count 6 --batch-size 6
```

Live mode submits paid OpenAI requests. Export `OPENAI_API_KEY` and an explicitly chosen `OPENAI_MODEL`, then:

```sh
make setup MODE=live
make worker MODE=live

# In another terminal:
bin/batchctl demo --mode live --count 5 --run-id live-001
```

Live jobs use `/v1/responses`, JSONL uploaded with purpose `batch`, and `completion_window: "24h"`. Polling defaults to 30 seconds, the result deadline is 25 hours, and the Nexus operation timeout is 26 hours. Fake and live dispatch are separated by endpoint and mode. [OpenAI Batch guide](https://developers.openai.com/api/docs/guides/batch)

## Bounds and remaining limits

Default batches contain at most **100 requests** or **512 KiB** of encoded JSONL, with a five-second collection window. Configurable upper bounds are **50,000 requests** and **200,000,000 bytes** per batch. Input envelopes remain bounded to 8 KiB and normalized results to 4 KiB. Raw provider files are capped at 200 MB. `--max-admissions 0` means no per-run admission cap; a positive value adds an optional database-enforced cap.

The collection window plus provider result deadline must be less than the operation timeout, leaving time for external completion.

This removes batch-size growth from Workflow payloads and per-item completion events from batch histories. Activities still materialize a bounded provider manifest/file in worker memory, and enrollment for one run takes a short Postgres row lock. Production throughput, connection sizing, retention, and partitioning need measurement; a larger configured limit is not a throughput benchmark.

External completion uses the conventional `ErrResultPending` path. Ordinary Activity timeouts span enrollment and external work; separate first-class phase timeout/retry policies remain unsupported. Heartbeat timeout recovery and transactional reattachment cover worker loss before and after enrollment. Externally reported business failures are nonretryable and do not resubmit a provider job. The keeper adds periodic heartbeat RPCs per pending request; size the service for that load.

```sh
bin/batchctl capabilities
bin/batchctl capabilities --require-handoff  # rejects unsupported phase policies
```

## Deploy and upgrade Workers

New batches use Workflow type `BatchWorkflowV2` on a separate `-batches-v2` task queue. Workers retain the original `BatchWorkflow` implementation on the original queue to replay and drain existing executions. Schema migration v2 is additive; old rows retain an admission-based deadline approximation because their original SAA deadline was not stored.

The CLI isolates enrollment from batch Activities and exposes `--service-concurrency` and `--batch-concurrency` (both default to 32). Set batch concurrency for available memory, provider capacity and the expected number of concurrent polling Activities. Large provider files can occupy several times their encoded size in memory.

For an upgrade, run the schema migration, start the new Workers, and keep the legacy Workflow registration until old executions and required replay retention have drained. Run saved-history replay tests before deployment. Future incompatible Workflow changes need a new type/queue generation or replay-compatible patches; do not edit the frozen legacy implementation.

## Verification

```sh
make test
make test-store         # real Postgres required
make test-integration   # real Postgres and Temporal required

# Abrupt process failure: endpoint/server/Postgres up, other fake workers stopped
python3 scripts/verify-hardkill.py
```

The suites cover SQL concurrency and ownership, sealing, dispatch recovery, result reconciliation, exact-token completion, partial delivery recovery, and batch-history payload bounds. See [VERIFICATION.md](VERIFICATION.md) for observed results and their limits.
