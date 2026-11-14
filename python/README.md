# Python application

This copy follows the same PostgreSQL → batch Workflow → external Activity completion flow as the Go application. It uses Python 3.11+, Temporal Python SDK 1.33.0, asyncpg and HTTPX. Nexus invokes a real Standalone Activity, which records its exact attempt token before calling `activity.raise_complete_async()`.

Start with the [system diagram](../README.md#how-batching-works) and [architecture reading guide](../docs/architecture.md) for the request lifecycle, recovery rules and matching Go/Python source files.

## Run locally

Start PostgreSQL and Temporal from the repository root:

```sh
make postgres          # or: make postgres-docker
make server            # includes activity.enableCallbacks=true
```

Install the Python application:

```sh
uv sync --project python --extra test
test -f .env || cp .env.example .env
chmod 600 .env
```

Fill in `OPENAI_API_KEY` and `OPENAI_MODEL` in the repository-root `.env`. Choose a model available to your account that supports Responses and Batch. The provided file sets `NEXUS_BATCH_MODE=live`, which selects the real OpenAI provider at `https://api.openai.com/v1`.

```sh
make python-init
make python-setup
make python-worker
```

`python-init` creates the development database if needed and initializes its schema. A custom PostgreSQL account needs permission to create the database, or an administrator can create it first.

In another terminal, from the repository root:

```sh
make python-demo
python/.venv/bin/batchctl-python demo --run-id python-sample-001 --input samples/documents.jsonl
```

Each run ID must be new and unique across both applications in the Temporal namespace. `make python-demo` submits the three supplied documents through the selected provider. Durations in configuration, CLI flags and reports use **seconds**. A short `--wait` stops waiting without canceling durable work. Use `inspect` to see later progress.

```sh
python/.venv/bin/batchctl-python inspect --run-id python-sample-001
python/.venv/bin/batchctl-python finish --run-id python-sample-001
python/.venv/bin/batchctl-python cancel --run-id python-sample-001 --request-id library
python/.venv/bin/batchctl-python cleanup --run-id python-sample-001
```

The CLI loads `.env` before reading its mode, model, Temporal connection and database settings. Exported environment values take precedence over the file; explicit CLI flags take precedence over both. `.env` is ignored by Git.

The default database is `nexus_batch_python`, configured with `PYTHON_DATABASE_URL`. Fake mode uses Nexus endpoint `llm-batch-python`; caller, enrollment and batch queues have the `nexus-batch-python-` prefix. Live mode adds `-live`, so the supplied `.env` selects `llm-batch-python-live`. Go and Python use independent databases and queues, so both can run on the same Temporal server.

## Structure

| File | Purpose |
| --- | --- |
| `models.py` | Typed payloads, validation, JSONL and serialization |
| `store.py`, `schema.sql` | Transactions, leases, outcomes and delivery checkpoints |
| `provider.py` | OpenAI HTTP adapter, persistent fake provider, result reconciliation |
| `activities.py` | Enrollment, provider calls and exact-token completion |
| `workflows.py` | Caller and batch control flow |
| `nexus_service.py` | Nexus operation backed by a Standalone Activity |
| `runtime.py` | Worker pools, dispatch and completion keeper |
| `cli.py` | Local setup, demo, inspection and cleanup |

Creation has one Activity attempt and one HTTP attempt. Ambiguous submissions are reconciled by metadata and uploaded file ID. Polling stays within one heartbeat-enabled Activity. First-committed outcomes and exact-target checkpoints make delivery retryable after a restart or lost acknowledgement.

## Fake and live providers

Live mode uses the key and model from `.env`. A missing key stops a live worker before it connects to PostgreSQL or Temporal; a missing model stops a live demo before it starts any Workflows. The key is read locally and is excluded from Workflow payloads, PostgreSQL records and reports.

The fake provider persists jobs and files under `.state/python-fake` and shares them safely between processes. To run without OpenAI, explicitly select fake mode:

```sh
make python-setup PYTHON_MODE=fake
make python-worker PYTHON_MODE=fake
# Another terminal:
make python-demo PYTHON_MODE=fake
```

Stop the normal fake worker before running another scenario on its queues:

```sh
python/.venv/bin/batchctl-python worker --mode fake --scenario mixed --state-dir .state/python-mixed
```

Override the configured mode for a single CLI command with `--mode`, or for a Make target with `PYTHON_MODE`. Both the worker and its demo must select the same mode.

Live mode creates jobs through `/v1/batches` with `endpoint: "/v1/responses"`. Chat Completions support is on hold. The bounds and recovery semantics described in the [repository README](../README.md) apply to both applications.

Standalone Activities are generally available in Python SDK 1.33.0; using them as Nexus operations remains experimental. [SDK release notes](https://github.com/temporalio/sdk-python/releases/tag/1.33.0)

See [testing instructions](../docs/testing.md) for unit, PostgreSQL and real Temporal tests. Throughput needs to be measured for this implementation independently.

## Typed bulk pipelines

The Python service also exposes `llm-batch.responses`. One bulk submission starts
one Nexus operation and one backing Standalone Activity. Its items join the same
run's count, byte, and collection-window batching as ordinary `respond` callers.
Submitting 1,000 items does not start 1,000 Nexus operations. The new bulk API is
Python-only; the existing Go and Python single-request APIs keep their contracts.

An application Activity writes an immutable manifest with explicit custom IDs:

```python
from pydantic import BaseModel
from nexus_batch.bulk import BulkRequest, write_requests
from nexus_batch.bulk_store import BulkStore


class Sentiment(BaseModel):
    label: str


# Inside an Activity; store is the service's namespace-scoped Store.
requests = await write_requests(
    BulkStore(store),
    (BulkRequest(str(row.id), {"input": row.text}) for row in rows),
    output_type=Sentiment,
)
```

The writer accepts synchronous and asynchronous iterables. It stores the strict
JSON Schema alongside the input and returns only an ID, digest, and count. An
empty iterable or duplicate custom ID is invalid. The run selects the model;
individual request bodies can preserve Responses fields such as `instructions`,
structured `input`, and output options. A supplied model must match the run and
streaming requests are rejected.

The application Workflow passes that reference through one operation:

```python
from datetime import timedelta
from nexus_batch.bulk import responses

results = await responses(
    run_id="reviews-2026-11-14",
    submission_id="reviews-2026-11-14-classify",
    requests=requests,
    endpoint="llm-batch-python",
    operation_timeout=timedelta(hours=26),
)
```

Create the run and its immutable configuration before starting the Workflow, as
in the existing CLI. Submission IDs are unique within the database's Temporal
namespace. Separate submissions may reuse item custom IDs; the service assigns
distinct provider identities. Retries of an operation reattach to its original
membership and deadline rather than admitting another copy.

Consumer Activities load pages or iterate without putting results into Workflow
history:

```python
from nexus_batch.bulk import iter_results

async for item in iter_results(BulkStore(store), results, output_type=Sentiment):
    if item.ok:
        await db.upsert_sentiment(item.custom_id, item.output.label)
    else:
        # item.request, item.response, item.error, and item.parse_error preserve
        # material for inspection or a deliberate repair operation.
        await db.record_failure(item.custom_id, item.status, item.parse_error)
```

The service validates portable JSON Schema, without importing application
classes. Consumer Activities apply the supplied Pydantic model, including custom
Python validators. A local validation failure has `status="parse_failed"` and a
`parse_error` beginning with `local validation:`; it preserves the raw material
and does not change service aggregate counts. Omitting `output_type` when writing
and loading selects raw mode. `unfinished_requests(store, results)` can write a
new manifest of expired or cancelled items and returns `None` when none exist;
it preserves the original portable schema unless an explicit `output_type`
replaces it. Submitting that manifest is an explicit new operation.

`load_page(store, results, offset=0, limit=100)` returns items plus `next_offset`.
Continue with that offset: the byte bound can shorten a page before its item
limit. Request items are limited to 1 MiB, stored outcomes to 3 MiB, schema
documents to 64 KiB, and pages to 1,000 items and 4 MiB. References have constant
size as item count grows. One input manifest is limited to 100,000 items and
256 MiB. Bounded Workflow payloads do not eliminate Activity
memory use: provider Activities still materialize a configured, bounded provider
batch. All producers, service workers, and consumers need access to the same
namespace-scoped PostgreSQL store. Retention and provider-file cleanup remain
operator responsibilities.

Cancellation detaches that recipient and leaves other recipients' shared
provider work intact. Persisted partial outcomes remain available through
`await BulkStore(store).get_result(submission_id, partial=True)` in an Activity;
a cancelled Nexus operation does not return a normal result reference.

### Two-stage example

With PostgreSQL and Temporal running, stop other Python fake workers on the same
queues and run:

```sh
uv run --project python python -m nexus_batch.examples.review_replies.run --count 12
```

This command hosts the service and application workers, uses the persistent fake
provider, and creates a new run automatically. It classifies the reviews, selects
negative reviews in an Activity, and submits a second bulk manifest to draft
replies. Its final JSON contains aggregate counts and result references; stored
requests and replies remain in PostgreSQL. No OpenAI key is required, even when
the repository `.env` selects live mode. Pass `--count 2000` for a larger run or
`--run-id` to choose a new explicit identity.
