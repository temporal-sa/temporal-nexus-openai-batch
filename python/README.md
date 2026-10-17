# Python application

This copy follows the same PostgreSQL → batch Workflow → external Activity completion flow as the Go application. It uses Python 3.11+, Temporal Python SDK 1.33.0, asyncpg and HTTPX. Nexus invokes a real Standalone Activity, which records its exact attempt token before calling `activity.raise_complete_async()`.

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

Live mode uses the Responses Batch endpoint. Chat Completions support is on hold. The bounds and recovery semantics described in the [repository README](../README.md) apply to both applications.

Standalone Activities are generally available in Python SDK 1.33.0; using them as Nexus operations remains experimental. [SDK release notes](https://github.com/temporalio/sdk-python/releases/tag/1.33.0)

See [testing instructions](../docs/testing.md) for unit, PostgreSQL and real Temporal tests. Throughput needs to be measured for this implementation independently.
