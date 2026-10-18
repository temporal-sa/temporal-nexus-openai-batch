# Temporal Nexus OpenAI Batch prototype

Independent Temporal Workflows call `llm-batch.respond`. Nexus starts a Standalone Activity, which enrolls the request in PostgreSQL and waits for external completion. A dispatcher starts one Workflow per sealed batch. Its Activities upload a JSONL file to OpenAI, submit and poll the batch, save its results, and complete the original Activities.

This repository contains equivalent Go and Python implementations. Both currently use the **Responses API**. Fake mode simulates OpenAI while using real Temporal and PostgreSQL.

## Repository

| Path | Purpose |
| --- | --- |
| `go/` | Go application, tests and [structure](go/README.md) |
| `python/` | Python application, tests and [run instructions](python/README.md) |
| `scripts/` | Shared PostgreSQL and Temporal development services |
| `samples` | Three example documents |
| `docs/testing.md` | Test commands and coverage |

## How batching works

PostgreSQL owns request bodies, batch membership, fingerprints, exact Activity attempt tokens, outcomes and delivery checkpoints. Batch Workflow histories contain a small reference, provider identifiers and aggregate reports. Each caller retains its own request and result in its history.

Enrollment locks the run row, validates ownership, appends the request and seals batches at their count or encoded-byte limit. The first request starts a fixed collection window; later requests do not extend it. A dispatcher seals overdue batches and drains a leased outbox. Stable Workflow IDs prevent a lost dispatch acknowledgement from starting another batch execution.

An independent keeper heartbeats enrolled Activities after they release their worker slots. A newer attempt of the same Activity execution can reattach to its existing membership and outcome. Completion always uses the recorded attempt token. Checkpoints reject stale attempts, and retries reconcile an Activity that completed before its acknowledgement was lost.

Upload and batch creation are separate Activities. Creation has **one Activity attempt and one HTTP attempt**. An uncertain submission is looked up using metadata and the input file ID. Zero or multiple matches produce `SUBMISSION_UNKNOWN`, without another creation call. This favors avoiding duplicate provider jobs; exactly-once billing is not guaranteed.

Provider work uses an absolute deadline capped by the earliest recipient deadline, retaining time for delivery. One heartbeat-enabled polling Activity keeps polling iterations out of Workflow history. Result storage and final bookkeeping continue retrying after recipients expire.

## Run the Go application

Use Go 1.26+, PostgreSQL 17 and Temporal CLI 1.9.1 / Server 1.32.0. The repository's runtime script installs the pinned Temporal CLI. On macOS, native PostgreSQL can be installed with `brew install postgresql@17`.

Run the following commands from the repository root. The Makefile builds and tests the module in `go/`; the executable is written to `bin/batchctl`.

```sh
make runtime
make postgres          # or: make postgres-docker
make server
```

`make server` runs:

```sh
temporal server start-dev --ip 127.0.0.1 --port 7234 --ui-port 8234 \
  --db-filename .state/temporal-saa.db \
  --dynamic-config-value activity.enableCallbacks=true
```

In another terminal:

```sh
make setup
make worker
```

In a third terminal:

```sh
make demo
bin/batchctl demo --run-id sample-001 --input samples/documents.jsonl
bin/batchctl demo --run-id larger-001 --count 2000 --batch-size 1000
```

Use a new run ID each time, unique across both applications in the Temporal namespace. The CLI prints a JSON report and returns nonzero for failed or unconfirmed outcomes. Go report durations use nanoseconds.

The [Temporal UI](http://localhost:8234/namespaces/default/workflows) shows `caller/RUN_ID/REQUEST_ID`, its Nexus-linked Standalone Activity, and shared `batch/RUN_ID-SEQUENCE` Workflows. Enrollment and batch Activities use separate worker pools; `--service-concurrency` and `--batch-concurrency` each default to 32.

The default Go database URL is:

```text
postgres://nexus_batch:nexus_batch@127.0.0.1:54329/nexus_batch?sslmode=disable
```

Override it with `DATABASE_URL`. Schema initialization is automatic when the store opens; `make init-db` runs it explicitly. The Python CLI loads the repository-root `.env` automatically; exported values take precedence. `.env.example` lists the settings. Export the variables when using the Go CLI. Each implementation uses its own database and queues. Use a separate database for each Temporal cluster.

## Inspect and control

```sh
bin/batchctl inspect --run-id sample-001
bin/batchctl finish --run-id sample-001
bin/batchctl cancel --run-id sample-001 --request-id library
bin/batchctl cleanup --run-id sample-001
```

`finish` closes admission and seals a partial batch. `cancel` cancels one caller; detached Activities can remain pending until completion or timeout, and other batch members continue. `cleanup` stops admission and submission, cancels recorded batch Workflows and terminates their Activities. It retains database records, history and provider files. A provider job accepted before cleanup can continue running.

Stop workers and Temporal with Ctrl-C. `make postgres-stop` stops the native development cluster. `docker compose down` preserves the database volume.

## Providers and bounds

Fake scenarios are `success`, `mixed`, `expired`, `missing`, `duplicate`, `unknown`, `malformed`, `submission-unknown` and `submission-unreconciled`. Run one scenario per service queue:

```sh
bin/batchctl worker --mode fake --scenario mixed --state-dir .state/mixed
```

For paid OpenAI requests, export `OPENAI_API_KEY` and an explicit `OPENAI_MODEL`, then run `make setup MODE=live`, `make worker MODE=live` and `make live`. Live jobs use `/v1/responses`, file purpose `batch` and `completion_window: "24h"`. [OpenAI Batch documentation](https://developers.openai.com/api/docs/guides/batch)

| Setting | Default | Maximum |
| --- | --- | --- |
| Requests per batch | 100 | 50,000 |
| Encoded JSONL per batch | 512 KiB | 200,000,000 bytes |
| Collection window | 5 seconds | Configurable |
| Request envelope | 8 KiB | 8 KiB |
| Normalized result | 4 KiB | 4 KiB |

Live polling defaults to 30 seconds, the result deadline to 25 hours and the operation timeout to 26 hours. The collection window plus result deadline must leave time for completion. `--max-admissions 0` removes the per-run admission cap.

This is a preproduction prototype. Activities materialize bounded files in memory; concurrent long polling needs worker capacity. Production throughput, retention and provider-file cleanup still need validation. External completion uses ordinary Activity timeouts and heartbeat recovery. Nexus-backed Standalone Activities remain experimental in the Python SDK.
