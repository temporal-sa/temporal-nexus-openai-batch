# Temporal Nexus OpenAI Batch prototype

Independent Temporal Workflows call `llm-batch.respond`, which starts a **Standalone Activity through Nexus**. That Activity durably enrolls one document, records its attempt-scoped completion token, and returns `activity.ErrResultPending`. The batch service groups requests into OpenAI Batch API jobs and later calls `Client.CompleteActivity` for each result. Temporal completes each Nexus operation from its Activity's outcome. Only OpenAI is simulated in fake mode.

Start with fake mode. Live mode submits paid API requests and requires an explicitly configured model.

## External async completion

This prototype uses **conventional external async completion on a real Nexus-invoked SAA**. Returning `ErrResultPending` releases the Worker slot; the current SDK does not report a first-class handoff to the server. Describe therefore shows `RUNNING`/`STARTED` while external work is pending. Ordinary Activity timeouts continue across enrollment and external completion. The SAA uses one Activity attempt, with short idempotent enrollment retries inside its Worker body, so a late external failure cannot automatically resubmit the provider job. No short heartbeat timeout is set after handoff.

Worker-crash recovery is verified **after durable enrollment**, when the completion token survives in the coordinator's history. A process killed before enrollment can leave its single-attempt SAA waiting until the ordinary operation timeout: two minutes in fake mode or 26 hours live. Fast recovery before enrollment and independent phase retry rules need further implementation; they are not claimed by this prototype.

```sh
bin/batchctl capabilities
bin/batchctl capabilities --require-handoff  # explicitly fails on this public stack
```

The report labels its backing as `nexus_standalone_activity` and its completion mode as `external_async_completion_legacy`. It never labels this as the unavailable two-phase protocol. [Published Nexus SAA documentation](https://docs.temporal.io/nexus/standalone-activity), [Server callback gate](https://github.com/temporalio/temporal/blob/v1.32.0/chasm/lib/activity/config.go)

## Run locally

The toolchain is Go **1.26.3**, Temporal Go SDK **1.49.0** (pinned in `go.mod`), and the local pinned Temporal CLI **1.9.1**, which bundles Server **1.32.0** and UI **2.54.1**. Go 1.26 or later, Python 3, curl, and a SHA-256 utility are required. No Docker or application database is required. SAA completion callbacks require the newer server and `activity.enableCallbacks=true`; `make server` sets that flag.

Clone the repository and install the pinned official CLI locally, with its release checksum verified:

```sh
git clone https://github.com/temporal-sa/temporal-nexus-openai-batch.git
cd temporal-nexus-openai-batch
make runtime
```

The full 21-case integration suite passes on a fresh dev server started with only `--dynamic-config-value activity.enableCallbacks=true`; no `activity.enableStandalone` override is needed on the pinned runtime. `make server` uses that configuration. CLI 1.7.2 / Server 1.31.1 can complete an SAA but cannot dispatch its Nexus completion callback; use the pinned runtime.

Use three terminals in this directory:

```sh
# Terminal 1: persistent local Temporal service, loopback only
make server
```

```sh
# Terminal 2: build, register the Nexus endpoint, start service and caller workers
make build
make setup
make worker
```

```sh
# Terminal 3: 100 synthetic documents, default five-second collection window
make demo

# Or use the supplied three-document input file
bin/batchctl demo --run-id sample-001 --input samples/documents.jsonl
```

Each run ID must be new, including after a completed run. The CLI prints a JSON report and exits nonzero if any document failed. Durations in the report are nanoseconds. `simulated: true` identifies fake-provider runs; elapsed time also includes real Temporal orchestration.

Open [Temporal Web UI](http://localhost:8234/namespaces/default/workflows). Find `caller/RUN_ID/document-0001`, then follow its Nexus operation link to the Standalone Activity `request-activity/RUN_ID/...`. There is no per-request handler Workflow. `batch-coordinator/RUN_ID` and `batch/RUN_ID-0001` contain shared batching orchestration.

The dev server uses port **7234**, UI **8234**, and `.state/temporal-saa.db`.

## Inspect and control a run

```sh
bin/batchctl inspect --run-id sample-001
bin/batchctl cancel --run-id sample-001 --request-id library
bin/batchctl finish --run-id sample-001
bash scripts/cleanup.sh sample-001
```

`inspect` reports the Nexus backing, completion mode, admitted and buffered requests, batch sizes, provider IDs, current states, failure counts, and external completion totals. `finish` closes admission and flushes the current buffer; children continue until completion. The demo launcher normally finishes its run automatically after all callers return.

`cleanup` requests cancellation for the named run's coordinator, caller, and batch Workflows and explicitly terminates its remaining Standalone Activities. It preserves history, local fake state, and OpenAI jobs/files. Caller cancellation requests SAA cancellation through Nexus; after `ErrResultPending` there is no active Worker to forward it to the batch queue. The SAA can remain pending until external completion or timeout, and a successful external completion can still win. Shared upstream jobs continue. Stop Workers and the local server with Ctrl-C in their terminals.

Use `--address` and `--namespace`, or `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`, for another local service. The setup script uses the same environment variables. This prototype's CLI targets a local development server; production authentication and deployment are outside its scope.

## Fake failure scenarios

Restart the fake worker with a scenario and use a new demo run ID:

```sh
bin/batchctl worker --mode fake --scenario mixed --state-dir .state/fake
bin/batchctl demo --run-id mixed-001 --count 6 --batch-size 6
```

Run one service worker configuration per service queue when injecting scenarios. Supported scenarios are `success`, `mixed`, `expired`, `missing`, `duplicate`, `unknown`, `malformed`, `submission-unknown`, and `submission-unreconciled`.

The fake writes jobs and provider files under its state directory using atomic replacement and process locking. Restarting Workers with the same directory preserves submitted jobs. `submission-unknown` creates a job and loses the response, then allows positive reconciliation. `submission-unreconciled` hides the created job from discovery: callers receive `SUBMISSION_UNKNOWN`, and no replacement is submitted.

To check restart behavior manually, use `--delay 20s`, start a demo, wait until `inspect` shows a provider batch ID, then stop and restart the Worker with the same state directory. The demo should resume the original batch. The integration suite automates this test with a new Worker and provider instance.

## Live OpenAI mode

Export `OPENAI_API_KEY` and `OPENAI_MODEL` in your shell. `.env.example` lists the available environment variables; the program does not load `.env` automatically. No credential is stored in Workflow inputs or provider metadata.

```sh
# Register a separate endpoint and start a separate service queue
make setup MODE=live
make worker MODE=live

# In another terminal: five short documents, at most 128 generated tokens each
bin/batchctl demo --mode live --count 5 --run-id live-001
```

The model is selected with `OPENAI_MODEL` or `--model`. The service uses `/v1/responses`, uploads JSONL with purpose `batch`, and creates the upstream job with `completion_window: "24h"`. The five-second collection window is independent of that provider processing window. Live polling defaults to 30 seconds; the service result deadline is 25 hours and the Nexus operation timeout is 26 hours. [OpenAI Batch documentation](https://developers.openai.com/api/docs/guides/batch)

`--wait 2m` limits how long the CLI waits; it does not cancel the durable operations. Unconfirmed operations appear as `pending`, separately from known failures. If the CLI stops waiting, `inspect` can still show the job and the Workers continue processing it. Use `finish` when you are done admitting requests. The upstream job and uploaded files are deliberately retained for inspection; remove them through your provider tooling when no longer needed.

Fake and live endpoints target different queues, so their providers cannot process each other's batches. Caller Workers share a queue and use the endpoint in each run's configuration.

## Contract and bounds

```json
{"run_id":"demo-001","request_id":"document-042","input":"Summarize this document: ..."}
```

```json
{"request_id":"document-042","text":"The document describes ...","usage":{"input_tokens":80,"output_tokens":30}}
```

The examples are illustrative. `usage` may be null. A failed item fails its own Nexus operation. Machine failure types include invalid input, duplicate identity, run closure/capacity, item failure, expiry, missing/invalid results, deadlines, and submission uncertainty.

Defaults are 100 requests and 512 KiB of encoded JSONL per batch, 8 KiB per input envelope, 4 KiB per normalized result, and 1,000 unique admissions per run. The coordinator validates the actual encoded JSONL length, including line separators, before admitting a record. Request and run identifiers have a bounded character set. The launcher rejects duplicate input IDs before starting any Workflows.

An enrollment retry from the same SAA execution and attempt token returns its existing assignment, even after the buffer has sealed. A separate Nexus operation using the same `(run_id, request_id)` fails as a duplicate. Input fingerprints, Activity IDs, Activity run IDs, and exact attempt tokens protect result correlation. Completion tokens stay in service-side Workflow history and never appear in user-facing reports or logs.

## Architecture and recovery

`CallerWorkflow` makes one Nexus call. A `NewTemporalOperation` handler calls `temporalnexus.StartActivity`, which attaches the Nexus completion callback to the SAA. `SubmitRequest` persists its task token through an acknowledged coordinator Update and returns `ErrResultPending`. The coordinator seals its buffer without yielding and starts an immutable `BatchWorkflow`. The child uploads, submits, polls, and reconciles both result files by `custom_id`. Its `CompleteResult` Activities complete each original SAA with the recorded token. The SAA callback completes the original Nexus operation.

The per-request path uses no `NewWorkflowRunOperation`, waiting Workflow, or result Signal. Shared batching still uses Workflows. Ordinary Workflow Updates acknowledge durable enrollment and close admission. [Go Nexus SDK](https://pkg.go.dev/go.temporal.io/sdk/temporalnexus), [Go external async completion](https://docs.temporal.io/develop/go/activities/asynchronous-activity)

Uploads and batch creation are separate persisted Activity steps. Batch creation has **one attempt**, including at the HTTP layer. If a response is lost, recovery searches provider metadata and checks the uploaded file ID. Exactly one matching job can resume processing. Zero or multiple matches produce `SUBMISSION_UNKNOWN`; absence from listing never authorizes an automatic replacement. Inspect the report's input file ID, logical batch ID, and `run_id`/`logical_batch_id` metadata in OpenAI before taking any manual action. The prototype does not promise exactly-once execution or billing and does not offer an automatic resubmission command.

Unknown and malformed result rows appear in diagnostics. Missing or duplicate results fail the affected requests. Completed items remain successful in partially expired batches. Repeated completion after a lost acknowledgment inspects the exact Activity execution and its recorded outcome; it never falls back to completing the latest attempt by ID. Stale or closed targets are skipped. Shared coordinator progress signals expose provider IDs before completion.

Each request has one SAA, with batching shared across requests. This bounded example does not benchmark 50,000-request batches or run indefinitely. External batching storage, long-running coordinator rotation, production tenancy, and first-class handoff need further implementation and measurement.

## Verification

```sh
make test
make test-integration   # requires make server; creates isolated endpoints/queues

# Abrupt process failure check: server and endpoint up, other fake workers STOPPED
python3 scripts/verify-hardkill.py
```

Unit tests cover serialization bounds, admission and sealing, deduplication, cancellation, provider HTTP contracts, persistent fake state, and result reconciliation. Integration tests use a real Temporal server and real Nexus; only OpenAI is faked. They inspect caller histories to ensure the caller made a Nexus operation rather than bypassing it with an Activity.

See [VERIFICATION.md](VERIFICATION.md) for the observed toolchain, results, and limits of the live smoke test.
