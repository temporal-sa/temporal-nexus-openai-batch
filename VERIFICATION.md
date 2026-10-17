# Postgres prototype verification

Verified on October 17, 2026, with Go 1.26.3, Temporal Go SDK 1.49.0, API 1.63.5, Nexus SDK 0.7.0, pgx 5.11.0, native PostgreSQL 17.11, and Temporal CLI 1.9.1 (Server 1.32.0).

The final aggregate integration run passed **27 Temporal cases and seven SQL cases** in **260.126 seconds**. A separate stop/dispatch race test passed under `-race`, bringing the verified Temporal total to **28 cases**. These runs used real PostgreSQL and Temporal with the fake provider, including the large-manifest and concurrent-worker cases below.

## Storage boundary

`Caller Workflow → Nexus SAA → Postgres enrollment → dispatch outbox → Batch Workflow(BatchRef) → Activities reading Postgres/provider → external SAA completion`

Postgres owns requests, exact completion targets/tokens, membership, per-item outcomes and delivery checkpoints. No coordinator Workflow is created. Batch Workflow histories contain references, provider control metadata and bounded aggregate reports. Request bodies remain in their individual caller/SAA executions and in Postgres.

## Review fixes

The current implementation adds these recovery checks:

- Absolute SAA deadlines cap provider work even when sealing happens late. Time-skipping tests reject paid work after expiry and preserve the configured delivery reserve.
- Report and outcome persistence recover from storage outages that cross the recipient deadline; terminal bookkeeping remains retryable.
- A single polling Activity covers a simulated 25-hour provider wait, keeping polling iterations out of Workflow history.
- Real worker loss before enrollment recovered the same SAA on attempt 2, with one admission and one provider job. A second test deliberately suppressed post-enrollment heartbeats and verified reattachment to the original provider job.
- A real stop/dispatch race paused a dispatcher after its database claim, stopped the run, then resumed dispatch. The batch finalized with `RUN_STOPPED` and zero provider jobs. This test also passed under the race detector.
- An offline replayer verifies a sanitized, previously recorded 87-event legacy history with both Workflow generations registered. The test caught a registration alias collision, which was fixed by giving the new implementation its own Go function and Workflow type, `BatchWorkflowV2`.

The CLI completed three requests through the separate enrollment and V2 batch queues. A focused cleanup test verifies that recorded resources are still canceled/terminated when supplemental Temporal visibility fails. SQL tests additionally cover stop fencing, deadline persistence, heartbeat leases, stale checkpoint rejection and late reattachment after batch completion.

## Large batches and concurrent workers

A real Temporal/Postgres test completed **1,101 Nexus callers** across two provider batches. The first batch held 1,000 documents; its actual stored manifest encoded to **4,074,359 bytes**, exceeding the ordinary 2 MiB Temporal payload bound. It remained entirely in Postgres and Activity memory. Both batch histories passed guards requiring:

- Workflow input payloads no larger than 4 KiB.
- No document bodies, completion targets/tokens, per-item outcomes or database credentials in serialized history.
- A bounded aggregate Activity/event count after accounting for provider polling.

The large-manifest test completed in **54.56 seconds**. A separate **1,201-caller** test passed with two service workers and two endpoint-scoped dispatchers, creating thirteen 100-item-or-smaller provider batches. Its observed elapsed time was **66.75 seconds**.

These are correctness checks on a local dev server with a fake provider. They are not production throughput or provider latency benchmarks. Configuration accepts up to 50,000 requests / 200,000,000 bytes per batch, but those maxima were not benchmarked.

## Database and recovery checks

Real PostgreSQL tests use two independent pools and concurrent transactions. They verify atomic count/byte accounting, duplicate enrollment ownership, immutable membership, timer/byte sealing, optional capacity, namespace/endpoint isolation, exclusive dispatch leases, lease expiry and reopening, first-committed results, and monotonic delivery checkpoints.

The Temporal suite additionally covers:

- Real SAA worker-slot release, Activity links and Nexus callback completion.
- Enrollment committing before its acknowledgment is lost, without another admission or provider submission.
- Dispatcher failure after starting the batch Workflow but before its database acknowledgment.
- Worker replacement during provider polling, after SAA enrollment, and during partial delivery.
- Stale Activity attempts and both successful/failed completion acknowledgment loss.
- Caller cancellation and malformed, missing, duplicate, expired or uncertain provider results.
- A request sent through the wrong Nexus endpoint being rejected before SAA creation or enrollment.

Regression tests also keep provider completion separate from final batch delivery, enforce that collection plus result deadlines fit within the operation timeout, and verify retries through temporary storage outages. One delivery Activity pages through stored recipients; batch histories do not schedule one Activity per result.

## CLI and abrupt process failure

The earlier Postgres baseline CLI test completed **100/100 requests** through one fake batch in **6.95 seconds**, with 100 confirmed deliveries. A fresh three-request smoke test through the V2 batch queue completed all three in **9.24 seconds**, reporting `backing: nexus_standalone_activity`, `storage: postgres`, and `completion_mode: external_async_completion_legacy`.

`scripts/verify-hardkill.py` killed its own worker with **SIGKILL** after Postgres recorded an in-progress provider batch. A replacement worker completed **3/3 requests** through the original provider batch. Exactly one fake provider job existed after recovery. Elapsed time: **46.66 seconds**, including the deliberately delayed provider.

## Reproduce

```sh
make postgres           # native PostgreSQL 17 installed first
# Or: make postgres-docker
make runtime
make server

# In another terminal:
make test
make test-store
make test-integration
```

For the process-kill test, register the normal fake endpoint, build the CLI, and stop other fake workers first:

```sh
make build
make setup
python3 scripts/verify-hardkill.py
```

`go test -race ./cmd/... ./internal/...`, `go vet ./...`, shell syntax checks and whitespace checks passed. Real SQL and Temporal suites are opt-in; ordinary unit tests do not contact either server.

Generated evidence is local and ignored, including `.state/review-fixes-integration-final.log`, `review-fixes-stop-race.log`, `review-fixes-cli.json`, `review-fixes-hardkill.log`, and `hardkill-proof.json`. Raw provider identifiers, histories, completion tokens and database files are excluded from the public source. The committed replay fixture uses synthetic identifiers and retains the historical command sequence.

The refactored store path was tested with real PostgreSQL and Temporal while OpenAI was faked. Provider HTTP contract tests cover the real wire format using a local HTTP server; paid OpenAI jobs were not rerun for this refactor. External completion still uses conventional `ErrResultPending`, with ordinary Activity timeouts spanning the operation and heartbeat-based retries covering enrollment loss. Production throughput, provider-file cleanup and retention policies remain outside the established results.
