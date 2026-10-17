# Postgres prototype verification

Verified on October 17, 2026, with Go 1.26.3, Temporal Go SDK 1.49.0, API 1.63.5, Nexus SDK 0.7.0, pgx 5.11.0, native PostgreSQL 17.11, and Temporal CLI 1.9.1 (Server 1.32.0).

The final aggregate integration run passed **25 Temporal cases and three SQL subcases** in **202.537 seconds**. It used real PostgreSQL and Temporal with the fake provider, including the large-manifest and concurrent-worker cases below.

## Storage boundary

`Caller Workflow → Nexus SAA → Postgres enrollment → dispatch outbox → Batch Workflow(BatchRef) → Activities reading Postgres/provider → external SAA completion`

Postgres owns requests, exact completion targets/tokens, membership, per-item outcomes and delivery checkpoints. No coordinator Workflow is created. Batch Workflow histories contain references, provider control metadata and bounded aggregate reports. Request bodies remain in their individual caller/SAA executions and in Postgres.

## Large batches and concurrent workers

A real Temporal/Postgres test completed **1,101 Nexus callers** across two provider batches. The first batch held 1,000 documents; its actual stored manifest encoded to **4,074,343 bytes**, exceeding the ordinary 2 MiB Temporal payload bound. It remained entirely in Postgres and Activity memory. Both batch histories passed guards requiring:

- Workflow input payloads no larger than 4 KiB.
- No document bodies, completion targets/tokens, per-item outcomes or database credentials in serialized history.
- A bounded aggregate Activity/event count after accounting for provider polling.

The large-manifest test completed in **51.70 seconds**. A separate **1,201-caller** test passed with two service workers and two endpoint-scoped dispatchers, creating thirteen 100-item-or-smaller provider batches. Its observed elapsed time was **69.09 seconds**.

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

A CLI smoke test completed **100/100 requests** through one Postgres-backed fake batch in **6.95 seconds**, with 100 confirmed deliveries. A fresh three-request smoke test after the final reporting changes completed all three in **7.89 seconds**, reporting `backing: nexus_standalone_activity`, `storage: postgres`, and `completion_mode: external_async_completion_legacy`.

`scripts/verify-hardkill.py` killed its own worker with **SIGKILL** after Postgres recorded an in-progress provider batch. A replacement worker completed **3/3 requests** through the original provider batch. Exactly one fake provider job existed after recovery. Elapsed time: **46.88 seconds**, including the deliberately delayed provider.

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

Generated evidence is local and ignored, including `.state/postgres-integration-tests-final.log`, `postgres-store-tests.log`, `postgres-large-recovery-tests.log`, `postgres-cli-100.json`, `postgres-current-003.json`, `postgres-hardkill-test.log`, and `hardkill-proof.json`. Raw provider identifiers, histories, completion tokens and database files are excluded from the public source.

The refactored store path was tested with real PostgreSQL and Temporal while OpenAI was faked. Provider HTTP contract tests cover the real wire format using a local HTTP server; paid OpenAI jobs were not rerun for this refactor. External completion still uses conventional `ErrResultPending`, with ordinary Activity timeouts spanning the operation. Recovery before enrollment commits and production throughput/retention policies remain outside the established results.
