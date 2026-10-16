# Prototype verification

Verified on October 16, 2026, with Go 1.26.3, Temporal Go SDK 1.49.0, API 1.63.5, Nexus SDK 0.7.0, and Temporal CLI 1.9.1 (Server 1.32.0, UI 2.54.1).

## Verified request path

`CallerWorkflow → Nexus → Standalone Activity → durable enrollment → ErrResultPending → external CompleteActivity → Nexus completion`

Each request is backed by a real Standalone Activity. Shared coordinator and batch Workflows collect requests, submit and poll the provider job, and deliver results.

Real-server tests inspect the caller's Nexus history and decode its operation token to identify the exact SAA execution. They verify the reverse Activity link, pending `RUNNING`/`STARTED` state, and successful completion of the original caller. A second Activity executes with the worker restricted to one Activity slot, proving that `ErrResultPending` releases the slot.

## Observed results

- All **21 integration cases passed** on Server 1.32.0. A fresh callback-only dev server completed the suite in **63.321 seconds**; a subsequent run against an upgraded persistent server passed in **75.751 seconds**.
- Cases cover count/timer/byte sealing, duplicate request IDs, cancellation isolation, worker replacement, stale attempt tokens, lost completion acknowledgments, admission bounds, provider failures, malformed results, and uncertain submission.
- The admission test accepted 1,000 requests, rejected request 1,001 with `RUN_FULL`, and created ten batches. A delivery-restart test replaced the worker after one of 100 completion Activities; all callers completed through the original provider batch.
- Lost-acknowledgment tests cover successful external completion and a typed failure. The first completion RPC commits but appears to fail to the worker; its retry reconciles the recorded SAA outcome without another completion RPC or provider job.
- A stale attempt token cannot complete a newer attempt. The test explicitly enables two attempts to exercise this guard; production Nexus-started SAAs use one attempt.
- A CLI run with 100 synthetic documents completed all 100 Nexus-backed SAAs through one fake-provider batch: 36,692 bytes of JSONL, 100 deliveries, zero skipped results, and **13.02 seconds** elapsed.
- A one-second CLI wait reported one pending outcome and zero failures. Inspection confirmed durable SAA enrollment; scoped cleanup canceled its caller/coordinator and terminated the pending SAA before provider submission.
- `go test -race ./...`, `go vet ./...`, and shell syntax checks passed. The race command skips real-server integration cases unless explicitly enabled; the 21-case suite ran separately with `NEXUS_INTEGRATION=1`.
- Provider HTTP tests cover multipart upload, the Batch wire contract, single-attempt batch creation, and paginated reconciliation of uncertain submission.

## Required server configuration

Use CLI 1.9.1 / Server 1.32.0 with `activity.enableCallbacks=true`. The 21-case suite passed with this as the only dynamic-config override; no `activity.enableStandalone` override was needed.

```sh
make runtime
make server

# In another terminal:
make test-integration
```

A compatibility test against CLI 1.7.2 / Server 1.31.1 started and externally completed the SAA, but its Nexus caller timed out after 60 seconds. That server does not implement [SAA callback registration](https://github.com/temporalio/temporal/blob/v1.31.1/chasm/lib/activity/handler.go). The required support is present in the pinned [Server 1.32.0](https://github.com/temporalio/temporal/blob/v1.32.0/chasm/lib/activity/config.go). After a runtime/database upgrade, existing completed Workflow history remained accessible and both the full suite and an endpoint smoke test passed.

## Abrupt worker failure

`scripts/verify-hardkill.py` sent **SIGKILL** to its own worker after the coordinator recorded an in-progress provider job and the request tokens were durably enrolled. A replacement process completed all three Nexus-backed SAAs through the same provider batch. The persistent fake provider contained exactly one job. Observed elapsed time: **47.10 seconds**, including the deliberately delayed fake provider.

Recovery is established after durable enrollment and during provider polling. A crash before enrollment can leave the single-attempt SAA pending until its ordinary operation timeout: two minutes in fake mode or 26 hours live.

## Live OpenAI smoke test

Five short synthetic documents completed through real OpenAI Files and Batch APIs, Nexus-invoked SAAs, and external Activity completion. All five results were delivered from one provider batch, with zero failures or pending callers. Model: `gpt-4.1-nano`; output bound: 128 tokens per request. Total elapsed time: **158.55 seconds**. The input JSONL contained 1,840 bytes and the provider's final status was `completed`.

Provider identifiers and raw execution evidence are retained locally and are excluded from the repository.

## Completion model and limits

The completion mode is explicitly labeled `external_async_completion_legacy`. `ErrResultPending` releases the worker slot; ordinary Activity timeouts continue across the full operation. The prototype does not implement separate completion phases with independent timeouts and retry policies. `batchctl capabilities --require-handoff` explicitly rejects that unsupported mode.

Generated jobs, files, execution histories, completion tokens, database backups, and verification logs live under ignored local state directories. They are not included in the public source.

These tests establish behavior within the prototype's bounded workload. Fake-provider timings are not OpenAI latency or production throughput measurements. The prototype does not promise exactly-once provider execution or billing.
