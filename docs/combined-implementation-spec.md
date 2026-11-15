# Combined typed bulk batching implementation

## Purpose and scope

Add the typed bulk pipeline interface and explicit external references of the
OpenAI batch library to the PostgreSQL-backed Nexus batching service. The service
remains responsible for collecting compatible work, provider submission,
recovery, and completion. Application workflows coordinate stages without
loading the requests or responses into workflow memory or history.

The first implementation extends the Python application. The existing Python
single-request operation and Go application remain supported. Go parity for the
new bulk operation is outside this change. This is an implementation of the
combined design, rather than a dependency on the internal prototype package.

## Public behavior

1. An application activity writes an immutable input manifest from an iterable
   or asynchronous iterable of requests. Each request has an explicit custom ID
   and an OpenAI Responses request body.
2. A workflow submits that manifest through one `llm-batch.responses` Nexus
   operation. A bulk submission owns one backing Standalone Activity, regardless
   of its item count.
3. The service admits those items to the existing run's count, byte, and fixed
   collection-window batching. Multiple bulk submissions and individual callers
   in the same compatible run can share a provider batch.
4. The service persists one terminal outcome per item. It completes the bulk
   operation only when input admission is complete and all item outcomes exist.
5. The operation returns a small result-set reference and aggregate counts.
   Application activities load or iterate typed results, select follow-up
   requests, and write a manifest for the next stage.

## Payload and storage requirements

- Nexus and workflow messages carry opaque manifest/result-set IDs, digests,
  counts, deadlines, and bounded control fields. They never contain complete
  manifests, request bodies, provider responses, schema documents, completion
  tokens, or a list of all chunk references.
- PostgreSQL initially provides the shared artifact store. Producers, service
  workers, and consumers must have access to the same namespace-scoped store.
- Input manifests are immutable after sealing. Their count and content digest
  are validated before admission. Duplicate custom IDs, empty input, changed
  content, invalid bodies, model conflicts, and an individual provider line that
  cannot fit a configured batch fail before paid work is submitted.
- Input production and result loading run only in activities. Store access and
  iteration are paginated. Item and page byte bounds prevent an unlimited
  collection from being loaded by a convenience helper.
- Separate submissions may reuse a custom ID. A stable provider ID derived from
  submission identity and item position prevents cross-submission collisions.
- Existing `respond` request/result byte bounds remain unchanged. The bulk path
  stores larger values externally and returns references.

## Typed request and result contract

- Preserve arbitrary supported Responses request fields, including instructions,
  structured inputs, and output formatting. Enforce the run's model and reject
  streaming requests.
- Structured output schemas are stored as canonical JSON Schema with a digest.
  The service validates the portable schema and never imports a caller-selected
  Python module, class, or function.
- Schema identity is part of the bulk submission. Requests retain their schema
  when sharing a provider batch; the provider's single-model constraint remains
  enforced by the run.
- Raw mode preserves the original request and each uniquely matched provider
  response within the stored-item bounds. Typed mode additionally records parsed
  JSON or an explicit parse failure. Oversized or ambiguous duplicate responses
  become explicit item failures.
- Provider errors, missing/duplicate output, expiry, refusal, incomplete output,
  and schema validation errors produce per-item outcomes. They do not erase
  other successes or fail an otherwise valid bulk recipient.
- Consumer activities perform final Pydantic validation. Custom Python
  validators can add local parse failures beyond service JSON Schema validation;
  service aggregate counts must not claim that those validators already ran.

## Admission and completion ownership

- Add a bulk-recipient ledger separate from per-item request ownership. It holds
  submission identity, immutable input identity, admission progress, original
  deadline, exact Activity execution/attempt target, and delivery checkpoint.
- Bulk items own membership and outcomes, not an individual completion token.
  Existing single-request recipients keep their existing completion path.
- Admission is paginated and retry-safe. A lost acknowledgement does not add
  duplicate membership or reset the deadline. Newer attempts of the same
  Activity may reattach; a different operation cannot take over a submission ID.
- The admission Activity owns heartbeats while pages remain. The external keeper
  takes over after admission completes, allowing a lost worker to time out and
  retry. Worker shutdown, pause, reset, and timeout preserve admission progress;
  explicit recipient cancellation releases unused capacity reservations.
- Readiness requires both closed admission and an outcome for every input item.
  Completion returns one stable result reference. The keeper retries delivery
  after process loss and reconciles an accepted completion whose acknowledgement
  was lost. Checkpoints reject stale attempt targets.
- Cancelling one recipient does not cancel provider work shared with another
  recipient. Already persisted outcomes remain available by submission ID.
- Stopping a run fences further admission and submission for both API paths.

## Provider execution and recovery

- Keep the existing provider `BatchWorkflow` as the single orchestration engine.
  Reuse request construction, external references, and result parsing through
  activities; do not create a second provider orchestrator per bulk caller.
- Preserve count/encoded-byte sealing, stable dispatch IDs and leased outbox,
  absolute deadlines, one paid create attempt, and lookup-only reconciliation
  of uncertain submissions. An unknown submission never permits another POST.
- Store outcomes atomically before completing recipients. A retry uses the first
  committed outcome set.
- Keep provider files until result persistence succeeds. Any future deletion
  runs as a separate recoverable step after the committed result reference.
- Poll within a heartbeat Activity and keep per-item delivery outside workflow
  event loops. Workflow reports contain bounded aggregates.

## Application interface and example

Provide activity-side input writing, typed result iteration, chunk/page loading,
and selection of unfinished items. Provide a workflow-side `responses` helper
that calls the bulk Nexus operation and returns a generic result reference.
References can be passed between application stages without reading their data
inside the workflow.

Provide a generic inference example accepting caller-owned JSONL requests and an
optional portable output schema. Submit every valid input without inspecting its
meaning or selecting a subset. Return only aggregate counts and a result-set
reference. Business filtering and chaining belong to client applications. Fake
output generation uses deterministic schema fixtures without task-specific rules.

## Required verification

1. Existing Python unit, store, workflow, and Nexus integration tests pass.
2. Existing Go unit tests pass when its pinned toolchain is available.
3. Two bulk submissions and ordinary requests share batches within one run;
   identical custom IDs in different submissions remain isolated.
4. A large bulk submission creates one Nexus operation and one backing Activity.
   Histories contain bounded control payloads and no documents, provider
   responses, schema documents, or completion tokens.
5. Typed success, raw mode, API failure, expiry, shuffled/missing/duplicate output,
   and schema-invalid results preserve exactly one terminal outcome per item.
6. Duplicate IDs, invalid manifests/schemas, changed submission content, and
   oversized individual lines fail before provider creation.
7. Retrying input admission, outcome persistence, and completion after a lost
   acknowledgement does not repeat provider work or change stored outcomes.
8. A newer Activity attempt reattaches without renewing deadlines; stale tokens
   cannot complete or checkpoint it.
9. Partial results remain inspectable after cancellation or process loss, and
   cancellation leaves other recipients' shared work intact.
10. The generic example runs end to end using real Temporal and PostgreSQL with
    a fake provider. Arbitrary prompts and schemas pass through without filtering;
    raw and structured modes both return an outcome for every input. Paid OpenAI
    requests are not required for verification.

## Operational limits

This change does not establish production throughput, introduce automatic data
retention/deletion, or remove the Python SDK's experimental Standalone Activity
dependency. Document configured artifact/item/page limits, shared database access,
and the difference between bounded workflow messages and activity memory.
