# Testing

Unit tests run without external services. Database and Nexus tests are explicit opt-in commands and use real PostgreSQL and Temporal. OpenAI is simulated; HTTP contract tests use a local server or mock transport.

## Go

```sh
make test
make test-store         # PostgreSQL required
make test-integration   # PostgreSQL and Temporal required
```

`DATABASE_URL` selects the Go database. `TEMPORAL_ADDRESS` defaults to `127.0.0.1:7234`. Start Temporal with `--dynamic-config-value activity.enableCallbacks=true` and stop application workers before integration tests own their queues.

For an abrupt worker process failure, register the fake endpoint, build the CLI, and stop other fake workers:

```sh
make build
make setup
python3 scripts/verify-hardkill.py
```

The suites cover concurrent admission, fixed-window/count/byte sealing, duplicate ownership, dispatch lease recovery, deadline handling, stop/submission races, uncertain provider creation, result reconciliation, worker loss before and after enrollment, exact-attempt completion and partial delivery recovery. Large-manifest tests inspect Workflow history for bounded payloads and absence of batch documents, tokens and individual outcomes.

## Python

```sh
make python-install
make python-test
make python-test-store
make python-test-workflows
make python-test-integration
```

`PYTHON_DATABASE_URL` selects the separate Python database. `make python-init` creates it and initializes the schema. Store tests isolate and remove their own namespaces. Integration tests retain their run records and histories for inspection, then stop their own runs.

Workflow tests use Temporal's time-skipping test server, which the SDK downloads on first use. They cover deadlines crossed during storage outages, no provider work after expiry, one-attempt creation with lost acknowledgement, and bookkeeping retries beyond recipient expiry.

CLI subprocesses in the integration tests explicitly select fake mode, even when the local `.env` selects live. Configuration tests cover `.env` loading, overrides, live-provider selection and missing credentials without calling OpenAI.

Integration tests own the Python task queues; stop `python-worker` first. They exercise real Nexus callbacks and inspect the backing Standalone Activities. Provider scenarios include success, per-request errors, expiry, missing/duplicate/unknown/malformed output, and ambiguous submissions. Recovery checks use persisted fake-provider state and real process loss.

Both implementations use bounded manifests in Activity memory and PostgreSQL. Passing local correctness tests does not establish production throughput, maximum-size batch performance, live provider latency, retention behavior or provider-file cleanup. Paid OpenAI submissions are outside the automated suites.
