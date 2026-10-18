# Go application

The Go module, application source and tests live in this directory. Run the Make targets from the repository root; they share local PostgreSQL, Temporal and sample input with the Python application.

Start with the [system diagram](../README.md#how-batching-works) and [architecture reading guide](../docs/architecture.md) to follow a request through Nexus, PostgreSQL, OpenAI and external Activity completion.

| Path | Purpose |
| --- | --- |
| `cmd/batchctl/` | Command line application |
| `internal/model/` | Request types, configuration and JSONL encoding |
| `internal/store/` | PostgreSQL admission, batches and delivery checkpoints |
| `internal/orchestration/` | Nexus service, Activities, Workflows and dispatch |
| `internal/provider/` | OpenAI and persistent fake providers |
| `integration/` | Tests against PostgreSQL and Temporal |
| `scripts/` | Go endpoint setup, run cleanup and worker crash verification |
| `go.mod`, `go.sum` | Go dependencies |

From the repository root:

```sh
make build             # writes bin/batchctl
make test
make setup             # requires Temporal
make worker            # requires PostgreSQL and Temporal
```

For direct Go commands from the repository root:

```sh
go -C go test ./...
go -C go vet ./...
```

Inside `go/`, run `go test ./...` or `go vet ./...` directly.

See the [repository README](../README.md) for local services, live OpenAI configuration and demo commands, and [testing instructions](../docs/testing.md) for integration and recovery checks.
