SHELL := /bin/bash
export GOCACHE ?= $(CURDIR)/.cache/go-build
export GOMODCACHE ?= $(CURDIR)/.cache/go-mod
TEMPORAL_ADDRESS ?= 127.0.0.1:7234
MODE ?= fake

.PHONY: build test test-integration runtime server setup worker demo live inspect clean
build:
	go build -o bin/batchctl ./cmd/batchctl
test:
	go test -race ./internal/... ./cmd/...
test-integration: build
	NEXUS_INTEGRATION=1 TEMPORAL_ADDRESS=$(TEMPORAL_ADDRESS) go test -v -count=1 -timeout=8m ./integration
runtime:
	bash scripts/temporal-runtime.sh --install
server:
	mkdir -p .state
	bash scripts/temporal-runtime.sh server start-dev --ip 127.0.0.1 --port 7234 --ui-port 8234 --db-filename .state/temporal-saa.db --dynamic-config-value activity.enableCallbacks=true
setup:
	TEMPORAL_ADDRESS=$(TEMPORAL_ADDRESS) bash scripts/setup.sh $(MODE)
worker: build
	bin/batchctl worker --mode $(MODE) --address $(TEMPORAL_ADDRESS)
demo: build
	bin/batchctl demo --address $(TEMPORAL_ADDRESS)
live: build
	bin/batchctl demo --mode live --count 5 --address $(TEMPORAL_ADDRESS)
inspect: build
	bin/batchctl inspect --run-id $(RUN_ID) --address $(TEMPORAL_ADDRESS)
clean:
	@echo 'Run bash scripts/cleanup.sh RUN_ID to cancel only one prototype run; see README.'
