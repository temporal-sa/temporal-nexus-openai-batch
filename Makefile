SHELL := /bin/bash
export GOCACHE ?= $(CURDIR)/.cache/go-build
export GOMODCACHE ?= $(CURDIR)/.cache/go-mod
TEMPORAL_ADDRESS ?= 127.0.0.1:7234
MODE ?= fake
PYTHON := python/.venv/bin/python
PYTHON_DATABASE_URL ?= postgres://nexus_batch:nexus_batch@127.0.0.1:54329/nexus_batch_python?sslmode=disable

.PHONY: build test test-integration test-store runtime postgres postgres-docker postgres-stop init-db server setup worker demo live inspect clean
build:
	go build -o bin/batchctl ./cmd/batchctl
test:
	go test -race ./internal/... ./cmd/...
test-integration: build
	NEXUS_INTEGRATION=1 TEMPORAL_ADDRESS=$(TEMPORAL_ADDRESS) go test -v -count=1 -timeout=8m ./integration
test-store:
	POSTGRES_TEST=1 go test -v -race -count=1 -run TestPostgres ./internal/store ./integration
postgres:
	bash scripts/postgres-runtime.sh start
postgres-stop:
	bash scripts/postgres-runtime.sh stop
postgres-docker:
	docker compose up -d --wait postgres
init-db: build
	bin/batchctl init-db
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

.PHONY: python-install python-init python-setup python-worker python-demo python-test python-test-store python-test-workflows python-test-integration
python-install:
	uv sync --project python --extra test
python-init:
	PYTHON_DATABASE_URL='$(PYTHON_DATABASE_URL)' $(PYTHON) -m nexus_batch init-db
python-setup:
	$(PYTHON) -m nexus_batch setup --mode $(MODE) --address $(TEMPORAL_ADDRESS)
python-worker:
	PYTHON_DATABASE_URL='$(PYTHON_DATABASE_URL)' $(PYTHON) -m nexus_batch worker --mode $(MODE) --address $(TEMPORAL_ADDRESS)
python-demo:
	PYTHON_DATABASE_URL='$(PYTHON_DATABASE_URL)' $(PYTHON) -m nexus_batch demo --mode $(MODE) --address $(TEMPORAL_ADDRESS) --run-id python-$$(date +%s)
python-test:
	$(PYTHON) -m pytest -q python/tests
	$(PYTHON) -m ruff check python
	$(PYTHON) -m ruff format --check python
python-test-store:
	PYTHON_DATABASE_URL='$(PYTHON_DATABASE_URL)' $(PYTHON) -m pytest -q python/tests/test_store.py
python-test-workflows:
	PYTHON_WORKFLOW_TESTS=1 $(PYTHON) -m pytest -q python/tests/test_workflows.py
python-test-integration:
	NEXUS_INTEGRATION=1 PYTHON_DATABASE_URL='$(PYTHON_DATABASE_URL)' TEMPORAL_ADDRESS=$(TEMPORAL_ADDRESS) $(PYTHON) -m pytest -q python/tests/test_integration.py
