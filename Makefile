VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS := -s -w -X github.com/404khai/lode/internal/version.Version=$(VERSION) -X github.com/404khai/lode/internal/version.Commit=$(COMMIT)
BINS    := lode-adit lode-vein
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test lint proto up down e2e corruption dedup replication s3compat chaos

build:
	@mkdir -p bin
	@for b in $(BINS); do go build -ldflags '$(LDFLAGS)' -o bin/$$b ./cmd/$$b || exit 1; done

test:
	go test -race ./...  # set LODE_TEST_DSN to include Postgres-backed tests

lint:
	golangci-lint run ./...
	buf lint

proto:
	buf generate

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

e2e:
	test/e2e/e2e.sh

corruption:
	test/e2e/corruption.sh

dedup:
	test/e2e/dedup.sh

replication:
	test/e2e/replication.sh

s3compat:
	test/s3compat/run.sh

chaos:
	@echo "make $@: not available until a later phase (see AGENTS.md §6)"; exit 1
