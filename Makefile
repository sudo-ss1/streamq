SHELL := /bin/bash
.DEFAULT_GOAL := build

# Pinned tool versions. Keep GOLANGCI_LINT_VERSION in sync with .github/workflows/ci.yml.
BUF_VERSION                := v1.73.0
GOLANGCI_LINT_VERSION      := v2.14.0
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

TOOLS_DIR := $(CURDIR)/.tools
TOOLS_BIN := $(TOOLS_DIR)/bin
export PATH := $(TOOLS_BIN):$(PATH)

# One stamp file per tool+version, so bumping a version triggers a reinstall.
BUF_STAMP           := $(TOOLS_DIR)/.buf-$(BUF_VERSION)
GOLANGCI_STAMP      := $(TOOLS_DIR)/.golangci-lint-$(GOLANGCI_LINT_VERSION)
PROTOC_GEN_GO_STAMP := $(TOOLS_DIR)/.protoc-gen-go-$(PROTOC_GEN_GO_VERSION)
PROTOC_GEN_GRPC_STAMP := $(TOOLS_DIR)/.protoc-gen-go-grpc-$(PROTOC_GEN_GO_GRPC_VERSION)

PROTO_FILES := $(shell find api -name '*.proto' 2>/dev/null)

.PHONY: tools gen build test test-int lint fmt fuzz bench cluster-up cluster-down chaos clean

tools: $(BUF_STAMP) $(GOLANGCI_STAMP) $(PROTOC_GEN_GO_STAMP) $(PROTOC_GEN_GRPC_STAMP)

$(BUF_STAMP):
	GOBIN=$(TOOLS_BIN) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	@touch $@

$(GOLANGCI_STAMP):
	GOBIN=$(TOOLS_BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@touch $@

$(PROTOC_GEN_GO_STAMP):
	GOBIN=$(TOOLS_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@touch $@

$(PROTOC_GEN_GRPC_STAMP):
	GOBIN=$(TOOLS_BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@touch $@

gen: $(BUF_STAMP) $(PROTOC_GEN_GO_STAMP) $(PROTOC_GEN_GRPC_STAMP)
ifeq ($(strip $(PROTO_FILES)),)
	@echo "gen: no .proto files under api/ yet; skipping"
else
	buf lint
	buf generate
endif

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

test-int:
ifeq ($(wildcard test/integration),)
	@echo "test-int: no integration tests until M2; skipping"
else
	go test -race -count=1 -tags=integration ./test/integration/... ./test/linearizability/...
endif

lint: $(GOLANGCI_STAMP)
	golangci-lint run ./...

fmt: $(GOLANGCI_STAMP)
	golangci-lint fmt

fuzz:
	go test ./internal/record -run='^$$' -fuzz=FuzzValidate -fuzztime=30s

bench:
	go test -run='^$$' -bench=. -benchmem ./internal/storage/...

cluster-up:
	docker compose -f deploy/docker-compose.yml up -d --build

cluster-down:
	docker compose -f deploy/docker-compose.yml down -v

# Chaos scenarios against a dedicated compose project (M9 plan R14); results in test/chaos/results.
chaos: build
	go run ./test/chaos/cmd/chaos -compose deploy/docker-compose.yml -override deploy/docker-compose.chaos.yml \
		-project streamq-chaos -ctl ./bin/streamqctl -out test/chaos/results

clean:
	rm -rf bin/
