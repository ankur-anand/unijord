BUF ?= buf
GO ?= go
GOLANGCI_LINT ?= golangci-lint
GOCACHE ?= $(CURDIR)/.gocache
GOLANGCI_LINT_CACHE ?= $(CURDIR)/.golangci-lint-cache
PROTO_PATH ?= proto/eventlake

.PHONY: proto proto-build proto-gen proto-lint test vet lint check

proto: proto-build proto-gen

proto-build:
	$(BUF) build

proto-gen: proto-build
	$(BUF) generate --path $(PROTO_PATH)

proto-lint:
	$(BUF) lint --path $(PROTO_PATH)

test:
	mkdir -p $(GOCACHE)
	GOCACHE=$(GOCACHE) $(GO) test -v -race ./...

vet:
	mkdir -p $(GOCACHE)
	GOCACHE=$(GOCACHE) $(GO) vet ./...

lint:
	mkdir -p $(GOCACHE) $(GOLANGCI_LINT_CACHE)
	GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) $(GOLANGCI_LINT) run

check: test vet lint
