GO ?= go
GOLANGCI_LINT ?= golangci-lint

.PHONY: test race vet lint check

test:
	$(GO) test ./partitionlog/...

race:
	$(GO) test -race ./partitionlog/...

vet:
	$(GO) vet ./partitionlog/...

lint:
	$(GOLANGCI_LINT) run ./partitionlog/...

check: test vet lint
