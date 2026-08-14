GO ?= go
GOLANGCI_LINT ?= golangci-lint
FUZZTIME ?= 1m

.PHONY: test race vet lint check compatibility fuzz fuzz-segformat fuzz-segreader

test:
	$(GO) test ./partitionlog/...

race:
	$(GO) test -race ./partitionlog/...

vet:
	$(GO) vet ./partitionlog/...

lint:
	$(GOLANGCI_LINT) run ./partitionlog/...

check: test vet lint

compatibility:
	$(GO) test ./partitionlog/segreader -run '^TestSegmentCompatibilityCorpus$$' -count=1

fuzz: fuzz-segformat fuzz-segreader

fuzz-segformat:
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseFilePreamble$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseBlockPreamble$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseIndexPreamble$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseBlockIndexEntry$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseTrailer$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzDecodeRawBlock$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./partitionlog/segformat -run '^$$' -fuzz '^FuzzParseBlockIndex$$' -fuzztime=$(FUZZTIME)

fuzz-segreader:
	$(GO) test ./partitionlog/segreader -run '^$$' -fuzz '^FuzzOpenAndScanSegment$$' -fuzztime=$(FUZZTIME)
