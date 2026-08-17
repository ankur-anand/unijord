GO ?= go
GOLANGCI_LINT ?= golangci-lint

PACKAGES ?= ./partitionlog/... ./internal/blobstore/... ./internal/registry
TESTFLAGS ?=
FUZZTIME ?= 1m
STRESSCOUNT ?= 100
MODELCOUNT ?= 10
SOAKTIMEOUT ?= 30m

FUZZ_SEGFORMAT_TARGETS := \
	FuzzParseFilePreamble \
	FuzzParseBlockPreamble \
	FuzzParseIndexPreamble \
	FuzzParseBlockIndexEntry \
	FuzzParseTrailer \
	FuzzDecodeRawBlock \
	FuzzParseBlockIndex

.DEFAULT_GOAL := help

.PHONY: help test race vet lint verify check compatibility \
	fuzz fuzz-segformat fuzz-segreader fuzz-one \
	stress stress-write stress-read stress-lifecycle \
	integration soak

help: ## Show the supported development and CI commands.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run unit tests. Override PACKAGES or TESTFLAGS for a focused run.
	$(GO) test $(PACKAGES) $(TESTFLAGS)

race: ## Run tests with the race detector.
	$(GO) test -race $(PACKAGES) $(TESTFLAGS)

vet: ## Run Go static analysis.
	$(GO) vet $(PACKAGES)

lint: ## Run golangci-lint.
	$(GOLANGCI_LINT) run $(PACKAGES)

verify: test vet compatibility ## Run the toolchain-only checks required by CI.

check: verify lint ## Run all local checks, including golangci-lint.

compatibility: ## Verify the committed segment-format compatibility corpus.
	$(GO) test ./partitionlog/segreader -run '^TestSegmentCompatibilityCorpus$$' -count=1

fuzz: fuzz-segformat fuzz-segreader ## Run every fuzz target for FUZZTIME.

fuzz-segformat:
	@for target in $(FUZZ_SEGFORMAT_TARGETS); do \
		$(MAKE) --no-print-directory fuzz-one \
			FUZZ_PACKAGE=./partitionlog/segformat \
			FUZZ_TARGET=$$target || exit $$?; \
	done

fuzz-segreader:
	@$(MAKE) --no-print-directory fuzz-one \
		FUZZ_PACKAGE=./partitionlog/segreader \
		FUZZ_TARGET=FuzzOpenAndScanSegment

fuzz-one:
	@test -n "$(FUZZ_PACKAGE)" || (echo "FUZZ_PACKAGE is required"; exit 1)
	@test -n "$(FUZZ_TARGET)" || (echo "FUZZ_TARGET is required"; exit 1)
	$(GO) test $(FUZZ_PACKAGE) -run '^$$' -fuzz '^$(FUZZ_TARGET)$$' -fuzztime=$(FUZZTIME)

stress: stress-write stress-read stress-lifecycle ## Run all repeated race and model tests.

stress-write: ## Stress segment publication, fencing, and catalog ordering.
	$(GO) test -race ./partitionlog/segwriter -run '^TestWriterPipelineStress$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog -run '^TestMemoryCatalog(IdempotentRetryOfLastAppend|RejectsIdempotentRetryAfterFenceMoves)$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog/blob -run '^Test(BlobCatalogIdempotentRetryChecksCurrentHead|AppendSegmentReconcilesHistoricalCommitAfterHeadAdvances)$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog/writeradapter -run '^TestSessionRejectsExpectedNextLSNMismatch$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/writer -run '^Test(WriterStopsWhenCatalogFenceMoves|WriterCommittedNotifiesAfterPublicationAndOnClose)$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog -run '^TestMemoryCatalogRejectsSegmentFromDifferentWriterIdentity$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog/blob -run '^Test(BlobCatalogRejectsSegmentFromDifferentWriterIdentity|BlobCatalogStaleWriterCannotReportRetentionNoOp|BlobCatalogRetentionReconcilesHistoricalCommitAfterFenceMoves|LoadPageRejectsObjectKeyMetadataMismatch)$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/catalog/blob -run '^TestBlobCatalogMatchesMemoryCatalogAcrossLongHistory$$' -count=$(MODELCOUNT)

stress-read: ## Stress concurrent segment reads and cache coalescing.
	$(GO) test -race ./partitionlog/segreader -run '^TestReaderConcurrentScanStress$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/reader -run '^TestSegmentReaderCacheCoalescesConcurrentOpens$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./partitionlog/blob/cache -run '^TestStoreCoalescesConcurrentReads$$' -count=$(STRESSCOUNT)

stress-lifecycle: ## Stress lifecycle retry, checkpoint, and scheduling behavior.
	$(GO) test -race ./partitionlog/blob/lifecycle -run '^Test(ReclaimerResumesAfterDeleteFailureUsingLastObjectKey|ReclaimerRetriesAfterListThrottleWithoutSkippingObjects|ReclaimerRetriesAfterStateCASThrottle|ReclaimerResumesAfterDeleteContextTimeout|ExecuteDeletesUsesBoundedConcurrency|ExecuteDeletesUsesNativeBatchesAndReportsFirstFailureCheckpoint|SchedulerBoundsConcurrencyAndFairlyRequeuesContinuation|SchedulerUsesConfiguredPartitionConcurrency|SchedulerDefersAnUnboundedContinuationAtPassLimit|SchedulerRunTimeoutExhaustsRetryBudget|SchedulerCancellationWaitsForStartedPasses)$$' -count=$(STRESSCOUNT)
	$(GO) test -race ./internal/blobstore/s3 -run '^TestDeleteBatchUsesConfiguredSDKRetryForSlowDown$$' -count=$(STRESSCOUNT)

integration: ## Run live S3, GCS, and Azure lifecycle conformance tests.
	$(GO) test -race ./partitionlog/s3 ./partitionlog/gcs ./partitionlog/azure -run 'LifecycleConformance$$' -integration -count=1

soak: ## Run provider lifecycle soak tests for PARTITIONLOG_LIFECYCLE_SOAK.
	@if [ -z "$(PARTITIONLOG_LIFECYCLE_SOAK)" ]; then echo "set PARTITIONLOG_LIFECYCLE_SOAK to a duration such as 5m"; exit 1; fi
	$(GO) test ./partitionlog/s3 ./partitionlog/gcs ./partitionlog/azure -run 'LifecycleSoak$$' -integration -count=1 -timeout=$(SOAKTIMEOUT)
