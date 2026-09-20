package isledb

import "time"

// RunOperationalProfile is the E19 sizing decision record consumed by
// run-only service wiring. ProductionReady remains false until the external
// E20 qualification gates pass.
type RunOperationalProfile struct {
	Name            string
	ProductionReady bool

	ForegroundTargetBytes  uint64
	SlotChargedLimitBytes  uint64
	SlabBytes              uint64
	LargeThresholdBytes    uint64
	MaxResidence           time.Duration
	HeadResolutionBatch    int
	MaxHeadResolutionBatch int
	MaxConcurrentReaders   int

	L0Trigger                  int
	CompactedOutputTargetBytes uint64
	LevelSizeMultiplier        int
	MergeFanIn                 int
	CompactionConcurrency      int
	UploadConcurrency          int
	LifecycleConcurrency       int
	ProviderRetryAttempts      int
	ProviderRetryBackoff       time.Duration
	KafkaFetchBytes            uint64
	KafkaDeliveryReserveBytes  uint64
	MaxActivePartitions        uint32
	ProcessMemoryBytes         uint64
}

// MeasuredRunOperationalProfile returns only values supported by the local
// E19 file/CPU matrix. External-provider and broker settings remain zero until
// their qualification gates pass.
func MeasuredRunOperationalProfile() RunOperationalProfile {
	return RunOperationalProfile{
		Name:                   "2026-09-20-local-file",
		ForegroundTargetBytes:  4 << 20,
		SlotChargedLimitBytes:  32 << 20,
		SlabBytes:              64 << 10,
		LargeThresholdBytes:    64 << 10,
		MaxResidence:           time.Second,
		HeadResolutionBatch:    1000,
		MaxHeadResolutionBatch: 4096,
		MaxConcurrentReaders:   8,
	}
}
