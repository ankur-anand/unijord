package runfile

import "fmt"

const (
	timelineSlabTargetBytes      uint64 = 64 << 10
	timelineReferenceChargeBytes uint64 = 32
	timelineSpillLengthBytes     uint64 = 2
	minimumTimelineBytes         uint64 = 1
	minimumTimelineRecordBytes          = minimumTimelineBytes + timelineSpillLengthBytes + timelineReferenceChargeBytes
)

// timelineSlabBatch owns timeline bytes in lazy, fixed-capacity slabs while
// retaining ordinary []byte values for the existing bytewise sort path.
//
// timelineSorter owns this component. A caller that receives added=false with
// no error must first consume the current timelines successfully, then call
// reset, and finally retry the same timeline. Slices returned by timelines are
// valid only until reset and must not escape the consuming operation.
type timelineSlabBatch struct {
	budget uint64

	batch        [][]byte
	slabs        [][]byte
	slabIndex    int
	slabOffset   int
	slabCapacity int

	logicalUsed      uint64
	logicalHighWater uint64
	reservedBytes    uint64
	reservedLimit    uint64
	resets           uint64
}

type timelineSlabBatchMetrics struct {
	LogicalUsed      uint64
	LogicalHighWater uint64
	ReservedBytes    uint64
	ReservedLimit    uint64
	Slabs            uint64
	Resets           uint64
}

// newTimelineSlabBatch constructs a batch with stable backing capacities.
// descriptorEntryCount is an untrusted allocation hint, never semantic truth.
// A zero hint still permits one record so a later descriptor comparison can
// classify malformed metadata as corruption rather than a resource failure.
func newTimelineSlabBatch(memoryBudget, descriptorEntryCount uint64) (*timelineSlabBatch, error) {
	if memoryBudget < minimumTimelineRecordBytes {
		return nil, fmt.Errorf(
			"%w: timeline batch budget %d is below minimum record charge %d",
			ErrVerificationResource,
			memoryBudget,
			minimumTimelineRecordBytes,
		)
	}

	maxInt := uint64(^uint(0) >> 1)
	maximumRecordsByBudget := memoryBudget / minimumTimelineRecordBytes
	referenceCapacity := max(descriptorEntryCount, uint64(1))
	referenceCapacity = min(referenceCapacity, maximumRecordsByBudget, maxInt)
	if referenceCapacity == 0 {
		return nil, fmt.Errorf("%w: timeline batch has no reference capacity", ErrVerificationResource)
	}

	slabCapacity := min(timelineSlabTargetBytes, memoryBudget)
	maxAcceptedTimeline := min(MaxTimelineBytes, memoryBudget-timelineSpillLengthBytes-timelineReferenceChargeBytes)
	minimumUsedClosedSlab := slabCapacity - maxAcceptedTimeline + 1
	maximumSlabs := memoryBudget/minimumUsedClosedSlab + 1
	if maximumSlabs == 0 || maximumSlabs > maxInt {
		return nil, fmt.Errorf(
			"%w: timeline slab count %d exceeds platform limit %d",
			ErrVerificationResource,
			maximumSlabs,
			maxInt,
		)
	}

	referenceReserved, ok := checkedMultiply(referenceCapacity, timelineReferenceChargeBytes)
	if !ok {
		return nil, fmt.Errorf("%w: timeline reference reservation overflows", ErrVerificationResource)
	}
	slabReferencesReserved, ok := checkedMultiply(maximumSlabs, timelineReferenceChargeBytes)
	if !ok {
		return nil, fmt.Errorf("%w: timeline slab-reference reservation overflows", ErrVerificationResource)
	}
	initialReserved, ok := checkedAdd(referenceReserved, slabReferencesReserved)
	if !ok {
		return nil, fmt.Errorf("%w: timeline initial reservation overflows", ErrVerificationResource)
	}
	slabDataLimit, ok := checkedMultiply(maximumSlabs, slabCapacity)
	if !ok {
		return nil, fmt.Errorf("%w: timeline slab-data reservation overflows", ErrVerificationResource)
	}
	reservedLimit, ok := checkedAdd(initialReserved, slabDataLimit)
	if !ok {
		return nil, fmt.Errorf("%w: timeline retained-capacity limit overflows", ErrVerificationResource)
	}

	return &timelineSlabBatch{
		budget:        memoryBudget,
		batch:         make([][]byte, 0, int(referenceCapacity)),
		slabs:         make([][]byte, 0, int(maximumSlabs)),
		slabCapacity:  int(slabCapacity),
		reservedBytes: initialReserved,
		reservedLimit: reservedLimit,
	}, nil
}

// tryAdd copies timeline into batch-owned storage. It returns false with no
// error and without publishing the timeline when the caller must consume and
// reset the current batch first.
func (b *timelineSlabBatch) tryAdd(timeline []byte) (bool, error) {
	if len(timeline) == 0 {
		return false, corruptRunf("timeline slab batch has an empty timeline")
	}
	if uint64(len(timeline)) > MaxTimelineBytes {
		return false, runTooLargef("timeline length %d exceeds %d", len(timeline), MaxTimelineBytes)
	}

	recordBytes, ok := checkedAdd(uint64(len(timeline)), timelineSpillLengthBytes+timelineReferenceChargeBytes)
	if !ok {
		return false, runTooLargef("timeline record memory calculation overflows")
	}
	if recordBytes > b.budget {
		return false, fmt.Errorf(
			"%w: timeline record requires %d bytes, budget is %d",
			ErrVerificationResource,
			recordBytes,
			b.budget,
		)
	}
	if len(b.batch) == cap(b.batch) || recordBytes > b.budget-b.logicalUsed {
		if len(b.batch) == 0 {
			return false, fmt.Errorf("%w: empty timeline slab batch cannot admit a valid record", ErrVerificationResource)
		}
		return false, nil
	}

	owned, ok := b.copyTimeline(timeline)
	if !ok {
		if len(b.batch) == 0 {
			return false, fmt.Errorf("%w: empty timeline slab batch exhausted retained capacity", ErrVerificationResource)
		}
		return false, nil
	}

	b.batch = append(b.batch, owned)
	b.logicalUsed += recordBytes
	b.logicalHighWater = max(b.logicalHighWater, b.logicalUsed)
	return true, nil
}

// copyTimeline copies before publishing any batch or cursor state. A newly
// allocated slab is appended only after the copy has completed.
func (b *timelineSlabBatch) copyTimeline(timeline []byte) ([]byte, bool) {
	targetIndex := b.slabIndex
	targetOffset := b.slabOffset
	if len(b.slabs) == 0 {
		targetIndex = 0
		targetOffset = 0
	} else if len(timeline) > b.slabCapacity-targetOffset {
		targetIndex++
		targetOffset = 0
	}
	if targetIndex > len(b.slabs) || targetIndex >= cap(b.slabs) {
		return nil, false
	}

	var slab []byte
	newSlab := targetIndex == len(b.slabs)
	if newSlab {
		nextReserved, ok := checkedAdd(b.reservedBytes, uint64(b.slabCapacity))
		if !ok || nextReserved > b.reservedLimit {
			return nil, false
		}
		slab = make([]byte, b.slabCapacity)
	} else {
		slab = b.slabs[targetIndex]
	}

	end := targetOffset + len(timeline)
	if targetOffset < 0 || end < targetOffset || end > len(slab) {
		return nil, false
	}
	owned := slab[targetOffset:end]
	copy(owned, timeline)

	if newSlab {
		b.slabs = append(b.slabs, slab)
		b.reservedBytes += uint64(b.slabCapacity)
	}
	b.slabIndex = targetIndex
	b.slabOffset = end
	return owned, true
}

// timelines exposes the active slab-backed values for in-place sorting and
// consumption. The returned slice and its elements are invalid after reset.
func (b *timelineSlabBatch) timelines() [][]byte {
	return b.batch
}

// reset retains all backing allocations for reuse. Callers must invoke it
// only after the current batch has been consumed successfully.
func (b *timelineSlabBatch) reset() {
	if len(b.batch) == 0 {
		return
	}
	b.batch = b.batch[:0]
	b.slabIndex = 0
	b.slabOffset = 0
	b.logicalUsed = 0
	b.resets++
}

func (b *timelineSlabBatch) metrics() timelineSlabBatchMetrics {
	return timelineSlabBatchMetrics{
		LogicalUsed:      b.logicalUsed,
		LogicalHighWater: b.logicalHighWater,
		ReservedBytes:    b.reservedBytes,
		ReservedLimit:    b.reservedLimit,
		Slabs:            uint64(len(b.slabs)),
		Resets:           b.resets,
	}
}
