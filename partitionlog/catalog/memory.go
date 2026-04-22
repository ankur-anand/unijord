package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// MemoryCatalog is an in-process Catalog implementation. It is intended for
// tests and for shaping higher-level writer APIs before a durable catalog
// backend exists.
type MemoryCatalog struct {
	mu         sync.RWMutex
	partitions map[uint32]*memoryPartition
}

type memoryPartition struct {
	state    PartitionState
	segments []SegmentRef
}

func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{partitions: make(map[uint32]*memoryPartition)}
}

func (c *MemoryCatalog) LoadPartition(ctx context.Context, partition uint32) (PartitionState, error) {
	if err := ctx.Err(); err != nil {
		return PartitionState{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[partition]
	if !ok {
		return PartitionState{Partition: partition}, nil
	}
	return data.state, nil
}

func (c *MemoryCatalog) AppendSegment(ctx context.Context, req AppendSegmentRequest) (PartitionState, error) {
	if err := ctx.Err(); err != nil {
		return PartitionState{}, err
	}
	if err := req.validate(); err != nil {
		return PartitionState{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(req.Partition)
	state := data.state

	if retry, ok := idempotentRetry(data, req); ok {
		return retry, nil
	}
	if req.WriterEpoch < state.WriterEpoch {
		return PartitionState{}, fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, req.WriterEpoch, state.WriterEpoch)
	}
	if req.ExpectedNextLSN != state.NextLSN {
		return PartitionState{}, fmt.Errorf("%w: expected_next_lsn=%d current=%d", ErrConflict, req.ExpectedNextLSN, state.NextLSN)
	}
	if req.Segment.BaseLSN != state.NextLSN {
		return PartitionState{}, fmt.Errorf("%w: segment base_lsn=%d current=%d", ErrConflict, req.Segment.BaseLSN, state.NextLSN)
	}
	if last, ok := state.Last(); ok {
		if req.Segment.MinTimestampMS < last.MaxTimestampMS {
			return PartitionState{}, fmt.Errorf("%w: segment min_ts=%d previous max_ts=%d", ErrTimestampOrder, req.Segment.MinTimestampMS, last.MaxTimestampMS)
		}
	}
	if err := ensureUniqueSegment(data.segments, req.Segment); err != nil {
		return PartitionState{}, err
	}

	state.WriterEpoch = req.WriterEpoch
	state.NextLSN = req.Segment.NextLSN()
	if !state.HasLastSegment {
		state.OldestLSN = req.Segment.BaseLSN
	}
	state.LastSegment = req.Segment
	state.HasLastSegment = true
	state.SegmentCount++

	data.state = state
	data.segments = append(data.segments, req.Segment)
	return state, nil
}

func (c *MemoryCatalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (SegmentRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[partition]
	if !ok {
		return SegmentRef{}, false, nil
	}
	i := firstSegmentAtOrAfter(data.segments, lsn)
	if i == len(data.segments) {
		return SegmentRef{}, false, nil
	}
	segment := data.segments[i]
	if lsn < segment.BaseLSN || lsn > segment.LastLSN {
		return SegmentRef{}, false, nil
	}
	return segment, true, nil
}

func (c *MemoryCatalog) ListSegments(ctx context.Context, req ListSegmentsRequest) (SegmentPage, error) {
	if err := ctx.Err(); err != nil {
		return SegmentPage{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[req.Partition]
	if !ok {
		return SegmentPage{}, nil
	}
	limit := req.normalizedLimit()
	start := firstSegmentAtOrAfter(data.segments, req.FromLSN)
	if start == len(data.segments) {
		return SegmentPage{}, nil
	}
	end := start + limit
	if end > len(data.segments) {
		end = len(data.segments)
	}
	page := SegmentPage{
		Segments: append([]SegmentRef(nil), data.segments[start:end]...),
		HasMore:  end < len(data.segments),
	}
	if page.HasMore {
		page.NextLSN = data.segments[end].BaseLSN
	}
	return page, nil
}

func (c *MemoryCatalog) getOrCreateLocked(partition uint32) *memoryPartition {
	data, ok := c.partitions[partition]
	if ok {
		return data
	}
	data = &memoryPartition{state: PartitionState{Partition: partition}}
	c.partitions[partition] = data
	return data
}

func firstSegmentAtOrAfter(segments []SegmentRef, lsn uint64) int {
	return sort.Search(len(segments), func(i int) bool {
		return segments[i].LastLSN >= lsn
	})
}

func idempotentRetry(data *memoryPartition, req AppendSegmentRequest) (PartitionState, bool) {
	last, ok := data.state.Last()
	if !ok {
		return PartitionState{}, false
	}
	if req.ExpectedNextLSN != last.BaseLSN {
		return PartitionState{}, false
	}
	if last != req.Segment {
		return PartitionState{}, false
	}
	return data.state, true
}

func ensureUniqueSegment(segments []SegmentRef, segment SegmentRef) error {
	for _, existing := range segments {
		if existing.SegmentUUID == segment.SegmentUUID {
			return fmt.Errorf("%w: duplicate segment_uuid", ErrConflict)
		}
		if existing.URI == segment.URI {
			return fmt.Errorf("%w: duplicate uri", ErrConflict)
		}
	}
	return nil
}
