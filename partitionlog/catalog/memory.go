package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// MemoryCatalog is an in-process catalog implementation. It is intended for
// tests and for shaping higher-level writer APIs before a durable backend
// exists.
type MemoryCatalog struct {
	mu         sync.RWMutex
	partitions map[uint32]*memoryPartition
}

type memoryPartition struct {
	state       PartitionState
	writerID    [16]byte
	headVersion uint64
	segments    []SegmentRef
}

type memoryWriterSession struct {
	cat       *MemoryCatalog
	partition uint32
	writerID  [16]byte

	mu          sync.Mutex
	writerEpoch uint64
	headVersion uint64
	state       PartitionState
}

func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{partitions: make(map[uint32]*memoryPartition)}
}

func (c *MemoryCatalog) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (WriterSession, error) {
	state, epoch, headVersion, err := c.acquireWriter(ctx, partition, writerID)
	if err != nil {
		return nil, err
	}
	return &memoryWriterSession{
		cat:         c,
		partition:   partition,
		writerID:    writerID,
		writerEpoch: epoch,
		headVersion: headVersion,
		state:       state,
	}, nil
}

func (c *MemoryCatalog) acquireWriter(ctx context.Context, partition uint32, writerID [16]byte) (PartitionState, uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return PartitionState{}, 0, 0, err
	}
	if err := validateWriterID(writerID); err != nil {
		return PartitionState{}, 0, 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(partition)
	data.state.WriterEpoch++
	data.writerID = writerID
	data.headVersion++
	return data.state, data.state.WriterEpoch, data.headVersion, nil
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

func (c *MemoryCatalog) appendSegment(ctx context.Context, partition uint32, writerID [16]byte, writerEpoch uint64, expectedNextLSN uint64, segment SegmentRef) (PartitionState, uint64, error) {
	if err := ctx.Err(); err != nil {
		return PartitionState{}, 0, err
	}
	if err := validateAppendSegment(partition, expectedNextLSN, writerEpoch, segment); err != nil {
		return PartitionState{}, 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(partition)
	state := data.state

	if retry, ok := idempotentRetry(data, expectedNextLSN, segment); ok {
		return retry, data.headVersion, nil
	}
	if state.WriterEpoch == 0 {
		return PartitionState{}, 0, fmt.Errorf("%w: writer fence not acquired", ErrStaleWriter)
	}
	if writerEpoch < state.WriterEpoch {
		return PartitionState{}, 0, fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, writerEpoch, state.WriterEpoch)
	}
	if writerEpoch > state.WriterEpoch {
		return PartitionState{}, 0, fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, writerEpoch, state.WriterEpoch)
	}
	if writerID != data.writerID {
		return PartitionState{}, 0, fmt.Errorf("%w: writer_id mismatch", ErrStaleWriter)
	}
	if expectedNextLSN != state.NextLSN {
		return PartitionState{}, 0, fmt.Errorf("%w: expected_next_lsn=%d current=%d", ErrConflict, expectedNextLSN, state.NextLSN)
	}
	if segment.BaseLSN != state.NextLSN {
		return PartitionState{}, 0, fmt.Errorf("%w: segment base_lsn=%d current=%d", ErrConflict, segment.BaseLSN, state.NextLSN)
	}
	if last, ok := state.Last(); ok {
		if segment.MinTimestampMS < last.MaxTimestampMS {
			return PartitionState{}, 0, fmt.Errorf("%w: segment min_ts=%d previous max_ts=%d", ErrTimestampOrder, segment.MinTimestampMS, last.MaxTimestampMS)
		}
	}
	if err := ensureUniqueSegment(data.segments, segment); err != nil {
		return PartitionState{}, 0, err
	}

	state.NextLSN = segment.NextLSN()
	if !state.HasLastSegment {
		state.OldestLSN = segment.BaseLSN
	}
	state.LastSegment = segment
	state.HasLastSegment = true
	state.SegmentCount++

	data.state = state
	data.headVersion++
	data.segments = append(data.segments, segment)
	return state, data.headVersion, nil
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

func idempotentRetry(data *memoryPartition, expectedNextLSN uint64, segment SegmentRef) (PartitionState, bool) {
	last, ok := data.state.Last()
	if !ok {
		return PartitionState{}, false
	}
	if expectedNextLSN != last.BaseLSN {
		return PartitionState{}, false
	}
	if last != segment {
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

func (s *memoryWriterSession) Head() PartitionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *memoryWriterSession) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writerEpoch
}

func (s *memoryWriterSession) WriterID() [16]byte {
	return s.writerID
}

func (s *memoryWriterSession) AppendSegment(ctx context.Context, segment SegmentRef) (PartitionState, error) {
	s.mu.Lock()
	partition := s.partition
	writerID := s.writerID
	writerEpoch := s.writerEpoch
	expectedNextLSN := s.state.NextLSN
	if last, ok := s.state.Last(); ok && segment.BaseLSN == last.BaseLSN {
		expectedNextLSN = segment.BaseLSN
	}
	s.mu.Unlock()

	state, headVersion, err := s.cat.appendSegment(ctx, partition, writerID, writerEpoch, expectedNextLSN, segment)
	if err != nil {
		return PartitionState{}, err
	}

	s.mu.Lock()
	s.state = state
	s.headVersion = headVersion
	s.mu.Unlock()
	return state, nil
}
