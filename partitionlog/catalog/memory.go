package catalog

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

// MemoryCatalog is an in-process catalog implementation. It is intended for
// tests and for shaping higher-level writer APIs before a durable backend
// exists.
type MemoryCatalog struct {
	mu         sync.RWMutex
	streamID   string
	partitions map[uint32]*memoryPartition
	retention  map[uint32]RetentionRequest
}

type memoryPartition struct {
	state       pmeta.PartitionHead
	writerID    [16]byte
	headVersion uint64
	segments    []pmeta.SegmentRef
}

type memoryWriterSession struct {
	cat       *MemoryCatalog
	partition uint32
	writerID  [16]byte

	mu          sync.Mutex
	writerEpoch uint64
	headVersion uint64
	state       pmeta.PartitionHead
}

// NewMemoryCatalog returns an in-process catalog for tests and local tools.
func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{
		partitions: make(map[uint32]*memoryPartition),
		retention:  make(map[uint32]RetentionRequest),
	}
}

// NewMemory is the short constructor for the in-process catalog.
func NewMemory() *MemoryCatalog {
	return NewMemoryCatalog()
}

func (c *MemoryCatalog) InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if nextLSN == math.MaxUint64 {
		return pmeta.PartitionHead{}, false, fmt.Errorf("%w: next_lsn exhausted", ErrInvalidRequest)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	if data, ok := c.partitions[partition]; ok {
		return data.state, false, nil
	}
	state := pmeta.PartitionHead{
		StreamID:  c.streamID,
		Partition: partition,
		NextLSN:   nextLSN,
		OldestLSN: nextLSN,
	}
	c.partitions[partition] = &memoryPartition{state: state}
	return state, true, nil
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

func (c *MemoryCatalog) acquireWriter(ctx context.Context, partition uint32, writerID [16]byte) (pmeta.PartitionHead, uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, 0, 0, err
	}
	if err := ValidateWriterID(writerID); err != nil {
		return pmeta.PartitionHead{}, 0, 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(partition)
	data.state.StreamID = c.streamID
	data.state.WriterEpoch++
	data.writerID = writerID
	data.headVersion++
	return data.state, data.state.WriterEpoch, data.headVersion, nil
}

func (c *MemoryCatalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[partition]
	if !ok {
		return pmeta.PartitionHead{StreamID: c.streamID, Partition: partition}, nil
	}
	return data.state, nil
}

func (c *MemoryCatalog) RequestRetention(ctx context.Context, partition uint32, request RetentionRequest) (RetentionRequest, error) {
	if err := ctx.Err(); err != nil {
		return RetentionRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return RetentionRequest{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retention == nil {
		c.retention = make(map[uint32]RetentionRequest)
	}
	current, ok := c.retention[partition]
	if ok {
		switch {
		case request.PolicyVersion == current.PolicyVersion && request.BeforeLSN == current.BeforeLSN:
			return current, nil
		case request.PolicyVersion <= current.PolicyVersion:
			return RetentionRequest{}, fmt.Errorf("%w: policy_version=%d current=%d", ErrRetentionRegression, request.PolicyVersion, current.PolicyVersion)
		case request.BeforeLSN < current.BeforeLSN:
			return RetentionRequest{}, fmt.Errorf("%w: before_lsn=%d current=%d", ErrRetentionRegression, request.BeforeLSN, current.BeforeLSN)
		}
	}
	c.retention[partition] = request
	return request, nil
}

func (c *MemoryCatalog) LoadRetentionRequest(ctx context.Context, partition uint32) (RetentionRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return RetentionRequest{}, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	request, ok := c.retention[partition]
	return request, ok, nil
}

func (c *MemoryCatalog) applyPendingRetention(ctx context.Context, partition uint32, writerID [16]byte, writerEpoch uint64) (RetentionApplyResult, uint64, error) {
	if err := ctx.Err(); err != nil {
		return RetentionApplyResult{}, 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(partition)
	state := data.state
	if state.WriterEpoch != writerEpoch || data.writerID != writerID {
		return RetentionApplyResult{}, 0, fmt.Errorf("%w: writer fence moved", ErrStaleWriter)
	}
	request, ok := c.retention[partition]
	if !ok || request.PolicyVersion <= state.AppliedRetentionVersion {
		return RetentionApplyResult{Head: state, Request: request}, data.headVersion, nil
	}
	if request.BeforeLSN < state.AppliedRetentionLSN {
		return RetentionApplyResult{}, 0, fmt.Errorf("%w: before_lsn=%d applied=%d", ErrRetentionRegression, request.BeforeLSN, state.AppliedRetentionLSN)
	}

	target := request.BeforeLSN
	if target > state.NextLSN {
		target = state.NextLSN
	}
	if target > state.OldestLSN && len(data.segments) > 0 {
		start := firstSegmentAtOrAfter(data.segments, target)
		if start == len(data.segments) {
			data.segments = nil
			state.OldestLSN = state.NextLSN
		} else {
			state.OldestLSN = data.segments[start].BaseLSN
			data.segments = append([]pmeta.SegmentRef(nil), data.segments[start:]...)
		}
	}
	state.AppliedRetentionLSN = target
	state.AppliedRetentionVersion = request.PolicyVersion
	data.state = state
	data.headVersion++
	return RetentionApplyResult{Head: state, Request: request, Applied: true}, data.headVersion, nil
}

func (c *MemoryCatalog) appendSegment(ctx context.Context, partition uint32, writerID [16]byte, writerEpoch uint64, expectedNextLSN uint64, segment pmeta.SegmentRef, knownRetry bool) (pmeta.PartitionHead, uint64, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, 0, err
	}
	if err := ValidateAppendSegment(c.streamID, partition, expectedNextLSN, writerEpoch, segment); err != nil {
		return pmeta.PartitionHead{}, 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[uint32]*memoryPartition)
	}
	data := c.getOrCreateLocked(partition)
	state := data.state

	if segment.WriterTag != writerID {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: segment writer_tag does not match writer_id", ErrInvalidRequest)
	}
	if retry, ok := idempotentRetry(data, expectedNextLSN, segment); ok {
		return retry, data.headVersion, nil
	}
	if knownRetry && segment.BaseLSN < state.OldestLSN {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: partition=%d: segment base_lsn=%d is below retained oldest_lsn=%d", ErrCommitIndeterminate, partition, segment.BaseLSN, state.OldestLSN)
	}
	if state.WriterEpoch == 0 {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: writer fence not acquired", ErrStaleWriter)
	}
	if writerEpoch < state.WriterEpoch {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, writerEpoch, state.WriterEpoch)
	}
	if writerEpoch > state.WriterEpoch {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, writerEpoch, state.WriterEpoch)
	}
	if writerID != data.writerID {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: writer_id mismatch", ErrStaleWriter)
	}
	if expectedNextLSN != state.NextLSN {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: expected_next_lsn=%d current=%d", ErrConflict, expectedNextLSN, state.NextLSN)
	}
	if segment.BaseLSN != state.NextLSN {
		return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: segment base_lsn=%d current=%d", ErrConflict, segment.BaseLSN, state.NextLSN)
	}
	if last, ok := state.Last(); ok {
		if segment.MinTimestampMS < last.MaxTimestampMS {
			return pmeta.PartitionHead{}, 0, fmt.Errorf("%w: segment min_ts=%d previous max_ts=%d", ErrTimestampOrder, segment.MinTimestampMS, last.MaxTimestampMS)
		}
	}
	if err := ensureUniqueSegment(data.segments, segment); err != nil {
		return pmeta.PartitionHead{}, 0, err
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

func (c *MemoryCatalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[partition]
	if !ok {
		return pmeta.SegmentRef{}, false, nil
	}
	i := firstSegmentAtOrAfter(data.segments, lsn)
	if i == len(data.segments) {
		return pmeta.SegmentRef{}, false, nil
	}
	segment := data.segments[i]
	if lsn < segment.BaseLSN || lsn > segment.LastLSN {
		return pmeta.SegmentRef{}, false, nil
	}
	return segment, true, nil
}

func (c *MemoryCatalog) LookupTimestamp(ctx context.Context, req TimestampLookupRequest) (TimestampLookupResult, error) {
	if err := ctx.Err(); err != nil {
		return TimestampLookupResult{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[req.Partition]
	if !ok {
		return TimestampLookupResult{
			Head: pmeta.PartitionHead{StreamID: c.streamID, Partition: req.Partition},
		}, nil
	}
	result := TimestampLookupResult{Head: data.state}
	i := firstSegmentAtOrAfterTimestamp(data.segments, req.TimestampMS)
	if i == len(data.segments) {
		return result, nil
	}
	result.Segment = data.segments[i]
	result.Found = true
	return result, nil
}

func (c *MemoryCatalog) ListSegments(ctx context.Context, req ListSegmentsRequest) (pmeta.SegmentPage, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentPage{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.partitions[req.Partition]
	if !ok {
		return pmeta.SegmentPage{}, nil
	}
	limit := req.NormalizedLimit()
	start := firstSegmentAtOrAfter(data.segments, req.FromLSN)
	if start == len(data.segments) {
		return pmeta.SegmentPage{}, nil
	}
	end := start + limit
	if end > len(data.segments) {
		end = len(data.segments)
	}
	page := pmeta.SegmentPage{
		Segments: append([]pmeta.SegmentRef(nil), data.segments[start:end]...),
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
	data = &memoryPartition{state: pmeta.PartitionHead{StreamID: c.streamID, Partition: partition}}
	c.partitions[partition] = data
	return data
}

func firstSegmentAtOrAfter(segments []pmeta.SegmentRef, lsn uint64) int {
	return sort.Search(len(segments), func(i int) bool {
		return segments[i].LastLSN >= lsn
	})
}

func firstSegmentAtOrAfterTimestamp(segments []pmeta.SegmentRef, timestampMS int64) int {
	return sort.Search(len(segments), func(i int) bool {
		return segments[i].MaxTimestampMS >= timestampMS
	})
}

func idempotentRetry(data *memoryPartition, expectedNextLSN uint64, segment pmeta.SegmentRef) (pmeta.PartitionHead, bool) {
	if expectedNextLSN != segment.BaseLSN {
		return pmeta.PartitionHead{}, false
	}
	i := firstSegmentAtOrAfter(data.segments, segment.BaseLSN)
	if i == len(data.segments) {
		return pmeta.PartitionHead{}, false
	}
	if data.segments[i] != segment {
		return pmeta.PartitionHead{}, false
	}
	return data.state, true
}

func ensureUniqueSegment(segments []pmeta.SegmentRef, segment pmeta.SegmentRef) error {
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

func (s *memoryWriterSession) Head() pmeta.PartitionHead {
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

func (s *memoryWriterSession) AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	s.mu.Lock()
	partition := s.partition
	writerID := s.writerID
	writerEpoch := s.writerEpoch
	retryState := s.state
	knownRetry := false
	expectedNextLSN := s.state.NextLSN
	if last, ok := s.state.Last(); ok && segment == last {
		expectedNextLSN = segment.BaseLSN
		knownRetry = true
	} else if last, ok := s.state.Last(); ok && segment.BaseLSN == last.BaseLSN {
		expectedNextLSN = segment.BaseLSN
	}
	s.mu.Unlock()

	state, headVersion, err := s.cat.appendSegment(ctx, partition, writerID, writerEpoch, expectedNextLSN, segment, knownRetry)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	if knownRetry {
		// Reconciliation confirms this session's old commit; it does not rebase
		// the stale writer onto the current partition head.
		state = retryState
	}

	s.mu.Lock()
	s.state = state
	s.headVersion = headVersion
	s.mu.Unlock()
	return state, nil
}

func (s *memoryWriterSession) ApplyPendingRetention(ctx context.Context) (RetentionApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, headVersion, err := s.cat.applyPendingRetention(ctx, s.partition, s.writerID, s.writerEpoch)
	if err != nil {
		return RetentionApplyResult{}, err
	}
	s.state = result.Head
	s.headVersion = headVersion
	return result, nil
}
