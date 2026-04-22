package catalog

import (
	"context"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const (
	DefaultSegmentPageLimit = 128
	MaxSegmentPageLimit     = 1024
)

// Catalog stores partition-level segment references. Implementations must make
// AppendSegment atomic per partition. The read API is intentionally bounded:
// callers can load the partition head, find one covering segment, or page
// through segment refs. No method returns the full partition history.
type Catalog interface {
	LoadPartition(ctx context.Context, partition uint32) (PartitionState, error)
	AppendSegment(ctx context.Context, req AppendSegmentRequest) (PartitionState, error)
	FindSegment(ctx context.Context, partition uint32, lsn uint64) (SegmentRef, bool, error)
	ListSegments(ctx context.Context, req ListSegmentsRequest) (SegmentPage, error)
}

// PartitionState is the bounded head state for one partition. Segment history
// is read through ListSegments or FindSegment, not embedded here.
type PartitionState struct {
	Partition      uint32
	NextLSN        uint64
	OldestLSN      uint64
	WriterEpoch    uint64
	SegmentCount   uint64
	LastSegment    SegmentRef
	HasLastSegment bool
}

func (s PartitionState) Last() (SegmentRef, bool) {
	if !s.HasLastSegment {
		return SegmentRef{}, false
	}
	return s.LastSegment, true
}

// SegmentRef is the durable catalog record for one committed segment object.
type SegmentRef struct {
	URI              string
	Partition        uint32
	WriterEpoch      uint64
	SegmentUUID      [16]byte
	WriterTag        [16]byte
	BaseLSN          uint64
	LastLSN          uint64
	MinTimestampMS   int64
	MaxTimestampMS   int64
	RecordCount      uint32
	BlockCount       uint32
	SizeBytes        uint64
	BlockIndexOffset uint64
	BlockIndexLength uint32
	Codec            segformat.Codec
	HashAlgo         segformat.HashAlgo
	SegmentHash      uint64
	TrailerHash      uint64
}

func (s SegmentRef) Validate() error {
	if s.URI == "" {
		return fmt.Errorf("%w: empty uri", ErrInvalidSegment)
	}
	if s.SegmentUUID == ([16]byte{}) {
		return fmt.Errorf("%w: empty segment_uuid", ErrInvalidSegment)
	}
	if s.BaseLSN > s.LastLSN {
		return fmt.Errorf("%w: base_lsn=%d last_lsn=%d", ErrInvalidSegment, s.BaseLSN, s.LastLSN)
	}
	if s.LastLSN == math.MaxUint64 {
		return fmt.Errorf("%w: %w: last_lsn=%d", ErrInvalidSegment, ErrLSNExhausted, s.LastLSN)
	}
	wantRecords := s.LastLSN - s.BaseLSN + 1
	if s.RecordCount == 0 || uint64(s.RecordCount) != wantRecords {
		return fmt.Errorf("%w: record_count=%d want=%d", ErrInvalidSegment, s.RecordCount, wantRecords)
	}
	if s.BlockCount == 0 {
		return fmt.Errorf("%w: empty block_count", ErrInvalidSegment)
	}
	if s.SizeBytes == 0 {
		return fmt.Errorf("%w: empty size_bytes", ErrInvalidSegment)
	}
	if s.BlockIndexLength == 0 {
		return fmt.Errorf("%w: empty block_index_length", ErrInvalidSegment)
	}
	if s.MinTimestampMS > s.MaxTimestampMS {
		return fmt.Errorf("%w: min_timestamp_ms=%d max_timestamp_ms=%d", ErrInvalidSegment, s.MinTimestampMS, s.MaxTimestampMS)
	}
	if err := s.Codec.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSegment, err)
	}
	if err := s.HashAlgo.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSegment, err)
	}
	return nil
}

func (s SegmentRef) NextLSN() uint64 {
	return s.LastLSN + 1
}

type AppendSegmentRequest struct {
	Partition       uint32
	ExpectedNextLSN uint64
	WriterEpoch     uint64
	Segment         SegmentRef
}

func (r AppendSegmentRequest) validate() error {
	if r.Partition != r.Segment.Partition {
		return fmt.Errorf("%w: request partition=%d segment partition=%d", ErrInvalidRequest, r.Partition, r.Segment.Partition)
	}
	if r.WriterEpoch != r.Segment.WriterEpoch {
		return fmt.Errorf("%w: request writer_epoch=%d segment writer_epoch=%d", ErrInvalidRequest, r.WriterEpoch, r.Segment.WriterEpoch)
	}
	if r.ExpectedNextLSN != r.Segment.BaseLSN {
		return fmt.Errorf("%w: expected_next_lsn=%d segment base_lsn=%d", ErrConflict, r.ExpectedNextLSN, r.Segment.BaseLSN)
	}
	if err := r.Segment.Validate(); err != nil {
		return err
	}
	return nil
}

type ListSegmentsRequest struct {
	Partition uint32
	FromLSN   uint64
	Limit     int
}

func (r ListSegmentsRequest) normalizedLimit() int {
	switch {
	case r.Limit <= 0:
		return DefaultSegmentPageLimit
	case r.Limit > MaxSegmentPageLimit:
		return MaxSegmentPageLimit
	default:
		return r.Limit
	}
}

type SegmentPage struct {
	Segments []SegmentRef
	NextLSN  uint64
	HasMore  bool
}

func (p SegmentPage) Clone() SegmentPage {
	p.Segments = append([]SegmentRef(nil), p.Segments...)
	return p
}
