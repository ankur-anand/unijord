package catalog

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

const (
	DefaultSegmentPageLimit = pmeta.DefaultSegmentPageLimit
	MaxSegmentPageLimit     = pmeta.MaxSegmentPageLimit
)

// Reader exposes the bounded read-only catalog surface.
type Reader interface {
	LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error)
	FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error)
	ListSegments(ctx context.Context, req ListSegmentsRequest) (pmeta.SegmentPage, error)
}

// WriterManager owns the write-side catalog surface for one partition.
type WriterManager interface {
	// InitializePartition creates an empty partition head at a chosen next LSN
	// when the partition does not already exist.
	InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error)

	// OpenWriter issues one fenced writer session for one partition.
	OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (WriterSession, error)
}

// WriterSession owns one fenced append flow for a partition. It carries any
// backend-specific hot state needed for steady-state append.
//
// A WriterSession is not safe for concurrent use. Higher layers should publish
// segments through one ordered commit loop.
type WriterSession interface {
	Head() pmeta.PartitionHead
	Epoch() uint64
	WriterID() [16]byte
	AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error)
}

type ListSegmentsRequest struct {
	Partition uint32
	FromLSN   uint64
	Limit     int
}

func (r ListSegmentsRequest) NormalizedLimit() int {
	return r.normalizedLimit()
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

func ValidateWriterID(writerID [16]byte) error {
	if writerID == ([16]byte{}) {
		return fmt.Errorf("%w: empty writer_id", ErrInvalidRequest)
	}
	return nil
}

func ValidateAppendSegment(streamID string, partition uint32, expectedNextLSN uint64, writerEpoch uint64, segment pmeta.SegmentRef) error {
	if streamID != segment.StreamID {
		return fmt.Errorf("%w: request stream_id=%q segment stream_id=%q", ErrInvalidRequest, streamID, segment.StreamID)
	}
	if partition != segment.Partition {
		return fmt.Errorf("%w: request partition=%d segment partition=%d", ErrInvalidRequest, partition, segment.Partition)
	}
	if writerEpoch != segment.WriterEpoch {
		return fmt.Errorf("%w: request writer_epoch=%d segment writer_epoch=%d", ErrInvalidRequest, writerEpoch, segment.WriterEpoch)
	}
	if expectedNextLSN != segment.BaseLSN {
		return fmt.Errorf("%w: expected_next_lsn=%d segment base_lsn=%d", ErrConflict, expectedNextLSN, segment.BaseLSN)
	}
	if err := segment.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSegment, err)
	}
	return nil
}
