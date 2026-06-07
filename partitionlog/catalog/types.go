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

type PartitionState = pmeta.PartitionHead
type SegmentRef = pmeta.SegmentRef
type SegmentPage = pmeta.SegmentPage

// Reader exposes the bounded read-only catalog surface.
type Reader interface {
	LoadPartition(ctx context.Context, partition uint32) (PartitionState, error)
	FindSegment(ctx context.Context, partition uint32, lsn uint64) (SegmentRef, bool, error)
	ListSegments(ctx context.Context, req ListSegmentsRequest) (SegmentPage, error)
}

// WriterManager issues one fenced writer session for one partition.
type WriterManager interface {
	OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (WriterSession, error)
}

// WriterSession owns one fenced append flow for a partition. It carries any
// backend-specific hot state needed for steady-state append.
//
// A WriterSession is not safe for concurrent use. Higher layers should publish
// segments through one ordered commit loop.
type WriterSession interface {
	Head() PartitionState
	Epoch() uint64
	WriterID() [16]byte
	AppendSegment(ctx context.Context, segment SegmentRef) (PartitionState, error)
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

func validateWriterID(writerID [16]byte) error {
	if writerID == ([16]byte{}) {
		return fmt.Errorf("%w: empty writer_id", ErrInvalidRequest)
	}
	return nil
}

func validateAppendSegment(partition uint32, expectedNextLSN uint64, writerEpoch uint64, segment SegmentRef) error {
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
