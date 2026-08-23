package catalog

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

const (
	DefaultSegmentPageLimit        = pmeta.DefaultSegmentPageLimit
	MaxSegmentPageLimit            = pmeta.MaxSegmentPageLimit
	RetentionRequestVersion uint16 = 1
)

// Reader exposes the bounded read-only catalog surface.
type Reader interface {
	LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error)
	FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error)
	LookupTimestamp(ctx context.Context, req TimestampLookupRequest) (TimestampLookupResult, error)
	ListSegments(ctx context.Context, req ListSegmentsRequest) (pmeta.SegmentPage, error)
}

// TimestampLookupRequest asks for the earliest retained segment whose maximum
// timestamp is at least TimestampMS. Catalogs rely on their global
// nondecreasing timestamp invariant to answer this without scanning by LSN.
type TimestampLookupRequest struct {
	Partition   uint32
	TimestampMS int64
}

// TimestampLookupResult returns the segment and the exact partition-head
// snapshot against which it was selected. Found is false when the partition is
// empty or TimestampMS is newer than every retained segment.
type TimestampLookupResult struct {
	Head    pmeta.PartitionHead
	Segment pmeta.SegmentRef
	Found   bool
}

// WriterManager owns the write-side catalog surface for one partition.
type WriterManager interface {
	// InitializePartition creates an empty partition head at a chosen next LSN
	// when the partition does not already exist.
	InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error)

	// OpenWriter issues one fenced writer session for one partition. writerID
	// identifies one writer incarnation and must not be shared by concurrent
	// writers. A replacement writer uses a new ID.
	OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (WriterSession, error)
}

// WriterSession owns one fenced append flow for a partition. It carries any
// backend-specific hot state needed for steady-state append.
//
// A WriterSession is not safe for concurrent use. Higher layers should publish
// segments through one ordered commit loop.
type WriterSession interface {
	// Head returns the session's cached catalog snapshot. It is not a
	// linearizable read of the current partition head; use Reader.LoadPartition
	// when the latest authoritative state is required.
	Head() pmeta.PartitionHead
	Epoch() uint64
	WriterID() [16]byte

	// AppendSegment returns the catalog state produced by the acknowledged
	// commit. When an ambiguous commit is reconciled from later history, the
	// returned state can precede the partition's current authoritative head.
	AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error)
}

// RetentionManager stores the latest desired retention boundary for each
// partition. It never changes partition visibility itself.
type RetentionManager interface {
	RequestRetention(ctx context.Context, partition uint32, request RetentionRequest) (RetentionRequest, error)
	LoadRetentionRequest(ctx context.Context, partition uint32) (RetentionRequest, bool, error)
}

// RetentionWriterSession is implemented by writer sessions that can apply a
// pending retention request through the same fenced head mutation path used
// for segment publication.
type RetentionWriterSession interface {
	ApplyPendingRetention(ctx context.Context) (RetentionApplyResult, error)
}

// RetentionRequest is the latest monotonic retention command for one
// partition. BeforeLSN means records below that LSN should be retired at
// immutable-segment granularity.
type RetentionRequest struct {
	Version       uint16 `json:"version"`
	PolicyVersion uint64 `json:"policy_version"`
	BeforeLSN     uint64 `json:"before_lsn"`
	CreatedUnixMS int64  `json:"created_unix_ms"`
}

func (r RetentionRequest) Validate() error {
	if r.Version != RetentionRequestVersion {
		return fmt.Errorf("%w: retention version=%d want=%d", ErrInvalidRequest, r.Version, RetentionRequestVersion)
	}
	if r.PolicyVersion == 0 {
		return fmt.Errorf("%w: empty retention policy_version", ErrInvalidRequest)
	}
	if r.CreatedUnixMS < 0 {
		return fmt.Errorf("%w: negative retention created_unix_ms=%d", ErrInvalidRequest, r.CreatedUnixMS)
	}
	return nil
}

type RetentionApplyResult struct {
	Head    pmeta.PartitionHead
	Request RetentionRequest
	Applied bool
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
