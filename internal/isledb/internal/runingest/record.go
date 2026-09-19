package runingest

import (
	"errors"
	"math"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

var (
	ErrInvalidRecord = errors.New("runingest: invalid source record")
	ErrBatchLimit    = errors.New("runingest: record exceeds absolute slot limits")
	ErrBatchSealed   = errors.New("runingest: batch is sealed")
	ErrBatchClosed   = errors.New("runingest: batch is closed")
	ErrBatchClock    = errors.New("runingest: residence clock regressed")
	ErrBatchConfig   = errors.New("runingest: invalid batch configuration")
	ErrBatchIndex    = errors.New("runingest: invalid batch index")
)

type RecordFlags uint8

const (
	RecordTimestampPresent RecordFlags = 1 << iota
	RecordSeal
)

// BorrowedHeader is an ordered view, including duplicate/empty keys and the
// nil-versus-empty value distinction. The alias permits the frozen codec to
// inspect it without another header descriptor array. This internal boundary
// is not E14's public API, which must define its own DTOs.
type BorrowedHeader = runcontract.Header

type GapKind uint8

const (
	GapNone          GapKind = iota
	GapReadCommitted         // Broker-confirmed aborted/control records, not filtering.
	GapCompacted             // Broker-confirmed compacted range, not retention loss.
)

// SourceGap attests to the exact missing half-open range. Only the source
// adapter can obtain this evidence; a numeric offset jump alone is not proof.
type SourceGap struct {
	From, To int64
	Kind     GapKind
}

// BorrowedRecord and every referenced byte remain caller-owned and immutable
// only for AppendBorrowed. No field or header slice is retained by the slot.
// EpochPrefixConfirmed attests that the committed prefix survived an epoch
// advance. It never authorizes epoch regression or missing source history.
type BorrowedRecord struct {
	Offset, TimestampMS  int64
	LeaderEpoch          int32
	Flags                RecordFlags
	Timeline, Value      []byte
	Headers              []BorrowedHeader
	Annotations          []byte
	Gap                  SourceGap
	EpochPrefixConfirmed bool
}

func (r BorrowedRecord) event() runcontract.Event {
	kind := uint8(runcontract.Append)
	if r.Flags&RecordSeal != 0 {
		kind = runcontract.Seal
	}
	return runcontract.Event{Kind: kind, TimestampPresent: r.Flags&RecordTimestampPresent != 0,
		Timestamp: r.TimestampMS, Offset: uint64(r.Offset), LeaderEpoch: r.LeaderEpoch,
		Payload: r.Value, Headers: r.Headers, Annotations: r.Annotations}
}

func (r BorrowedRecord) validate(next int64, epoch int32, maxEvent uint64) (int, error) {
	if r.Flags & ^(RecordTimestampPresent|RecordSeal) != 0 || r.Offset < next ||
		r.Offset == math.MaxInt64 || r.LeaderEpoch < -1 ||
		(epoch >= 0 && r.LeaderEpoch < epoch) ||
		(r.LeaderEpoch > epoch && !r.EpochPrefixConfirmed) {
		return 0, ErrInvalidRecord
	}
	if r.Offset == next {
		if r.Gap != (SourceGap{}) {
			return 0, ErrInvalidRecord
		}
	} else if r.Gap.From != next || r.Gap.To != r.Offset ||
		(r.Gap.Kind != GapReadCommitted && r.Gap.Kind != GapCompacted) {
		return 0, ErrInvalidRecord
	}
	if err := validTimeline(r.Timeline); err != nil {
		return 0, errors.Join(ErrInvalidRecord, err)
	}
	n, err := runcontract.EventSize(r.event())
	if err != nil {
		return 0, errors.Join(ErrInvalidRecord, err)
	}
	if uint64(n) > maxEvent {
		return 0, ErrBatchLimit
	}
	return n, nil
}

// recordRef contains no borrowed or owned source slices, assigned LSN, sequence,
// head, or sorting state. Source timestamp presence and kind are in Flags.
type recordRef struct {
	Offset, TimestampMS int64
	Value               arenaRef
	Timeline            timelineID
	LeaderEpoch         int32
	Flags               RecordFlags
}

// SourceInterval counts gaps independently of logical records. FirstObserved
// is -1 until the first successful append. No trailing broker position is kept.
type SourceInterval struct {
	Expected, FirstObserved, Next int64
	ExpectedEpoch, IntervalEpoch  int32
	GapCount, MissingOffsets      uint64
}
