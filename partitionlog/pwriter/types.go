package pwriter

import (
	"context"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

const (
	DefaultMaxSegmentRecords  uint32 = 16_384
	DefaultMaxSegmentRawBytes uint64 = 64 << 20
)

type UUIDGen func() ([16]byte, error)

type WriterIdentity struct {
	Epoch uint64
	Tag   [16]byte
}

type Snapshot struct {
	Head     pmeta.PartitionHead
	Identity WriterIdentity
}

type PublishRequest struct {
	ExpectedNextLSN uint64
	Segment         pmeta.SegmentRef
}

type Session interface {
	Snapshot() Snapshot
	PublishSegment(ctx context.Context, req PublishRequest) (Snapshot, error)
}

type RollPolicy struct {
	MaxSegmentRecords  uint32
	MaxSegmentRawBytes uint64
}

type Options struct {
	Session     Session
	SinkFactory SinkFactory

	SegmentOptions segwriter.Options
	Roll           RollPolicy

	Clock   func() time.Time
	UUIDGen UUIDGen
}

type Record struct {
	TimestampMS int64
	Value       []byte
}

type AppendResult struct {
	LSN   uint64
	Flush *FlushResult
}

type FlushResult struct {
	Snapshot Snapshot
	Segment  pmeta.SegmentRef
}

type SegmentInfo struct {
	Partition     uint32
	BaseLSN       uint64
	WriterEpoch   uint64
	WriterTag     [16]byte
	SegmentUUID   [16]byte
	CreatedUnixMS int64
}

type SinkFactory interface {
	NewSegmentSink(ctx context.Context, info SegmentInfo) (segwriter.Sink, error)
}

type SinkFactoryFunc func(ctx context.Context, info SegmentInfo) (segwriter.Sink, error)

func (f SinkFactoryFunc) NewSegmentSink(ctx context.Context, info SegmentInfo) (segwriter.Sink, error) {
	return f(ctx, info)
}
