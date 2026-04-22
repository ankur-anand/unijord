package pwriter

import (
	"context"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

const (
	DefaultMaxSegmentRecords  uint32 = 16_384
	DefaultMaxSegmentRawBytes uint64 = 64 << 20
)

type UUIDGen func() ([16]byte, error)

type Options struct {
	Partition   uint32
	Catalog     catalog.Catalog
	SinkFactory SinkFactory

	WriterEpoch uint64
	WriterTag   [16]byte

	SegmentOptions segwriter.Options

	MaxSegmentRecords  uint32
	MaxSegmentRawBytes uint64

	Clock   func() time.Time
	UUIDGen UUIDGen
}

type Record struct {
	TimestampMS int64
	Value       []byte
}

type AppendResult struct {
	LSN     uint64
	Flushed bool
	Flush   FlushResult
}

type FlushResult struct {
	Flushed bool
	State   catalog.PartitionState
	Segment catalog.SegmentRef
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
