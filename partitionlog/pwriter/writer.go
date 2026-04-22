package pwriter

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

type Writer struct {
	opts Options

	state catalog.PartitionState

	active *activeSegment

	nextLSN uint64

	hasTimestamp  bool
	lastTimestamp int64

	closed  bool
	aborted bool
}

type activeSegment struct {
	writer   *segwriter.Writer
	baseLSN  uint64
	records  uint32
	rawBytes uint64
}

func DefaultOptions(partition uint32, cat catalog.Catalog, factory SinkFactory) Options {
	segmentOptions := segwriter.DefaultOptions(partition)
	return Options{
		Partition:          partition,
		Catalog:            cat,
		SinkFactory:        factory,
		SegmentOptions:     segmentOptions,
		MaxSegmentRecords:  DefaultMaxSegmentRecords,
		MaxSegmentRawBytes: DefaultMaxSegmentRawBytes,
		Clock:              func() time.Time { return time.Now().UTC() },
		UUIDGen:            randomUUID,
	}
}

func New(ctx context.Context, opts Options) (*Writer, error) {
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	state, err := normalized.Catalog.LoadPartition(ctx, normalized.Partition)
	if err != nil {
		return nil, err
	}
	if normalized.WriterEpoch < state.WriterEpoch {
		return nil, fmt.Errorf("%w: writer_epoch=%d current=%d", catalog.ErrStaleWriter, normalized.WriterEpoch, state.WriterEpoch)
	}

	w := &Writer{
		opts:    normalized,
		state:   state,
		nextLSN: state.NextLSN,
	}
	if last, ok := state.Last(); ok {
		w.hasTimestamp = true
		w.lastTimestamp = last.MaxTimestampMS
	}
	return w, nil
}

func (w *Writer) Append(ctx context.Context, record Record) (AppendResult, error) {
	if w.closed {
		return AppendResult{}, ErrClosed
	}
	if w.aborted {
		return AppendResult{}, ErrAborted
	}
	if w.nextLSN == math.MaxUint64 {
		return AppendResult{}, w.abortWith(ctx, catalog.ErrLSNExhausted)
	}
	if w.hasTimestamp && record.TimestampMS < w.lastTimestamp {
		err := fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, record.TimestampMS, w.lastTimestamp)
		return AppendResult{}, w.abortWith(ctx, err)
	}

	recordSize := uint64(segformat.RecordHeaderSize + len(record.Value))
	if w.shouldRollBefore(recordSize) {
		if _, err := w.flushActive(ctx); err != nil {
			return AppendResult{}, w.abortWith(ctx, err)
		}
	}
	if w.active == nil {
		if err := w.startSegment(ctx); err != nil {
			return AppendResult{}, w.abortWith(ctx, err)
		}
	}

	lsn := w.nextLSN
	if err := w.active.writer.Append(ctx, segwriter.Record{
		LSN:         lsn,
		TimestampMS: record.TimestampMS,
		Value:       record.Value,
	}); err != nil {
		return AppendResult{}, w.abortWith(ctx, err)
	}

	w.nextLSN++
	w.hasTimestamp = true
	w.lastTimestamp = record.TimestampMS
	w.active.records++
	w.active.rawBytes += recordSize

	result := AppendResult{LSN: lsn}
	if w.shouldRollAfter() {
		flush, err := w.flushActive(ctx)
		if err != nil {
			return AppendResult{}, w.abortWith(ctx, err)
		}
		result.Flushed = flush.Flushed
		result.Flush = flush
	}
	return result, nil
}

func (w *Writer) Flush(ctx context.Context) (FlushResult, error) {
	if w.closed {
		return FlushResult{}, ErrClosed
	}
	if w.aborted {
		return FlushResult{}, ErrAborted
	}
	result, err := w.flushActive(ctx)
	if err != nil {
		return FlushResult{}, w.abortWith(ctx, err)
	}
	return result, nil
}

func (w *Writer) Close(ctx context.Context) (FlushResult, error) {
	if w.closed {
		return FlushResult{}, ErrClosed
	}
	if w.aborted {
		return FlushResult{}, ErrAborted
	}
	result, err := w.flushActive(ctx)
	if err != nil {
		return FlushResult{}, w.abortWith(ctx, err)
	}
	w.closed = true
	return result, nil
}

func (w *Writer) Abort(ctx context.Context) error {
	if w.closed || w.aborted {
		return nil
	}
	w.aborted = true
	if w.active != nil {
		return w.active.writer.Abort(ctx)
	}
	return nil
}

func (w *Writer) State() catalog.PartitionState {
	return w.state
}

func (w *Writer) startSegment(ctx context.Context) error {
	segmentUUID, err := w.opts.UUIDGen()
	if err != nil {
		return err
	}
	createdUnixMS := w.opts.Clock().UnixMilli()
	info := SegmentInfo{
		Partition:     w.opts.Partition,
		BaseLSN:       w.nextLSN,
		WriterEpoch:   w.opts.WriterEpoch,
		WriterTag:     w.opts.WriterTag,
		SegmentUUID:   segmentUUID,
		CreatedUnixMS: createdUnixMS,
	}
	sink, err := w.opts.SinkFactory.NewSegmentSink(ctx, info)
	if err != nil {
		return err
	}
	segmentOptions := w.opts.SegmentOptions
	segmentOptions.Partition = w.opts.Partition
	segmentOptions.SegmentUUID = segmentUUID
	segmentOptions.WriterTag = w.opts.WriterTag
	segmentOptions.CreatedUnixMS = createdUnixMS

	sw, err := segwriter.New(segmentOptions, sink)
	if err != nil {
		return err
	}
	w.active = &activeSegment{
		writer:  sw,
		baseLSN: w.nextLSN,
	}
	return nil
}

func (w *Writer) flushActive(ctx context.Context) (FlushResult, error) {
	if w.active == nil || w.active.records == 0 {
		return FlushResult{State: w.state}, nil
	}
	active := w.active

	result, err := active.writer.Close(ctx)
	if err != nil {
		return FlushResult{}, err
	}
	w.active = nil
	segment := segmentRefFromResult(result, w.opts.WriterEpoch)
	state, err := w.opts.Catalog.AppendSegment(ctx, catalog.AppendSegmentRequest{
		Partition:       w.opts.Partition,
		ExpectedNextLSN: active.baseLSN,
		WriterEpoch:     w.opts.WriterEpoch,
		Segment:         segment,
	})
	if err != nil {
		return FlushResult{}, err
	}

	w.state = state
	w.nextLSN = state.NextLSN
	return FlushResult{
		Flushed: true,
		State:   state,
		Segment: segment,
	}, nil
}

func segmentRefFromResult(result segwriter.Result, writerEpoch uint64) catalog.SegmentRef {
	m := result.Metadata
	return catalog.SegmentRef{
		URI:              result.Object.URI,
		Partition:        m.Partition,
		WriterEpoch:      writerEpoch,
		SegmentUUID:      m.SegmentUUID,
		WriterTag:        result.Trailer.WriterTag,
		BaseLSN:          m.BaseLSN,
		LastLSN:          m.LastLSN,
		MinTimestampMS:   m.MinTimestampMS,
		MaxTimestampMS:   m.MaxTimestampMS,
		RecordCount:      m.RecordCount,
		BlockCount:       m.BlockCount,
		SizeBytes:        result.Object.SizeBytes,
		BlockIndexOffset: m.BlockIndexOffset,
		BlockIndexLength: m.BlockIndexLength,
		Codec:            m.Codec,
		HashAlgo:         m.HashAlgo,
		SegmentHash:      m.SegmentHash,
		TrailerHash:      m.TrailerHash,
	}
}

func (w *Writer) shouldRollBefore(nextRecordSize uint64) bool {
	if w.active == nil || w.active.records == 0 {
		return false
	}
	if w.opts.MaxSegmentRawBytes > 0 && w.active.rawBytes+nextRecordSize > w.opts.MaxSegmentRawBytes {
		return true
	}
	return false
}

func (w *Writer) shouldRollAfter() bool {
	if w.active == nil {
		return false
	}
	if w.opts.MaxSegmentRecords > 0 && w.active.records >= w.opts.MaxSegmentRecords {
		return true
	}
	if w.opts.MaxSegmentRawBytes > 0 && w.active.rawBytes >= w.opts.MaxSegmentRawBytes {
		return true
	}
	return false
}

func (w *Writer) abortWith(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	w.aborted = true
	if w.active != nil {
		_ = w.active.writer.Abort(ctx)
	}
	return err
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.Catalog == nil {
		return Options{}, fmt.Errorf("%w: catalog is nil", ErrInvalidOptions)
	}
	if opts.SinkFactory == nil {
		return Options{}, fmt.Errorf("%w: sink factory is nil", ErrInvalidOptions)
	}
	if isZeroSegmentOptions(opts.SegmentOptions) {
		opts.SegmentOptions = segwriter.DefaultOptions(opts.Partition)
	}
	opts.SegmentOptions.Partition = opts.Partition
	if opts.MaxSegmentRecords == 0 {
		opts.MaxSegmentRecords = DefaultMaxSegmentRecords
	}
	if opts.MaxSegmentRawBytes == 0 {
		opts.MaxSegmentRawBytes = DefaultMaxSegmentRawBytes
	}
	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Now().UTC() }
	}
	if opts.UUIDGen == nil {
		opts.UUIDGen = randomUUID
	}
	return opts, nil
}

func isZeroSegmentOptions(opts segwriter.Options) bool {
	return opts.Partition == 0 &&
		opts.Codec == 0 &&
		opts.HashAlgo == 0 &&
		opts.TargetBlockSize == 0 &&
		opts.PartSize == 0 &&
		opts.SealParallelism == 0 &&
		opts.BlockBufferCount == 0 &&
		opts.UploadParallelism == 0 &&
		opts.UploadQueueSize == 0 &&
		opts.UploadLimiter == nil &&
		opts.SegmentUUID == ([16]byte{}) &&
		opts.WriterTag == ([16]byte{}) &&
		opts.CreatedUnixMS == 0
}

func randomUUID() ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return [16]byte{}, err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}
