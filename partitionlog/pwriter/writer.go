package pwriter

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

// Writer owns one partition's append flow. It is not safe for concurrent use.
type Writer struct {
	opts Options

	state Snapshot

	active *activeSegment

	nextLSN uint64

	hasTimestamp  bool
	lastTimestamp int64

	firstErr error
	closed   bool
	aborted  bool
}

type activeSegment struct {
	writer   *segwriter.Writer
	baseLSN  uint64
	records  uint32
	rawBytes uint64
}

func DefaultOptions(factory SinkFactory) Options {
	return Options{
		SinkFactory: factory,
		Roll: RollPolicy{
			MaxSegmentRecords:  DefaultMaxSegmentRecords,
			MaxSegmentRawBytes: DefaultMaxSegmentRawBytes,
		},
		Clock:   func() time.Time { return time.Now().UTC() },
		UUIDGen: randomUUID,
	}
}

func New(opts Options) (*Writer, error) {
	if opts.Session == nil {
		return nil, fmt.Errorf("%w: session is nil", ErrInvalidOptions)
	}
	snapshot := opts.Session.Snapshot()
	normalized, err := normalizeOptions(opts, snapshot)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		opts:    normalized,
		state:   snapshot,
		nextLSN: snapshot.Head.NextLSN,
	}
	if last, ok := snapshot.Head.Last(); ok {
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
		return AppendResult{}, w.abortWith(ctx, ErrLSNExhausted)
	}
	if w.hasTimestamp && record.TimestampMS < w.lastTimestamp {
		err := fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, record.TimestampMS, w.lastTimestamp)
		return AppendResult{}, w.abortWith(ctx, err)
	}

	recordSize := uint64(segformat.RecordHeaderSize + len(record.Value))
	if w.shouldRollBefore(recordSize) {
		flush, err := w.flushActive(ctx)
		if err != nil {
			return AppendResult{}, w.abortWith(ctx, err)
		}
		_ = flush
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
		return AppendResult{}, w.abortWith(ctx, wrapSegmentWrite(err))
	}

	w.nextLSN++
	w.hasTimestamp = true
	// Timestamp order is local writer state. If a later flush fails, the writer
	// becomes terminal, so there is no rollback path to maintain.
	w.lastTimestamp = record.TimestampMS
	w.active.records++
	w.active.rawBytes += recordSize

	result := AppendResult{LSN: lsn}
	if w.shouldRollAfter() {
		flush, err := w.flushActive(ctx)
		if err != nil {
			return AppendResult{}, w.abortWith(ctx, err)
		}
		result.Flush = flush
	}
	return result, nil
}

func (w *Writer) Flush(ctx context.Context) (*FlushResult, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if w.aborted {
		return nil, ErrAborted
	}
	result, err := w.flushActive(ctx)
	if err != nil {
		return nil, w.abortWith(ctx, err)
	}
	return result, nil
}

func (w *Writer) Close(ctx context.Context) (*FlushResult, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if w.aborted {
		return nil, ErrAborted
	}
	result, err := w.flushActive(ctx)
	if err != nil {
		return nil, w.abortWith(ctx, err)
	}
	w.closed = true
	return result, nil
}

func (w *Writer) Abort(ctx context.Context) error {
	if w.closed || w.aborted {
		return nil
	}
	w.aborted = true
	w.setFirstErr(ErrAborted)
	if w.active == nil {
		return nil
	}
	err := w.active.writer.Abort(ctx)
	w.active = nil
	if err != nil {
		return wrapSegmentWrite(err)
	}
	return nil
}

func (w *Writer) State() Snapshot {
	return w.state
}

func (w *Writer) Err() error {
	return w.firstErr
}

func (w *Writer) startSegment(ctx context.Context) error {
	segmentUUID, err := w.opts.UUIDGen()
	if err != nil {
		return wrapSegmentWrite(err)
	}
	createdUnixMS := w.opts.Clock().UnixMilli()
	info := SegmentInfo{
		Partition:     w.state.Head.Partition,
		BaseLSN:       w.nextLSN,
		WriterEpoch:   w.state.Identity.Epoch,
		WriterTag:     w.state.Identity.Tag,
		SegmentUUID:   segmentUUID,
		CreatedUnixMS: createdUnixMS,
	}
	sink, err := w.opts.SinkFactory.NewSegmentSink(ctx, info)
	if err != nil {
		return wrapSegmentWrite(err)
	}
	segmentOptions := w.opts.SegmentOptions
	segmentOptions.Partition = w.state.Head.Partition
	segmentOptions.SegmentUUID = segmentUUID
	segmentOptions.WriterTag = w.state.Identity.Tag
	segmentOptions.CreatedUnixMS = createdUnixMS

	sw, err := segwriter.New(segmentOptions, sink)
	if err != nil {
		return wrapSegmentWrite(err)
	}
	w.active = &activeSegment{
		writer:  sw,
		baseLSN: w.nextLSN,
	}
	return nil
}

func (w *Writer) flushActive(ctx context.Context) (*FlushResult, error) {
	if w.active == nil {
		return nil, nil
	}
	if w.active.records == 0 {
		_ = w.active.writer.Abort(ctx)
		w.active = nil
		return nil, nil
	}
	active := w.active

	result, err := active.writer.Close(ctx)
	if err != nil {
		return nil, wrapSegmentWrite(err)
	}
	w.active = nil

	segment := segmentRefFromResult(result, w.state.Identity)
	if err := segment.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSegmentWriteFailed, err)
	}

	next, err := w.opts.Session.PublishSegment(ctx, PublishRequest{
		ExpectedNextLSN: active.baseLSN,
		Segment:         segment,
	})
	if err != nil {
		return nil, normalizePublishErr(err)
	}
	if err := validatePublishedSnapshot(w.state, next, segment); err != nil {
		return nil, err
	}

	w.state = next
	w.nextLSN = next.Head.NextLSN
	return &FlushResult{
		Snapshot: next,
		Segment:  segment,
	}, nil
}

func segmentRefFromResult(result segwriter.Result, identity WriterIdentity) pmeta.SegmentRef {
	m := result.Metadata
	return pmeta.SegmentRef{
		URI:              result.Object.URI,
		Partition:        m.Partition,
		WriterEpoch:      identity.Epoch,
		SegmentUUID:      m.SegmentUUID,
		WriterTag:        identity.Tag,
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
	if w.opts.Roll.MaxSegmentRawBytes > 0 {
		if w.active.rawBytes >= w.opts.Roll.MaxSegmentRawBytes {
			return true
		}
		return nextRecordSize > w.opts.Roll.MaxSegmentRawBytes-w.active.rawBytes
	}
	return false
}

func (w *Writer) shouldRollAfter() bool {
	if w.active == nil {
		return false
	}
	if w.opts.Roll.MaxSegmentRecords > 0 && w.active.records >= w.opts.Roll.MaxSegmentRecords {
		return true
	}
	if w.opts.Roll.MaxSegmentRawBytes > 0 && w.active.rawBytes >= w.opts.Roll.MaxSegmentRawBytes {
		return true
	}
	return false
}

func (w *Writer) abortWith(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	w.aborted = true
	w.setFirstErr(err)
	if w.active != nil {
		_ = w.active.writer.Abort(ctx)
		w.active = nil
	}
	return err
}

func (w *Writer) setFirstErr(err error) {
	if err == nil || w.firstErr != nil {
		return
	}
	w.firstErr = err
}

func normalizeOptions(opts Options, snapshot Snapshot) (Options, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return Options{}, err
	}
	if opts.SinkFactory == nil {
		return Options{}, fmt.Errorf("%w: sink factory is nil", ErrInvalidOptions)
	}
	if isZeroSegmentOptions(opts.SegmentOptions) {
		opts.SegmentOptions = segwriter.DefaultOptions(snapshot.Head.Partition)
	}
	opts.SegmentOptions.Partition = snapshot.Head.Partition
	if opts.Roll.MaxSegmentRecords == 0 {
		opts.Roll.MaxSegmentRecords = DefaultMaxSegmentRecords
	}
	if opts.Roll.MaxSegmentRawBytes == 0 {
		opts.Roll.MaxSegmentRawBytes = DefaultMaxSegmentRawBytes
	}
	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Now().UTC() }
	}
	if opts.UUIDGen == nil {
		opts.UUIDGen = randomUUID
	}
	return opts, nil
}

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.Identity.Epoch == 0 {
		return fmt.Errorf("%w: empty writer epoch", ErrInvalidSession)
	}
	if err := validateHead(snapshot.Head); err != nil {
		return err
	}
	switch {
	case snapshot.Identity.Epoch < snapshot.Head.WriterEpoch:
		return fmt.Errorf("%w: writer_epoch=%d current=%d", ErrStaleWriter, snapshot.Identity.Epoch, snapshot.Head.WriterEpoch)
	case snapshot.Identity.Epoch > snapshot.Head.WriterEpoch:
		return fmt.Errorf("%w: writer_epoch=%d current=%d", ErrInvalidSession, snapshot.Identity.Epoch, snapshot.Head.WriterEpoch)
	default:
		return nil
	}
}

func validateHead(head pmeta.PartitionHead) error {
	if head.WriterEpoch == 0 {
		return fmt.Errorf("%w: empty head writer epoch", ErrInvalidSession)
	}
	if head.NextLSN < head.OldestLSN {
		return fmt.Errorf("%w: next_lsn=%d oldest_lsn=%d", ErrInvalidSession, head.NextLSN, head.OldestLSN)
	}
	if !head.HasLastSegment {
		if head.SegmentCount != 0 {
			return fmt.Errorf("%w: segment_count=%d without last segment", ErrInvalidSession, head.SegmentCount)
		}
		return nil
	}
	if head.SegmentCount == 0 {
		return fmt.Errorf("%w: missing segment count for last segment", ErrInvalidSession)
	}
	if err := head.LastSegment.Validate(); err != nil {
		return fmt.Errorf("%w: last segment: %w", ErrInvalidSession, err)
	}
	if head.LastSegment.Partition != head.Partition {
		return fmt.Errorf("%w: head partition=%d last segment partition=%d", ErrInvalidSession, head.Partition, head.LastSegment.Partition)
	}
	if head.NextLSN != head.LastSegment.NextLSN() {
		return fmt.Errorf("%w: next_lsn=%d last_segment_next_lsn=%d", ErrInvalidSession, head.NextLSN, head.LastSegment.NextLSN())
	}
	return nil
}

func validatePublishedSnapshot(current Snapshot, next Snapshot, segment pmeta.SegmentRef) error {
	if err := validateHead(next.Head); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPublishResult, err)
	}
	if next.Head.Partition != current.Head.Partition {
		return fmt.Errorf("%w: partition=%d current_partition=%d", ErrInvalidPublishResult, next.Head.Partition, current.Head.Partition)
	}
	if next.Identity != current.Identity {
		return fmt.Errorf("%w: identity changed from %+v to %+v", ErrInvalidPublishResult, current.Identity, next.Identity)
	}
	if next.Head.WriterEpoch != current.Identity.Epoch {
		return fmt.Errorf("%w: head writer_epoch=%d identity epoch=%d", ErrInvalidPublishResult, next.Head.WriterEpoch, current.Identity.Epoch)
	}
	if !next.Head.HasLastSegment {
		return fmt.Errorf("%w: missing last segment", ErrInvalidPublishResult)
	}
	if next.Head.LastSegment != segment {
		return fmt.Errorf("%w: published segment does not match returned last segment", ErrInvalidPublishResult)
	}
	if next.Head.NextLSN != segment.NextLSN() {
		return fmt.Errorf("%w: next_lsn=%d segment_next_lsn=%d", ErrInvalidPublishResult, next.Head.NextLSN, segment.NextLSN())
	}
	return nil
}

func wrapSegmentWrite(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSegmentWriteFailed) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrSegmentWriteFailed, err)
}

func normalizePublishErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrStaleWriter),
		errors.Is(err, ErrPublishFailed),
		errors.Is(err, ErrPublishIndeterminate),
		errors.Is(err, ErrInvalidPublishResult):
		return err
	default:
		return fmt.Errorf("%w: %w", ErrPublishFailed, err)
	}
}

func isZeroSegmentOptions(opts segwriter.Options) bool {
	return opts == segwriter.Options{}
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
