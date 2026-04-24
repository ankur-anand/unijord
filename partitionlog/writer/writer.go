package writer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

// Writer owns one partition's append flow. It is not safe for concurrent use.
type Writer struct {
	mu   sync.Mutex
	opts Options

	partition uint32
	identity  WriterIdentity

	committed         Snapshot
	optimisticNextLSN uint64

	active     *activeSegment
	nextCutSeq uint64

	hasTimestamp  bool
	lastTimestamp int64

	inflightSegments int
	inflightBytes    uint64

	detached []detachedSegment
	ready    []readySegment

	firstErr        error
	firstErrSurface bool
	closed          bool
	aborted         bool

	stateWake    chan struct{}
	finalizeWake chan struct{}
	publishWake  chan struct{}

	workerCtx    context.Context
	workerCancel context.CancelFunc
	workersWG    sync.WaitGroup
}

type activeSegment struct {
	writer   *segwriter.Writer
	baseLSN  uint64
	records  uint32
	rawBytes uint64
}

type detachedSegment struct {
	seq      uint64
	baseLSN  uint64
	estBytes uint64
	writer   *segwriter.Writer
}

type readySegment struct {
	seq             uint64
	expectedNextLSN uint64
	estBytes        uint64
	sizeBytes       uint64
	segment         pmeta.SegmentRef
}

func DefaultOptions(factory SinkFactory) Options {
	return Options{
		SinkFactory: factory,
		Roll: RollPolicy{
			MaxSegmentRecords:  DefaultMaxSegmentRecords,
			MaxSegmentRawBytes: DefaultMaxSegmentRawBytes,
		},
		Queue: QueuePolicy{
			MaxInflightSegments: DefaultMaxInflightSegments,
			MaxInflightBytes:    DefaultMaxInflightBytes,
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

	workerCtx, workerCancel := context.WithCancel(context.Background())
	w := &Writer{
		opts:              normalized,
		partition:         snapshot.Head.Partition,
		identity:          snapshot.Identity,
		committed:         snapshot,
		optimisticNextLSN: snapshot.Head.NextLSN,
		stateWake:         make(chan struct{}, 1),
		finalizeWake:      make(chan struct{}, 1),
		publishWake:       make(chan struct{}, 1),
		workerCtx:         workerCtx,
		workerCancel:      workerCancel,
	}
	if last, ok := snapshot.Head.Last(); ok {
		w.hasTimestamp = true
		w.lastTimestamp = last.MaxTimestampMS
	}
	w.workersWG.Add(2)
	go w.finalizeLoop()
	go w.publishLoop()
	return w, nil
}

func (w *Writer) Append(ctx context.Context, record Record) (AppendResult, error) {
	w.mu.Lock()
	if err := w.foregroundErrLocked(); err != nil {
		w.mu.Unlock()
		return AppendResult{}, err
	}
	if w.optimisticNextLSN == math.MaxUint64 {
		err := fmt.Errorf("%w: next_lsn=%d", ErrLSNExhausted, w.optimisticNextLSN)
		active, detached := w.failLocked(err)
		w.mu.Unlock()
		w.abortSegments(ctx, active, detached)
		return AppendResult{}, err
	}
	if w.hasTimestamp && record.TimestampMS < w.lastTimestamp {
		err := fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, record.TimestampMS, w.lastTimestamp)
		active, detached := w.failLocked(err)
		w.mu.Unlock()
		w.abortSegments(ctx, active, detached)
		return AppendResult{}, err
	}

	recordSize := uint64(segformat.RecordHeaderSize + len(record.Value))
	if w.shouldCutBeforeLocked(recordSize) {
		if err := w.cutLocked(ctx); err != nil {
			w.mu.Unlock()
			return AppendResult{}, err
		}
	}
	if w.active == nil {
		if err := w.startSegmentLocked(ctx); err != nil {
			w.mu.Unlock()
			return AppendResult{}, err
		}
	}

	lsn := w.optimisticNextLSN
	if err := w.active.writer.Append(ctx, segwriter.Record{
		LSN:         lsn,
		TimestampMS: record.TimestampMS,
		Value:       record.Value,
	}); err != nil {
		err = wrapSegmentWrite(err)
		active, detached := w.failLocked(err)
		w.mu.Unlock()
		w.abortSegments(ctx, active, detached)
		return AppendResult{}, err
	}

	w.optimisticNextLSN++
	w.hasTimestamp = true
	w.lastTimestamp = record.TimestampMS
	w.active.records++
	w.active.rawBytes += recordSize

	if w.shouldCutAfterLocked() {
		_ = w.cutLocked(ctx)
	}

	w.mu.Unlock()
	return AppendResult{LSN: lsn}, nil
}

func (w *Writer) Cut(ctx context.Context) error {
	w.mu.Lock()
	if err := w.foregroundErrLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	err := w.cutLocked(ctx)
	w.mu.Unlock()
	return err
}

func (w *Writer) Flush(ctx context.Context) (Snapshot, error) {
	var emptyActive *activeSegment

	w.mu.Lock()
	if err := w.foregroundErrLocked(); err != nil {
		w.mu.Unlock()
		return Snapshot{}, err
	}
	if w.active != nil && w.active.records > 0 {
		if err := w.detachActiveLocked(ctx); err != nil {
			w.mu.Unlock()
			return Snapshot{}, err
		}
	}
	if w.active != nil && w.active.records == 0 {
		emptyActive = w.active
		w.active = nil
	}
	w.mu.Unlock()

	if emptyActive != nil {
		_ = emptyActive.writer.Abort(ctx)
	}

	w.mu.Lock()
	if err := w.waitDrainedLocked(ctx); err != nil {
		w.mu.Unlock()
		return Snapshot{}, err
	}
	snapshot := w.committed
	w.mu.Unlock()
	return snapshot, nil
}

func (w *Writer) Close(ctx context.Context) (Snapshot, error) {
	var emptyActive *activeSegment

	w.mu.Lock()
	if err := w.foregroundErrLocked(); err != nil {
		w.mu.Unlock()
		return Snapshot{}, err
	}
	if w.active != nil && w.active.records > 0 {
		if err := w.detachActiveLocked(ctx); err != nil {
			w.mu.Unlock()
			return Snapshot{}, err
		}
	}
	if w.active != nil && w.active.records == 0 {
		emptyActive = w.active
		w.active = nil
	}
	w.mu.Unlock()

	if emptyActive != nil {
		_ = emptyActive.writer.Abort(ctx)
	}

	w.mu.Lock()
	if err := w.waitDrainedLocked(ctx); err != nil {
		w.mu.Unlock()
		return Snapshot{}, err
	}
	w.closed = true
	snapshot := w.committed
	w.workerCancel()
	w.signalAllLocked()
	w.mu.Unlock()

	w.workersWG.Wait()
	return snapshot, nil
}

func (w *Writer) Abort(ctx context.Context) error {
	w.mu.Lock()
	if w.closed || w.aborted {
		w.mu.Unlock()
		return nil
	}
	w.aborted = true
	if w.firstErr == nil {
		w.firstErr = ErrAborted
	}
	w.firstErrSurface = true
	active := w.active
	detached := append([]detachedSegment(nil), w.detached...)
	w.active = nil
	w.detached = nil
	w.workerCancel()
	w.signalAllLocked()
	w.mu.Unlock()

	w.abortSegments(ctx, active, detached)
	if err := waitGroupContext(ctx, &w.workersWG); err != nil {
		return err
	}
	return nil
}

func (w *Writer) State() State {
	w.mu.Lock()
	defer w.mu.Unlock()
	return State{
		Snapshot:          w.committed,
		OptimisticNextLSN: w.optimisticNextLSN,
		InflightSegments:  w.inflightSegments,
		InflightBytes:     w.inflightBytes,
	}
}

func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstErr
}

func (w *Writer) finalizeLoop() {
	defer w.workersWG.Done()

	for {
		w.mu.Lock()
		for len(w.detached) == 0 && w.workerCtx.Err() == nil {
			w.mu.Unlock()
			select {
			case <-w.finalizeWake:
			case <-w.workerCtx.Done():
				return
			}
			w.mu.Lock()
		}
		if len(w.detached) == 0 && w.workerCtx.Err() != nil {
			w.mu.Unlock()
			return
		}
		if len(w.detached) == 0 {
			w.mu.Unlock()
			continue
		}
		item := w.detached[0]
		w.detached = w.detached[1:]
		w.mu.Unlock()

		result, err := item.writer.Close(w.workerCtx)
		if err != nil {
			if w.workerCtx.Err() != nil {
				return
			}
			w.noteAsyncErr(wrapSegmentWrite(err))
			return
		}
		segment := segmentRefFromResult(result, w.identity)
		if err := segment.Validate(); err != nil {
			w.noteAsyncErr(fmt.Errorf("%w: %w", ErrSegmentWriteFailed, err))
			return
		}

		w.mu.Lock()
		switch {
		case result.Object.SizeBytes > item.estBytes:
			w.inflightBytes += result.Object.SizeBytes - item.estBytes
		case item.estBytes > result.Object.SizeBytes:
			diff := item.estBytes - result.Object.SizeBytes
			if w.inflightBytes >= diff {
				w.inflightBytes -= diff
			} else {
				w.inflightBytes = 0
			}
		}
		w.ready = append(w.ready, readySegment{
			seq:             item.seq,
			expectedNextLSN: item.baseLSN,
			estBytes:        item.estBytes,
			sizeBytes:       result.Object.SizeBytes,
			segment:         segment,
		})
		w.signalPublishLocked()
		w.signalStateLocked()
		w.mu.Unlock()
	}
}

func (w *Writer) publishLoop() {
	defer w.workersWG.Done()

	for {
		w.mu.Lock()
		for len(w.ready) == 0 && w.workerCtx.Err() == nil {
			w.mu.Unlock()
			select {
			case <-w.publishWake:
			case <-w.workerCtx.Done():
				return
			}
			w.mu.Lock()
		}
		if len(w.ready) == 0 && w.workerCtx.Err() != nil {
			w.mu.Unlock()
			return
		}
		if len(w.ready) == 0 {
			w.mu.Unlock()
			continue
		}
		item := w.ready[0]
		current := w.committed
		w.mu.Unlock()

		next, err := w.opts.Session.PublishSegment(w.workerCtx, PublishRequest{
			ExpectedNextLSN: item.expectedNextLSN,
			Segment:         item.segment,
		})
		if err != nil {
			w.noteAsyncErr(normalizePublishErr(err))
			return
		}
		if err := validatePublishedSnapshot(current, next, item.segment); err != nil {
			w.noteAsyncErr(err)
			return
		}

		w.mu.Lock()
		if len(w.ready) > 0 && w.ready[0].seq == item.seq {
			w.ready = w.ready[1:]
			if w.inflightSegments > 0 {
				w.inflightSegments--
			}
			if w.inflightBytes >= item.sizeBytes {
				w.inflightBytes -= item.sizeBytes
			} else {
				w.inflightBytes = 0
			}
		}
		w.committed = next
		w.signalStateLocked()
		stop := w.workerCtx.Err() != nil && w.inflightSegments == 0
		w.mu.Unlock()
		if stop {
			return
		}
	}
}

func (w *Writer) cutLocked(ctx context.Context) error {
	if w.active == nil || w.active.records == 0 {
		return nil
	}

	old := w.active
	estBytes := estimateInflightBytes(old.rawBytes, old.records, w.opts.SegmentOptions.Codec)
	if err := w.reserveInflightLocked(ctx, 1, estBytes); err != nil {
		return err
	}
	if err := w.startSegmentLocked(ctx); err != nil {
		w.releaseInflightLocked(1, estBytes)
		return err
	}
	w.detached = append(w.detached, detachedSegment{
		seq:      w.nextCutSeq,
		baseLSN:  old.baseLSN,
		estBytes: estBytes,
		writer:   old.writer,
	})
	w.nextCutSeq++
	w.signalFinalizeLocked()
	return nil
}

func (w *Writer) detachActiveLocked(ctx context.Context) error {
	if w.active == nil || w.active.records == 0 {
		return nil
	}

	old := w.active
	estBytes := estimateInflightBytes(old.rawBytes, old.records, w.opts.SegmentOptions.Codec)
	if err := w.reserveInflightLocked(ctx, 1, estBytes); err != nil {
		return err
	}
	w.active = nil
	w.detached = append(w.detached, detachedSegment{
		seq:      w.nextCutSeq,
		baseLSN:  old.baseLSN,
		estBytes: estBytes,
		writer:   old.writer,
	})
	w.nextCutSeq++
	w.signalFinalizeLocked()
	return nil
}

func (w *Writer) startSegmentLocked(ctx context.Context) error {
	segmentUUID, err := w.opts.UUIDGen()
	if err != nil {
		return wrapSegmentStart(err)
	}
	createdUnixMS := w.opts.Clock().UnixMilli()
	info := SegmentInfo{
		Partition:     w.partition,
		BaseLSN:       w.optimisticNextLSN,
		WriterEpoch:   w.identity.Epoch,
		WriterTag:     w.identity.Tag,
		SegmentUUID:   segmentUUID,
		CreatedUnixMS: createdUnixMS,
	}
	sink, err := w.opts.SinkFactory.NewSegmentSink(ctx, info)
	if err != nil {
		return wrapSegmentStart(err)
	}
	segmentOptions := w.opts.SegmentOptions
	segmentOptions.Partition = w.partition
	segmentOptions.SegmentUUID = segmentUUID
	segmentOptions.WriterTag = w.identity.Tag
	segmentOptions.CreatedUnixMS = createdUnixMS

	sw, err := segwriter.New(segmentOptions, sink)
	if err != nil {
		return wrapSegmentStart(err)
	}
	w.active = &activeSegment{
		writer:  sw,
		baseLSN: w.optimisticNextLSN,
	}
	return nil
}

func (w *Writer) shouldCutBeforeLocked(nextRecordSize uint64) bool {
	if w.active == nil || w.active.records == 0 {
		return false
	}
	if w.opts.Roll.MaxSegmentRecords > 0 && w.active.records >= w.opts.Roll.MaxSegmentRecords {
		return true
	}
	if w.opts.Roll.MaxSegmentRawBytes > 0 {
		if w.active.rawBytes >= w.opts.Roll.MaxSegmentRawBytes {
			return true
		}
		return nextRecordSize > w.opts.Roll.MaxSegmentRawBytes-w.active.rawBytes
	}
	return false
}

func (w *Writer) shouldCutAfterLocked() bool {
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

func (w *Writer) reserveInflightLocked(ctx context.Context, segments int, bytes uint64) error {
	for {
		if err := w.abortedErrLocked(); err != nil {
			return err
		}
		if w.canReserveLocked(segments, bytes) {
			w.inflightSegments += segments
			w.inflightBytes += bytes
			return nil
		}
		w.mu.Unlock()
		select {
		case <-w.stateWake:
		case <-ctx.Done():
			w.mu.Lock()
			return ctx.Err()
		}
		w.mu.Lock()
	}
}

func (w *Writer) canReserveLocked(segments int, bytes uint64) bool {
	if w.opts.Queue.MaxInflightSegments > 0 && w.inflightSegments+segments > w.opts.Queue.MaxInflightSegments {
		return false
	}
	if w.opts.Queue.MaxInflightBytes > 0 && w.inflightBytes+bytes > w.opts.Queue.MaxInflightBytes {
		return false
	}
	return true
}

func (w *Writer) releaseInflightLocked(segments int, bytes uint64) {
	if segments > 0 {
		w.inflightSegments -= segments
		if w.inflightSegments < 0 {
			w.inflightSegments = 0
		}
	}
	if bytes > 0 {
		if w.inflightBytes >= bytes {
			w.inflightBytes -= bytes
		} else {
			w.inflightBytes = 0
		}
	}
	w.signalStateLocked()
}

func (w *Writer) waitDrainedLocked(ctx context.Context) error {
	for {
		if err := w.abortedErrLocked(); err != nil {
			return err
		}
		if w.inflightSegments == 0 {
			return nil
		}
		w.mu.Unlock()
		select {
		case <-w.stateWake:
		case <-ctx.Done():
			w.mu.Lock()
			return ctx.Err()
		}
		w.mu.Lock()
	}
}

func (w *Writer) foregroundErrLocked() error {
	if w.closed {
		return ErrClosed
	}
	return w.abortedErrLocked()
}

func (w *Writer) abortedErrLocked() error {
	if !w.aborted {
		return nil
	}
	if w.firstErr != nil && !w.firstErrSurface {
		w.firstErrSurface = true
		return w.firstErr
	}
	return ErrAborted
}

func (w *Writer) failLocked(err error) (*activeSegment, []detachedSegment) {
	if err == nil {
		return nil, nil
	}
	if w.firstErr == nil {
		w.firstErr = err
	}
	w.firstErrSurface = true
	w.aborted = true
	active := w.active
	detached := append([]detachedSegment(nil), w.detached...)
	w.active = nil
	w.detached = nil
	w.workerCancel()
	w.signalAllLocked()
	return active, detached
}

func (w *Writer) noteAsyncErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.firstErr == nil {
		w.firstErr = err
	}
	w.aborted = true
	active := w.active
	detached := append([]detachedSegment(nil), w.detached...)
	w.active = nil
	w.detached = nil
	w.workerCancel()
	w.signalAllLocked()
	w.mu.Unlock()

	w.abortSegments(context.Background(), active, detached)
}

func (w *Writer) signalStateLocked() {
	select {
	case w.stateWake <- struct{}{}:
	default:
	}
}

func (w *Writer) signalFinalizeLocked() {
	select {
	case w.finalizeWake <- struct{}{}:
	default:
	}
}

func (w *Writer) signalPublishLocked() {
	select {
	case w.publishWake <- struct{}{}:
	default:
	}
}

func (w *Writer) signalAllLocked() {
	w.signalStateLocked()
	w.signalFinalizeLocked()
	w.signalPublishLocked()
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

func wrapSegmentStart(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSegmentStartFailed) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrSegmentStartFailed, err)
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
	if opts.Queue.MaxInflightSegments == 0 {
		opts.Queue.MaxInflightSegments = DefaultMaxInflightSegments
	}
	if opts.Queue.MaxInflightBytes == 0 {
		opts.Queue.MaxInflightBytes = DefaultMaxInflightBytes
	}
	if opts.Queue.MaxInflightSegments < 0 {
		return Options{}, fmt.Errorf("%w: negative max inflight segments %d", ErrInvalidOptions, opts.Queue.MaxInflightSegments)
	}
	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Now().UTC() }
	}
	if opts.UUIDGen == nil {
		opts.UUIDGen = randomUUID
	}
	return opts, nil
}

func estimateInflightBytes(rawBytes uint64, records uint32, codec segformat.Codec) uint64 {
	storedUpper := rawBytes
	if codec == segformat.CodecZstd && rawBytes > 0 {
		storedUpper = satAdd(rawBytes, (rawBytes+3)/4)
	}
	blockCountUpper := uint64(records)
	if blockCountUpper == 0 {
		blockCountUpper = 1
	}
	perBlockOverhead := uint64(segformat.BlockPreambleSize + segformat.BlockIndexEntrySize)
	fixedOverhead := uint64(segformat.FilePreambleSize + segformat.IndexPreambleSize + segformat.TrailerSize)
	return satAdd(fixedOverhead, satAdd(storedUpper, satMul(perBlockOverhead, blockCountUpper)))
}

func satAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func satMul(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
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

func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) abortSegments(ctx context.Context, active *activeSegment, detached []detachedSegment) {
	if active != nil {
		_ = active.writer.Abort(ctx)
	}
	for _, item := range detached {
		_ = item.writer.Abort(ctx)
	}
}
