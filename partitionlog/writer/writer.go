package writer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	"github.com/google/uuid"
)

const cleanupTimeout = 5 * time.Second

// Writer owns one partition's append flow. Calls that mutate the writer must
// be serialized. State, Err, and Committed may be used by observer goroutines.
type Writer struct {
	mu        sync.Mutex
	sessionMu sync.Mutex
	opts      Options

	streamID  string
	partition uint32
	identity  WriterIdentity

	committed         Snapshot
	optimisticNextLSN uint64

	active     *activeSegment
	nextCutSeq uint64
	// activeTransitionDone is non-nil while one goroutine owns the active
	// segment transition. The channel is closed when that transition ends.
	activeTransitionDone chan struct{}

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

	stateWake        chan struct{}
	committedChanged chan struct{}
	commitClosed     bool
	finalizeWake     chan struct{}
	publishWake      chan struct{}
	ageWake          chan struct{}

	workerCtx    context.Context
	workerCancel context.CancelCauseFunc
	workersWG    sync.WaitGroup

	drainStarted    bool
	drainDone       chan struct{}
	drainCleanupErr error
	drainWorkerErr  error
}

type activeSegment struct {
	writer        *segwriter.Writer
	baseLSN       uint64
	records       uint32
	rawBytes      uint64
	firstRecordAt time.Time
}

type detachedSegment struct {
	seq      uint64
	baseLSN  uint64
	records  uint32
	rawBytes uint64
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
		Timeouts: OperationTimeouts{
			SegmentFinalize: DefaultSegmentFinalizeTimeout,
			CatalogPublish:  DefaultCatalogPublishTimeout,
		},
		Clock:   SystemClock{},
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

	workerCtx, workerCancel := context.WithCancelCause(context.Background())
	w := &Writer{
		opts:              normalized,
		streamID:          snapshot.Head.StreamID,
		partition:         snapshot.Head.Partition,
		identity:          snapshot.Identity,
		committed:         snapshot,
		optimisticNextLSN: snapshot.Head.NextLSN,
		stateWake:         make(chan struct{}, 1),
		committedChanged:  make(chan struct{}),
		finalizeWake:      make(chan struct{}, 1),
		publishWake:       make(chan struct{}, 1),
		ageWake:           make(chan struct{}, 1),
		workerCtx:         workerCtx,
		workerCancel:      workerCancel,
		drainDone:         make(chan struct{}),
	}
	if last, ok := snapshot.Head.Last(); ok {
		w.hasTimestamp = true
		w.lastTimestamp = last.MaxTimestampMS
	}
	w.workersWG.Add(2)
	go w.finalizeLoop()
	go w.publishLoop()
	if normalized.Roll.MaxSegmentAge > 0 {
		w.workersWG.Add(1)
		go w.ageLoop()
	}
	return w, nil
}

func (w *Writer) Append(ctx context.Context, record Record) (AppendResult, error) {
	w.mu.Lock()
	if err := w.waitActiveTransitionLocked(ctx); err != nil {
		err = w.surfaceReturnedErrLocked(err)
		w.mu.Unlock()
		return AppendResult{}, err
	}
	if w.optimisticNextLSN == math.MaxUint64 {
		err := fmt.Errorf("%w: next_lsn=%d", ErrLSNExhausted, w.optimisticNextLSN)
		w.failLocked(err)
		w.mu.Unlock()
		return AppendResult{}, err
	}
	if w.hasTimestamp && record.TimestampMS < w.lastTimestamp {
		err := fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, record.TimestampMS, w.lastTimestamp)
		w.failLocked(err)
		w.mu.Unlock()
		return AppendResult{}, err
	}

	recordSizeInt, err := segformat.RecordSize(record.Headers, record.Value)
	if err != nil {
		w.failLocked(err)
		w.mu.Unlock()
		return AppendResult{}, err
	}
	recordSize := uint64(recordSizeInt)
	if w.shouldCutBeforeLocked(recordSize) {
		if err := w.cutLocked(ctx); err != nil {
			err = w.surfaceReturnedErrLocked(err)
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
		Headers:     record.Headers,
		Value:       record.Value,
	}); err != nil {
		err = wrapSegmentWrite(err)
		w.failLocked(err)
		w.mu.Unlock()
		return AppendResult{}, err
	}

	firstRecordInSegment := w.active.records == 0
	w.optimisticNextLSN++
	w.hasTimestamp = true
	w.lastTimestamp = record.TimestampMS
	w.active.records++
	w.active.rawBytes += recordSize
	if firstRecordInSegment {
		w.active.firstRecordAt = w.opts.Clock.Now()
		w.signalAgeLocked()
	}

	if w.shouldCutAfterLocked() {
		w.tryCutAfterAppendLocked(ctx)
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
	err = w.surfaceReturnedErrLocked(err)
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
			err = w.surfaceReturnedErrLocked(err)
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
		if err := abortWriterBestEffort(emptyActive.writer); err != nil {
			err = wrapSegmentWrite(err)
			w.noteForegroundErr(err)
			return Snapshot{}, err
		}
	}

	w.mu.Lock()
	if err := w.waitDrainedLocked(ctx); err != nil {
		err = w.surfaceReturnedErrLocked(err)
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
	if w.closed {
		snapshot := w.committed
		w.startDrainLocked(ErrClosed, nil, nil)
		w.mu.Unlock()
		if err := w.waitDrain(ctx); err != nil {
			return Snapshot{}, err
		}
		return snapshot, nil
	}
	if w.aborted {
		err := w.surfaceAbortedErrLocked()
		w.startDrainLocked(w.firstErr, nil, nil)
		w.mu.Unlock()
		return Snapshot{}, errors.Join(err, w.waitDrain(ctx))
	}
	if w.active != nil && w.active.records > 0 {
		if err := w.detachActiveLocked(ctx); err != nil {
			err = w.surfaceReturnedErrLocked(err)
			terminal := w.aborted
			w.mu.Unlock()
			if terminal {
				return Snapshot{}, errors.Join(err, w.waitDrain(ctx))
			}
			return Snapshot{}, err
		}
	}
	if w.active != nil && w.active.records == 0 {
		emptyActive = w.active
		w.active = nil
	}
	w.mu.Unlock()

	if emptyActive != nil {
		if err := abortWriterBestEffort(emptyActive.writer); err != nil {
			err = wrapSegmentWrite(err)
			w.noteForegroundErr(err)
			return Snapshot{}, errors.Join(err, w.waitDrain(ctx))
		}
	}

	w.mu.Lock()
	if err := w.waitDrainedLocked(ctx); err != nil {
		err = w.surfaceReturnedErrLocked(err)
		terminal := w.aborted
		w.mu.Unlock()
		if terminal {
			return Snapshot{}, errors.Join(err, w.waitDrain(ctx))
		}
		return Snapshot{}, err
	}
	w.closed = true
	snapshot := w.committed
	w.startDrainLocked(ErrClosed, nil, nil)
	w.mu.Unlock()

	if err := w.waitDrain(ctx); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (w *Writer) Abort(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.startDrainLocked(ErrClosed, nil, nil)
		w.mu.Unlock()
		return w.waitDrain(ctx)
	}
	var terminalErr error
	if w.aborted {
		if w.firstErr != nil && !w.firstErrSurface {
			terminalErr = w.firstErr
			w.firstErrSurface = true
		}
		w.startDrainLocked(w.firstErr, nil, nil)
		w.mu.Unlock()
		return errors.Join(terminalErr, w.waitDrain(ctx))
	}
	w.aborted = true
	w.firstErr = ErrAborted
	w.firstErrSurface = true
	active := w.active
	detached := append([]detachedSegment(nil), w.detached...)
	w.active = nil
	w.detached = nil
	w.ready = nil
	w.startDrainLocked(ErrAborted, active, detached)
	w.mu.Unlock()
	return w.waitDrain(ctx)
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

// Committed returns a channel that is closed when the committed snapshot
// changes or the writer becomes terminal. Obtain the channel before reading
// State, then call Committed again after every wake. Inspect Err after a wake
// before waiting again.
//
// Closing the channel broadcasts one coalescible notification to all waiters.
// The channel remains closed after Close, Abort, or a terminal writer error.
func (w *Writer) Committed() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.committedChanged
}

func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstErr
}

// ApplyPendingRetention applies the latest retention request through this
// writer's fenced catalog session. It does not poll automatically; the
// partition owner chooses when to call it.
func (w *Writer) ApplyPendingRetention(ctx context.Context) (RetentionResult, error) {
	session, ok := w.opts.Session.(RetentionSession)
	if !ok {
		return RetentionResult{}, ErrRetentionUnsupported
	}

	w.sessionMu.Lock()
	w.mu.Lock()
	if err := w.foregroundErrLocked(); err != nil {
		w.mu.Unlock()
		w.sessionMu.Unlock()
		return RetentionResult{}, err
	}
	current := w.committed
	w.mu.Unlock()

	result, err := session.ApplyPendingRetention(ctx)
	if err != nil {
		w.sessionMu.Unlock()
		err = normalizeRetentionErr(err)
		if errors.Is(err, ErrStaleWriter) {
			w.noteForegroundErr(err)
		}
		return RetentionResult{}, err
	}
	if err := validateRetentionSnapshot(current, result); err != nil {
		w.sessionMu.Unlock()
		w.noteForegroundErr(err)
		return RetentionResult{}, err
	}

	w.mu.Lock()
	w.committed = result.Snapshot
	w.signalStateLocked()
	if result.Snapshot != current {
		w.signalCommittedLocked()
	}
	w.mu.Unlock()
	w.sessionMu.Unlock()
	return result, nil
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

		start := time.Now()
		operationCtx, cancel := context.WithTimeout(w.workerCtx, w.opts.Timeouts.SegmentFinalize)
		result, err := item.writer.Close(operationCtx)
		cancel()
		if err != nil {
			if w.workerCtx.Err() != nil {
				w.noteDrainWorkerErr(err, item)
				return
			}
			w.observe(MetricEvent{
				Name:      MetricSegmentFinalize,
				Partition: w.partition,
				StartLSN:  item.baseLSN,
				Records:   int(item.records),
				Bytes:     item.rawBytes,
				Duration:  time.Since(start),
				Err:       err,
			})
			w.noteAsyncErr(wrapSegmentWrite(err))
			return
		}
		segment := segmentRefFromResult(result, w.streamID, w.identity)
		if err := segment.Validate(); err != nil {
			w.observe(MetricEvent{
				Name:      MetricSegmentFinalize,
				Partition: w.partition,
				StartLSN:  item.baseLSN,
				Records:   int(item.records),
				Bytes:     result.Object.SizeBytes,
				Duration:  time.Since(start),
				Err:       err,
			})
			w.noteAsyncErr(fmt.Errorf("%w: %w", ErrSegmentWriteFailed, err))
			return
		}
		w.observe(MetricEvent{
			Name:       MetricSegmentFinalize,
			Partition:  w.partition,
			StartLSN:   segment.BaseLSN,
			NextLSN:    segment.NextLSN(),
			Records:    int(segment.RecordCount),
			Bytes:      result.Object.SizeBytes,
			SegmentURI: segment.URI,
			Duration:   time.Since(start),
		})

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
		w.mu.Unlock()

		w.sessionMu.Lock()
		w.mu.Lock()
		current := w.committed
		w.mu.Unlock()
		start := time.Now()
		operationCtx, cancel := context.WithTimeout(w.workerCtx, w.opts.Timeouts.CatalogPublish)
		next, err := w.opts.Session.PublishSegment(operationCtx, PublishRequest{
			ExpectedNextLSN: item.expectedNextLSN,
			Segment:         item.segment,
		})
		cancel()
		if err != nil {
			w.sessionMu.Unlock()
			w.observe(MetricEvent{
				Name:       MetricSegmentPublish,
				Partition:  w.partition,
				StartLSN:   item.segment.BaseLSN,
				NextLSN:    item.segment.NextLSN(),
				Records:    int(item.segment.RecordCount),
				Bytes:      item.sizeBytes,
				SegmentURI: item.segment.URI,
				Duration:   time.Since(start),
				Err:        err,
			})
			w.noteAsyncErr(normalizePublishErr(err))
			return
		}
		if err := validatePublishedSnapshot(current, next, item.segment); err != nil {
			w.sessionMu.Unlock()
			w.observe(MetricEvent{
				Name:       MetricSegmentPublish,
				Partition:  w.partition,
				StartLSN:   item.segment.BaseLSN,
				NextLSN:    item.segment.NextLSN(),
				Records:    int(item.segment.RecordCount),
				Bytes:      item.sizeBytes,
				SegmentURI: item.segment.URI,
				Duration:   time.Since(start),
				Err:        err,
			})
			w.noteAsyncErr(err)
			return
		}
		w.observe(MetricEvent{
			Name:       MetricSegmentPublish,
			Partition:  w.partition,
			StartLSN:   item.segment.BaseLSN,
			NextLSN:    item.segment.NextLSN(),
			Records:    int(item.segment.RecordCount),
			Bytes:      item.sizeBytes,
			SegmentURI: item.segment.URI,
			Duration:   time.Since(start),
		})

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
		w.signalCommittedLocked()
		stop := w.workerCtx.Err() != nil && w.inflightSegments == 0
		w.mu.Unlock()
		w.sessionMu.Unlock()
		if stop {
			return
		}
	}
}

func (w *Writer) ageLoop() {
	defer w.workersWG.Done()

	for {
		w.mu.Lock()
		for (w.active == nil || w.active.records == 0) && w.workerCtx.Err() == nil {
			w.mu.Unlock()
			select {
			case <-w.ageWake:
			case <-w.workerCtx.Done():
				return
			}
			w.mu.Lock()
		}
		if w.workerCtx.Err() != nil {
			w.mu.Unlock()
			return
		}

		wait := w.active.firstRecordAt.Add(w.opts.Roll.MaxSegmentAge).Sub(w.opts.Clock.Now())
		if wait > 0 {
			w.mu.Unlock()
			timer := w.opts.Clock.NewTimer(wait)
			select {
			case <-timer.C():
			case <-w.ageWake:
				stopTimer(timer)
			case <-w.workerCtx.Done():
				stopTimer(timer)
				return
			}
			continue
		}

		err := w.cutLocked(w.workerCtx)
		w.mu.Unlock()
		if err != nil {
			if w.workerCtx.Err() != nil {
				return
			}
			w.noteAsyncErr(err)
			return
		}
	}
}

func (w *Writer) cutLocked(ctx context.Context) error {
	if err := w.beginActiveTransitionLocked(ctx); err != nil {
		return err
	}
	defer w.endActiveTransitionLocked()

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
		records:  old.records,
		rawBytes: old.rawBytes,
		estBytes: estBytes,
		writer:   old.writer,
	})
	w.nextCutSeq++
	w.signalFinalizeLocked()
	return nil
}

func (w *Writer) tryCutAfterAppendLocked(ctx context.Context) {
	if err := w.cutLocked(ctx); err != nil {
		// The record is already accepted. Keep the active segment live; the
		// next Append/Cut/Flush/Close will retry the boundary before moving on.
		w.signalStateLocked()
	}
}

func (w *Writer) detachActiveLocked(ctx context.Context) error {
	if err := w.beginActiveTransitionLocked(ctx); err != nil {
		return err
	}
	defer w.endActiveTransitionLocked()

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
		records:  old.records,
		rawBytes: old.rawBytes,
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
	createdUnixMS := w.opts.Clock.Now().UnixMilli()
	info := SegmentInfo{
		StreamID:      w.streamID,
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
	if w.opts.Roll.MaxSegmentAge > 0 && !w.active.firstRecordAt.IsZero() && w.opts.Clock.Now().Sub(w.active.firstRecordAt) >= w.opts.Roll.MaxSegmentAge {
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
	if w.opts.Roll.MaxSegmentAge > 0 && w.active.records > 0 && !w.active.firstRecordAt.IsZero() && w.opts.Clock.Now().Sub(w.active.firstRecordAt) >= w.opts.Roll.MaxSegmentAge {
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

func (w *Writer) waitActiveTransitionLocked(ctx context.Context) error {
	for w.activeTransitionDone != nil {
		done := w.activeTransitionDone
		w.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			w.mu.Lock()
			return ctx.Err()
		}
		w.mu.Lock()
	}
	if w.closed {
		return ErrClosed
	}
	return w.abortedErrLocked()
}

func (w *Writer) beginActiveTransitionLocked(ctx context.Context) error {
	if err := w.waitActiveTransitionLocked(ctx); err != nil {
		return err
	}
	w.activeTransitionDone = make(chan struct{})
	return nil
}

func (w *Writer) endActiveTransitionLocked() {
	done := w.activeTransitionDone
	w.activeTransitionDone = nil
	close(done)
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
	return w.surfaceAbortedErrLocked()
}

func (w *Writer) abortedErrLocked() error {
	if !w.aborted {
		return nil
	}
	if w.firstErr != nil && !w.firstErrSurface {
		return w.firstErr
	}
	return ErrAborted
}

func (w *Writer) surfaceAbortedErrLocked() error {
	if !w.aborted {
		return nil
	}
	if w.firstErr != nil && !w.firstErrSurface {
		w.firstErrSurface = true
		return w.firstErr
	}
	return ErrAborted
}

// surfaceReturnedErrLocked records that a public operation returned the
// asynchronous terminal cause. Internal/background transitions may observe
// the same cause, but must not consume the caller's one useful diagnostic.
func (w *Writer) surfaceReturnedErrLocked(err error) error {
	if err == nil || !w.aborted || w.firstErr == nil || w.firstErrSurface {
		return err
	}
	if errors.Is(err, w.firstErr) {
		w.firstErrSurface = true
	}
	return err
}

func (w *Writer) failLocked(err error) {
	if err == nil {
		return
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
	w.ready = nil
	w.startDrainLocked(w.firstErr, active, detached)
}

func (w *Writer) noteAsyncErr(err error) {
	w.noteTerminalErr(err, false)
}

func (w *Writer) noteForegroundErr(err error) {
	w.noteTerminalErr(err, true)
}

func (w *Writer) noteTerminalErr(err error, surfaced bool) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.firstErr == nil {
		w.firstErr = err
	}
	if surfaced && errors.Is(err, w.firstErr) {
		w.firstErrSurface = true
	}
	w.aborted = true
	active := w.active
	detached := append([]detachedSegment(nil), w.detached...)
	w.active = nil
	w.detached = nil
	w.ready = nil
	w.startDrainLocked(w.firstErr, active, detached)
	w.mu.Unlock()
}

// startDrainLocked transfers shutdown ownership to one persistent coordinator.
// Public Close and Abort calls may stop waiting, but this drain continues and
// later calls join the same completion rather than starting competing cleanup.
func (w *Writer) startDrainLocked(cause error, active *activeSegment, detached []detachedSegment) {
	if w.drainStarted {
		return
	}
	w.drainStarted = true
	w.workerCancel(cause)
	w.signalAllLocked()
	go w.runDrain(active, detached)
}

func (w *Writer) runDrain(active *activeSegment, detached []detachedSegment) {
	cleanupErr := w.abortSegmentsBestEffort(active, detached)
	w.workersWG.Wait()

	w.mu.Lock()
	w.drainCleanupErr = errors.Join(cleanupErr, w.drainWorkerErr)
	close(w.drainDone)
	w.mu.Unlock()
}

func (w *Writer) noteDrainWorkerErr(err error, item detachedSegment) {
	err = withoutContextCancellation(err)
	if err == nil {
		return
	}
	w.mu.Lock()
	w.drainWorkerErr = errors.Join(w.drainWorkerErr, err)
	w.mu.Unlock()
	w.observe(MetricEvent{
		Name:      MetricSegmentCleanup,
		Partition: w.partition,
		StartLSN:  item.baseLSN,
		Records:   int(item.records),
		Bytes:     item.rawBytes,
		Err:       err,
	})
}

func (w *Writer) waitDrain(ctx context.Context) error {
	select {
	case <-w.drainDone:
		w.mu.Lock()
		err := w.drainCleanupErr
		w.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) observe(event MetricEvent) {
	if w.opts.Observer == nil {
		return
	}
	w.opts.Observer.Observe(event)
}

func (w *Writer) signalStateLocked() {
	select {
	case w.stateWake <- struct{}{}:
	default:
	}
}

func (w *Writer) signalCommittedLocked() {
	if w.commitClosed {
		return
	}
	close(w.committedChanged)
	w.committedChanged = make(chan struct{})
}

func (w *Writer) signalCommittedTerminalLocked() {
	if w.commitClosed {
		return
	}
	close(w.committedChanged)
	w.commitClosed = true
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

func (w *Writer) signalAgeLocked() {
	select {
	case w.ageWake <- struct{}{}:
	default:
	}
}

func (w *Writer) signalAllLocked() {
	w.signalStateLocked()
	w.signalCommittedTerminalLocked()
	w.signalFinalizeLocked()
	w.signalPublishLocked()
	w.signalAgeLocked()
}

func segmentRefFromResult(result segwriter.Result, streamID string, identity WriterIdentity) pmeta.SegmentRef {
	m := result.Metadata
	return pmeta.SegmentRef{
		URI:              result.Object.URI,
		StreamID:         streamID,
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
	if head.ReachableSegmentCount > head.SegmentCount {
		return fmt.Errorf("%w: reachable_segment_count=%d exceeds segment_count=%d", ErrInvalidSession, head.ReachableSegmentCount, head.SegmentCount)
	}
	if !head.HasLastSegment {
		if head.SegmentCount != 0 {
			return fmt.Errorf("%w: segment_count=%d without last segment", ErrInvalidSession, head.SegmentCount)
		}
		if head.ReachableSegmentCount != 0 {
			return fmt.Errorf("%w: reachable_segment_count=%d without last segment", ErrInvalidSession, head.ReachableSegmentCount)
		}
		return nil
	}
	if head.SegmentCount == 0 {
		return fmt.Errorf("%w: missing segment count for last segment", ErrInvalidSession)
	}
	if err := head.LastSegment.Validate(); err != nil {
		return fmt.Errorf("%w: last segment: %w", ErrInvalidSession, err)
	}
	if head.LastSegment.StreamID != head.StreamID {
		return fmt.Errorf("%w: head stream_id=%q last segment stream_id=%q", ErrInvalidSession, head.StreamID, head.LastSegment.StreamID)
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
	switch {
	case segment.BaseLSN != current.Head.NextLSN:
		return fmt.Errorf("%w: segment base_lsn=%d current_next_lsn=%d", ErrInvalidPublishResult, segment.BaseLSN, current.Head.NextLSN)
	case current.Head.SegmentCount == math.MaxUint64:
		return fmt.Errorf("%w: segment count exhausted", ErrInvalidPublishResult)
	case next.Head.Partition != current.Head.Partition:
		return fmt.Errorf("%w: partition=%d current_partition=%d", ErrInvalidPublishResult, next.Head.Partition, current.Head.Partition)
	case next.Head.StreamID != current.Head.StreamID:
		return fmt.Errorf("%w: stream_id=%q current_stream_id=%q", ErrInvalidPublishResult, next.Head.StreamID, current.Head.StreamID)
	case next.Identity != current.Identity:
		return fmt.Errorf("%w: identity changed from %+v to %+v", ErrInvalidPublishResult, current.Identity, next.Identity)
	case next.Head.WriterEpoch != current.Identity.Epoch:
		return fmt.Errorf("%w: head writer_epoch=%d identity epoch=%d", ErrInvalidPublishResult, next.Head.WriterEpoch, current.Identity.Epoch)
	case next.Head.SegmentCount != current.Head.SegmentCount+1:
		return fmt.Errorf("%w: segment_count=%d want=%d", ErrInvalidPublishResult, next.Head.SegmentCount, current.Head.SegmentCount+1)
	case next.Head.ReachableSegmentCount != current.Head.ReachableSegmentCount+1:
		return fmt.Errorf("%w: reachable_segment_count=%d want=%d", ErrInvalidPublishResult, next.Head.ReachableSegmentCount, current.Head.ReachableSegmentCount+1)
	case next.Head.OldestLSN != current.Head.OldestLSN:
		return fmt.Errorf("%w: publish changed oldest_lsn from %d to %d", ErrInvalidPublishResult, current.Head.OldestLSN, next.Head.OldestLSN)
	case next.Head.AppliedRetentionLSN != current.Head.AppliedRetentionLSN:
		return fmt.Errorf("%w: publish changed applied_retention_lsn", ErrInvalidPublishResult)
	case next.Head.AppliedRetentionVersion != current.Head.AppliedRetentionVersion:
		return fmt.Errorf("%w: publish changed applied_retention_version", ErrInvalidPublishResult)
	case !next.Head.HasLastSegment:
		return fmt.Errorf("%w: missing last segment", ErrInvalidPublishResult)
	case next.Head.LastSegment != segment:
		return fmt.Errorf("%w: published segment does not match returned last segment", ErrInvalidPublishResult)
	case next.Head.NextLSN != segment.NextLSN():
		return fmt.Errorf("%w: next_lsn=%d segment_next_lsn=%d", ErrInvalidPublishResult, next.Head.NextLSN, segment.NextLSN())
	}
	return nil
}

func validateRetentionSnapshot(current Snapshot, result RetentionResult) error {
	next := result.Snapshot
	if err := validateHead(next.Head); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPublishResult, err)
	}
	switch {
	case next.Identity != current.Identity:
		return fmt.Errorf("%w: retention changed writer identity", ErrInvalidPublishResult)
	case next.Head.StreamID != current.Head.StreamID:
		return fmt.Errorf("%w: retention changed stream_id", ErrInvalidPublishResult)
	case next.Head.Partition != current.Head.Partition:
		return fmt.Errorf("%w: retention changed partition", ErrInvalidPublishResult)
	case next.Head.NextLSN != current.Head.NextLSN:
		return fmt.Errorf("%w: retention changed next_lsn from %d to %d", ErrInvalidPublishResult, current.Head.NextLSN, next.Head.NextLSN)
	case next.Head.SegmentCount != current.Head.SegmentCount:
		return fmt.Errorf("%w: retention changed segment_count", ErrInvalidPublishResult)
	case next.Head.ReachableSegmentCount > current.Head.ReachableSegmentCount:
		return fmt.Errorf("%w: retention increased reachable_segment_count from %d to %d", ErrInvalidPublishResult, current.Head.ReachableSegmentCount, next.Head.ReachableSegmentCount)
	case next.Head.HasLastSegment != current.Head.HasLastSegment || next.Head.LastSegment != current.Head.LastSegment:
		return fmt.Errorf("%w: retention changed last segment", ErrInvalidPublishResult)
	case next.Head.OldestLSN < current.Head.OldestLSN:
		return fmt.Errorf("%w: retention moved oldest_lsn backward", ErrInvalidPublishResult)
	case next.Head.AppliedRetentionVersion < current.Head.AppliedRetentionVersion:
		return fmt.Errorf("%w: retention version moved backward", ErrInvalidPublishResult)
	case next.Head.AppliedRetentionLSN < current.Head.AppliedRetentionLSN:
		return fmt.Errorf("%w: retention lsn moved backward", ErrInvalidPublishResult)
	}
	if result.Applied {
		if result.PolicyVersion == 0 || next.Head.AppliedRetentionVersion != result.PolicyVersion {
			return fmt.Errorf("%w: applied retention policy version mismatch", ErrInvalidPublishResult)
		}
		if next.Head.AppliedRetentionLSN != result.RequestedLSN {
			return fmt.Errorf("%w: applied retention lsn mismatch", ErrInvalidPublishResult)
		}
	} else if next != current {
		return fmt.Errorf("%w: unapplied retention changed snapshot", ErrInvalidPublishResult)
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
	if errors.Is(err, segwriter.ErrTxnCommitIndeterminate) {
		indeterminate := fmt.Errorf("%w: %w", ErrSegmentCommitIndeterminate, err)
		return fmt.Errorf("%w: %w", ErrSegmentWriteFailed, indeterminate)
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

func normalizeRetentionErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrStaleWriter),
		errors.Is(err, ErrRetentionUnsupported),
		errors.Is(err, ErrRetentionFailed):
		return err
	default:
		return fmt.Errorf("%w: %w", ErrRetentionFailed, err)
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
	if opts.Roll.MaxSegmentAge < 0 {
		return Options{}, fmt.Errorf("%w: negative max segment age %s", ErrInvalidOptions, opts.Roll.MaxSegmentAge)
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
	if opts.Timeouts.SegmentFinalize == 0 {
		opts.Timeouts.SegmentFinalize = DefaultSegmentFinalizeTimeout
	}
	if opts.Timeouts.CatalogPublish == 0 {
		opts.Timeouts.CatalogPublish = DefaultCatalogPublishTimeout
	}
	if opts.Timeouts.SegmentFinalize < 0 {
		return Options{}, fmt.Errorf("%w: negative segment finalize timeout %s", ErrInvalidOptions, opts.Timeouts.SegmentFinalize)
	}
	if opts.Timeouts.CatalogPublish < 0 {
		return Options{}, fmt.Errorf("%w: negative catalog publish timeout %s", ErrInvalidOptions, opts.Timeouts.CatalogPublish)
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
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

func stopTimer(timer Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

func isZeroSegmentOptions(opts segwriter.Options) bool {
	return opts == segwriter.Options{}
}

func randomUUID() ([16]byte, error) {
	id, err := uuid.NewRandom()
	return [16]byte(id), err
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

// withoutContextCancellation removes shutdown's expected cancellation leaves
// while retaining sibling provider/cleanup failures from errors.Join trees.
func withoutContextCancellation(err error) error {
	if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := joined.Unwrap()
		kept := make([]error, 0, len(parts))
		for _, part := range parts {
			if residual := withoutContextCancellation(part); residual != nil {
				kept = append(kept, residual)
			}
		}
		return errors.Join(kept...)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if withoutContextCancellation(wrapped.Unwrap()) == nil {
			return nil
		}
	}
	return err
}

func (w *Writer) abortSegments(ctx context.Context, active *activeSegment, detached []detachedSegment) error {
	var cleanupErr error
	if active != nil {
		if err := active.writer.Abort(ctx); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			w.observe(MetricEvent{
				Name:      MetricSegmentCleanup,
				Partition: w.partition,
				StartLSN:  active.baseLSN,
				Records:   int(active.records),
				Bytes:     active.rawBytes,
				Err:       err,
			})
		}
	}
	for _, item := range detached {
		if err := item.writer.Abort(ctx); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			w.observe(MetricEvent{
				Name:      MetricSegmentCleanup,
				Partition: w.partition,
				StartLSN:  item.baseLSN,
				Records:   int(item.records),
				Bytes:     item.rawBytes,
				Err:       err,
			})
		}
	}
	return cleanupErr
}

func (w *Writer) abortSegmentsBestEffort(active *activeSegment, detached []detachedSegment) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return w.abortSegments(ctx, active, detached)
}

func abortWriterBestEffort(sw *segwriter.Writer) error {
	if sw == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return sw.Abort(ctx)
}
