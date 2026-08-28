package writer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

func TestWriterPublishFailureAbortsEveryOpenSegmentTransaction(t *testing.T) {
	publishStarted := make(chan struct{})
	releasePublish := make(chan struct{})
	var publishOnce sync.Once
	session := &sessionStub{
		snapshot: terminalCleanupSnapshot(),
		publish: func(ctx context.Context, _ PublishRequest, _ Snapshot) (Snapshot, error) {
			publishOnce.Do(func() { close(publishStarted) })
			select {
			case <-releasePublish:
				return Snapshot{}, fmt.Errorf("%w: injected", ErrPublishFailed)
			case <-ctx.Done():
				return Snapshot{}, ctx.Err()
			}
		},
	}
	factory := newTerminalCleanupFactory(1)
	w := newTerminalCleanupWriter(t, session, factory)

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 0) error = %v", err)
	}
	waitClosed(t, publishStarted, "first segment publication")

	appendOpenTransaction(t, w, factory, 1, 3)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 1) error = %v", err)
	}
	factory.sink(t, 1).waitCompleteStarted(t)

	appendOpenTransaction(t, w, factory, 2, 5)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 2) error = %v", err)
	}
	appendOpenTransaction(t, w, factory, 3, 7)

	close(releasePublish)
	waitForWriterError(t, w, ErrPublishFailed)
	waitForWriterWorkers(t, w)

	assertAbortCalls(t, factory.sink(t, 0), 0)
	assertAbortCalls(t, factory.sink(t, 1), 1)
	assertAbortCalls(t, factory.sink(t, 2), 1)
	assertAbortCalls(t, factory.sink(t, 3), 1)
}

func TestWriterFinalizeFailureAbortsEveryOpenSegmentTransaction(t *testing.T) {
	session := &sessionStub{snapshot: terminalCleanupSnapshot()}
	factory := newTerminalCleanupFactory(0)
	w := newTerminalCleanupWriter(t, session, factory)

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 0) error = %v", err)
	}
	factory.sink(t, 0).waitCompleteStarted(t)

	appendOpenTransaction(t, w, factory, 1, 3)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 1) error = %v", err)
	}
	appendOpenTransaction(t, w, factory, 2, 5)

	factory.sink(t, 0).failComplete(errors.New("injected complete failure"))
	waitForWriterError(t, w, ErrSegmentWriteFailed)
	waitForWriterWorkers(t, w)

	assertAbortCalls(t, factory.sink(t, 0), 1)
	assertAbortCalls(t, factory.sink(t, 1), 1)
	assertAbortCalls(t, factory.sink(t, 2), 1)
}

func TestWriterPreservesIndeterminateSegmentCommit(t *testing.T) {
	session := &sessionStub{snapshot: terminalCleanupSnapshot()}
	factory := newTerminalCleanupFactory(0)
	w := newTerminalCleanupWriter(t, session, factory)

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	factory.sink(t, 0).waitCompleteStarted(t)
	factory.sink(t, 0).failComplete(fmt.Errorf("%w: injected response loss", segwriter.ErrTxnCommitIndeterminate))

	waitForWriterWorkers(t, w)
	got := w.Err()
	for _, target := range []error{
		ErrSegmentWriteFailed,
		ErrSegmentCommitIndeterminate,
		segwriter.ErrTxnCommitIndeterminate,
	} {
		if !errors.Is(got, target) {
			t.Fatalf("Writer.Err() = %v, want errors.Is(%v)", got, target)
		}
	}
	assertAbortCalls(t, factory.sink(t, 0), 1)
}

func TestWriterAbortTimeoutCanRejoinTerminalDrain(t *testing.T) {
	publishErr := fmt.Errorf("%w: injected", ErrPublishFailed)
	publishStarted := make(chan struct{})
	releasePublish := make(chan struct{})
	var publishOnce sync.Once
	session := &sessionStub{
		snapshot: terminalCleanupSnapshot(),
		publish: func(ctx context.Context, _ PublishRequest, _ Snapshot) (Snapshot, error) {
			publishOnce.Do(func() { close(publishStarted) })
			select {
			case <-releasePublish:
				return Snapshot{}, publishErr
			case <-ctx.Done():
				return Snapshot{}, ctx.Err()
			}
		},
	}
	factory := newTerminalCleanupFactory()
	w := newTerminalCleanupWriter(t, session, factory)

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(segment 0) error = %v", err)
	}
	waitClosed(t, publishStarted, "first segment publication")

	appendOpenTransaction(t, w, factory, 1, 3)
	abortGate := factory.sink(t, 1).gateAbort()
	close(releasePublish)
	waitForWriterError(t, w, ErrPublishFailed)
	factory.sink(t, 1).waitAbortStarted(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := w.Abort(ctx)
	if !errors.Is(err, publishErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Abort() error = %v, want publish error and deadline", err)
	}

	close(abortGate)
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("retried Abort() error = %v", err)
	}
	assertAbortCalls(t, factory.sink(t, 1), 1)
}

func TestWriterSegmentFinalizeTimeoutIsTerminal(t *testing.T) {
	session := &sessionStub{snapshot: terminalCleanupSnapshot()}
	factory := newTerminalCleanupFactory(0)
	w := newTerminalCleanupWriter(t, session, factory)
	w.mu.Lock()
	w.opts.Timeouts.SegmentFinalize = 10 * time.Millisecond
	w.mu.Unlock()

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	factory.sink(t, 0).waitCompleteStarted(t)

	waitForWriterError(t, w, context.DeadlineExceeded)
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrSegmentWriteFailed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush() error = %v, want segment failure and deadline", err)
	}
}

func TestWriterCatalogPublishTimeoutIsTerminal(t *testing.T) {
	publishStarted := make(chan struct{})
	var once sync.Once
	session := &sessionStub{
		snapshot: terminalCleanupSnapshot(),
		publish: func(ctx context.Context, _ PublishRequest, _ Snapshot) (Snapshot, error) {
			once.Do(func() { close(publishStarted) })
			<-ctx.Done()
			return Snapshot{}, ctx.Err()
		},
	}
	w := newTerminalCleanupWriter(t, session, newTerminalCleanupFactory())
	w.mu.Lock()
	w.opts.Timeouts.CatalogPublish = 10 * time.Millisecond
	w.mu.Unlock()

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("value")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	waitClosed(t, publishStarted, "catalog publish")

	waitForWriterError(t, w, context.DeadlineExceeded)
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrPublishFailed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush() error = %v, want publish failure and deadline", err)
	}
}

func TestWriterAbortPreservesInflightFinalizeCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("provider cleanup failed")
	session := &sessionStub{snapshot: terminalCleanupSnapshot()}
	factory := newTerminalCleanupFactory(0)
	w := newTerminalCleanupWriter(t, session, factory)

	appendOpenTransaction(t, w, factory, 0, 1)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	sink := factory.sink(t, 0)
	sink.waitCompleteStarted(t)
	sink.setAbortErr(cleanupErr)

	err := w.Abort(context.Background())
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("Abort() error = %v, want cleanup error", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("Abort() error = %v, shutdown cancellation should be filtered", err)
	}
}

func terminalCleanupSnapshot() Snapshot {
	return Snapshot{
		Head: pmeta.PartitionHead{
			Partition:   1,
			WriterEpoch: 1,
		},
		Identity: WriterIdentity{
			Epoch: 1,
			Tag:   [16]byte{9, 8, 7},
		},
	}
}

func newTerminalCleanupWriter(t *testing.T, session Session, factory SinkFactory) *Writer {
	t.Helper()
	opts := testSessionOptions(session, factory)
	opts.Roll.MaxSegmentRecords = 100
	opts.Roll.MaxSegmentRawBytes = 1 << 30
	opts.Queue.MaxInflightSegments = 8
	opts.Queue.MaxInflightBytes = 2 << 30
	opts.SegmentOptions.TargetBlockSize = 32
	opts.SegmentOptions.PartSize = 16
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return w
}

func appendOpenTransaction(t *testing.T, w *Writer, factory *terminalCleanupFactory, sinkIndex int, timestampMS int64) {
	t.Helper()
	value := []byte("aaaaaaaaaaaaaaaaaaaaaaaa")
	if _, err := w.Append(context.Background(), Record{TimestampMS: timestampMS, Value: value}); err != nil {
		t.Fatalf("Append(first, sink %d) error = %v", sinkIndex, err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: timestampMS + 1, Value: value}); err != nil {
		t.Fatalf("Append(second, sink %d) error = %v", sinkIndex, err)
	}
	factory.sink(t, sinkIndex).waitBegin(t)
}

func waitForWriterError(t *testing.T, w *Writer, target error) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if err := w.Err(); errors.Is(err, target) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("Writer.Err() = %v, want %v", w.Err(), target)
		case <-ticker.C:
		}
	}
}

func waitForWriterWorkers(t *testing.T, w *Writer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := waitGroupContext(ctx, &w.workersWG); err != nil {
		t.Fatalf("writer workers did not stop: %v", err)
	}
}

func assertAbortCalls(t *testing.T, sink *terminalCleanupSink, want int32) {
	t.Helper()
	if got := sink.txn.abortCalls.Load(); got != want {
		t.Fatalf("sink %d Abort() calls = %d, want %d", sink.index, got, want)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

type terminalCleanupFactory struct {
	mu          sync.Mutex
	sinks       []*terminalCleanupSink
	gated       map[int]struct{}
	sinkCreated chan struct{}
}

func newTerminalCleanupFactory(gated ...int) *terminalCleanupFactory {
	f := &terminalCleanupFactory{
		gated:       make(map[int]struct{}, len(gated)),
		sinkCreated: make(chan struct{}, 16),
	}
	for _, index := range gated {
		f.gated[index] = struct{}{}
	}
	return f
}

func (f *terminalCleanupFactory) NewSegmentSink(context.Context, SegmentInfo) (segwriter.Sink, error) {
	f.mu.Lock()
	index := len(f.sinks)
	_, gated := f.gated[index]
	sink := newTerminalCleanupSink(index, gated)
	f.sinks = append(f.sinks, sink)
	f.mu.Unlock()
	f.sinkCreated <- struct{}{}
	return sink, nil
}

func (f *terminalCleanupFactory) sink(t *testing.T, index int) *terminalCleanupSink {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		f.mu.Lock()
		if index < len(f.sinks) {
			sink := f.sinks[index]
			f.mu.Unlock()
			return sink
		}
		f.mu.Unlock()
		select {
		case <-f.sinkCreated:
		case <-deadline:
			t.Fatalf("timed out waiting for sink %d", index)
		}
	}
}

type terminalCleanupSink struct {
	index int
	txn   *terminalCleanupTxn
}

func newTerminalCleanupSink(index int, gated bool) *terminalCleanupSink {
	txn := &terminalCleanupTxn{
		index:           index,
		beginStarted:    make(chan struct{}),
		completeStarted: make(chan struct{}),
	}
	if gated {
		txn.completeResult = make(chan error, 1)
	}
	return &terminalCleanupSink{index: index, txn: txn}
}

func (s *terminalCleanupSink) Begin(context.Context, segwriter.Plan) (segwriter.Txn, error) {
	s.txn.beginOnce.Do(func() { close(s.txn.beginStarted) })
	return s.txn, nil
}

func (s *terminalCleanupSink) waitBegin(t *testing.T) {
	t.Helper()
	waitClosed(t, s.txn.beginStarted, fmt.Sprintf("sink %d begin", s.index))
}

func (s *terminalCleanupSink) waitCompleteStarted(t *testing.T) {
	t.Helper()
	waitClosed(t, s.txn.completeStarted, fmt.Sprintf("sink %d complete", s.index))
}

func (s *terminalCleanupSink) failComplete(err error) {
	s.txn.completeResult <- err
}

type terminalCleanupTxn struct {
	index int

	mu   sync.Mutex
	size uint64

	beginStarted    chan struct{}
	completeStarted chan struct{}
	completeResult  chan error
	beginOnce       sync.Once
	completeOnce    sync.Once
	abortCalls      atomic.Int32
	abortStarted    chan struct{}
	abortStartOnce  sync.Once
	abortGate       chan struct{}
	abortErr        error
}

func (t *terminalCleanupTxn) Write(ctx context.Context, bytes []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	t.size += uint64(len(bytes))
	t.mu.Unlock()
	return nil
}

func (t *terminalCleanupTxn) Commit(ctx context.Context) (segwriter.CommittedObject, error) {
	t.completeOnce.Do(func() { close(t.completeStarted) })
	if t.completeResult != nil {
		select {
		case err := <-t.completeResult:
			return segwriter.CommittedObject{}, err
		case <-ctx.Done():
			return segwriter.CommittedObject{}, ctx.Err()
		}
	}
	t.mu.Lock()
	size := t.size
	t.mu.Unlock()
	return segwriter.CommittedObject{
		URI:       fmt.Sprintf("cleanup://segment/%d", t.index),
		SizeBytes: size,
		Token:     "complete",
	}, nil
}

func (t *terminalCleanupTxn) Abort(ctx context.Context) error {
	t.abortCalls.Add(1)
	t.abortStartOnce.Do(func() {
		if t.abortStarted != nil {
			close(t.abortStarted)
		}
	})
	if t.abortGate != nil {
		select {
		case <-t.abortGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.mu.Lock()
	err := t.abortErr
	t.mu.Unlock()
	return err
}

func (s *terminalCleanupSink) gateAbort() chan struct{} {
	s.txn.abortStarted = make(chan struct{})
	s.txn.abortGate = make(chan struct{})
	return s.txn.abortGate
}

func (s *terminalCleanupSink) waitAbortStarted(t *testing.T) {
	t.Helper()
	waitClosed(t, s.txn.abortStarted, fmt.Sprintf("sink %d abort", s.index))
}

func (s *terminalCleanupSink) setAbortErr(err error) {
	s.txn.mu.Lock()
	s.txn.abortErr = err
	s.txn.mu.Unlock()
}
