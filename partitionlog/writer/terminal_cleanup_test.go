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
	opts.Queue.MaxInflightBytes = 1 << 30
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
}

func (t *terminalCleanupTxn) UploadPart(ctx context.Context, part segwriter.Part) (segwriter.PartReceipt, error) {
	if err := ctx.Err(); err != nil {
		return segwriter.PartReceipt{}, err
	}
	t.mu.Lock()
	t.size += uint64(len(part.Bytes))
	t.mu.Unlock()
	return segwriter.PartReceipt{Number: part.Number, Token: fmt.Sprintf("part-%d", part.Number)}, nil
}

func (t *terminalCleanupTxn) Complete(ctx context.Context, _ []segwriter.PartReceipt) (segwriter.CommittedObject, error) {
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

func (t *terminalCleanupTxn) Abort(context.Context) error {
	t.abortCalls.Add(1)
	return nil
}
