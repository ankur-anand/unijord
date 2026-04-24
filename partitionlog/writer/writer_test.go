package writer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

func TestWriterFlushPublishesSegment(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	factory := newMemorySegmentFactory()
	w, err := New(testOptions(t, cat, factory))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := w.Append(context.Background(), Record{TimestampMS: 10, Value: []byte("alpha")})
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	second, err := w.Append(context.Background(), Record{TimestampMS: 11, Value: []byte("beta")})
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if first.LSN != 0 || second.LSN != 1 {
		t.Fatalf("assigned LSNs = %d,%d want 0,1", first.LSN, second.LSN)
	}

	snapshot, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if snapshot.Head.NextLSN != 2 || snapshot.Head.SegmentCount != 1 {
		t.Fatalf("flush snapshot = %+v", snapshot)
	}
	last, ok := snapshot.Head.Last()
	if !ok {
		t.Fatal("Flush() returned snapshot without last segment")
	}
	if got := factory.Bytes(last.URI); len(got) == 0 {
		t.Fatalf("stored bytes for %s are empty", last.URI)
	}
}

func TestWriterCutSwapsAndAllowsContinuedAppend(t *testing.T) {
	t.Parallel()

	session := newBlockingSession(1)
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 4,
		MaxInflightBytes:    1 << 20,
	}
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	second, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")})
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if second.LSN != 1 {
		t.Fatalf("Append(second) LSN = %d, want 1", second.LSN)
	}
	state := w.State()
	if state.OptimisticNextLSN != 2 {
		t.Fatalf("OptimisticNextLSN = %d, want 2", state.OptimisticNextLSN)
	}
	if state.InflightSegments != 1 {
		t.Fatalf("InflightSegments = %d, want 1", state.InflightSegments)
	}

	session.ReleaseOne()
	session.ReleaseOne()
	if _, err := w.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestWriterRollsByMaxSegmentRecords(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	factory := newMemorySegmentFactory()
	opts := testOptions(t, cat, factory)
	opts.Roll.MaxSegmentRecords = 2
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for i := 0; i < 5; i++ {
		result, err := w.Append(context.Background(), Record{TimestampMS: int64(i), Value: []byte("x")})
		if err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		if result.LSN != uint64(i) {
			t.Fatalf("Append(%d) LSN = %d, want %d", i, result.LSN, i)
		}
	}
	snapshot, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if snapshot.Head.NextLSN != 5 || snapshot.Head.SegmentCount != 3 {
		t.Fatalf("Close() snapshot = %+v", snapshot)
	}

	page, err := cat.ListSegments(context.Background(), catalog.ListSegmentsRequest{Partition: 1, Limit: 10})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if got, want := len(page.Segments), 3; got != want {
		t.Fatalf("segments = %d, want %d: %+v", got, want, page.Segments)
	}
	assertRange(t, page.Segments[0], 0, 1)
	assertRange(t, page.Segments[1], 2, 3)
	assertRange(t, page.Segments[2], 4, 4)
}

func TestWriterContinuesFromCatalogHead(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	preloadFence, err := cat.AcquireWriter(context.Background(), catalog.AcquireWriterRequest{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("AcquireWriter(preload) error = %v", err)
	}
	if _, err := cat.AppendSegment(context.Background(), catalog.AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 0,
		WriterEpoch:     preloadFence.Epoch,
		Segment:         testCatalogSegment(1, 0, 1, preloadFence.Epoch),
	}); err != nil {
		t.Fatalf("preload catalog error = %v", err)
	}

	factory := newMemorySegmentFactory()
	w, err := New(testOptions(t, cat, factory))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("next")})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.LSN != 2 {
		t.Fatalf("LSN = %d, want 2", result.LSN)
	}
	snapshot, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	last, ok := snapshot.Head.Last()
	if !ok {
		t.Fatal("Close() snapshot missing last segment")
	}
	assertRange(t, last, 2, 2)
}

func TestWriterRejectsTimestampRegression(t *testing.T) {
	t.Parallel()

	w, err := New(testOptions(t, catalog.NewMemoryCatalog(), newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 9, Value: []byte("b")}); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("Append(regression) error = %v, want %v", err, ErrTimestampOrder)
	}
	if !errors.Is(w.Err(), ErrTimestampOrder) {
		t.Fatalf("Err() = %v, want %v", w.Err(), ErrTimestampOrder)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 11, Value: []byte("c")}); !errors.Is(err, ErrAborted) {
		t.Fatalf("Append(after regression) error = %v, want %v", err, ErrAborted)
	}
}

func TestWriterCutBackpressureOnInflight(t *testing.T) {
	t.Parallel()

	session := newBlockingSession(1)
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 1,
		MaxInflightBytes:    1 << 20,
	}
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(first) error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Cut(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cut(second) error = %v, want %v", err, context.DeadlineExceeded)
	}

	session.ReleaseOne()
	time.Sleep(20 * time.Millisecond)
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut(third) error = %v", err)
	}
	session.ReleaseOne()
	if _, err := w.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestWriterFlushWaitsForInflight(t *testing.T) {
	t.Parallel()

	session := newBlockingSession(1)
	w, err := New(testSessionOptions(session, newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := w.Flush(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Flush() returned early with err=%v", err)
	case <-time.After(20 * time.Millisecond):
	}

	session.ReleaseOne()
	if err := <-done; err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestWriterAsyncPublishFailureSurfacesOnce(t *testing.T) {
	t.Parallel()

	session := &sessionStub{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   1,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{9, 8, 7},
			},
		},
		publish: func(context.Context, PublishRequest, Snapshot) (Snapshot, error) {
			return Snapshot{}, fmt.Errorf("%w: boom", ErrPublishFailed)
		},
	}
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := w.Cut(context.Background()); err != nil {
		t.Fatalf("Cut() error = %v", err)
	}
	for i := 0; i < 50; i++ {
		if errors.Is(w.Err(), ErrPublishFailed) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(w.Err(), ErrPublishFailed) {
		t.Fatalf("Err() = %v, want %v", w.Err(), ErrPublishFailed)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("Append(after async failure) error = %v, want %v", err, ErrPublishFailed)
	}
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrAborted) {
		t.Fatalf("Flush(after surfaced async failure) error = %v, want %v", err, ErrAborted)
	}
}

func TestWriterCutFailureBeforeSwapKeepsWriterUsable(t *testing.T) {
	t.Parallel()

	factory := &flakySegmentFactory{
		next: newMemorySegmentFactory(),
		fail: map[uint64]error{
			1: errors.New("start next segment failed"),
		},
	}
	opts := DefaultOptions(factory)
	opts.Session = &sessionStub{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   1,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{1},
			},
		},
		publish: func(_ context.Context, req PublishRequest, current Snapshot) (Snapshot, error) {
			next := current
			next.Head.NextLSN = req.Segment.NextLSN()
			next.Head.WriterEpoch = current.Identity.Epoch
			next.Head.SegmentCount++
			next.Head.LastSegment = req.Segment
			next.Head.HasLastSegment = true
			return next, nil
		},
	}
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := w.Cut(context.Background()); err == nil {
		t.Fatal("Cut() error = nil, want failure before swap")
	} else if !errors.Is(err, ErrSegmentStartFailed) {
		t.Fatalf("Cut() error = %v, want %v", err, ErrSegmentStartFailed)
	}
	if w.Err() != nil {
		t.Fatalf("Err() = %v, want nil after pre-swap cut failure", w.Err())
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); err != nil {
		t.Fatalf("Append(second) error = %v, want writer still usable", err)
	}
}

func TestWriterAppendStartFailureIsRetryable(t *testing.T) {
	t.Parallel()

	factory := &flakySegmentFactory{
		next: newMemorySegmentFactory(),
		fail: map[uint64]error{0: errors.New("sink unavailable")},
	}
	opts := testSessionOptions(&sessionStub{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   1,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{1},
			},
		},
		publish: func(_ context.Context, req PublishRequest, current Snapshot) (Snapshot, error) {
			next := current
			next.Head.NextLSN = req.Segment.NextLSN()
			next.Head.WriterEpoch = current.Identity.Epoch
			next.Head.SegmentCount++
			next.Head.LastSegment = req.Segment
			next.Head.HasLastSegment = true
			return next, nil
		},
	}, factory)
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err == nil {
		t.Fatal("Append() error = nil, want start failure")
	} else if !errors.Is(err, ErrSegmentStartFailed) {
		t.Fatalf("Append() error = %v, want %v", err, ErrSegmentStartFailed)
	}
	if w.Err() != nil {
		t.Fatalf("Err() = %v, want nil after append start failure", w.Err())
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(retry) error = %v, want writer still usable", err)
	}
}

func TestWriterCutBackpressureUsesEstimatedSegmentBytes(t *testing.T) {
	t.Parallel()

	opts := testSessionOptions(&sessionStub{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   1,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{1},
			},
		},
		publish: func(_ context.Context, req PublishRequest, current Snapshot) (Snapshot, error) {
			next := current
			next.Head.NextLSN = req.Segment.NextLSN()
			next.Head.WriterEpoch = current.Identity.Epoch
			next.Head.SegmentCount++
			next.Head.LastSegment = req.Segment
			next.Head.HasLastSegment = true
			return next, nil
		},
	}, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 1,
		MaxInflightBytes:    uint64(segformat.RecordHeaderSize + 1),
	}
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := w.Abort(context.Background()); err != nil {
			t.Fatalf("Abort() error = %v", err)
		}
	}()

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Cut(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cut() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if w.Err() != nil {
		t.Fatalf("Err() = %v, want nil after byte-budget backpressure", w.Err())
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); err != nil {
		t.Fatalf("Append(after blocked cut) error = %v, want writer still usable", err)
	}
}

func TestWriterCloseEmptyIsNoop(t *testing.T) {
	t.Parallel()

	w, err := New(testOptions(t, catalog.NewMemoryCatalog(), newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	snapshot, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if snapshot.Head.Partition != 1 || snapshot.Head.NextLSN != 0 {
		t.Fatalf("Close(empty) snapshot = %+v", snapshot)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("x")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append(after close) error = %v, want %v", err, ErrClosed)
	}
}

func TestWriterRejectsStaleEpochOnOpen(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	firstFence, err := cat.AcquireWriter(context.Background(), catalog.AcquireWriterRequest{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("AcquireWriter(first) error = %v", err)
	}
	if _, err := cat.AppendSegment(context.Background(), catalog.AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 0,
		WriterEpoch:     firstFence.Epoch,
		Segment:         testCatalogSegment(1, 0, 1, firstFence.Epoch),
	}); err != nil {
		t.Fatalf("preload catalog error = %v", err)
	}
	secondFence, err := cat.AcquireWriter(context.Background(), catalog.AcquireWriterRequest{
		Partition: 1,
		WriterID:  [16]byte{2},
	})
	if err != nil {
		t.Fatalf("AcquireWriter(second) error = %v", err)
	}
	if secondFence.Epoch <= firstFence.Epoch {
		t.Fatalf("second fence epoch = %d, want > %d", secondFence.Epoch, firstFence.Epoch)
	}

	opts := DefaultOptions(newMemorySegmentFactory())
	opts.Session = &sessionStub{
		snapshot: Snapshot{
			Head: secondFence.State,
			Identity: WriterIdentity{
				Epoch: firstFence.Epoch,
				Tag:   [16]byte{9, 8, 7},
			},
		},
	}
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	if _, err := New(opts); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("New(stale epoch) error = %v, want %v", err, ErrStaleWriter)
	}
}

func testOptions(t *testing.T, cat catalog.Catalog, factory SinkFactory) Options {
	t.Helper()

	opts := DefaultOptions(factory)
	opts.Session = newCatalogSession(t, cat, 1, [16]byte{9, 8, 7})
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 4,
		MaxInflightBytes:    1 << 20,
	}
	return opts
}

func testSessionOptions(session Session, factory SinkFactory) Options {
	opts := DefaultOptions(factory)
	opts.Session = session
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	opts.Roll.MaxSegmentRecords = 1
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 2,
		MaxInflightBytes:    1 << 20,
	}
	return opts
}

func newTestSegmentOptions(partition uint32) segwriter.Options {
	opts := segwriter.DefaultOptions(partition)
	opts.Codec = segformat.CodecNone
	opts.HashAlgo = segformat.HashXXH64
	opts.TargetBlockSize = 128
	opts.PartSize = 128
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 1
	opts.UploadQueueSize = 1
	return opts
}

type sessionStub struct {
	mu       sync.Mutex
	snapshot Snapshot
	publish  func(ctx context.Context, req PublishRequest, current Snapshot) (Snapshot, error)
}

func (s *sessionStub) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *sessionStub) PublishSegment(ctx context.Context, req PublishRequest) (Snapshot, error) {
	s.mu.Lock()
	current := s.snapshot
	publish := s.publish
	s.mu.Unlock()
	if publish == nil {
		return Snapshot{}, fmt.Errorf("%w: publish not configured", ErrPublishFailed)
	}
	next, err := publish(ctx, req, current)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	s.snapshot = next
	s.mu.Unlock()
	return next, nil
}

func newCatalogSession(t testing.TB, cat catalog.Catalog, partition uint32, writerTag [16]byte) Session {
	t.Helper()

	fence, err := cat.AcquireWriter(context.Background(), catalog.AcquireWriterRequest{
		Partition: partition,
		WriterID:  writerTag,
	})
	if err != nil {
		t.Fatalf("AcquireWriter() error = %v", err)
	}
	return &sessionStub{
		snapshot: Snapshot{
			Head: fence.State,
			Identity: WriterIdentity{
				Epoch: fence.Epoch,
				Tag:   writerTag,
			},
		},
		publish: func(ctx context.Context, req PublishRequest, current Snapshot) (Snapshot, error) {
			state, err := cat.AppendSegment(ctx, catalog.AppendSegmentRequest{
				Partition:       current.Head.Partition,
				ExpectedNextLSN: req.ExpectedNextLSN,
				WriterEpoch:     current.Identity.Epoch,
				Segment:         req.Segment,
			})
			if err != nil {
				if errors.Is(err, catalog.ErrStaleWriter) {
					return Snapshot{}, fmt.Errorf("%w: %w", ErrStaleWriter, err)
				}
				return Snapshot{}, fmt.Errorf("%w: %w", ErrPublishFailed, err)
			}
			return Snapshot{
				Head: state,
				Identity: WriterIdentity{
					Epoch: current.Identity.Epoch,
					Tag:   current.Identity.Tag,
				},
			}, nil
		},
	}
}

type blockingSession struct {
	mu       sync.Mutex
	snapshot Snapshot
	release  chan struct{}
}

func newBlockingSession(partition uint32) *blockingSession {
	return &blockingSession{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   partition,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{9, 8, 7},
			},
		},
		release: make(chan struct{}, 16),
	}
}

func (s *blockingSession) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *blockingSession) PublishSegment(ctx context.Context, req PublishRequest) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case <-s.release:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.snapshot
	next := Snapshot{
		Head: pmeta.PartitionHead{
			Partition:      current.Head.Partition,
			NextLSN:        req.Segment.NextLSN(),
			OldestLSN:      current.Head.OldestLSN,
			WriterEpoch:    current.Identity.Epoch,
			SegmentCount:   current.Head.SegmentCount + 1,
			LastSegment:    req.Segment,
			HasLastSegment: true,
		},
		Identity: current.Identity,
	}
	s.snapshot = next
	return next, nil
}

func (s *blockingSession) ReleaseOne() {
	s.release <- struct{}{}
}

type flakySegmentFactory struct {
	mu    sync.Mutex
	next  *memorySegmentFactory
	calls uint64
	fail  map[uint64]error
}

func (f *flakySegmentFactory) NewSegmentSink(ctx context.Context, info SegmentInfo) (segwriter.Sink, error) {
	f.mu.Lock()
	call := f.calls
	f.calls++
	err := f.fail[call]
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return f.next.NewSegmentSink(ctx, info)
}

func newSequenceUUIDGen() UUIDGen {
	var n byte
	return func() ([16]byte, error) {
		n++
		return [16]byte{n}, nil
	}
}

type memorySegmentFactory struct {
	mu    sync.Mutex
	sinks map[string]*segwriter.MemorySink
}

func newMemorySegmentFactory() *memorySegmentFactory {
	return &memorySegmentFactory{sinks: make(map[string]*segwriter.MemorySink)}
}

func (f *memorySegmentFactory) NewSegmentSink(_ context.Context, info SegmentInfo) (segwriter.Sink, error) {
	uri := fmt.Sprintf("memory://p%08d/%020d-%x", info.Partition, info.BaseLSN, info.SegmentUUID)
	sink := segwriter.NewMemorySink(uri)

	f.mu.Lock()
	f.sinks[uri] = sink
	f.mu.Unlock()
	return sink, nil
}

func (f *memorySegmentFactory) Bytes(uri string) []byte {
	f.mu.Lock()
	sink := f.sinks[uri]
	f.mu.Unlock()
	if sink == nil {
		return nil
	}
	return sink.Bytes()
}

func testCatalogSegment(partition uint32, baseLSN uint64, lastLSN uint64, epoch uint64) pmeta.SegmentRef {
	recordCount := uint32(lastLSN - baseLSN + 1)
	return pmeta.SegmentRef{
		URI:              fmt.Sprintf("memory://preloaded/%d-%d", baseLSN, lastLSN),
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      [16]byte{byte(baseLSN + 1), byte(lastLSN + 1), byte(epoch)},
		WriterTag:        [16]byte{1},
		BaseLSN:          baseLSN,
		LastLSN:          lastLSN,
		MinTimestampMS:   int64(baseLSN),
		MaxTimestampMS:   int64(lastLSN),
		RecordCount:      recordCount,
		BlockCount:       1,
		SizeBytes:        128,
		BlockIndexOffset: 64,
		BlockIndexLength: 64,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      1,
		TrailerHash:      2,
	}
}

func assertRange(t *testing.T, segment pmeta.SegmentRef, baseLSN uint64, lastLSN uint64) {
	t.Helper()
	if segment.BaseLSN != baseLSN || segment.LastLSN != lastLSN {
		t.Fatalf("segment range = %d-%d, want %d-%d", segment.BaseLSN, segment.LastLSN, baseLSN, lastLSN)
	}
}
