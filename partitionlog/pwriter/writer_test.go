package pwriter

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

	flush, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if flush == nil {
		t.Fatal("Flush() = nil, want flushed segment")
	}
	if flush.Snapshot.Head.NextLSN != 2 || flush.Snapshot.Head.SegmentCount != 1 {
		t.Fatalf("flush snapshot = %+v", flush.Snapshot)
	}
	if got := factory.Bytes(flush.Segment.URI); len(got) == 0 {
		t.Fatalf("stored bytes for %s are empty", flush.Segment.URI)
	}

	segment, ok, err := cat.FindSegment(context.Background(), 1, 1)
	if err != nil {
		t.Fatalf("FindSegment() error = %v", err)
	}
	if !ok || segment != flush.Segment {
		t.Fatalf("FindSegment(1) = %+v ok=%v, want %+v", segment, ok, flush.Segment)
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
	flush, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if flush == nil {
		t.Fatal("Close() = nil, want flushed tail segment")
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
	if got, want := factory.Count(), 3; got != want {
		t.Fatalf("stored objects = %d, want %d", got, want)
	}
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
	flush, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if flush == nil {
		t.Fatal("Close() = nil, want flushed segment")
	}
	assertRange(t, flush.Segment, 2, 2)
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

func TestWriterFlushFailureIsTerminalAndLeavesOrphanObject(t *testing.T) {
	t.Parallel()

	factory := newMemorySegmentFactory()
	opts := DefaultOptions(factory)
	opts.Session = &sessionStub{
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
		publish: func(ctx context.Context, _ PublishRequest, _ Snapshot) (Snapshot, error) {
			if err := ctx.Err(); err != nil {
				return Snapshot{}, err
			}
			return Snapshot{}, fmt.Errorf("%w: catalog append failed", ErrPublishFailed)
		},
	}
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(1)
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("Flush() error = %v, want %v", err, ErrPublishFailed)
	}
	if !errors.Is(w.Err(), ErrPublishFailed) {
		t.Fatalf("Err() = %v, want %v", w.Err(), ErrPublishFailed)
	}
	if got := factory.Count(); got != 1 {
		t.Fatalf("stored objects = %d, want orphaned object count 1", got)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); !errors.Is(err, ErrAborted) {
		t.Fatalf("Append(after failed flush) error = %v, want %v", err, ErrAborted)
	}
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrAborted) {
		t.Fatalf("Flush(after failed flush) error = %v, want %v", err, ErrAborted)
	}
}

func TestWriterCloseEmptyIsNoop(t *testing.T) {
	t.Parallel()

	w, err := New(testOptions(t, catalog.NewMemoryCatalog(), newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if result != nil {
		t.Fatalf("Close(empty) = %+v, want nil", result)
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

func (f *memorySegmentFactory) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sinks)
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
