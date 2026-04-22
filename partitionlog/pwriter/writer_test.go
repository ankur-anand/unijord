package pwriter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

func TestWriterFlushPublishesSegment(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	factory := newMemorySegmentFactory()
	w, err := New(context.Background(), testOptions(cat, factory))
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
	if !flush.Flushed {
		t.Fatal("Flush().Flushed = false, want true")
	}
	if flush.State.NextLSN != 2 || flush.State.SegmentCount != 1 {
		t.Fatalf("flush state = %+v", flush.State)
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
	opts := testOptions(cat, factory)
	opts.MaxSegmentRecords = 2
	w, err := New(context.Background(), opts)
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
	if flush, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	} else if !flush.Flushed {
		t.Fatal("Close().Flushed = false, want true")
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
	if _, err := cat.AppendSegment(context.Background(), catalog.AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 0,
		WriterEpoch:     1,
		Segment:         testCatalogSegment(1, 0, 1, 1),
	}); err != nil {
		t.Fatalf("preload catalog error = %v", err)
	}

	factory := newMemorySegmentFactory()
	w, err := New(context.Background(), testOptions(cat, factory))
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
	if flush, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	} else {
		assertRange(t, flush.Segment, 2, 2)
	}
}

func TestWriterRejectsTimestampRegression(t *testing.T) {
	t.Parallel()

	w, err := New(context.Background(), testOptions(catalog.NewMemoryCatalog(), newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 9, Value: []byte("b")}); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("Append(regression) error = %v, want %v", err, ErrTimestampOrder)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 11, Value: []byte("c")}); !errors.Is(err, ErrAborted) {
		t.Fatalf("Append(after regression) error = %v, want %v", err, ErrAborted)
	}
}

func TestWriterFlushFailureIsTerminalAndLeavesOrphanObject(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("catalog append failed")
	cat := &failingCatalog{err: wantErr}
	factory := newMemorySegmentFactory()
	w, err := New(context.Background(), testOptions(cat, factory))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Flush(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Flush() error = %v, want %v", err, wantErr)
	}
	if got := factory.Count(); got != 1 {
		t.Fatalf("stored objects = %d, want orphaned object count 1", got)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); !errors.Is(err, ErrAborted) {
		t.Fatalf("Append(after failed flush) error = %v, want %v", err, ErrAborted)
	}
}

func TestWriterCloseEmptyIsNoop(t *testing.T) {
	t.Parallel()

	w, err := New(context.Background(), testOptions(catalog.NewMemoryCatalog(), newMemorySegmentFactory()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if result.Flushed {
		t.Fatal("Close(empty).Flushed = true, want false")
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("x")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append(after close) error = %v, want %v", err, ErrClosed)
	}
}

func TestWriterRejectsStaleEpochOnOpen(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	if _, err := cat.AppendSegment(context.Background(), catalog.AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 0,
		WriterEpoch:     2,
		Segment:         testCatalogSegment(1, 0, 1, 2),
	}); err != nil {
		t.Fatalf("preload catalog error = %v", err)
	}

	opts := testOptions(cat, newMemorySegmentFactory())
	opts.WriterEpoch = 1
	if _, err := New(context.Background(), opts); !errors.Is(err, catalog.ErrStaleWriter) {
		t.Fatalf("New(stale epoch) error = %v, want %v", err, catalog.ErrStaleWriter)
	}
}

func testOptions(cat catalog.Catalog, factory SinkFactory) Options {
	opts := DefaultOptions(1, cat, factory)
	opts.WriterEpoch = 1
	opts.WriterTag = [16]byte{9, 8, 7}
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = segwriter.DefaultOptions(1)
	opts.SegmentOptions.Codec = segformat.CodecNone
	opts.SegmentOptions.HashAlgo = segformat.HashXXH64
	opts.SegmentOptions.TargetBlockSize = 128
	opts.SegmentOptions.PartSize = 128
	opts.SegmentOptions.SealParallelism = 1
	opts.SegmentOptions.BlockBufferCount = 3
	opts.SegmentOptions.UploadParallelism = 1
	opts.SegmentOptions.UploadQueueSize = 1
	return opts
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

type failingCatalog struct {
	err error
}

func (c *failingCatalog) LoadPartition(ctx context.Context, partition uint32) (catalog.PartitionState, error) {
	if err := ctx.Err(); err != nil {
		return catalog.PartitionState{}, err
	}
	return catalog.PartitionState{Partition: partition}, nil
}

func (c *failingCatalog) AppendSegment(context.Context, catalog.AppendSegmentRequest) (catalog.PartitionState, error) {
	return catalog.PartitionState{}, c.err
}

func (c *failingCatalog) FindSegment(context.Context, uint32, uint64) (catalog.SegmentRef, bool, error) {
	return catalog.SegmentRef{}, false, nil
}

func (c *failingCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (catalog.SegmentPage, error) {
	return catalog.SegmentPage{}, nil
}

func testCatalogSegment(partition uint32, baseLSN uint64, lastLSN uint64, epoch uint64) catalog.SegmentRef {
	recordCount := uint32(lastLSN - baseLSN + 1)
	return catalog.SegmentRef{
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

func assertRange(t *testing.T, segment catalog.SegmentRef, baseLSN uint64, lastLSN uint64) {
	t.Helper()
	if segment.BaseLSN != baseLSN || segment.LastLSN != lastLSN {
		t.Fatalf("segment range = %d-%d, want %d-%d", segment.BaseLSN, segment.LastLSN, baseLSN, lastLSN)
	}
}
