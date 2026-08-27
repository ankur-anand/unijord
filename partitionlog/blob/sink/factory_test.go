package sink

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	objmultipart "github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	uploadstream "github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestBlobWriterEndToEndWithBlobCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := blob.NewMemory(blob.Options{SegmentRootPrefix: "segments"})
	if err != nil {
		t.Fatalf("blob.NewMemory() error = %v", err)
	}
	segmentStore := objmultipart.NewMemoryStore()

	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	opts := plwriter.DefaultOptions(factory)
	opts.Session = newCatalogWriterSession(t, cat, 7, [16]byte{7, 7, 7})
	opts.Clock = plwriter.ClockFunc(func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() })
	opts.UUIDGen = sequenceUUIDGen()
	opts.Roll.MaxSegmentRecords = 2
	opts.SegmentOptions = segwriter.DefaultOptions(7)
	opts.SegmentOptions.Codec = segformat.CodecNone
	opts.SegmentOptions.HashAlgo = segformat.HashXXH64
	opts.SegmentOptions.TargetBlockSize = 64
	opts.SegmentOptions.PartSize = 64
	opts.SegmentOptions.SealParallelism = 1
	opts.SegmentOptions.BlockBufferCount = 3
	opts.SegmentOptions.UploadParallelism = 2
	opts.SegmentOptions.UploadQueueSize = 2

	writer, err := plwriter.New(opts)
	if err != nil {
		t.Fatalf("writer.New() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		result, err := writer.Append(ctx, plwriter.Record{
			TimestampMS: int64(100 + i),
			Value:       []byte(fmt.Sprintf("event-%d", i)),
		})
		if err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		if result.LSN != uint64(i) {
			t.Fatalf("Append(%d) LSN = %d, want %d", i, result.LSN, i)
		}
	}
	if _, err := writer.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	state, err := cat.LoadPartition(ctx, 7)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if state.NextLSN != 5 || state.SegmentCount != 3 {
		t.Fatalf("state = %+v, want next_lsn=5 segment_count=3", state)
	}

	page, err := cat.ListSegments(ctx, pcatalog.ListSegmentsRequest{Partition: 7, Limit: 10})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 3 {
		t.Fatalf("segments = %d, want 3: %+v", len(page.Segments), page.Segments)
	}
	for _, segment := range page.Segments {
		body, _, err := segmentStore.Read(ctx, segment.URI)
		if err != nil {
			t.Fatalf("segmentStore.Read(%s) error = %v", segment.URI, err)
		}
		if len(body) == 0 || uint64(len(body)) != segment.SizeBytes {
			t.Fatalf("segment %s bytes=%d size_bytes=%d", segment.URI, len(body), segment.SizeBytes)
		}
	}

	staging, err := segmentStore.List(ctx, "segments/staging/")
	if err != nil {
		t.Fatalf("List(staging) error = %v", err)
	}
	if len(staging) != 0 {
		t.Fatalf("staging objects after complete = %+v, want none", staging)
	}
}

func TestBlobSinkMovesMultipartSchedulingOutOfSegwriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newObservedMultipartStore(objmultipart.NewMemoryStore(), 2)
	pool, err := uploadstream.NewBufferPool(64, 5)
	if err != nil {
		t.Fatalf("NewBufferPool() error = %v", err)
	}
	factory, err := New(store, Options{Prefix: "segments", BufferPool: pool})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sink, err := factory.NewSegmentSink(ctx, plwriter.SegmentInfo{
		Partition:     1,
		BaseLSN:       0,
		WriterEpoch:   1,
		SegmentUUID:   [16]byte{4, 2},
		CreatedUnixMS: 1_776_263_000_000,
	})
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}
	opts := segwriter.DefaultOptions(1)
	opts.Codec = segformat.CodecNone
	opts.TargetBlockSize = 128
	opts.PartSize = 64
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	w, err := segwriter.New(opts, sink)
	if err != nil {
		t.Fatalf("segwriter.New() error = %v", err)
	}

	type closeResult struct {
		result segwriter.Result
		err    error
	}
	done := make(chan closeResult, 1)
	go func() {
		for i := 0; i < 20; i++ {
			if err := w.Append(ctx, segwriter.Record{
				LSN:         uint64(i),
				TimestampMS: int64(i),
				Value:       make([]byte, 96),
			}); err != nil {
				done <- closeResult{err: err}
				return
			}
		}
		result, err := w.Close(ctx)
		done <- closeResult{result: result, err: err}
	}()

	select {
	case <-store.reached:
		close(store.release)
	case <-time.After(5 * time.Second):
		t.Fatal("multipart stream did not start two concurrent provider uploads")
	}

	var closed closeResult
	select {
	case closed = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("segment writer did not finish after multipart uploads were released")
	}
	if closed.err != nil {
		t.Fatalf("segment write error = %v", closed.err)
	}
	partNumbers, maxActive := store.snapshot()
	if len(partNumbers) < 2 {
		t.Fatalf("multipart parts = %v, want at least two", partNumbers)
	}
	slices.Sort(partNumbers)
	for i, number := range partNumbers {
		if number != i+1 {
			t.Fatalf("sorted part_numbers = %v, want contiguous numbers", partNumbers)
		}
	}
	if maxActive != 2 {
		t.Fatalf("maximum concurrent provider uploads = %d, want 2", maxActive)
	}
	if pool.InUseBytes() != 0 || pool.PeakInUseBytes() > pool.CapacityBytes() {
		t.Fatalf("buffer pool usage = current:%d peak:%d capacity:%d", pool.InUseBytes(), pool.PeakInUseBytes(), pool.CapacityBytes())
	}
	body, attrs, err := store.inner.Read(ctx, closed.result.Object.URI)
	if err != nil {
		t.Fatalf("Read(committed segment) error = %v", err)
	}
	if uint64(len(body)) != closed.result.Metadata.SizeBytes || attrs.SizeBytes != closed.result.Metadata.SizeBytes {
		t.Fatalf("committed sizes = body:%d attrs:%d metadata:%d", len(body), attrs.SizeBytes, closed.result.Metadata.SizeBytes)
	}
}

func TestBlobSegmentTxnAbortDeletesStagingParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	segmentStore := objmultipart.NewMemoryStore()

	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sink, err := factory.NewSegmentSink(ctx, plwriter.SegmentInfo{
		Partition:     1,
		BaseLSN:       10,
		WriterEpoch:   2,
		SegmentUUID:   [16]byte{1, 2, 3},
		CreatedUnixMS: 1000,
	})
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}
	txn, err := sink.Begin(ctx, segwriter.Plan{Partition: 1, PartSize: 4, Codec: segformat.CodecNone, HashAlgo: segformat.HashXXH64})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if err := txn.Write(ctx, []byte("abcd")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := txn.Abort(ctx); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := txn.Write(ctx, []byte("efgh")); !errors.Is(err, segwriter.ErrTxnAborted) {
		t.Fatalf("Write(after abort) error = %v, want %v", err, segwriter.ErrTxnAborted)
	}

	staging, err := segmentStore.List(ctx, "segments/staging/")
	if err != nil {
		t.Fatalf("List(staging) error = %v", err)
	}
	if len(staging) != 0 {
		t.Fatalf("staging objects after abort = %+v, want none", staging)
	}
}

func TestBlobSegmentTxnRejectsExistingFinalObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	segmentStore := objmultipart.NewMemoryStore()

	info := plwriter.SegmentInfo{
		Partition:     1,
		BaseLSN:       0,
		WriterEpoch:   1,
		SegmentUUID:   [16]byte{9, 9, 9},
		CreatedUnixMS: 1000,
	}
	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := writeOneObject(ctx, factory, info, []byte("first")); err != nil {
		t.Fatalf("writeOneObject(first) error = %v", err)
	}
	if err := writeOneObject(ctx, factory, info, []byte("second")); !errors.Is(err, objmultipart.ErrPreconditionFailed) {
		t.Fatalf("writeOneObject(second) error = %v, want %v", err, objmultipart.ErrPreconditionFailed)
	}
}

func TestFactoryRejectsNilStore(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); !errors.Is(err, plwriter.ErrInvalidOptions) {
		t.Fatalf("New(nil) error = %v, want %v", err, plwriter.ErrInvalidOptions)
	}
}

func TestFactoryRejectsInvalidStoreLimits(t *testing.T) {
	t.Parallel()

	if _, err := New(invalidLimitsStore{}, Options{}); !errors.Is(err, plwriter.ErrInvalidOptions) {
		t.Fatalf("New(invalid limits) error = %v, want %v", err, plwriter.ErrInvalidOptions)
	}
}

func TestSegmentSinkMapsBeginError(t *testing.T) {
	t.Parallel()

	factory, err := New(beginErrorStore{err: objmultipart.ErrInvalidStore}, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sink, err := factory.NewSegmentSink(context.Background(), plwriter.SegmentInfo{
		Partition:   1,
		BaseLSN:     10,
		WriterEpoch: 2,
		SegmentUUID: [16]byte{1},
	})
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}
	_, err = sink.Begin(context.Background(), segwriter.Plan{
		Partition: 1,
		PartSize:  1,
		Codec:     segformat.CodecNone,
		HashAlgo:  segformat.HashXXH64,
	})
	if !errors.Is(err, segwriter.ErrInvalidOptions) {
		t.Fatalf("Begin() error = %v, want %v", err, segwriter.ErrInvalidOptions)
	}
}

func TestLayoutUsesNormalizedPrefix(t *testing.T) {
	t.Parallel()

	info := plwriter.SegmentInfo{
		StreamID:    "hosts/host-a/events",
		Partition:   7,
		BaseLSN:     100,
		WriterEpoch: 3,
		SegmentUUID: [16]byte{1, 2, 3},
	}
	layout := NewLayout("/root/")
	if layout.Prefix() != "root" {
		t.Fatalf("Prefix() = %q, want %q", layout.Prefix(), "root")
	}
	if got := layout.SegmentKey(info); got != "root/segments/b78/streams/645c418edae21662304240f5181b1b63c713bfc0b062a2c3b1b84387aa786c91/p00000007/seg-00000000000000000100-e00000000000000000003-01020300000000000000000000000000.plseg" {
		t.Fatalf("SegmentKey() = %q", got)
	}
	if got := layout.StagingPrefix(info); got != "root/staging/b78/streams/645c418edae21662304240f5181b1b63c713bfc0b062a2c3b1b84387aa786c91/p00000007/seg-00000000000000000100-e00000000000000000003-01020300000000000000000000000000" {
		t.Fatalf("StagingPrefix() = %q", got)
	}
}

func writeOneObject(ctx context.Context, factory *Factory, info plwriter.SegmentInfo, body []byte) error {
	sink, err := factory.NewSegmentSink(ctx, info)
	if err != nil {
		return err
	}
	txn, err := sink.Begin(ctx, segwriter.Plan{Partition: info.Partition, PartSize: len(body), Codec: segformat.CodecNone, HashAlgo: segformat.HashXXH64})
	if err != nil {
		return err
	}
	if err := txn.Write(ctx, body); err != nil {
		_ = txn.Abort(ctx)
		return err
	}
	if _, err := txn.Commit(ctx); err != nil {
		_ = txn.Abort(ctx)
		return err
	}
	return nil
}

func sequenceUUIDGen() plwriter.UUIDGen {
	var n byte
	return func() ([16]byte, error) {
		n++
		return [16]byte{n}, nil
	}
}

type beginErrorStore struct {
	err error
}

type invalidLimitsStore struct{}

func (invalidLimitsStore) Limits() objmultipart.Limits { return objmultipart.Limits{} }

func (invalidLimitsStore) Begin(context.Context, string, objmultipart.Options) (objmultipart.Session, error) {
	panic("Begin must not be called when limits are invalid")
}

type observedMultipartStore struct {
	inner *objmultipart.MemoryStore

	mu        sync.Mutex
	parts     []int
	active    int
	maxActive int
	target    int
	reached   chan struct{}
	reachOnce sync.Once
	release   chan struct{}
}

func newObservedMultipartStore(inner *objmultipart.MemoryStore, target int) *observedMultipartStore {
	return &observedMultipartStore{
		inner:   inner,
		target:  target,
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *observedMultipartStore) Limits() objmultipart.Limits { return s.inner.Limits() }

func (s *observedMultipartStore) Begin(ctx context.Context, key string, opts objmultipart.Options) (objmultipart.Session, error) {
	session, err := s.inner.Begin(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	return &observedMultipartSession{Session: session, store: s}, nil
}

func (s *observedMultipartStore) snapshot() ([]int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.parts...), s.maxActive
}

type observedMultipartSession struct {
	objmultipart.Session
	store *observedMultipartStore
}

func (s *observedMultipartSession) PutPart(ctx context.Context, part objmultipart.Part) (objmultipart.Receipt, error) {
	s.store.mu.Lock()
	s.store.parts = append(s.store.parts, part.Number)
	s.store.active++
	if s.store.active > s.store.maxActive {
		s.store.maxActive = s.store.active
	}
	if s.store.active >= s.store.target {
		s.store.reachOnce.Do(func() { close(s.store.reached) })
	}
	s.store.mu.Unlock()
	defer func() {
		s.store.mu.Lock()
		s.store.active--
		s.store.mu.Unlock()
	}()
	select {
	case <-s.store.release:
	case <-ctx.Done():
		return objmultipart.Receipt{}, ctx.Err()
	}
	return s.Session.PutPart(ctx, part)
}

func (s beginErrorStore) Limits() objmultipart.Limits {
	return objmultipart.NewMemoryStore().Limits()
}

func (s beginErrorStore) Begin(context.Context, string, objmultipart.Options) (objmultipart.Session, error) {
	return nil, s.err
}
