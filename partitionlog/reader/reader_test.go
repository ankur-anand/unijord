package reader

import (
	"bytes"
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

func TestConsumeSingleSegment(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 20)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[:10])
	if result.NextLSN != 10 {
		t.Fatalf("NextLSN = %d, want 10", result.NextLSN)
	}
	if result.Head.NextLSN != 20 {
		t.Fatalf("Head.NextLSN = %d, want 20", result.Head.NextLSN)
	}
}

func TestConsumeAcrossSegments(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 8)
	fixture.appendSegment(t, 8, 7)
	fixture.appendSegment(t, 15, 9)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 20})

	result, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 6, Limit: 13})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[6:19])
	if result.NextLSN != 19 {
		t.Fatalf("NextLSN = %d, want 19", result.NextLSN)
	}
}

func TestConsumeAfter(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 10)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.ConsumeAfter(context.Background(), ConsumeAfterRequest{Partition: fixture.partition, StartAfterLSN: 4, Limit: 3})
	if err != nil {
		t.Fatalf("ConsumeAfter() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[5:8])
	if result.NextLSN != 8 {
		t.Fatalf("NextLSN = %d, want 8", result.NextLSN)
	}
}

func TestConsumeAfterRejectsExhaustedLSN(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})

	_, err := r.ConsumeAfter(context.Background(), ConsumeAfterRequest{Partition: fixture.partition, StartAfterLSN: ^uint64(0), Limit: 1})
	if !errors.Is(err, ErrLSNExhausted) {
		t.Fatalf("ConsumeAfter() error = %v, want %v", err, ErrLSNExhausted)
	}
}

func TestFetchRecord(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 10)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.Fetch(context.Background(), FetchRequest{Partition: fixture.partition, LSN: 4})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !result.Found {
		t.Fatal("Fetch() found=false, want true")
	}
	assertRecordsEqual(t, []Record{result.Record}, fixture.records[4:5])
	if result.Head.NextLSN != 10 {
		t.Fatalf("Head.NextLSN = %d, want 10", result.Head.NextLSN)
	}
}

func TestFetchEmptyAndPastHeadReturnsNotFound(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})

	result, err := r.Fetch(context.Background(), FetchRequest{Partition: fixture.partition, LSN: 0})
	if err != nil {
		t.Fatalf("Fetch(empty) error = %v", err)
	}
	if result.Found || result.Head.HasLastSegment {
		t.Fatalf("Fetch(empty) = %+v, want not found at empty head", result)
	}

	fixture.appendSegment(t, 0, 5)
	result, err = r.Fetch(context.Background(), FetchRequest{Partition: fixture.partition, LSN: 99})
	if err != nil {
		t.Fatalf("Fetch(past head) error = %v", err)
	}
	if result.Found || result.Head.NextLSN != 5 {
		t.Fatalf("Fetch(past head) = %+v, want not found at head_next=5", result)
	}
}

func TestFetchRejectsExpiredLSN(t *testing.T) {
	t.Parallel()

	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      3,
			OldestLSN:      10,
			NextLSN:        20,
			WriterEpoch:    1,
			HasLastSegment: true,
		},
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Fetch(context.Background(), FetchRequest{Partition: 3, LSN: 9})
	if !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Fetch() error = %v, want %v", err, ErrLSNExpired)
	}
	var expired LSNExpiredError
	if !errors.As(err, &expired) {
		t.Fatalf("Fetch() error = %T, want LSNExpiredError", err)
	}
	if expired.Requested != 9 || expired.Oldest != 10 || expired.HeadNext != 20 {
		t.Fatalf("expired = %+v", expired)
	}
}

func TestFetchDetectsCatalogMissInsideHead(t *testing.T) {
	t.Parallel()

	segment := validSegmentRef(3, 0, 9)
	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      3,
			OldestLSN:      0,
			NextLSN:        10,
			WriterEpoch:    1,
			SegmentCount:   1,
			LastSegment:    segment,
			HasLastSegment: true,
		},
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Fetch(context.Background(), FetchRequest{Partition: 3, LSN: 4})
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Fetch() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestFetchMapsStoreReadFailure(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	r, err := New(fixture.catalog, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Fetch(context.Background(), FetchRequest{Partition: segment.Partition, LSN: 2})
	if !errors.Is(err, ErrStoreRead) {
		t.Fatalf("Fetch() error = %v, want %v", err, ErrStoreRead)
	}
}

func TestConsumeRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})

	_, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: -1})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestConsumeEmptyAndPastHead(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})

	result, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Consume(empty) error = %v", err)
	}
	if len(result.Records) != 0 || result.NextLSN != 0 || result.Head.HasLastSegment {
		t.Fatalf("Consume(empty) = %+v, want no records at empty head", result)
	}

	fixture.appendSegment(t, 0, 5)
	result, err = r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 99, Limit: 10})
	if err != nil {
		t.Fatalf("Consume(past head) error = %v", err)
	}
	if len(result.Records) != 0 || result.NextLSN != 99 || result.Head.NextLSN != 5 {
		t.Fatalf("Consume(past head) = %+v", result)
	}
}

func TestConsumeRejectsExpiredLSN(t *testing.T) {
	t.Parallel()

	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      3,
			OldestLSN:      10,
			NextLSN:        20,
			WriterEpoch:    1,
			HasLastSegment: true,
		},
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Consume(context.Background(), ConsumeRequest{Partition: 3, StartLSN: 9, Limit: 1})
	if !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrLSNExpired)
	}
	var expired LSNExpiredError
	if !errors.As(err, &expired) {
		t.Fatalf("Consume() error = %T, want LSNExpiredError", err)
	}
	if expired.Requested != 9 || expired.Oldest != 10 || expired.HeadNext != 20 {
		t.Fatalf("expired = %+v", expired)
	}
}

func TestConsumeDetectsCatalogGap(t *testing.T) {
	t.Parallel()

	segment := validSegmentRef(3, 10, 19)
	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      3,
			OldestLSN:      0,
			NextLSN:        20,
			WriterEpoch:    1,
			HasLastSegment: true,
			LastSegment:    segment,
			SegmentCount:   1,
		},
		pages: []pmeta.SegmentPage{{Segments: []pmeta.SegmentRef{segment}}},
	}
	r, err := New(cat, newTestSegmentStore(map[string][]byte{segment.URI: []byte("not-used")}), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Consume(context.Background(), ConsumeRequest{Partition: 3, StartLSN: 0, Limit: 1})
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestConsumeMapsStoreReadFailure(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	r, err := New(fixture.catalog, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Consume(context.Background(), ConsumeRequest{Partition: segment.Partition, StartLSN: 0, Limit: 1})
	if !errors.Is(err, ErrStoreRead) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrStoreRead)
	}
}

func TestConsumeMapsCorruptSegment(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	object := fixture.storeObject(segment.URI)
	object[len(object)-1] ^= 0xff
	fixture.store.Put(segment.URI, object)
	r := fixture.openReader(t, Options{})

	_, err := r.Consume(context.Background(), ConsumeRequest{Partition: segment.Partition, StartLSN: 0, Limit: 1})
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestConsumeUsesInitialHeadSnapshot(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 5)
	second := fixture.appendSegment(t, 5, 5)
	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      fixture.partition,
			OldestLSN:      0,
			NextLSN:        5,
			WriterEpoch:    1,
			SegmentCount:   1,
			LastSegment:    first,
			HasLastSegment: true,
		},
		pages: []pmeta.SegmentPage{{Segments: []pmeta.SegmentRef{first, second}}},
	}
	r, err := New(cat, fixture.store, Options{MaxRecordsPerBatch: 20})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 20})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[:5])
	if result.NextLSN != 5 {
		t.Fatalf("NextLSN = %d, want 5", result.NextLSN)
	}
}

func TestConsumeFromTimestampAcrossSegments(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 5)
	fixture.appendSegment(t, 5, 5)
	fixture.appendSegment(t, 10, 5)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: 10_007,
		Limit:       4,
	})
	if err != nil {
		t.Fatalf("ConsumeFromTimestamp() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[7:11])
	if result.NextLSN != 11 {
		t.Fatalf("NextLSN = %d, want 11", result.NextLSN)
	}
}

func TestConsumeFromTimestampBeforeOldestStartsAtOldest(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 5)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: 1,
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("ConsumeFromTimestamp() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[:3])
	if result.NextLSN != 3 {
		t.Fatalf("NextLSN = %d, want 3", result.NextLSN)
	}
}

func TestConsumeFromTimestampAfterHeadReturnsEmptyAtHead(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 5)
	fixture.appendSegment(t, 5, 5)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 10})

	result, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: 99_000,
		Limit:       3,
	})
	if err != nil {
		t.Fatalf("ConsumeFromTimestamp() error = %v", err)
	}
	if len(result.Records) != 0 || result.NextLSN != 10 || result.Head.NextLSN != 10 {
		t.Fatalf("ConsumeFromTimestamp(after head) = %+v", result)
	}
}

func TestConsumeFromTimestampRejectsNegativeLimit(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})

	_, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: 1,
		Limit:       -1,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ConsumeFromTimestamp() error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestConsumeFromTimestampUsesInitialHeadSnapshot(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 5)
	second := fixture.appendSegment(t, 5, 5)
	cat := &stubCatalog{
		head: pmeta.PartitionHead{
			Partition:      fixture.partition,
			OldestLSN:      0,
			NextLSN:        5,
			WriterEpoch:    1,
			SegmentCount:   1,
			LastSegment:    first,
			HasLastSegment: true,
		},
		pages: []pmeta.SegmentPage{{Segments: []pmeta.SegmentRef{first, second}}},
	}
	r, err := New(cat, fixture.store, Options{MaxRecordsPerBatch: 20})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: 10_006,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("ConsumeFromTimestamp() error = %v", err)
	}
	if len(result.Records) != 0 || result.NextLSN != 5 || result.Head.NextLSN != 5 {
		t.Fatalf("ConsumeFromTimestamp(snapshot) = %+v", result)
	}
}

func TestSegmentReaderCacheAvoidsRepeatedMetadataReads(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	store := newCountingSegmentStore(fixture.store)
	cache := MustNewSegmentReaderCache(8)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch: 20,
		SegmentCache:       cache,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 20}); err != nil {
		t.Fatalf("Consume(first) error = %v", err)
	}
	metadataReads := store.metadataReads(segment)
	if metadataReads != 3 {
		t.Fatalf("metadata reads after first consume = %d, want 3", metadataReads)
	}
	if cache.Len() != 1 {
		t.Fatalf("cache len = %d, want 1", cache.Len())
	}

	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 20}); err != nil {
		t.Fatalf("Consume(second) error = %v", err)
	}
	if got := store.metadataReads(segment); got != metadataReads {
		t.Fatalf("metadata reads after cached consume = %d, want %d", got, metadataReads)
	}
}

func TestSegmentReaderCacheCoalescesConcurrentOpens(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	store := newCountingSegmentStore(fixture.store)
	store.delay = 25 * time.Millisecond
	cache := MustNewSegmentReaderCache(8)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch: 20,
		SegmentCache:       cache,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const goroutines = 12
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 20})
			if err != nil {
				errs <- err
				return
			}
			if len(result.Records) != 20 {
				errs <- fmt.Errorf("records=%d want=20", len(result.Records))
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := store.metadataReads(segment); got != 3 {
		t.Fatalf("metadata reads after concurrent consumes = %d, want 3", got)
	}
}

func TestSegmentReaderCacheEvictsByEntryLimit(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 5)
	second := fixture.appendSegment(t, 5, 5)
	store := newCountingSegmentStore(fixture.store)
	cache := MustNewSegmentReaderCache(1)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch: 5,
		SegmentCache:       cache,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 1}); err != nil {
		t.Fatalf("Consume(first segment) error = %v", err)
	}
	if got := store.metadataReads(first); got != 3 {
		t.Fatalf("first segment metadata reads = %d, want 3", got)
	}
	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 5, Limit: 1}); err != nil {
		t.Fatalf("Consume(second segment) error = %v", err)
	}
	if got := store.metadataReads(second); got != 3 {
		t.Fatalf("second segment metadata reads = %d, want 3", got)
	}
	if cache.Len() != 1 {
		t.Fatalf("cache len = %d, want 1", cache.Len())
	}
	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 0, Limit: 1}); err != nil {
		t.Fatalf("Consume(first after eviction) error = %v", err)
	}
	if got := store.metadataReads(first); got != 6 {
		t.Fatalf("first segment metadata reads after eviction = %d, want 6", got)
	}
}

type readerFixture struct {
	partition uint32
	writerID  [16]byte
	catalog   *catalog.MemoryCatalog
	session   catalog.WriterSession
	store     *testSegmentStore
	objects   map[string][]byte
	records   []Record
	nextURI   int
}

func newReaderFixture(t *testing.T) *readerFixture {
	t.Helper()
	cat := catalog.NewMemoryCatalog()
	writerID := [16]byte{9, 8, 7}
	session, err := cat.OpenWriter(context.Background(), 3, writerID)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	return &readerFixture{
		partition: 3,
		writerID:  writerID,
		catalog:   cat,
		session:   session,
		store:     newTestSegmentStore(nil),
		objects:   make(map[string][]byte),
	}
}

func (f *readerFixture) openReader(t *testing.T, opts Options) *Reader {
	t.Helper()
	r, err := New(f.catalog, f.store, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return r
}

func (f *readerFixture) appendSegment(t *testing.T, baseLSN uint64, count int) pmeta.SegmentRef {
	t.Helper()
	uri := fmt.Sprintf("memory://reader-test/%d", f.nextURI)
	f.nextURI++
	records := makeWriterRecords(count, baseLSN, 10_000+int64(baseLSN), 48)
	sink := segwriter.NewMemorySink(uri)
	opts := segwriter.DefaultOptions(f.partition)
	opts.Codec = segformat.CodecNone
	opts.HashAlgo = segformat.HashXXH64
	opts.TargetBlockSize = 256
	opts.PartSize = 128
	opts.SealParallelism = 2
	opts.BlockBufferCount = 5
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	opts.SegmentUUID = [16]byte{1, 2, 3, byte(f.nextURI)}
	opts.WriterTag = f.writerID
	opts.CreatedUnixMS = 1_776_263_000_000 + int64(f.nextURI)

	w, err := segwriter.New(opts, sink)
	if err != nil {
		t.Fatalf("segwriter.New() error = %v", err)
	}
	for _, record := range records {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	object := sink.Bytes()
	f.store.Put(result.Object.URI, object)
	f.objects[result.Object.URI] = object
	segment := pmeta.SegmentRef{
		URI:              result.Object.URI,
		Partition:        result.Metadata.Partition,
		WriterEpoch:      f.session.Epoch(),
		SegmentUUID:      result.Metadata.SegmentUUID,
		WriterTag:        f.writerID,
		BaseLSN:          result.Metadata.BaseLSN,
		LastLSN:          result.Metadata.LastLSN,
		MinTimestampMS:   result.Metadata.MinTimestampMS,
		MaxTimestampMS:   result.Metadata.MaxTimestampMS,
		RecordCount:      result.Metadata.RecordCount,
		BlockCount:       result.Metadata.BlockCount,
		SizeBytes:        result.Object.SizeBytes,
		BlockIndexOffset: result.Metadata.BlockIndexOffset,
		BlockIndexLength: result.Metadata.BlockIndexLength,
		Codec:            result.Metadata.Codec,
		HashAlgo:         result.Metadata.HashAlgo,
		SegmentHash:      result.Metadata.SegmentHash,
		TrailerHash:      result.Metadata.TrailerHash,
	}
	if _, err := f.session.AppendSegment(context.Background(), segment); err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	f.records = append(f.records, convertRecords(f.partition, records)...)
	return segment
}

func (f *readerFixture) storeObject(uri string) []byte {
	body, ok := f.objects[uri]
	if !ok {
		panic(fmt.Sprintf("missing object %s", uri))
	}
	return append([]byte(nil), body...)
}

func makeWriterRecords(count int, baseLSN uint64, baseTS int64, valueSize int) []segwriter.Record {
	records := make([]segwriter.Record, count)
	for i := range records {
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte('a' + (i+j)%26)
		}
		records[i] = segwriter.Record{
			LSN:         baseLSN + uint64(i),
			TimestampMS: baseTS + int64(i),
			Value:       value,
		}
	}
	return records
}

func convertRecords(partition uint32, records []segwriter.Record) []Record {
	out := make([]Record, len(records))
	for i, record := range records {
		out[i] = Record{
			Partition:   partition,
			LSN:         record.LSN,
			TimestampMS: record.TimestampMS,
			Headers:     segformat.CloneHeaders(record.Headers),
			Value:       append([]byte(nil), record.Value...),
		}
	}
	return out
}

func assertRecordsEqual(t *testing.T, got []Record, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Partition != want[i].Partition ||
			got[i].LSN != want[i].LSN ||
			got[i].TimestampMS != want[i].TimestampMS ||
			!headersEqual(got[i].Headers, want[i].Headers) ||
			!bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("record[%d] = {partition:%d lsn:%d ts:%d headers:%v value:%q}, want {partition:%d lsn:%d ts:%d headers:%v value:%q}",
				i, got[i].Partition, got[i].LSN, got[i].TimestampMS, got[i].Headers, got[i].Value,
				want[i].Partition, want[i].LSN, want[i].TimestampMS, want[i].Headers, want[i].Value)
		}
	}
}

func headersEqual(a []segformat.Header, b []segformat.Header) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func validSegmentRef(partition uint32, base uint64, last uint64) pmeta.SegmentRef {
	count := uint32(last - base + 1)
	return pmeta.SegmentRef{
		URI:              fmt.Sprintf("memory://segment/%d-%d", base, last),
		Partition:        partition,
		WriterEpoch:      1,
		SegmentUUID:      [16]byte{1, 2, 3},
		WriterTag:        [16]byte{4, 5, 6},
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   int64(base),
		MaxTimestampMS:   int64(last),
		RecordCount:      count,
		BlockCount:       1,
		SizeBytes:        1,
		BlockIndexOffset: 1,
		BlockIndexLength: segformat.IndexPreambleSize + segformat.BlockIndexEntrySize,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
	}
}

type testSegmentStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newTestSegmentStore(objects map[string][]byte) *testSegmentStore {
	s := &testSegmentStore{objects: make(map[string][]byte, len(objects))}
	for uri, body := range objects {
		s.objects[uri] = append([]byte(nil), body...)
	}
	return s
}

func (s *testSegmentStore) Put(uri string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[uri] = append([]byte(nil), body...)
}

func (s *testSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	body, ok := s.objects[uri]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("object not found: %s", uri)
	}
	if off > uint64(len(body)) {
		return nil, fmt.Errorf("offset=%d beyond object size=%d", off, len(body))
	}
	if n > uint64(len(body))-off {
		return nil, fmt.Errorf("range offset=%d length=%d beyond object size=%d", off, n, len(body))
	}
	start := int(off)
	end := start + int(n)
	return append([]byte(nil), body[start:end]...), nil
}

type countingSegmentStore struct {
	inner SegmentStore

	mu    sync.Mutex
	reads map[segmentReadKey]int
	delay time.Duration
}

type segmentReadKey struct {
	uri string
	off uint64
	n   uint64
}

func newCountingSegmentStore(inner SegmentStore) *countingSegmentStore {
	return &countingSegmentStore{
		inner: inner,
		reads: make(map[segmentReadKey]int),
	}
}

func (s *countingSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	s.reads[segmentReadKey{uri: uri, off: off, n: n}]++
	s.mu.Unlock()
	return s.inner.ReadAt(ctx, uri, off, n)
}

func (s *countingSegmentStore) metadataReads(segment pmeta.SegmentRef) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[segmentReadKey{uri: segment.URI, off: segment.SizeBytes - segformat.TrailerSize, n: segformat.TrailerSize}] +
		s.reads[segmentReadKey{uri: segment.URI, off: 0, n: segformat.FilePreambleSize}] +
		s.reads[segmentReadKey{uri: segment.URI, off: segment.BlockIndexOffset, n: uint64(segment.BlockIndexLength)}]
}

type stubCatalog struct {
	head  pmeta.PartitionHead
	pages []pmeta.SegmentPage
}

func (s *stubCatalog) LoadPartition(_ context.Context, _ uint32) (pmeta.PartitionHead, error) {
	return s.head, nil
}

func (s *stubCatalog) FindSegment(_ context.Context, _ uint32, _ uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (s *stubCatalog) ListSegments(_ context.Context, _ catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	if len(s.pages) == 0 {
		return pmeta.SegmentPage{}, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, nil
}
