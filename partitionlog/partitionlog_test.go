package partitionlog

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestLogWriteAndReadWithMemoryStore(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	log, err := Open(Options{Store: store})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	w, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := w.Append(ctx, Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	if _, err := w.Append(ctx, Record{TimestampMS: 11, Value: []byte("b")}); err != nil {
		t.Fatalf("Append(1) error = %v", err)
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := log.Reader().Partition(1).Read(ctx, ReadRequest{
		StartLSN: 0,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(got.Records))
	}
	if got.Records[0].LSN != 0 || string(got.Records[0].Value) != "a" {
		t.Fatalf("record[0] = %+v, want lsn=0 value=a", got.Records[0])
	}
	if got.Records[1].LSN != 1 || string(got.Records[1].Value) != "b" {
		t.Fatalf("record[1] = %+v, want lsn=1 value=b", got.Records[1])
	}
}

func TestPartitionReaderFreshnessOnTailRefreshesOnlyAtTail(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	log, err := Open(Options{
		Store: store,
		Reader: ReaderOptions{
			Refresh: RefreshPolicy{
				PollInterval: 5 * time.Millisecond,
			},
		},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	partition := log.Reader().Partition(1)

	empty, err := partition.Read(ctx, ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: FreshnessCached,
	})
	if err != nil {
		t.Fatalf("initial cached Read() error = %v", err)
	}
	if len(empty.Records) != 0 {
		t.Fatalf("initial cached records = %d, want 0", len(empty.Records))
	}

	w, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := w.Append(ctx, Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	stale, err := partition.Read(ctx, ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: FreshnessCached,
	})
	if err != nil {
		t.Fatalf("stale cached Read() error = %v", err)
	}
	if len(stale.Records) != 0 {
		t.Fatalf("stale cached records = %d, want 0", len(stale.Records))
	}

	fresh, err := partition.Read(ctx, ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: FreshnessOnTail,
	})
	if err != nil {
		t.Fatalf("on-tail Read() error = %v", err)
	}
	if len(fresh.Records) != 1 || string(fresh.Records[0].Value) != "a" {
		t.Fatalf("fresh records = %+v, want one value=a", fresh.Records)
	}
}

func TestPartitionCursorAdvancesPosition(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	log, err := Open(Options{Store: store})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	w, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for i, value := range []string{"a", "b"} {
		if _, err := w.Append(ctx, Record{TimestampMS: int64(10 + i), Value: []byte(value)}); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	cursor, err := log.Reader().Partition(1).Cursor(CursorOptions{StartLSN: 0, Limit: 1})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	got, err := cursor.Next(ctx)
	if err != nil {
		t.Fatalf("Next(0) error = %v", err)
	}
	if len(got.Records) != 1 || got.Records[0].LSN != 0 {
		t.Fatalf("Next(0) records = %+v, want lsn 0", got.Records)
	}
	if cursor.Position() != 1 {
		t.Fatalf("Position() = %d, want 1", cursor.Position())
	}
	got, err = cursor.Next(ctx)
	if err != nil {
		t.Fatalf("Next(1) error = %v", err)
	}
	if len(got.Records) != 1 || got.Records[0].LSN != 1 {
		t.Fatalf("Next(1) records = %+v, want lsn 1", got.Records)
	}
}

func TestWatchTailRequiresExplicitPartition(t *testing.T) {
	log, err := Open(Options{Store: newTestStore(t)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	watch, err := log.Reader().Watch(context.Background(), WatchOptions{
		Partitions: []uint32{1},
	})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer func() { _ = watch.Close() }()

	if _, err := watch.Tail(TailOptions{Partition: 2}); err == nil {
		t.Fatal("Tail(unwatched partition) error = nil, want error")
	}
}

func TestWatchTailerWaitsForPublishedRecords(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	log, err := Open(Options{Store: store})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	watch, err := log.Reader().Watch(ctx, WatchOptions{
		Partitions: []uint32{1},
	})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer func() { _ = watch.Close() }()

	tailer, err := watch.Tail(TailOptions{Partition: 1, StartLSN: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	defer func() { _ = tailer.Close() }()

	resultCh := make(chan ReadResult, 1)
	errCh := make(chan error, 1)
	nextCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go func() {
		result, err := tailer.Next(nextCtx)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	time.Sleep(20 * time.Millisecond)
	writer, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := writer.Append(ctx, Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("Tailer.Next() error = %v", err)
	case result := <-resultCh:
		if len(result.Records) != 1 || string(result.Records[0].Value) != "a" {
			t.Fatalf("tail records = %+v, want one value=a", result.Records)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for tail result")
	}
}

func TestLogWriterBatchMaxDelayMakesLowVolumeRecordVisible(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	log, err := Open(Options{Store: store})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	w, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
		Batch: BatchPolicy{
			MaxDelay: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = w.Abort(context.Background()) }()

	if _, err := w.Append(ctx, Record{TimestampMS: 10, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	waitForVisibleRecords(t, log.Reader(), 1, ReadRequest{
		StartLSN: 0,
		Limit:    10,
	}, 1)
}

func TestLogOpenWriterRejectsInvalidPublicWriterOptions(t *testing.T) {
	log, err := Open(Options{Store: newTestStore(t)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := log.OpenWriter(context.Background(), WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
		Batch: BatchPolicy{
			MaxDelay: -time.Second,
		},
	}); err == nil {
		t.Fatal("OpenWriter(negative batch max delay) error = nil, want error")
	}
	if _, err := log.OpenWriter(context.Background(), WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
		Pipeline: WriterPipelineOptions{
			UploadParallelism: -1,
		},
	}); err == nil {
		t.Fatal("OpenWriter(negative upload parallelism) error = nil, want error")
	}
}

func TestLogWriterPipelineOptionsAreAccepted(t *testing.T) {
	log, err := Open(Options{Store: newTestStore(t)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	w, err := log.OpenWriter(context.Background(), WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
		Pipeline: WriterPipelineOptions{
			BlockBytes:        128,
			PartBytes:         128,
			SealParallelism:   1,
			BlockBuffers:      3,
			UploadParallelism: 1,
			UploadQueueSize:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = w.Abort(context.Background()) }()

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("x")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func TestLogMetricsObserverReceivesPublicAndBackgroundEvents(t *testing.T) {
	ctx := context.Background()
	metrics := &recordingMetrics{}
	log, err := Open(Options{
		Store:   newTestStore(t),
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	w, err := log.OpenWriter(ctx, WriterOptions{
		Partition: 1,
		WriterID:  [16]byte{1},
		Batch:     BatchPolicy{MaxRecords: 1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := w.Append(ctx, Record{TimestampMS: 1, Value: []byte("x")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := log.Reader().Partition(1).Read(ctx, ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: FreshnessLatest,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("records = %d, want 1", len(got.Records))
	}

	events := metrics.events()
	for _, name := range []MetricName{
		MetricWriterAppend,
		MetricWriterFlush,
		MetricWriterSegmentFinalize,
		MetricWriterSegmentPublish,
		MetricReaderCatalogRefresh,
		MetricReaderSegmentRead,
		MetricReaderRead,
	} {
		if !hasMetric(events, name) {
			t.Fatalf("missing metric %s in events %+v", name, events)
		}
	}
	appendMetric, ok := findMetric(events, MetricWriterAppend)
	if !ok {
		t.Fatal("missing append metric")
	}
	if appendMetric.Partition != 1 || appendMetric.LSN != 0 || appendMetric.Records != 1 || appendMetric.Bytes == 0 || appendMetric.Err != nil {
		t.Fatalf("append metric = %+v, want partition=1 lsn=0 records=1 bytes>0 err=nil", appendMetric)
	}
	readMetric, ok := findMetric(events, MetricReaderRead)
	if !ok {
		t.Fatal("missing read metric")
	}
	if readMetric.Partition != 1 || readMetric.StartLSN != 0 || readMetric.NextLSN != 1 || readMetric.Records != 1 || readMetric.Err != nil {
		t.Fatalf("read metric = %+v, want partition=1 start=0 next=1 records=1 err=nil", readMetric)
	}
}

func waitForVisibleRecords(t *testing.T, r *Reader, partition uint32, req ReadRequest, want int) ReadResult {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := r.Partition(partition).Read(context.Background(), req)
		if err == nil && len(result.Records) >= want {
			return result
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d records", want)
		case <-ticker.C:
		}
	}
}

type recordingMetrics struct {
	mu    sync.Mutex
	items []Metric
}

func (m *recordingMetrics) Observe(event Metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, event)
}

func (m *recordingMetrics) events() []Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Metric(nil), m.items...)
}

func hasMetric(events []Metric, name MetricName) bool {
	_, ok := findMetric(events, name)
	return ok
}

func findMetric(events []Metric, name MetricName) (Metric, bool) {
	for _, event := range events {
		if event.Name == name {
			return event, true
		}
	}
	return Metric{}, false
}

type testStore struct {
	catalog *catalog.MemoryCatalog
	sink    *segmentsink.Factory
	source  *testSegmentStore
}

func newTestStore(t *testing.T) *testStore {
	t.Helper()
	objects := multipart.NewMemoryStore()
	sinkFactory, err := segmentsink.New(objects, segmentsink.Options{})
	if err != nil {
		t.Fatalf("sink.New() error = %v", err)
	}
	return &testStore{
		catalog: catalog.NewMemory(),
		sink:    sinkFactory,
		source:  &testSegmentStore{objects: objects},
	}
}

func (s *testStore) WriterManager() catalog.WriterManager {
	return s.catalog
}

func (s *testStore) ReaderCatalog() catalog.Reader {
	return s.catalog
}

func (s *testStore) SinkFactory() writer.SinkFactory {
	return s.sink
}

func (s *testStore) SegmentStore() SegmentStore {
	return s.source
}

type testSegmentStore struct {
	objects *multipart.MemoryStore
}

func (s *testSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	body, _, err := s.objects.Read(ctx, uri)
	if err != nil {
		return nil, err
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
