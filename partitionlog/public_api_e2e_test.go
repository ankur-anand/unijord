package partitionlog_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	plazure "github.com/ankur-anand/unijord/partitionlog/azure"
	plgcs "github.com/ankur-anand/unijord/partitionlog/gcs"
	pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
)

func TestPublicAPIEndToEndAcrossBlobStores(t *testing.T) {
	for _, tc := range publicAPIStoreCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runPublicAPIEndToEnd(t, tc.open(t, "partitionlog-public-"+tc.name))
		})
	}
}

type publicAPIStoreCase struct {
	name string
	open func(t *testing.T, prefix string) partitionlog.Store
}

func publicAPIStoreCases() []publicAPIStoreCase {
	return []publicAPIStoreCase{
		{
			name: "s3",
			open: func(t *testing.T, prefix string) partitionlog.Store {
				t.Helper()
				const bucket = "segments"
				store, err := pls3.New(pls3.Options{
					Client: newFakeS3Client(t, bucket),
					Bucket: bucket,
					Prefix: prefix,
				})
				if err != nil {
					t.Fatalf("s3.New() error = %v", err)
				}
				return store
			},
		},
		{
			name: "gcs",
			open: func(t *testing.T, prefix string) partitionlog.Store {
				t.Helper()
				const bucket = "segments"
				store, err := plgcs.New(plgcs.Options{
					Client: newFakeGCSClient(t, bucket),
					Bucket: bucket,
					Prefix: prefix,
				})
				if err != nil {
					t.Fatalf("gcs.New() error = %v", err)
				}
				return store
			},
		},
		{
			name: "azure",
			open: func(t *testing.T, prefix string) partitionlog.Store {
				t.Helper()
				server := newFakeAzureBlobServer(t)
				store, err := plazure.New(plazure.Options{
					Container: newFakeAzureContainerClient(t, server.URL, "container"),
					Prefix:    prefix,
				})
				if err != nil {
					t.Fatalf("azure.New() error = %v", err)
				}
				return store
			},
		},
	}
}

func runPublicAPIEndToEnd(t *testing.T, store partitionlog.Store) {
	t.Helper()
	ctx := context.Background()
	const (
		partition      uint32 = 101
		tailPartition  uint32 = 102
		delayPartition uint32 = 103
	)

	log, err := partitionlog.Open(partitionlog.Options{
		Store: store,
		Reader: partitionlog.ReaderOptions{
			MaxRecordsPerBatch: 3,
			RangeCacheBytes:    8 << 20,
			OpenSegmentReaders: 8,
			Refresh: partitionlog.RefreshPolicy{
				PollInterval:           5 * time.Millisecond,
				MaxConcurrentRefreshes: 4,
				RefreshTimeout:         time.Second,
			},
		},
	})
	if err != nil {
		t.Fatalf("partitionlog.Open() error = %v", err)
	}

	partitionReader := log.Reader().Partition(partition)
	cachedEmpty, err := partitionReader.Read(ctx, partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: partitionlog.FreshnessCached,
	})
	if err != nil {
		t.Fatalf("initial cached Read() error = %v", err)
	}
	if len(cachedEmpty.Records) != 0 || cachedEmpty.NextLSN != 0 {
		t.Fatalf("initial cached Read() = %+v, want empty at next_lsn=0", cachedEmpty)
	}

	w, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: partition,
		WriterID:  [16]byte{1, 0, 1},
		Batch: partitionlog.BatchPolicy{
			MaxRecords: 2,
			MaxBytes:   4 << 10,
		},
		Backpressure: partitionlog.BackpressurePolicy{
			MaxPendingBatches: 4,
			MaxPendingBytes:   8 << 20,
		},
		Pipeline: partitionlog.WriterPipelineOptions{
			BlockBytes:        256,
			PartBytes:         512,
			SealParallelism:   1,
			BlockBuffers:      3,
			UploadParallelism: 2,
			UploadQueueSize:   2,
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = w.Abort(context.Background()) }()

	want := []partitionlog.Record{
		{
			TimestampMS: 1_000,
			Headers: []partitionlog.Header{
				{Key: []byte("type"), Value: []byte("start")},
				{Key: []byte("source"), Value: []byte("public-api")},
			},
			Value: []byte("alpha"),
		},
		{TimestampMS: 1_001, Headers: []partitionlog.Header{{Key: []byte("type"), Value: []byte("data")}}, Value: []byte("bravo")},
		{TimestampMS: 1_002, Headers: []partitionlog.Header{{Key: []byte("type"), Value: []byte("data")}}, Value: []byte("charlie")},
		{TimestampMS: 1_003, Headers: []partitionlog.Header{{Key: []byte("type"), Value: []byte("data")}}, Value: []byte("delta")},
		{TimestampMS: 1_004, Headers: []partitionlog.Header{{Key: []byte("type"), Value: []byte("end")}}, Value: []byte("echo")},
	}
	for i, record := range want {
		result, err := w.Append(ctx, record)
		if err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		if result.LSN != uint64(i) {
			t.Fatalf("Append(%d) LSN = %d, want %d", i, result.LSN, i)
		}
		if i == 0 {
			if err := w.Cut(ctx); err != nil {
				t.Fatalf("Cut() error = %v", err)
			}
		}
	}
	snapshot, err := w.Close(ctx)
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if snapshot.Head.Partition != partition || snapshot.Head.NextLSN != uint64(len(want)) {
		t.Fatalf("Close() snapshot head = %+v, want partition=%d next_lsn=%d", snapshot.Head, partition, len(want))
	}
	if snapshot.Head.SegmentCount != 3 {
		t.Fatalf("Close() segment_count = %d, want 3", snapshot.Head.SegmentCount)
	}
	if w.Err() != nil {
		t.Fatalf("Writer.Err() = %v, want nil", w.Err())
	}
	if state := w.State(); state.Snapshot.Head != snapshot.Head {
		t.Fatalf("State().Snapshot.Head = %+v, want %+v", state.Snapshot.Head, snapshot.Head)
	}

	stale, err := partitionReader.Read(ctx, partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: partitionlog.FreshnessCached,
	})
	if err != nil {
		t.Fatalf("stale cached Read() error = %v", err)
	}
	if len(stale.Records) != 0 {
		t.Fatalf("stale cached records = %d, want 0 before on-tail refresh", len(stale.Records))
	}

	first, err := partitionReader.Read(ctx, partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: partitionlog.FreshnessOnTail,
	})
	if err != nil {
		t.Fatalf("on-tail Read(first) error = %v", err)
	}
	assertPublicRecords(t, partition, 0, first.Records, want[:3])
	if first.NextLSN != 3 {
		t.Fatalf("Read(first) NextLSN = %d, want 3", first.NextLSN)
	}

	second, err := partitionReader.Read(ctx, partitionlog.ReadRequest{
		StartLSN:  first.NextLSN,
		Limit:     10,
		Freshness: partitionlog.FreshnessCached,
	})
	if err != nil {
		t.Fatalf("cached Read(second) error = %v", err)
	}
	assertPublicRecords(t, partition, 3, second.Records, want[3:])
	if second.NextLSN != uint64(len(want)) {
		t.Fatalf("Read(second) NextLSN = %d, want %d", second.NextLSN, len(want))
	}

	head, err := partitionReader.Head(ctx)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if head != snapshot.Head {
		t.Fatalf("Head() = %+v, want %+v", head, snapshot.Head)
	}

	cursor, err := partitionReader.Cursor(partitionlog.CursorOptions{StartLSN: 0, Limit: 2})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	cursorBatch, err := cursor.Next(ctx)
	if err != nil {
		t.Fatalf("Cursor.Next() error = %v", err)
	}
	assertPublicRecords(t, partition, 0, cursorBatch.Records, want[:2])
	if cursor.Position() != 2 {
		t.Fatalf("Cursor.Position() = %d, want 2", cursor.Position())
	}
	fork := cursor.Fork()
	cursor.Seek(4)
	last, err := cursor.Next(ctx)
	if err != nil {
		t.Fatalf("Cursor.Next(after Seek) error = %v", err)
	}
	assertPublicRecords(t, partition, 4, last.Records, want[4:])
	replayed, err := fork.Next(ctx)
	if err != nil {
		t.Fatalf("Fork.Next() error = %v", err)
	}
	assertPublicRecords(t, partition, 2, replayed.Records, want[2:4])
	if err := cursor.Close(); err != nil {
		t.Fatalf("Cursor.Close() error = %v", err)
	}
	if _, err := cursor.Next(ctx); err == nil {
		t.Fatal("Cursor.Next(after Close) error = nil, want error")
	}

	extraReader, err := log.NewReader(partitionlog.ReaderOptions{
		MaxRecordsPerBatch: 2,
		OpenSegmentReaders: 2,
	})
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	extraRead, err := extraReader.Partition(partition).Read(ctx, partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: partitionlog.FreshnessLatest,
	})
	if err != nil {
		t.Fatalf("extra reader Read() error = %v", err)
	}
	assertPublicRecords(t, partition, 0, extraRead.Records, want[:2])

	reopened, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: partition,
		WriterID:  [16]byte{2, 0, 1},
		Batch:     partitionlog.BatchPolicy{MaxRecords: 1},
	})
	if err != nil {
		t.Fatalf("OpenWriter(reopen) error = %v", err)
	}
	defer func() { _ = reopened.Abort(context.Background()) }()
	reopenRecord := partitionlog.Record{
		TimestampMS: 1_005,
		Headers:     []partitionlog.Header{{Key: []byte("type"), Value: []byte("reopen")}},
		Value:       []byte("foxtrot"),
	}
	reopenResult, err := reopened.Append(ctx, reopenRecord)
	if err != nil {
		t.Fatalf("Append(reopen) error = %v", err)
	}
	if reopenResult.LSN != uint64(len(want)) {
		t.Fatalf("Append(reopen) LSN = %d, want %d", reopenResult.LSN, len(want))
	}
	reopenedSnapshot, err := reopened.Close(ctx)
	if err != nil {
		t.Fatalf("Close(reopen) error = %v", err)
	}
	if reopenedSnapshot.Head.NextLSN != uint64(len(want)+1) {
		t.Fatalf("Close(reopen) next_lsn = %d, want %d", reopenedSnapshot.Head.NextLSN, len(want)+1)
	}
	freshReopen, err := partitionReader.Read(ctx, partitionlog.ReadRequest{
		StartLSN:  uint64(len(want)),
		Limit:     10,
		Freshness: partitionlog.FreshnessLatest,
	})
	if err != nil {
		t.Fatalf("Read(reopen) error = %v", err)
	}
	assertPublicRecords(t, partition, uint64(len(want)), freshReopen.Records, []partitionlog.Record{reopenRecord})

	runPublicAPITailFlow(t, log, tailPartition)
	runPublicAPIMaxDelayFlow(t, log, delayPartition)
}

func runPublicAPITailFlow(t *testing.T, log *partitionlog.Log, partition uint32) {
	t.Helper()
	ctx := context.Background()
	watch, err := log.Reader().Watch(ctx, partitionlog.WatchOptions{
		Partitions: []uint32{partition},
	})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer func() { _ = watch.Close() }()

	if err := watch.AddPartition(partition + 100); err != nil {
		t.Fatalf("Watch.AddPartition() error = %v", err)
	}
	watch.RemovePartition(partition + 100)

	tailer, err := watch.Tail(partitionlog.TailOptions{Partition: partition, StartLSN: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Watch.Tail() error = %v", err)
	}
	defer func() { _ = tailer.Close() }()

	resultCh := make(chan partitionlog.ReadResult, 1)
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

	w, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: partition,
		WriterID:  [16]byte{3, 0, 1},
	})
	if err != nil {
		t.Fatalf("OpenWriter(tail) error = %v", err)
	}
	defer func() { _ = w.Abort(context.Background()) }()
	record := partitionlog.Record{TimestampMS: 2_000, Headers: []partitionlog.Header{{Key: []byte("type"), Value: []byte("tail")}}, Value: []byte("tail-one")}
	if _, err := w.Append(ctx, record); err != nil {
		t.Fatalf("Append(tail) error = %v", err)
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush(tail) error = %v", err)
	}
	if _, err := w.Close(ctx); err != nil {
		t.Fatalf("Close(tail) error = %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("Tailer.Next() error = %v", err)
	case result := <-resultCh:
		assertPublicRecords(t, partition, 0, result.Records, []partitionlog.Record{record})
		if tailer.Position() != 1 {
			t.Fatalf("Tailer.Position() = %d, want 1", tailer.Position())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for tail result")
	}
	if err := watch.Close(); err != nil {
		t.Fatalf("Watch.Close() error = %v", err)
	}
	if err := watch.AddPartition(partition + 1); !errors.Is(err, partitionlog.ErrWatchClosed) {
		t.Fatalf("Watch.AddPartition(after Close) error = %v, want %v", err, partitionlog.ErrWatchClosed)
	}
}

func runPublicAPIMaxDelayFlow(t *testing.T, log *partitionlog.Log, partition uint32) {
	t.Helper()
	ctx := context.Background()
	w, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: partition,
		WriterID:  [16]byte{4, 0, 1},
		Batch: partitionlog.BatchPolicy{
			MaxDelay: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("OpenWriter(max delay) error = %v", err)
	}
	defer func() { _ = w.Abort(context.Background()) }()

	record := partitionlog.Record{
		TimestampMS: 3_000,
		Headers:     []partitionlog.Header{{Key: []byte("type"), Value: []byte("max-delay")}},
		Value:       []byte("delayed"),
	}
	if _, err := w.Append(ctx, record); err != nil {
		t.Fatalf("Append(max delay) error = %v", err)
	}
	visible := waitForPublicRecords(t, log.Reader(), partition, partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     10,
		Freshness: partitionlog.FreshnessLatest,
	}, 1)
	assertPublicRecords(t, partition, 0, visible.Records, []partitionlog.Record{record})
	if _, err := w.Close(ctx); err != nil {
		t.Fatalf("Close(max delay) error = %v", err)
	}
}

func waitForPublicRecords(t *testing.T, r *partitionlog.Reader, partition uint32, req partitionlog.ReadRequest, want int) partitionlog.ReadResult {
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
			t.Fatalf("timed out waiting for %d visible records", want)
		case <-ticker.C:
		}
	}
}

func assertPublicRecords(t *testing.T, partition uint32, baseLSN uint64, got []partitionlog.ReadRecord, want []partitionlog.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Partition != partition {
			t.Fatalf("record[%d].Partition = %d, want %d", i, got[i].Partition, partition)
		}
		if got[i].LSN != baseLSN+uint64(i) {
			t.Fatalf("record[%d].LSN = %d, want %d", i, got[i].LSN, baseLSN+uint64(i))
		}
		if got[i].TimestampMS != want[i].TimestampMS {
			t.Fatalf("record[%d].TimestampMS = %d, want %d", i, got[i].TimestampMS, want[i].TimestampMS)
		}
		if string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("record[%d].Value = %q, want %q", i, got[i].Value, want[i].Value)
		}
		assertPublicHeaders(t, fmt.Sprintf("record[%d].Headers", i), got[i].Headers, want[i].Headers)
	}
}

func assertPublicHeaders(t *testing.T, label string, got []partitionlog.Header, want []partitionlog.Header) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s len = %d, want %d", label, len(got), len(want))
	}
	for i := range want {
		if string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("%s[%d] = %q:%q, want %q:%q", label, i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}
