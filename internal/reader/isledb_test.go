package reader

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/config"
	"github.com/ankur-anand/unijord/internal/writer"
)

func TestIsleBackendConsumePartition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	result, err := backend.ConsumePartition(ctx, 0, 1, 10)
	if err != nil {
		t.Fatalf("ConsumePartition() error = %v", err)
	}

	if len(result.Events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(result.Events))
	}
	if result.Events[0].LSN != 2 || string(result.Events[0].Value) != "b" {
		t.Fatalf("events[0] = (%d,%q), want (2,%q)", result.Events[0].LSN, result.Events[0].Value, "b")
	}
	if result.NextStartAfterLSN != 3 {
		t.Fatalf("next_start_after_lsn = %d, want 3", result.NextStartAfterLSN)
	}
	if result.HighWatermarkLSN != 3 {
		t.Fatalf("high_watermark_lsn = %d, want 3", result.HighWatermarkLSN)
	}
}

func TestIsleBackendGetPartitionHead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	result, err := backend.GetPartitionHead(ctx, 0)
	if err != nil {
		t.Fatalf("GetPartitionHead() error = %v", err)
	}
	if result.Partition != 0 {
		t.Fatalf("partition = %d, want 0", result.Partition)
	}
	if result.HighWatermarkLSN != 3 {
		t.Fatalf("high_watermark_lsn = %d, want 3", result.HighWatermarkLSN)
	}
}

func TestIsleBackendListPartitionHeads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 1, [][]byte{
		[]byte("b1"),
	})
	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a1"),
		[]byte("a2"),
		[]byte("a3"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           2,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	results, err := backend.ListPartitionHeads(ctx)
	if err != nil {
		t.Fatalf("ListPartitionHeads() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Partition != 0 || results[0].HighWatermarkLSN != 3 {
		t.Fatalf("results[0] = (%d,%d), want (0,3)", results[0].Partition, results[0].HighWatermarkLSN)
	}
	if results[1].Partition != 1 || results[1].HighWatermarkLSN != 1 {
		t.Fatalf("results[1] = (%d,%d), want (1,1)", results[1].Partition, results[1].HighWatermarkLSN)
	}
}

func TestIsleBackendTailPartition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
			[]byte("b"),
		})
	}()

	stopErr := errors.New("stop after first tail event")
	err = backend.TailPartition(ctx, 0, 1, false, func(event Event) error {
		if event.LSN != 2 {
			t.Errorf("tail event lsn = %d, want 2", event.LSN)
		}
		if got := string(event.Value); got != "b" {
			t.Errorf("tail event value = %q, want %q", got, "b")
		}
		return stopErr
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("TailPartition() error = %v, want %v", err, stopErr)
	}
}

func TestIsleBackendConsumeUsesSharedRefreshState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 300 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("b"),
	})

	result, err := backend.ConsumePartition(ctx, 0, 1, 10)
	if err != nil {
		t.Fatalf("ConsumePartition() error = %v", err)
	}
	if len(result.Events) != 0 {
		t.Fatalf("len(events) = %d, want 0 before shared refresh advances", len(result.Events))
	}

	time.Sleep(350 * time.Millisecond)

	result, err = backend.ConsumePartition(ctx, 0, 1, 10)
	if err != nil {
		t.Fatalf("ConsumePartition() after poll error = %v", err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("len(events) after poll = %d, want 1", len(result.Events))
	}
	if got := string(result.Events[0].Value); got != "b" {
		t.Fatalf("events[0].value = %q, want %q", got, "b")
	}
}

func TestIsleBackendTailDoesNotBlockOtherConsumers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	slowErr := errors.New("slow done")
	fastErr := errors.New("fast done")
	slowDone := make(chan time.Time, 1)
	fastDone := make(chan time.Time, 1)
	errCh := make(chan error, 2)

	go func() {
		errCh <- backend.TailPartition(ctx, 0, 0, false, func(event Event) error {
			if event.LSN != 1 {
				t.Errorf("slow tail lsn = %d, want 1", event.LSN)
			}
			time.Sleep(150 * time.Millisecond)
			slowDone <- time.Now()
			return slowErr
		})
	}()

	go func() {
		errCh <- backend.TailPartition(ctx, 0, 0, false, func(event Event) error {
			if event.LSN != 1 {
				t.Errorf("fast tail lsn = %d, want 1", event.LSN)
			}
			fastDone <- time.Now()
			return fastErr
		})
	}()

	time.Sleep(30 * time.Millisecond)
	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
	})

	var fastAt time.Time
	select {
	case fastAt = <-fastDone:
	case <-time.After(time.Second):
		t.Fatal("fast tail did not receive event")
	}

	var slowAt time.Time
	select {
	case slowAt = <-slowDone:
	case <-time.After(time.Second):
		t.Fatal("slow tail did not receive event")
	}

	if !fastAt.Before(slowAt) {
		t.Fatalf("fast tail completed at %s, slow tail completed at %s; want fast tail to finish first", fastAt, slowAt)
	}

	gotErrors := []error{<-errCh, <-errCh}
	if !containsError(gotErrors, slowErr) || !containsError(gotErrors, fastErr) {
		t.Fatalf("tail errors = %v, want both %v and %v", gotErrors, slowErr, fastErr)
	}
}

func TestIsleBackendTailPartitionFromNowSkipsHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
		[]byte("b"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 20 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
			[]byte("c"),
		})
	}()

	stopErr := errors.New("stop after first live event")
	err = backend.TailPartition(ctx, 0, 0, true, func(event Event) error {
		if event.LSN != 3 {
			t.Errorf("tail event lsn = %d, want 3", event.LSN)
		}
		if got := string(event.Value); got != "c" {
			t.Errorf("tail event value = %q, want %q", got, "c")
		}
		return stopErr
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("TailPartition(from_now) error = %v, want %v", err, stopErr)
	}
}

func TestIsleBackendTailPartitionFromNowUsesPublishedViewBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	bucketURL := "file://" + rootDir
	namespace := "order-events"

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("a"),
		[]byte("b"),
	})

	cfg := config.ReaderConfig{
		BucketURL:            bucketURL,
		Namespace:            namespace,
		Partitions:           1,
		CacheDir:             filepath.Join(rootDir, "cache"),
		ManifestPollInterval: 200 * time.Millisecond,
	}
	backend, err := OpenIsleBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleBackend() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close() error = %v", err)
		}
	})

	controller, ok := backend.partitions[0]
	if !ok {
		t.Fatal("partition 0 controller not found")
	}

	time.Sleep(250 * time.Millisecond)

	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("c"),
	})

	refreshCtx, cancel := context.WithTimeout(ctx, time.Second)
	view, changed, err := controller.coord.Refresh(refreshCtx)
	cancel()
	if err != nil {
		t.Fatalf("coord.Refresh() error = %v", err)
	}
	if view == nil {
		t.Fatal("coord.Refresh() view = nil")
	}
	if !changed {
		_ = view.Close()
		t.Fatal("coord.Refresh() changed = false, want true")
	}
	if got := viewHighWatermark(view); got != 3 {
		_ = view.Close()
		t.Fatalf("view high watermark = %d, want 3", got)
	}
	_ = view.Close()

	done := make(chan error, 1)
	stopErr := errors.New("stop after first live event")
	go func() {
		done <- backend.TailPartition(ctx, 0, 0, true, func(event Event) error {
			if event.LSN != 4 {
				t.Errorf("tail event lsn = %d, want 4", event.LSN)
			}
			if got := string(event.Value); got != "d" {
				t.Errorf("tail event value = %q, want %q", got, "d")
			}
			return stopErr
		})
	}()

	time.Sleep(50 * time.Millisecond)
	writePartitionEvents(t, ctx, bucketURL, namespace, 0, [][]byte{
		[]byte("d"),
	})

	select {
	case err := <-done:
		if !errors.Is(err, stopErr) {
			t.Fatalf("TailPartition(from_now) error = %v, want %v", err, stopErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TailPartition(from_now) did not return")
	}
}

func writePartitionEvents(t *testing.T, ctx context.Context, bucketURL, namespace string, partition int, values [][]byte) {
	t.Helper()

	cfg := config.WriterConfig{
		BucketURL:      bucketURL,
		Namespace:      namespace,
		Partition:      partition,
		GRPCListen:     ":0",
		MetricsListen:  ":0",
		MemtableSizeMB: 4,
		FlushInterval:  0,
		LogLevel:       "error",
		WALDir:         filepath.Join(t.TempDir(), fmt.Sprintf("wal-%d", partition)),
		WALSegmentMB:   16,
		WALBytesSync:   1 << 20,
		WALMaxSync:     50 * time.Millisecond,
	}

	appender, err := writer.OpenIsleAppender(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenIsleAppender() error = %v", err)
	}
	for _, value := range values {
		if _, err := appender.Append(ctx, value); err != nil {
			t.Fatalf("Append(%q) error = %v", value, err)
		}
	}
	if err := appender.Close(); err != nil {
		t.Fatalf("appender.Close() error = %v", err)
	}
}

func containsError(errs []error, want error) bool {
	for _, err := range errs {
		if errors.Is(err, want) {
			return true
		}
	}
	return false
}
