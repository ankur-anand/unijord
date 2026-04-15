package writer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/unijord/internal/config"
	"github.com/ankur-anand/unijord/internal/recordbin"
	"github.com/ankur-anand/walfs"
)

func TestIsleAppenderPersistsEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.WriterConfig{
		BucketURL:      "mem://eventlake",
		Namespace:      "order-events",
		Partition:      2,
		MemtableSizeMB: 4,
		FlushInterval:  0,
		WALDir:         filepath.Join(t.TempDir(), "wal"),
		WALSegmentMB:   16,
		WALBytesSync:   1 << 20,
		WALMaxSync:     50 * time.Millisecond,
	}

	store := blobstore.NewMemory(cfg.StoragePrefix())
	appender, err := openIsleAppenderWithStore(ctx, cfg, store)
	if err != nil {
		t.Fatalf("openIsleAppenderWithStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := appender.Close(); err != nil {
			t.Fatalf("appender.Close() error = %v", err)
		}
	})

	first, err := appender.Append(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if first.TimestampMS == 0 {
		t.Fatal("first TimestampMS = 0, want non-zero")
	}
	second, err := appender.Append(ctx, []byte("world"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	third, err := appender.Append(ctx, []byte("again"))
	if err != nil {
		t.Fatalf("Append(third) error = %v", err)
	}

	if err := appender.flushNow(ctx); err != nil {
		t.Fatalf("flushNow() error = %v", err)
	}

	readerOpts := isledb.DefaultReaderOpenOptions()
	readerOpts.CacheDir = t.TempDir()
	reader, err := isledb.OpenReader(ctx, store, readerOpts)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("reader.Close() error = %v", err)
		}
	})

	got, found, err := reader.Get(ctx, encodeLSNKey(first.LSN))
	if err != nil {
		t.Fatalf("reader.Get(first) error = %v", err)
	}
	if !found {
		t.Fatal("reader.Get(first) found = false, want true")
	}
	stored, err := recordbin.DecodeStoredValue(got)
	if err != nil {
		t.Fatalf("DecodeStoredValue(first) error = %v", err)
	}
	if string(stored.Value) != "hello" {
		t.Fatalf("reader.Get(first) = %q, want %q", stored.Value, "hello")
	}
	if stored.TimestampMS == 0 {
		t.Fatal("first timestamp_ms = 0, want non-zero")
	}

	got, found, err = reader.Get(ctx, encodeLSNKey(third.LSN))
	if err != nil {
		t.Fatalf("reader.Get(third) error = %v", err)
	}
	if !found {
		t.Fatal("reader.Get(third) found = false, want true")
	}
	stored, err = recordbin.DecodeStoredValue(got)
	if err != nil {
		t.Fatalf("DecodeStoredValue(third) error = %v", err)
	}
	if string(stored.Value) != "again" {
		t.Fatalf("reader.Get(third) = %q, want %q", stored.Value, "again")
	}

	got, found, err = reader.Get(ctx, encodeLSNKey(second.LSN))
	if err != nil {
		t.Fatalf("reader.Get(second) error = %v", err)
	}
	if !found {
		t.Fatal("reader.Get(second) found = false, want true")
	}
	stored, err = recordbin.DecodeStoredValue(got)
	if err != nil {
		t.Fatalf("DecodeStoredValue(second) error = %v", err)
	}
	if string(stored.Value) != "world" {
		t.Fatalf("reader.Get(second) = %q, want %q", stored.Value, "world")
	}
}

func TestIsleAppenderReplaysWALOnStartup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rootDir := t.TempDir()
	cfg := config.WriterConfig{
		BucketURL:      "mem://eventlake",
		Namespace:      "order-events",
		Partition:      7,
		MemtableSizeMB: 4,
		FlushInterval:  0,
		WALDir:         filepath.Join(rootDir, "wal"),
		WALSegmentMB:   16,
		WALBytesSync:   1 << 20,
		WALMaxSync:     50 * time.Millisecond,
	}

	wal, err := walfs.NewWALog(cfg.WALDir, walFileExt,
		walfs.WithMaxSegmentSize(int64(cfg.WALSegmentMB)*1024*1024),
		walfs.WithBytesPerSync(cfg.WALBytesSync),
	)
	if err != nil {
		t.Fatalf("NewWALog() error = %v", err)
	}

	records := []Record{
		{LSN: 1, TimestampMS: 1001, Value: []byte("hello")},
		{LSN: 2, TimestampMS: 1002, Value: []byte("world")},
	}
	for i, record := range records {
		if _, err := wal.Write(encodeWALRecord(record.LSN, record.TimestampMS, record.Value), uint64(i+1)); err != nil {
			t.Fatalf("wal.Write(%d) error = %v", i, err)
		}
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("wal.Sync() error = %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close() error = %v", err)
	}

	store := blobstore.NewMemory(cfg.StoragePrefix())
	appender, err := openIsleAppenderWithStore(ctx, cfg, store)
	if err != nil {
		t.Fatalf("openIsleAppenderWithStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := appender.Close(); err != nil {
			t.Fatalf("appender.Close() error = %v", err)
		}
	})

	readerOpts := isledb.DefaultReaderOpenOptions()
	readerOpts.CacheDir = t.TempDir()
	reader, err := isledb.OpenReader(ctx, store, readerOpts)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("reader.Close() error = %v", err)
		}
	})

	for _, record := range records {
		got, found, err := reader.Get(ctx, encodeLSNKey(record.LSN))
		if err != nil {
			t.Fatalf("reader.Get(lsn=%d) error = %v", record.LSN, err)
		}
		if !found {
			t.Fatalf("reader.Get(lsn=%d) found = false, want true", record.LSN)
		}
		stored, err := recordbin.DecodeStoredValue(got)
		if err != nil {
			t.Fatalf("DecodeStoredValue(lsn=%d) error = %v", record.LSN, err)
		}
		if string(stored.Value) != string(record.Value) {
			t.Fatalf("reader.Get(lsn=%d) = %q, want %q", record.LSN, stored.Value, record.Value)
		}
		if stored.TimestampMS != record.TimestampMS {
			t.Fatalf("reader.Get(lsn=%d) timestamp_ms = %d, want %d", record.LSN, stored.TimestampMS, record.TimestampMS)
		}
	}

	if appender.nextLSN != 3 {
		t.Fatalf("nextLSN = %d, want 3", appender.nextLSN)
	}
}
