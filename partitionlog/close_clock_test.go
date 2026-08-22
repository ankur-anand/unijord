package partitionlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestLogCloseClosesDefaultReader(t *testing.T) {
	log, err := Open(Options{Store: newTestStore(t)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := log.Reader().Head(context.Background(), 7); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("default Reader error after Log.Close = %v, want %v", err, ErrReaderClosed)
	}
	if _, err := log.NewReader(ReaderOptions{}); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("NewReader() after Log.Close error = %v, want %v", err, ErrLogClosed)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestLogRequestRetentionUsesConfiguredClock(t *testing.T) {
	want := time.UnixMilli(1_800_000_000_123).UTC()
	store := newTestStore(t)
	opts := Options{
		Store: store,
		Clock: func() time.Time { return want },
	}

	log, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	result, err := log.RequestRetention(context.Background(), RetentionRequest{
		Partition:     7,
		PolicyVersion: 1,
		BeforeLSN:     10,
	})
	if err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	if result.CreatedUnixMS != want.UnixMilli() {
		t.Fatalf("CreatedUnixMS = %d, want %d", result.CreatedUnixMS, want.UnixMilli())
	}

	w, err := log.OpenWriter(context.Background(), WriterOptions{Partition: 8, WriterID: [16]byte{1}})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("value")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	snapshot, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Writer.Close() error = %v", err)
	}
	body, _, err := store.source.objects.Read(context.Background(), snapshot.Head.LastSegment.URI)
	if err != nil {
		t.Fatalf("Read(segment) error = %v", err)
	}
	trailer, err := segformat.ParseTrailer(body[len(body)-segformat.TrailerSize:], uint64(len(body)))
	if err != nil {
		t.Fatalf("ParseTrailer() error = %v", err)
	}
	if trailer.CreatedUnixMS != want.UnixMilli() {
		t.Fatalf("segment CreatedUnixMS = %d, want %d", trailer.CreatedUnixMS, want.UnixMilli())
	}
}
