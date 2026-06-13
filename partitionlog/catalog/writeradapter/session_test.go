package writeradapter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/writeradapter"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestSessionPublishesToCatalog(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	writerID := [16]byte{1, 2, 3}
	ws, err := cat.OpenWriter(context.Background(), 7, writerID)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	session, err := writeradapter.New(ws)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	snapshot := session.Snapshot()
	if snapshot.Head.Partition != 7 || snapshot.Identity.Epoch != 1 || snapshot.Identity.Tag != writerID {
		t.Fatalf("Snapshot() = %+v", snapshot)
	}

	segment := validSegmentRef(7, 0, 2, ws.Epoch(), writerID)
	next, err := session.PublishSegment(context.Background(), writer.PublishRequest{Segment: segment})
	if err != nil {
		t.Fatalf("PublishSegment() error = %v", err)
	}
	if next.Head.NextLSN != 3 || next.Head.SegmentCount != 1 {
		t.Fatalf("PublishSegment() snapshot = %+v", next)
	}
	loaded, err := cat.LoadPartition(context.Background(), 7)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.NextLSN != 3 {
		t.Fatalf("catalog NextLSN = %d, want 3", loaded.NextLSN)
	}
}

func TestSessionMapsStaleWriter(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	first, err := cat.OpenWriter(context.Background(), 7, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	if _, err := cat.OpenWriter(context.Background(), 7, [16]byte{2}); err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	session, err := writeradapter.New(first)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	segment := validSegmentRef(7, 0, 2, first.Epoch(), first.WriterID())
	_, err = session.PublishSegment(context.Background(), writer.PublishRequest{Segment: segment})
	if !errors.Is(err, writer.ErrStaleWriter) {
		t.Fatalf("PublishSegment(stale) error = %v, want %v", err, writer.ErrStaleWriter)
	}
}

func TestSessionRejectsNil(t *testing.T) {
	t.Parallel()

	if _, err := writeradapter.New(nil); err == nil {
		t.Fatal("New(nil) error = nil, want error")
	}
	var s *writeradapter.Session
	if _, err := s.PublishSegment(context.Background(), writer.PublishRequest{}); !errors.Is(err, writer.ErrInvalidSession) {
		t.Fatalf("PublishSegment(nil) error = %v, want %v", err, writer.ErrInvalidSession)
	}
}

func validSegmentRef(partition uint32, base uint64, last uint64, epoch uint64, writerID [16]byte) pmeta.SegmentRef {
	count := uint32(last - base + 1)
	return pmeta.SegmentRef{
		URI:              "memory://catalogsession/test",
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      [16]byte{9, 8, 7},
		WriterTag:        writerID,
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   1,
		MaxTimestampMS:   int64(count),
		RecordCount:      count,
		BlockCount:       1,
		SizeBytes:        512,
		BlockIndexOffset: 256,
		BlockIndexLength: segformat.IndexPreambleSize + segformat.BlockIndexEntrySize,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
	}
}
