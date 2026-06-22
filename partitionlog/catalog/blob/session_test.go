package blob

import (
	"context"
	"errors"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func TestBlobCatalogWriterBuffersSegmentsInHeadBeforeLeafSeal(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	segment := testSegmentRef(1, 0, 9, ws.Epoch())
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if state.NextLSN != 10 || state.SegmentCount != 1 || state.LastSegment != segment {
		t.Fatalf("state = %+v", state)
	}

	head, _, err := cat.loadHead(context.Background(), 1)
	if err != nil {
		t.Fatalf("loadHead() error = %v", err)
	}
	if len(head.ActiveSegments) != 1 || head.ActiveSegments[0] != segment {
		t.Fatalf("active_segments = %+v, want segment", head.ActiveSegments)
	}
	if head.LeafFrontier != nil || len(head.IndexFrontier) != 0 {
		t.Fatalf("leaf_frontier=%+v index_frontier=%+v, want no sealed pages", head.LeafFrontier, head.IndexFrontier)
	}
	objects, err := cat.backend.List(context.Background(), ListOptions{Prefix: PagePrefix(cat.opts.Prefix, cat.opts.StreamID, 1)})
	if err != nil {
		t.Fatalf("List(pages) error = %v", err)
	}
	if len(objects.Objects) != 0 {
		t.Fatalf("page objects = %+v, want none", objects.Objects)
	}
}

func TestBlobCatalogInitializePartitionAtCheckpoint(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}

	const nextLSN uint64 = 18_900_001
	head, created, err := cat.InitializePartition(context.Background(), 1, nextLSN)
	if err != nil {
		t.Fatalf("InitializePartition() error = %v", err)
	}
	if !created {
		t.Fatal("InitializePartition() created = false, want true")
	}
	if head.NextLSN != nextLSN || head.OldestLSN != nextLSN || head.HasLastSegment {
		t.Fatalf("initialized head = %+v", head)
	}

	again, created, err := cat.InitializePartition(context.Background(), 1, 1)
	if err != nil {
		t.Fatalf("InitializePartition(existing) error = %v", err)
	}
	if created {
		t.Fatal("InitializePartition(existing) created = true, want false")
	}
	if again.NextLSN != nextLSN || again.OldestLSN != nextLSN {
		t.Fatalf("existing head = %+v, want next_lsn=%d", again, nextLSN)
	}

	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	segment := testSegmentRef(1, nextLSN, nextLSN+9, ws.Epoch())
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if state.NextLSN != nextLSN+10 || state.OldestLSN != nextLSN || state.LastSegment != segment {
		t.Fatalf("state = %+v", state)
	}
}

func TestBlobCatalogWriterSealsLeafAndCarriesOldLeafIntoIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for _, segment := range []pmeta.SegmentRef{
		testSegmentRef(1, 0, 9, ws.Epoch()),
		testSegmentRef(1, 10, 19, ws.Epoch()),
		testSegmentRef(1, 20, 29, ws.Epoch()),
		testSegmentRef(1, 30, 39, ws.Epoch()),
	} {
		if _, err := ws.AppendSegment(context.Background(), segment); err != nil {
			t.Fatalf("AppendSegment(%d-%d) error = %v", segment.BaseLSN, segment.LastLSN, err)
		}
	}

	head, _, err := cat.loadHead(context.Background(), 1)
	if err != nil {
		t.Fatalf("loadHead() error = %v", err)
	}
	if len(head.ActiveSegments) != 0 {
		t.Fatalf("active_segments = %+v, want empty after exact leaf seal", head.ActiveSegments)
	}
	if head.LeafFrontier == nil || head.LeafFrontier.SeqLo != 20 || head.LeafFrontier.SeqHi != 39 {
		t.Fatalf("leaf_frontier = %+v, want 20-39", head.LeafFrontier)
	}
	if len(head.IndexFrontier) != 1 || head.IndexFrontier[0].Level != 1 || head.IndexFrontier[0].SeqLo != 0 || head.IndexFrontier[0].SeqHi != 19 {
		t.Fatalf("index_frontier = %+v, want l01 0-19", head.IndexFrontier)
	}
	index, err := cat.loadIndex(context.Background(), head.IndexFrontier[0], cat.opts.StreamID, 1)
	if err != nil {
		t.Fatalf("loadIndex(index_frontier[0]) error = %v", err)
	}
	if len(index.Refs) != 1 || index.Refs[0].SeqLo != 0 || index.Refs[0].SeqHi != 19 {
		t.Fatalf("index refs = %+v, want leaf 0-19", index.Refs)
	}
}

func TestBlobCatalogReaderFindsAndListsAcrossIndexLeafAndActiveSegments(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for _, segment := range []pmeta.SegmentRef{
		testSegmentRef(1, 0, 9, ws.Epoch()),
		testSegmentRef(1, 10, 19, ws.Epoch()),
		testSegmentRef(1, 20, 29, ws.Epoch()),
		testSegmentRef(1, 30, 39, ws.Epoch()),
		testSegmentRef(1, 40, 49, ws.Epoch()),
	} {
		if _, err := ws.AppendSegment(context.Background(), segment); err != nil {
			t.Fatalf("AppendSegment(%d-%d) error = %v", segment.BaseLSN, segment.LastLSN, err)
		}
	}

	for _, tc := range []struct {
		lsn  uint64
		base uint64
	}{
		{lsn: 5, base: 0},
		{lsn: 25, base: 20},
		{lsn: 45, base: 40},
	} {
		segment, ok, err := cat.FindSegment(context.Background(), 1, tc.lsn)
		if err != nil {
			t.Fatalf("FindSegment(%d) error = %v", tc.lsn, err)
		}
		if !ok || segment.BaseLSN != tc.base {
			t.Fatalf("FindSegment(%d) = %+v ok=%v, want base %d", tc.lsn, segment, ok, tc.base)
		}
	}
	if _, ok, err := cat.FindSegment(context.Background(), 1, 50); err != nil || ok {
		t.Fatalf("FindSegment(50) ok=%v err=%v, want not found", ok, err)
	}

	page, err := cat.ListSegments(context.Background(), pcatalog.ListSegmentsRequest{Partition: 1, FromLSN: 5, Limit: 3})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 3 || !page.HasMore || page.NextLSN != 30 {
		t.Fatalf("page = %+v, want 3 segments has_more next_lsn=30", page)
	}
	if page.Segments[0].BaseLSN != 0 || page.Segments[1].BaseLSN != 10 || page.Segments[2].BaseLSN != 20 {
		t.Fatalf("page segments = %+v", page.Segments)
	}
}

func TestBlobCatalogWriterFenceRejectsStaleSession(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	second, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
	if err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	if second.Epoch() != first.Epoch()+1 {
		t.Fatalf("epochs first=%d second=%d", first.Epoch(), second.Epoch())
	}
	if _, err := first.AppendSegment(context.Background(), testSegmentRef(1, 0, 9, first.Epoch())); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("stale AppendSegment() error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}

func TestBlobCatalogWriterIdempotentLastAppend(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	segment := testSegmentRef(1, 0, 9, ws.Epoch())
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	retry, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment(retry) error = %v", err)
	}
	if retry != state || retry.SegmentCount != 1 {
		t.Fatalf("retry state = %+v want %+v", retry, state)
	}
}

func TestBlobCatalogIdempotentRetryChecksCurrentHead(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	firstSegment := testSegmentRef(1, 0, 9, first.Epoch())
	if _, err := first.AppendSegment(context.Background(), firstSegment); err != nil {
		t.Fatalf("AppendSegment(first segment) error = %v", err)
	}

	second, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
	if err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	if _, err := second.AppendSegment(context.Background(), testSegmentRef(1, 10, 19, second.Epoch())); err != nil {
		t.Fatalf("AppendSegment(second segment) error = %v", err)
	}

	if _, err := first.AppendSegment(context.Background(), firstSegment); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("stale retry error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}
