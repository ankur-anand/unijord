package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestMemoryCatalogAppendAndLoadPartition(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 3, 1)
	first := testSegment(3, 0, 9, ws.Epoch())
	state, err := ws.AppendSegment(context.Background(), first)
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	if state.Partition != 3 || state.NextLSN != 10 || state.WriterEpoch != 1 || state.SegmentCount != 1 || !state.HasLastSegment {
		t.Fatalf("state after first append = %+v", state)
	}
	if state.LastSegment != first {
		t.Fatalf("last segment = %+v, want %+v", state.LastSegment, first)
	}

	second := testSegment(3, 10, 19, ws.Epoch())
	state, err = ws.AppendSegment(context.Background(), second)
	if err != nil {
		t.Fatalf("AppendSegment(second) error = %v", err)
	}
	if state.NextLSN != 20 || state.SegmentCount != 2 || state.LastSegment != second {
		t.Fatalf("state after second append = %+v", state)
	}

	loaded, err := cat.LoadPartition(context.Background(), 3)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.NextLSN != 20 || loaded.SegmentCount != 2 || loaded.LastSegment != second {
		t.Fatalf("loaded state = %+v", loaded)
	}
}

func TestMemoryCatalogLoadMissingPartitionReturnsEmptyState(t *testing.T) {
	t.Parallel()

	state, err := NewMemoryCatalog().LoadPartition(context.Background(), 42)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if state.Partition != 42 || state.NextLSN != 0 || state.WriterEpoch != 0 || state.SegmentCount != 0 || state.HasLastSegment {
		t.Fatalf("missing partition state = %+v", state)
	}
}

func TestMemoryCatalogRejectsExpectedNextLSNMismatch(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	if _, err := ws.AppendSegment(context.Background(), testSegment(1, 1, 2, ws.Epoch())); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(wrong first lsn) error = %v, want %v", err, ErrConflict)
	}

	if _, err := ws.AppendSegment(context.Background(), testSegment(1, 0, 2, ws.Epoch())); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	if _, err := ws.AppendSegment(context.Background(), testSegment(1, 4, 5, ws.Epoch())); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(gap) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogRejectsTimestampRegressionAcrossSegments(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	first := testSegment(1, 0, 2, ws.Epoch())
	first.MinTimestampMS = 100
	first.MaxTimestampMS = 200
	if _, err := ws.AppendSegment(context.Background(), first); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	second := testSegment(1, 3, 4, ws.Epoch())
	second.MinTimestampMS = 199
	second.MaxTimestampMS = 250
	if _, err := ws.AppendSegment(context.Background(), second); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("AppendSegment(timestamp regression) error = %v, want %v", err, ErrTimestampOrder)
	}
}

func TestMemoryCatalogWriterEpochFence(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	first := mustOpenWriter(t, cat, 1, 1)
	if _, err := first.AppendSegment(context.Background(), testSegment(1, 0, 2, first.Epoch())); err != nil {
		t.Fatalf("AppendSegment(epoch %d) error = %v", first.Epoch(), err)
	}
	second := mustOpenWriter(t, cat, 1, 2)

	stale := testSegment(1, 3, 4, first.Epoch())
	if _, err := first.AppendSegment(context.Background(), stale); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("AppendSegment(stale writer) error = %v, want %v", err, ErrStaleWriter)
	}

	newer := testSegment(1, 3, 4, second.Epoch())
	state, err := second.AppendSegment(context.Background(), newer)
	if err != nil {
		t.Fatalf("AppendSegment(newer writer) error = %v", err)
	}
	if state.WriterEpoch != second.Epoch() || state.NextLSN != 5 {
		t.Fatalf("state after newer writer = %+v", state)
	}
}

func TestMemoryCatalogIdempotentRetryOfLastAppend(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	segment := testSegment(1, 0, 2, ws.Epoch())
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	retry, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment(retry) error = %v", err)
	}
	if retry.NextLSN != state.NextLSN || retry.SegmentCount != 1 {
		t.Fatalf("retry state = %+v, want one segment next_lsn=%d", retry, state.NextLSN)
	}

	mutated := segment
	mutated.SizeBytes++
	if _, err := ws.AppendSegment(context.Background(), mutated); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(mutated retry) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogRejectsDuplicateSegmentIdentity(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	first := testSegment(1, 0, 2, ws.Epoch())
	if _, err := ws.AppendSegment(context.Background(), first); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	dupUUID := testSegment(1, 3, 4, ws.Epoch())
	dupUUID.SegmentUUID = first.SegmentUUID
	if _, err := ws.AppendSegment(context.Background(), dupUUID); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(duplicate uuid) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogFindSegment(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	for _, segment := range []pmeta.SegmentRef{
		testSegment(1, 0, 4, ws.Epoch()),
		testSegment(1, 5, 9, ws.Epoch()),
		testSegment(1, 10, 14, ws.Epoch()),
	} {
		if _, err := ws.AppendSegment(context.Background(), segment); err != nil {
			t.Fatalf("AppendSegment(%d-%d) error = %v", segment.BaseLSN, segment.LastLSN, err)
		}
	}

	segment, ok, err := cat.FindSegment(context.Background(), 1, 7)
	if err != nil {
		t.Fatalf("FindSegment() error = %v", err)
	}
	if !ok || segment.BaseLSN != 5 || segment.LastLSN != 9 {
		t.Fatalf("FindSegment(7) = %+v ok=%v, want segment 5-9", segment, ok)
	}

	if _, ok, err := cat.FindSegment(context.Background(), 1, 15); err != nil || ok {
		t.Fatalf("FindSegment(15) ok=%v err=%v, want not found", ok, err)
	}
	if _, ok, err := cat.FindSegment(context.Background(), 2, 0); err != nil || ok {
		t.Fatalf("FindSegment(missing partition) ok=%v err=%v, want not found", ok, err)
	}
}

func TestMemoryCatalogListSegmentsIsPagedAndBounded(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	for _, segment := range []pmeta.SegmentRef{
		testSegment(1, 0, 4, ws.Epoch()),
		testSegment(1, 5, 9, ws.Epoch()),
		testSegment(1, 10, 14, ws.Epoch()),
		testSegment(1, 15, 19, ws.Epoch()),
	} {
		if _, err := ws.AppendSegment(context.Background(), segment); err != nil {
			t.Fatalf("AppendSegment(%d-%d) error = %v", segment.BaseLSN, segment.LastLSN, err)
		}
	}

	page, err := cat.ListSegments(context.Background(), ListSegmentsRequest{Partition: 1, FromLSN: 6, Limit: 2})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 2 || !page.HasMore || page.NextLSN != 15 {
		t.Fatalf("page = %+v, want 2 segments, has_more, next_lsn=15", page)
	}
	if page.Segments[0].BaseLSN != 5 || page.Segments[1].BaseLSN != 10 {
		t.Fatalf("page segments = %+v", page.Segments)
	}

	next, err := cat.ListSegments(context.Background(), ListSegmentsRequest{Partition: 1, FromLSN: page.NextLSN, Limit: 2})
	if err != nil {
		t.Fatalf("ListSegments(next) error = %v", err)
	}
	if len(next.Segments) != 1 || next.HasMore {
		t.Fatalf("next page = %+v, want final single segment", next)
	}
}

func TestListSegmentsRequestLimitIsCapped(t *testing.T) {
	t.Parallel()

	if got := (ListSegmentsRequest{}).NormalizedLimit(); got != DefaultSegmentPageLimit {
		t.Fatalf("default limit = %d, want %d", got, DefaultSegmentPageLimit)
	}
	if got := (ListSegmentsRequest{Limit: MaxSegmentPageLimit + 1}).NormalizedLimit(); got != MaxSegmentPageLimit {
		t.Fatalf("capped limit = %d, want %d", got, MaxSegmentPageLimit)
	}
	if got := (ListSegmentsRequest{Limit: 7}).NormalizedLimit(); got != 7 {
		t.Fatalf("explicit limit = %d, want 7", got)
	}
}

func TestMemoryCatalogReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	if _, err := ws.AppendSegment(context.Background(), testSegment(1, 0, 2, ws.Epoch())); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	page, err := cat.ListSegments(context.Background(), ListSegmentsRequest{Partition: 1, FromLSN: 0, Limit: 1})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	page.Segments[0].URI = "mutated"

	again, err := cat.ListSegments(context.Background(), ListSegmentsRequest{Partition: 1, FromLSN: 0, Limit: 1})
	if err != nil {
		t.Fatalf("ListSegments(again) error = %v", err)
	}
	if again.Segments[0].URI == "mutated" {
		t.Fatal("ListSegments returned aliased segment slice")
	}
}

func TestMemoryCatalogRejectsInvalidSegment(t *testing.T) {
	t.Parallel()

	segment := testSegment(1, 0, 2, 1)
	segment.URI = ""
	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	if _, err := ws.AppendSegment(context.Background(), segment); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("AppendSegment(invalid segment) error = %v, want %v", err, ErrInvalidSegment)
	}

	segment = testSegment(1, 0, 2, 1)
	segment.Codec = segformat.Codec(99)
	cat = NewMemoryCatalog()
	ws = mustOpenWriter(t, cat, 1, 2)
	if _, err := ws.AppendSegment(context.Background(), segment); !errors.Is(err, ErrInvalidSegment) || !errors.Is(err, segformat.ErrUnsupportedCodec) {
		t.Fatalf("AppendSegment(invalid codec) error = %v, want %v wrapping %v", err, ErrInvalidSegment, segformat.ErrUnsupportedCodec)
	}
}

func TestMemoryCatalogHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cat := NewMemoryCatalog()
	if _, err := cat.LoadPartition(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadPartition(canceled) error = %v, want %v", err, context.Canceled)
	}
	ws := mustOpenWriter(t, cat, 1, 1)
	if _, err := ws.AppendSegment(ctx, testSegment(1, 0, 1, ws.Epoch())); !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendSegment(canceled) error = %v, want %v", err, context.Canceled)
	}
	if _, err := cat.OpenWriter(ctx, 1, [16]byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenWriter(canceled) error = %v, want %v", err, context.Canceled)
	}
}

func TestMemoryCatalogOpenWriterIncrementsEpoch(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	first := mustOpenWriter(t, cat, 9, 1)
	second := mustOpenWriter(t, cat, 9, 2)
	if first.Epoch() != 1 || second.Epoch() != 2 {
		t.Fatalf("epochs = %d,%d want 1,2", first.Epoch(), second.Epoch())
	}
	if second.Head().WriterEpoch != 2 || second.Head().Partition != 9 {
		t.Fatalf("second session head = %+v", second.Head())
	}
}

func TestMemoryCatalogOpenWriterRejectsZeroWriterID(t *testing.T) {
	t.Parallel()

	_, err := NewMemoryCatalog().OpenWriter(context.Background(), 1, [16]byte{})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("OpenWriter(zero writer id) error = %v, want %v", err, ErrInvalidRequest)
	}
}

func mustOpenWriter(t *testing.T, cat *MemoryCatalog, partition uint32, id byte) WriterSession {
	t.Helper()
	ws, err := cat.OpenWriter(context.Background(), partition, [16]byte{id})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	return ws
}

func testSegment(partition uint32, baseLSN uint64, lastLSN uint64, epoch uint64) pmeta.SegmentRef {
	var uuid [16]byte
	uuid[0] = byte(partition)
	uuid[1] = byte(baseLSN)
	uuid[2] = byte(lastLSN)
	uuid[3] = byte(epoch)
	if uuid == ([16]byte{}) {
		uuid[15] = 1
	}
	recordCount := uint32(lastLSN - baseLSN + 1)
	return pmeta.SegmentRef{
		URI:              fmt.Sprintf("memory://p%d/%d-%d-e%d", partition, baseLSN, lastLSN, epoch),
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      uuid,
		WriterTag:        [16]byte{9, 8, 7},
		BaseLSN:          baseLSN,
		LastLSN:          lastLSN,
		MinTimestampMS:   int64(baseLSN),
		MaxTimestampMS:   int64(lastLSN),
		RecordCount:      recordCount,
		BlockCount:       1,
		SizeBytes:        512,
		BlockIndexOffset: 256,
		BlockIndexLength: 128,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      1,
		TrailerHash:      2,
	}
}
