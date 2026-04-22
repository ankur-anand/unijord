package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestMemoryCatalogAppendAndLoadPartition(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	first := testSegment(3, 0, 9, 1)
	state, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       3,
		ExpectedNextLSN: 0,
		WriterEpoch:     1,
		Segment:         first,
	})
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	if state.Partition != 3 || state.NextLSN != 10 || state.WriterEpoch != 1 || state.SegmentCount != 1 || !state.HasLastSegment {
		t.Fatalf("state after first append = %+v", state)
	}
	if state.LastSegment != first {
		t.Fatalf("last segment = %+v, want %+v", state.LastSegment, first)
	}

	second := testSegment(3, 10, 19, 1)
	state, err = cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       3,
		ExpectedNextLSN: 10,
		WriterEpoch:     1,
		Segment:         second,
	})
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
	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 1,
		WriterEpoch:     1,
		Segment:         testSegment(1, 1, 2, 1),
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(wrong first lsn) error = %v, want %v", err, ErrConflict)
	}

	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 0,
		WriterEpoch:     1,
		Segment:         testSegment(1, 0, 2, 1),
	}); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 4,
		WriterEpoch:     1,
		Segment:         testSegment(1, 4, 5, 1),
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(gap) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogRejectsTimestampRegressionAcrossSegments(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	first := testSegment(1, 0, 2, 1)
	first.MinTimestampMS = 100
	first.MaxTimestampMS = 200
	if _, err := cat.AppendSegment(context.Background(), appendReq(first)); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	second := testSegment(1, 3, 4, 1)
	second.MinTimestampMS = 199
	second.MaxTimestampMS = 250
	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 3,
		WriterEpoch:     1,
		Segment:         second,
	}); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("AppendSegment(timestamp regression) error = %v, want %v", err, ErrTimestampOrder)
	}
}

func TestMemoryCatalogWriterEpochFence(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	if _, err := cat.AppendSegment(context.Background(), appendReq(testSegment(1, 0, 2, 2))); err != nil {
		t.Fatalf("AppendSegment(epoch 2) error = %v", err)
	}

	stale := testSegment(1, 3, 4, 1)
	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 3,
		WriterEpoch:     1,
		Segment:         stale,
	}); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("AppendSegment(stale writer) error = %v, want %v", err, ErrStaleWriter)
	}

	newer := testSegment(1, 3, 4, 3)
	state, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 3,
		WriterEpoch:     3,
		Segment:         newer,
	})
	if err != nil {
		t.Fatalf("AppendSegment(newer writer) error = %v", err)
	}
	if state.WriterEpoch != 3 || state.NextLSN != 5 {
		t.Fatalf("state after newer writer = %+v", state)
	}
}

func TestMemoryCatalogIdempotentRetryOfLastAppend(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	req := appendReq(testSegment(1, 0, 2, 1))
	state, err := cat.AppendSegment(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	retry, err := cat.AppendSegment(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendSegment(retry) error = %v", err)
	}
	if retry.NextLSN != state.NextLSN || retry.SegmentCount != 1 {
		t.Fatalf("retry state = %+v, want one segment next_lsn=%d", retry, state.NextLSN)
	}

	mutated := req
	mutated.Segment.SizeBytes++
	if _, err := cat.AppendSegment(context.Background(), mutated); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(mutated retry) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogRejectsDuplicateSegmentIdentity(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	first := testSegment(1, 0, 2, 1)
	if _, err := cat.AppendSegment(context.Background(), appendReq(first)); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	dupUUID := testSegment(1, 3, 4, 1)
	dupUUID.SegmentUUID = first.SegmentUUID
	if _, err := cat.AppendSegment(context.Background(), AppendSegmentRequest{
		Partition:       1,
		ExpectedNextLSN: 3,
		WriterEpoch:     1,
		Segment:         dupUUID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("AppendSegment(duplicate uuid) error = %v, want %v", err, ErrConflict)
	}
}

func TestMemoryCatalogFindSegment(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	for _, segment := range []SegmentRef{
		testSegment(1, 0, 4, 1),
		testSegment(1, 5, 9, 1),
		testSegment(1, 10, 14, 1),
	} {
		if _, err := cat.AppendSegment(context.Background(), appendReq(segment)); err != nil {
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
	for _, segment := range []SegmentRef{
		testSegment(1, 0, 4, 1),
		testSegment(1, 5, 9, 1),
		testSegment(1, 10, 14, 1),
		testSegment(1, 15, 19, 1),
	} {
		if _, err := cat.AppendSegment(context.Background(), appendReq(segment)); err != nil {
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

	if got := (ListSegmentsRequest{}).normalizedLimit(); got != DefaultSegmentPageLimit {
		t.Fatalf("default limit = %d, want %d", got, DefaultSegmentPageLimit)
	}
	if got := (ListSegmentsRequest{Limit: MaxSegmentPageLimit + 1}).normalizedLimit(); got != MaxSegmentPageLimit {
		t.Fatalf("capped limit = %d, want %d", got, MaxSegmentPageLimit)
	}
	if got := (ListSegmentsRequest{Limit: 7}).normalizedLimit(); got != 7 {
		t.Fatalf("explicit limit = %d, want 7", got)
	}
}

func TestMemoryCatalogReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	cat := NewMemoryCatalog()
	if _, err := cat.AppendSegment(context.Background(), appendReq(testSegment(1, 0, 2, 1))); err != nil {
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
	if _, err := NewMemoryCatalog().AppendSegment(context.Background(), appendReq(segment)); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("AppendSegment(invalid segment) error = %v, want %v", err, ErrInvalidSegment)
	}

	segment = testSegment(1, 0, 2, 1)
	segment.Codec = segformat.Codec(99)
	if _, err := NewMemoryCatalog().AppendSegment(context.Background(), appendReq(segment)); !errors.Is(err, ErrInvalidSegment) || !errors.Is(err, segformat.ErrUnsupportedCodec) {
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
	if _, err := cat.AppendSegment(ctx, appendReq(testSegment(1, 0, 1, 1))); !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendSegment(canceled) error = %v, want %v", err, context.Canceled)
	}
}

func appendReq(segment SegmentRef) AppendSegmentRequest {
	return AppendSegmentRequest{
		Partition:       segment.Partition,
		ExpectedNextLSN: segment.BaseLSN,
		WriterEpoch:     segment.WriterEpoch,
		Segment:         segment,
	}
}

func testSegment(partition uint32, baseLSN uint64, lastLSN uint64, epoch uint64) SegmentRef {
	var uuid [16]byte
	uuid[0] = byte(partition)
	uuid[1] = byte(baseLSN)
	uuid[2] = byte(lastLSN)
	uuid[3] = byte(epoch)
	if uuid == ([16]byte{}) {
		uuid[15] = 1
	}
	recordCount := uint32(lastLSN - baseLSN + 1)
	return SegmentRef{
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
