package blob

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type catalogModel interface {
	pcatalog.Reader
	pcatalog.WriterManager
	pcatalog.RetentionManager
}

func TestBlobCatalogMatchesMemoryCatalogAcrossLongHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memory := pcatalog.NewMemoryCatalog()
	blobCatalog, err := NewMemory(Options{LeafSegmentLimit: 3, IndexRefLimit: 3})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}

	writerID := [16]byte{1}
	memoryWriter, err := memory.OpenWriter(ctx, 7, writerID)
	if err != nil {
		t.Fatalf("memory OpenWriter() error = %v", err)
	}
	blobWriter, err := blobCatalog.OpenWriter(ctx, 7, writerID)
	if err != nil {
		t.Fatalf("blob OpenWriter() error = %v", err)
	}
	policyVersion := uint64(0)
	var last pmeta.SegmentRef

	for i := 0; i < 180; i++ {
		if i == 90 {
			oldMemory := memoryWriter
			oldBlob := blobWriter
			oldMemoryHead := oldMemory.Head()
			oldBlobHead := oldBlob.Head()
			writerID = [16]byte{2}
			memoryWriter, err = memory.OpenWriter(ctx, 7, writerID)
			if err != nil {
				t.Fatalf("memory replacement OpenWriter() error = %v", err)
			}
			blobWriter, err = blobCatalog.OpenWriter(ctx, 7, writerID)
			if err != nil {
				t.Fatalf("blob replacement OpenWriter() error = %v", err)
			}
			memoryRetry, err := oldMemory.AppendSegment(ctx, last)
			if err != nil {
				t.Fatalf("memory committed retry error = %v", err)
			}
			if memoryRetry != oldMemoryHead {
				t.Fatalf("memory committed retry = %+v, want %+v", memoryRetry, oldMemoryHead)
			}
			blobRetry, err := oldBlob.AppendSegment(ctx, last)
			if err != nil {
				t.Fatalf("blob committed retry error = %v", err)
			}
			if blobRetry != oldBlobHead {
				t.Fatalf("blob committed retry = %+v, want %+v", blobRetry, oldBlobHead)
			}
		}

		base := uint64(i * 7)
		last = modelSegment(7, base, base+6, memoryWriter.Epoch(), writerID)
		memoryHead, err := memoryWriter.AppendSegment(ctx, last)
		if err != nil {
			t.Fatalf("memory AppendSegment(%d) error = %v", i, err)
		}
		blobHead, err := blobWriter.AppendSegment(ctx, last)
		if err != nil {
			t.Fatalf("blob AppendSegment(%d) error = %v", i, err)
		}
		if blobHead != memoryHead {
			t.Fatalf("head after append %d differs\nblob:   %+v\nmemory: %+v", i, blobHead, memoryHead)
		}

		if i == 37 || i == 91 || i == 137 || i == 179 {
			policyVersion++
			before := memoryHead.NextLSN
			if before > 73 {
				before -= 73
			}
			request := pcatalog.RetentionRequest{
				Version:       pcatalog.RetentionRequestVersion,
				PolicyVersion: policyVersion,
				BeforeLSN:     before,
				CreatedUnixMS: int64(policyVersion),
			}
			memoryRequest, err := memory.RequestRetention(ctx, 7, request)
			if err != nil {
				t.Fatalf("memory RequestRetention(%d) error = %v", i, err)
			}
			blobRequest, err := blobCatalog.RequestRetention(ctx, 7, request)
			if err != nil {
				t.Fatalf("blob RequestRetention(%d) error = %v", i, err)
			}
			if blobRequest != memoryRequest {
				t.Fatalf("retention request after append %d differs: blob=%+v memory=%+v", i, blobRequest, memoryRequest)
			}

			memoryResult, err := memoryWriter.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
			if err != nil {
				t.Fatalf("memory ApplyPendingRetention(%d) error = %v", i, err)
			}
			blobResult, err := blobWriter.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
			if err != nil {
				t.Fatalf("blob ApplyPendingRetention(%d) error = %v", i, err)
			}
			if blobResult != memoryResult {
				t.Fatalf("retention result after append %d differs\nblob:   %+v\nmemory: %+v", i, blobResult, memoryResult)
			}
		}

		if i%11 == 0 || i == 179 {
			assertCatalogModelsEqual(t, ctx, memory, blobCatalog, 7)
		}
	}
}

func assertCatalogModelsEqual(t *testing.T, ctx context.Context, memory, blobCatalog catalogModel, partition uint32) {
	t.Helper()

	memoryHead, err := memory.LoadPartition(ctx, partition)
	if err != nil {
		t.Fatalf("memory LoadPartition() error = %v", err)
	}
	blobHead, err := blobCatalog.LoadPartition(ctx, partition)
	if err != nil {
		t.Fatalf("blob LoadPartition() error = %v", err)
	}
	if blobHead != memoryHead {
		t.Fatalf("loaded heads differ\nblob:   %+v\nmemory: %+v", blobHead, memoryHead)
	}

	memorySegments := collectModelSegments(t, ctx, memory, partition)
	blobSegments := collectModelSegments(t, ctx, blobCatalog, partition)
	if !slices.Equal(blobSegments, memorySegments) {
		t.Fatalf("listed segments differ\nblob:   %+v\nmemory: %+v", blobSegments, memorySegments)
	}

	probes := []uint64{0, memoryHead.OldestLSN, memoryHead.NextLSN}
	if memoryHead.OldestLSN > 0 {
		probes = append(probes, memoryHead.OldestLSN-1)
	}
	if memoryHead.NextLSN > memoryHead.OldestLSN {
		probes = append(probes, memoryHead.NextLSN-1)
	}
	for _, lsn := range probes {
		memorySegment, memoryFound, err := memory.FindSegment(ctx, partition, lsn)
		if err != nil {
			t.Fatalf("memory FindSegment(%d) error = %v", lsn, err)
		}
		blobSegment, blobFound, err := blobCatalog.FindSegment(ctx, partition, lsn)
		if err != nil {
			t.Fatalf("blob FindSegment(%d) error = %v", lsn, err)
		}
		if blobFound != memoryFound || blobSegment != memorySegment {
			t.Fatalf("FindSegment(%d) differs: blob=(%+v,%v) memory=(%+v,%v)", lsn, blobSegment, blobFound, memorySegment, memoryFound)
		}
	}

	timestampProbes := []int64{-1, memoryHead.LastSegment.MaxTimestampMS, memoryHead.LastSegment.MaxTimestampMS + 1}
	if len(memorySegments) > 0 {
		timestampProbes = append(timestampProbes, memorySegments[0].MinTimestampMS, memorySegments[0].MaxTimestampMS)
	}
	for _, timestampMS := range timestampProbes {
		req := pcatalog.TimestampLookupRequest{Partition: partition, TimestampMS: timestampMS}
		memoryResult, err := memory.LookupTimestamp(ctx, req)
		if err != nil {
			t.Fatalf("memory LookupTimestamp(%d) error = %v", timestampMS, err)
		}
		blobResult, err := blobCatalog.LookupTimestamp(ctx, req)
		if err != nil {
			t.Fatalf("blob LookupTimestamp(%d) error = %v", timestampMS, err)
		}
		if blobResult != memoryResult {
			t.Fatalf("LookupTimestamp(%d) differs: blob=%+v memory=%+v", timestampMS, blobResult, memoryResult)
		}
	}
}

func TestBlobCatalogLookupTimestampReadsOneTreePath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := &countingGetBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	writerID := [16]byte{1}
	writer, err := cat.OpenWriter(ctx, 3, writerID)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	var first pmeta.SegmentRef
	for i := 0; i < 128; i++ {
		base := uint64(i * 7)
		segment := modelSegment(3, base, base+6, writer.Epoch(), writerID)
		if i == 0 {
			first = segment
		}
		if _, err := writer.AppendSegment(ctx, segment); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", i, err)
		}
	}

	head, _, err := cat.loadHead(ctx, 3)
	if err != nil {
		t.Fatalf("loadHead() error = %v", err)
	}
	roots := reachableRoots(head)
	if len(roots) == 0 || roots[0].Level < 2 {
		t.Fatalf("catalog did not build a multi-level tree: roots=%+v", roots)
	}
	backend.gets.Store(0)
	got, err := cat.LookupTimestamp(ctx, pcatalog.TimestampLookupRequest{Partition: 3, TimestampMS: first.MinTimestampMS})
	if err != nil {
		t.Fatalf("LookupTimestamp() error = %v", err)
	}
	if !got.Found || got.Segment != first {
		t.Fatalf("LookupTimestamp() = %+v, want first segment %+v", got, first)
	}
	wantGets := int64(roots[0].Level) + 2 // head + one page at each tree level
	if gotGets := backend.gets.Load(); gotGets != wantGets {
		t.Fatalf("backend Get calls = %d, want one tree path (%d)", gotGets, wantGets)
	}

	backend.gets.Store(0)
	missing, err := cat.LookupTimestamp(ctx, pcatalog.TimestampLookupRequest{
		Partition:   3,
		TimestampMS: head.LastSegment.MaxTimestampMS + 1,
	})
	if err != nil {
		t.Fatalf("LookupTimestamp(newer than head) error = %v", err)
	}
	if missing.Found {
		t.Fatalf("LookupTimestamp(newer than head) = %+v, want not found", missing)
	}
	if gotGets := backend.gets.Load(); gotGets != 1 {
		t.Fatalf("backend Get calls for newer timestamp = %d, want head only", gotGets)
	}
}

func TestBlobCatalogLookupTimestampChoosesEarlierSegmentAtPageBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	writerID := [16]byte{1}
	writer, err := cat.OpenWriter(ctx, 9, writerID)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	segments := []pmeta.SegmentRef{
		modelSegment(9, 0, 6, writer.Epoch(), writerID),
		modelSegment(9, 7, 13, writer.Epoch(), writerID),
		modelSegment(9, 14, 20, writer.Epoch(), writerID),
		modelSegment(9, 21, 27, writer.Epoch(), writerID),
	}
	segments[0].MinTimestampMS, segments[0].MaxTimestampMS = 100, 199
	segments[1].MinTimestampMS, segments[1].MaxTimestampMS = 199, 299
	segments[2].MinTimestampMS, segments[2].MaxTimestampMS = 299, 399
	segments[3].MinTimestampMS, segments[3].MaxTimestampMS = 400, 499
	for _, segment := range segments {
		if _, err := writer.AppendSegment(ctx, segment); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", segment.BaseLSN, err)
		}
	}

	got, err := cat.LookupTimestamp(ctx, pcatalog.TimestampLookupRequest{Partition: 9, TimestampMS: 199})
	if err != nil {
		t.Fatalf("LookupTimestamp() error = %v", err)
	}
	if !got.Found || got.Segment != segments[0] {
		t.Fatalf("LookupTimestamp() = %+v, want earliest segment %+v", got, segments[0])
	}
}

type countingGetBackend struct {
	Backend
	gets atomic.Int64
}

func (b *countingGetBackend) Get(ctx context.Context, key string) (Object, error) {
	b.gets.Add(1)
	return b.Backend.Get(ctx, key)
}

func collectModelSegments(t *testing.T, ctx context.Context, catalog pcatalog.Reader, partition uint32) []pmeta.SegmentRef {
	t.Helper()

	var segments []pmeta.SegmentRef
	from := uint64(0)
	for {
		page, err := catalog.ListSegments(ctx, pcatalog.ListSegmentsRequest{Partition: partition, FromLSN: from, Limit: 7})
		if err != nil {
			t.Fatalf("ListSegments(from=%d) error = %v", from, err)
		}
		segments = append(segments, page.Segments...)
		if !page.HasMore {
			return segments
		}
		if page.NextLSN <= from {
			t.Fatalf("ListSegments did not advance: from=%d next=%d", from, page.NextLSN)
		}
		from = page.NextLSN
	}
}

func modelSegment(partition uint32, base, last, epoch uint64, writerID [16]byte) pmeta.SegmentRef {
	var uuid [16]byte
	binary.BigEndian.PutUint64(uuid[:8], base+1)
	binary.BigEndian.PutUint64(uuid[8:], last+1)
	return pmeta.SegmentRef{
		URI:              fmt.Sprintf("model://p%08d/%020d-%020d-e%d", partition, base, last, epoch),
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      uuid,
		WriterTag:        writerID,
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   int64(base),
		MaxTimestampMS:   int64(last),
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        256,
		BlockIndexOffset: 128,
		BlockIndexLength: 64,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      base + 101,
		TrailerHash:      last + 101,
	}
}
