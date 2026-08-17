package blob

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
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
			writerID = [16]byte{2}
			memoryWriter, err = memory.OpenWriter(ctx, 7, writerID)
			if err != nil {
				t.Fatalf("memory replacement OpenWriter() error = %v", err)
			}
			blobWriter, err = blobCatalog.OpenWriter(ctx, 7, writerID)
			if err != nil {
				t.Fatalf("blob replacement OpenWriter() error = %v", err)
			}
			if _, err := oldMemory.AppendSegment(ctx, last); !errors.Is(err, pcatalog.ErrStaleWriter) {
				t.Fatalf("memory stale retry error = %v, want %v", err, pcatalog.ErrStaleWriter)
			}
			if _, err := oldBlob.AppendSegment(ctx, last); !errors.Is(err, pcatalog.ErrStaleWriter) {
				t.Fatalf("blob stale retry error = %v, want %v", err, pcatalog.ErrStaleWriter)
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
