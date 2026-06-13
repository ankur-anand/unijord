package sink

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	objmultipart "github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestBlobWriterEndToEndWithBlobCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := blob.NewMemory(blob.Options{})
	if err != nil {
		t.Fatalf("blob.NewMemory() error = %v", err)
	}
	segmentStore := objmultipart.NewMemoryStore()

	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	opts := plwriter.DefaultOptions(factory)
	opts.Session = newCatalogWriterSession(t, cat, 7, [16]byte{7, 7, 7})
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = sequenceUUIDGen()
	opts.Roll.MaxSegmentRecords = 2
	opts.SegmentOptions = segwriter.DefaultOptions(7)
	opts.SegmentOptions.Codec = segformat.CodecNone
	opts.SegmentOptions.HashAlgo = segformat.HashXXH64
	opts.SegmentOptions.TargetBlockSize = 64
	opts.SegmentOptions.PartSize = 64
	opts.SegmentOptions.SealParallelism = 1
	opts.SegmentOptions.BlockBufferCount = 3
	opts.SegmentOptions.UploadParallelism = 2
	opts.SegmentOptions.UploadQueueSize = 2

	writer, err := plwriter.New(opts)
	if err != nil {
		t.Fatalf("writer.New() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		result, err := writer.Append(ctx, plwriter.Record{
			TimestampMS: int64(100 + i),
			Value:       []byte(fmt.Sprintf("event-%d", i)),
		})
		if err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		if result.LSN != uint64(i) {
			t.Fatalf("Append(%d) LSN = %d, want %d", i, result.LSN, i)
		}
	}
	if _, err := writer.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	state, err := cat.LoadPartition(ctx, 7)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if state.NextLSN != 5 || state.SegmentCount != 3 {
		t.Fatalf("state = %+v, want next_lsn=5 segment_count=3", state)
	}

	page, err := cat.ListSegments(ctx, pcatalog.ListSegmentsRequest{Partition: 7, Limit: 10})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 3 {
		t.Fatalf("segments = %d, want 3: %+v", len(page.Segments), page.Segments)
	}
	for _, segment := range page.Segments {
		body, _, err := segmentStore.Read(ctx, segment.URI)
		if err != nil {
			t.Fatalf("segmentStore.Read(%s) error = %v", segment.URI, err)
		}
		if len(body) == 0 || uint64(len(body)) != segment.SizeBytes {
			t.Fatalf("segment %s bytes=%d size_bytes=%d", segment.URI, len(body), segment.SizeBytes)
		}
	}

	staging, err := segmentStore.List(ctx, "segments/staging/")
	if err != nil {
		t.Fatalf("List(staging) error = %v", err)
	}
	if len(staging) != 0 {
		t.Fatalf("staging objects after complete = %+v, want none", staging)
	}
}

func TestBlobSegmentTxnAbortDeletesStagingParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	segmentStore := objmultipart.NewMemoryStore()

	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sink, err := factory.NewSegmentSink(ctx, plwriter.SegmentInfo{
		Partition:     1,
		BaseLSN:       10,
		WriterEpoch:   2,
		SegmentUUID:   [16]byte{1, 2, 3},
		CreatedUnixMS: 1000,
	})
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}
	txn, err := sink.Begin(ctx, segwriter.Plan{Partition: 1, PartSize: 4, Codec: segformat.CodecNone, HashAlgo: segformat.HashXXH64})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := txn.UploadPart(ctx, segwriter.Part{Number: 1, Bytes: []byte("abcd")}); err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if err := txn.Abort(ctx); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if _, err := txn.UploadPart(ctx, segwriter.Part{Number: 2, Bytes: []byte("efgh")}); !errors.Is(err, segwriter.ErrTxnAborted) {
		t.Fatalf("UploadPart(after abort) error = %v, want %v", err, segwriter.ErrTxnAborted)
	}

	staging, err := segmentStore.List(ctx, "segments/staging/")
	if err != nil {
		t.Fatalf("List(staging) error = %v", err)
	}
	if len(staging) != 0 {
		t.Fatalf("staging objects after abort = %+v, want none", staging)
	}
}

func TestBlobSegmentTxnRejectsExistingFinalObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	segmentStore := objmultipart.NewMemoryStore()

	info := plwriter.SegmentInfo{
		Partition:     1,
		BaseLSN:       0,
		WriterEpoch:   1,
		SegmentUUID:   [16]byte{9, 9, 9},
		CreatedUnixMS: 1000,
	}
	factory, err := New(segmentStore, Options{Prefix: "segments"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := writeOneObject(ctx, factory, info, []byte("first")); err != nil {
		t.Fatalf("writeOneObject(first) error = %v", err)
	}
	if err := writeOneObject(ctx, factory, info, []byte("second")); !errors.Is(err, objmultipart.ErrPreconditionFailed) {
		t.Fatalf("writeOneObject(second) error = %v, want %v", err, objmultipart.ErrPreconditionFailed)
	}
}

func TestFactoryRejectsNilStore(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); !errors.Is(err, plwriter.ErrInvalidOptions) {
		t.Fatalf("New(nil) error = %v, want %v", err, plwriter.ErrInvalidOptions)
	}
}

func TestSegmentSinkMapsBeginError(t *testing.T) {
	t.Parallel()

	factory, err := New(beginErrorStore{err: objmultipart.ErrInvalidStore}, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sink, err := factory.NewSegmentSink(context.Background(), plwriter.SegmentInfo{
		Partition:   1,
		BaseLSN:     10,
		WriterEpoch: 2,
		SegmentUUID: [16]byte{1},
	})
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}
	_, err = sink.Begin(context.Background(), segwriter.Plan{
		Partition: 1,
		PartSize:  1,
		Codec:     segformat.CodecNone,
		HashAlgo:  segformat.HashXXH64,
	})
	if !errors.Is(err, segwriter.ErrInvalidOptions) {
		t.Fatalf("Begin() error = %v, want %v", err, segwriter.ErrInvalidOptions)
	}
}

func TestLayoutUsesNormalizedPrefix(t *testing.T) {
	t.Parallel()

	info := plwriter.SegmentInfo{
		Partition:   7,
		BaseLSN:     100,
		WriterEpoch: 3,
		SegmentUUID: [16]byte{1, 2, 3},
	}
	layout := NewLayout("/root/")
	if layout.Prefix() != "root" {
		t.Fatalf("Prefix() = %q, want %q", layout.Prefix(), "root")
	}
	if got := layout.SegmentKey(info); got != "root/segments/p00000007/seg-00000000000000000100-e00000000000000000003-01020300000000000000000000000000.plseg" {
		t.Fatalf("SegmentKey() = %q", got)
	}
	if got := layout.StagingPrefix(info); got != "root/staging/p00000007/seg-00000000000000000100-e00000000000000000003-01020300000000000000000000000000" {
		t.Fatalf("StagingPrefix() = %q", got)
	}
}

func writeOneObject(ctx context.Context, factory *Factory, info plwriter.SegmentInfo, body []byte) error {
	sink, err := factory.NewSegmentSink(ctx, info)
	if err != nil {
		return err
	}
	txn, err := sink.Begin(ctx, segwriter.Plan{Partition: info.Partition, PartSize: len(body), Codec: segformat.CodecNone, HashAlgo: segformat.HashXXH64})
	if err != nil {
		return err
	}
	receipt, err := txn.UploadPart(ctx, segwriter.Part{Number: 1, Bytes: body})
	if err != nil {
		_ = txn.Abort(ctx)
		return err
	}
	if _, err := txn.Complete(ctx, []segwriter.PartReceipt{receipt}); err != nil {
		_ = txn.Abort(ctx)
		return err
	}
	return nil
}

func sequenceUUIDGen() plwriter.UUIDGen {
	var n byte
	return func() ([16]byte, error) {
		n++
		return [16]byte{n}, nil
	}
}

type beginErrorStore struct {
	err error
}

func (s beginErrorStore) BeginMultipart(context.Context, string, objmultipart.Options) (objmultipart.Upload, error) {
	return nil, s.err
}
