package sinktest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

const (
	SegmentContentType = "application/vnd.eventlake.partition-segment"
)

type Object struct {
	Body        []byte
	ContentType string
	SizeBytes   uint64
}

type ReadObject func(ctx context.Context, key string) (Object, error)

func RunMultipartStore(t testing.TB, store multipart.Store, prefix string, read ReadObject) {
	t.Helper()
	ctx := context.Background()

	key := prefix + "/multipart/final.bin"
	part1 := bytes.Repeat([]byte("a"), 5<<20)
	part2 := []byte("tail")
	want := append(append([]byte(nil), part1...), part2...)

	upload, err := store.BeginMultipart(ctx, key, multipart.Options{
		ContentType:   SegmentContentType,
		StagingPrefix: prefix + "/multipart/staging/final",
	})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	r2, err := upload.UploadPart(ctx, multipart.Part{Number: 2, Bytes: part2})
	if err != nil {
		t.Fatalf("UploadPart(2) error = %v", err)
	}
	r1, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: part1})
	if err != nil {
		t.Fatalf("UploadPart(1) error = %v", err)
	}
	attrs, err := upload.Complete(ctx, []multipart.Receipt{r1, r2})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if attrs.Key != key {
		t.Fatalf("attrs.Key = %q, want %q", attrs.Key, key)
	}
	if attrs.SizeBytes != uint64(len(want)) {
		t.Fatalf("attrs.SizeBytes = %d, want %d", attrs.SizeBytes, len(want))
	}
	if attrs.Token == "" {
		t.Fatal("attrs.Token is empty")
	}
	got, err := read(ctx, key)
	if err != nil {
		t.Fatalf("read(%q) error = %v", key, err)
	}
	if !bytes.Equal(got.Body, want) {
		t.Fatalf("final object body mismatch: got %d bytes want %d bytes", len(got.Body), len(want))
	}
	if got.ContentType != SegmentContentType {
		t.Fatalf("ContentType = %q, want %q", got.ContentType, SegmentContentType)
	}
	if got.SizeBytes != uint64(len(want)) {
		t.Fatalf("read size = %d, want %d", got.SizeBytes, len(want))
	}

	assertPreconditionFailure(t, store, prefix+"/multipart/existing.bin")
	assertAbort(t, store, prefix+"/multipart/abort.bin")
	assertConcurrentUploadParts(t, store, prefix+"/multipart/concurrent.bin", read)
}

func RunSegmentWriter(t testing.TB, store multipart.Store, prefix string, read ReadObject) {
	t.Helper()
	ctx := context.Background()

	factory, err := sink.New(store, sink.Options{
		Prefix:      prefix + "/segments",
		ContentType: SegmentContentType,
	})
	if err != nil {
		t.Fatalf("sink.New() error = %v", err)
	}
	info := plwriter.SegmentInfo{
		Partition:     7,
		BaseLSN:       100,
		WriterEpoch:   3,
		SegmentUUID:   [16]byte{1, 2, 3, 4},
		CreatedUnixMS: 1_776_263_000_000,
	}
	sink, err := factory.NewSegmentSink(ctx, info)
	if err != nil {
		t.Fatalf("NewSegmentSink() error = %v", err)
	}

	opts := segwriter.DefaultOptions(info.Partition)
	opts.Codec = segformat.CodecNone
	opts.HashAlgo = segformat.HashXXH64
	opts.TargetBlockSize = 256
	opts.PartSize = 8 << 20
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	opts.SegmentUUID = info.SegmentUUID
	opts.WriterTag = [16]byte{9, 8, 7}
	opts.CreatedUnixMS = info.CreatedUnixMS

	w, err := segwriter.New(opts, sink)
	if err != nil {
		t.Fatalf("segwriter.New() error = %v", err)
	}
	for i := 0; i < 12; i++ {
		if err := w.Append(ctx, segwriter.Record{
			LSN:         info.BaseLSN + uint64(i),
			TimestampMS: 1_000 + int64(i),
			Value:       []byte(fmt.Sprintf("event-%02d", i)),
		}); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	result, err := w.Close(ctx)
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	obj, err := read(ctx, result.Object.URI)
	if err != nil {
		t.Fatalf("read segment %q error = %v", result.Object.URI, err)
	}
	if uint64(len(obj.Body)) != result.Object.SizeBytes {
		t.Fatalf("object bytes=%d result size=%d", len(obj.Body), result.Object.SizeBytes)
	}
	if obj.ContentType != SegmentContentType {
		t.Fatalf("ContentType = %q, want %q", obj.ContentType, SegmentContentType)
	}
	trailer := parseTrailer(t, obj.Body)
	if trailer.Partition != info.Partition || trailer.BaseLSN != info.BaseLSN || trailer.LastLSN != info.BaseLSN+11 {
		t.Fatalf("trailer lsn fields = partition=%d base=%d last=%d", trailer.Partition, trailer.BaseLSN, trailer.LastLSN)
	}
	if trailer.RecordCount != 12 || trailer.SegmentUUID != info.SegmentUUID {
		t.Fatalf("trailer = %+v", trailer)
	}
}

func assertPreconditionFailure(t testing.TB, store multipart.Store, key string) {
	t.Helper()
	ctx := context.Background()
	if err := writeSinglePart(ctx, store, key, "first"); err != nil {
		t.Fatalf("write first object error = %v", err)
	}
	if err := writeSinglePart(ctx, store, key, "second"); !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("write existing object error = %v, want %v", err, multipart.ErrPreconditionFailed)
	}
}

func assertAbort(t testing.TB, store multipart.Store, key string) {
	t.Helper()
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, key, multipart.Options{ContentType: SegmentContentType})
	if err != nil {
		t.Fatalf("BeginMultipart(abort) error = %v", err)
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("abort")})
	if err != nil {
		t.Fatalf("UploadPart(abort) error = %v", err)
	}
	if err := upload.Abort(ctx); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if _, err := upload.Complete(ctx, []multipart.Receipt{receipt}); !errors.Is(err, multipart.ErrAborted) {
		t.Fatalf("Complete(after abort) error = %v, want %v", err, multipart.ErrAborted)
	}
}

func assertConcurrentUploadParts(t testing.TB, store multipart.Store, key string, read ReadObject) {
	t.Helper()
	ctx := context.Background()

	parts := [][]byte{
		bytes.Repeat([]byte("a"), 5<<20),
		bytes.Repeat([]byte("b"), 5<<20),
		bytes.Repeat([]byte("c"), 5<<20),
		bytes.Repeat([]byte("d"), 5<<20),
		bytes.Repeat([]byte("e"), 5<<20),
		[]byte("tail"),
	}
	upload, err := store.BeginMultipart(ctx, key, multipart.Options{
		ContentType:   SegmentContentType,
		StagingPrefix: key + ".staging",
	})
	if err != nil {
		t.Fatalf("BeginMultipart(concurrent) error = %v", err)
	}

	receipts := make([]multipart.Receipt, len(parts))
	errs := make(chan error, len(parts))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, partBytes := range parts {
		i, partBytes := i, partBytes
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receipt, err := upload.UploadPart(ctx, multipart.Part{
				Number: i + 1,
				Bytes:  partBytes,
			})
			if err != nil {
				errs <- fmt.Errorf("UploadPart(%d): %w", i+1, err)
				return
			}
			receipts[i] = receipt
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			_ = upload.Abort(ctx)
			t.Fatal(err)
		}
	}

	attrs, err := upload.Complete(ctx, receipts)
	if err != nil {
		_ = upload.Abort(ctx)
		t.Fatalf("Complete(concurrent) error = %v", err)
	}
	var want []byte
	for _, part := range parts {
		want = append(want, part...)
	}
	if attrs.SizeBytes != uint64(len(want)) {
		t.Fatalf("concurrent attrs.SizeBytes = %d, want %d", attrs.SizeBytes, len(want))
	}
	got, err := read(ctx, key)
	if err != nil {
		t.Fatalf("read concurrent object error = %v", err)
	}
	if !bytes.Equal(got.Body, want) {
		t.Fatalf("concurrent final object body mismatch: got %d bytes want %d bytes", len(got.Body), len(want))
	}
	if got.ContentType != SegmentContentType {
		t.Fatalf("concurrent ContentType = %q, want %q", got.ContentType, SegmentContentType)
	}
}

func writeSinglePart(ctx context.Context, store multipart.Store, key, body string) error {
	upload, err := store.BeginMultipart(ctx, key, multipart.Options{ContentType: SegmentContentType})
	if err != nil {
		return err
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte(body)})
	if err != nil {
		_ = upload.Abort(ctx)
		return err
	}
	if _, err := upload.Complete(ctx, []multipart.Receipt{receipt}); err != nil {
		_ = upload.Abort(ctx)
		return err
	}
	return nil
}

func parseTrailer(t testing.TB, object []byte) segformat.Trailer {
	t.Helper()
	if len(object) < segformat.TrailerSize {
		t.Fatalf("object size=%d smaller than trailer size=%d", len(object), segformat.TrailerSize)
	}
	trailer, err := segformat.ParseTrailer(object[len(object)-segformat.TrailerSize:], uint64(len(object)))
	if err != nil {
		t.Fatalf("ParseTrailer() error = %v", err)
	}
	return trailer
}

func ReadAll(rc io.ReadCloser) ([]byte, error) {
	defer rc.Close()
	return io.ReadAll(rc)
}
