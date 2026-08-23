package sinktest

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	if err := store.Limits().Validate(); err != nil {
		t.Fatalf("store limits are invalid: %v", err)
	}

	key := prefix + "/multipart/final.bin"
	part1 := bytes.Repeat([]byte("a"), 5<<20)
	part2 := []byte("tail")
	want := append(append([]byte(nil), part1...), part2...)

	upload, err := store.Begin(ctx, key, multipart.Options{
		ContentType:   SegmentContentType,
		StagingPrefix: prefix + "/multipart/staging/final",
	})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	r2, err := upload.PutPart(ctx, multipart.NewPart(2, part2))
	if err != nil {
		t.Fatalf("UploadPart(2) error = %v", err)
	}
	r1, err := upload.PutPart(ctx, multipart.NewPart(1, part1))
	if err != nil {
		t.Fatalf("UploadPart(1) error = %v", err)
	}
	// PutPart must consume or copy its input before returning because the
	// streaming layer immediately recycles that buffer.
	clear(part1)
	clear(part2)
	request := multipart.NewCommitRequest([]multipart.Receipt{r1, r2})
	request.ObjectSHA256 = sha256.Sum256(want)
	attrs, err := upload.Commit(ctx, request)
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
	if attrs.SessionID == "" || attrs.ObjectSHA256 != request.ObjectSHA256 {
		t.Fatalf("attrs identity = %+v", attrs)
	}
	if retried, err := upload.Commit(ctx, request); err != nil || retried != attrs {
		t.Fatalf("idempotent Commit() = (%+v, %v), want (%+v, nil)", retried, err, attrs)
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
	RunSessionRetryContract(t, store, prefix+"/multipart/part-retry.bin")
	assertCleanup(t, store, prefix+"/multipart/cleanup.bin")
	assertConcurrentUploadParts(t, store, prefix+"/multipart/concurrent.bin", read)
}

func RunSessionRetryContract(t testing.TB, store multipart.Store, key string) {
	t.Helper()
	if err := store.Limits().Validate(); err != nil {
		t.Fatalf("Limits() error = %v", err)
	}
	ctx := context.Background()
	session, err := store.Begin(ctx, key, multipart.Options{ContentType: SegmentContentType})
	if err != nil {
		t.Fatalf("Begin(part retry) error = %v", err)
	}
	if session.Limits() != store.Limits() {
		t.Fatalf("session Limits() = %+v, want store Limits() %+v", session.Limits(), store.Limits())
	}
	part := multipart.NewPart(1, []byte("same-part"))
	first, err := session.PutPart(ctx, part)
	if err != nil {
		t.Fatalf("PutPart(first) error = %v", err)
	}
	if first.Number != part.Number || first.SizeBytes != uint64(len(part.Bytes)) || first.ChecksumSHA256 != part.ChecksumSHA256 {
		t.Fatalf("PutPart(first) receipt = %+v, want matching part identity", first)
	}
	retry, err := session.PutPart(ctx, part)
	if err != nil || retry != first {
		t.Fatalf("PutPart(retry) = (%+v, %v), want (%+v, nil)", retry, err, first)
	}
	if _, err := session.PutPart(ctx, multipart.NewPart(1, []byte("different-part"))); !errors.Is(err, multipart.ErrPartConflict) {
		t.Fatalf("PutPart(conflict) error = %v, want ErrPartConflict", err)
	}
	if err := session.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup(part retry) error = %v", err)
	}
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

func assertCleanup(t testing.TB, store multipart.Store, key string) {
	t.Helper()
	ctx := context.Background()
	upload, err := store.Begin(ctx, key, multipart.Options{ContentType: SegmentContentType})
	if err != nil {
		t.Fatalf("Begin(cleanup) error = %v", err)
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("abort")))
	if err != nil {
		t.Fatalf("PutPart(cleanup) error = %v", err)
	}
	if err := upload.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt})); !errors.Is(err, multipart.ErrCleaned) {
		t.Fatalf("Commit(after cleanup) error = %v, want %v", err, multipart.ErrCleaned)
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
	upload, err := store.Begin(ctx, key, multipart.Options{
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
			receipt, err := upload.PutPart(ctx, multipart.NewPart(i+1, partBytes))
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
			_ = upload.Cleanup(ctx)
			t.Fatal(err)
		}
	}

	request := multipart.NewCommitRequest(receipts)
	var want []byte
	for _, part := range parts {
		want = append(want, part...)
	}
	request.ObjectSHA256 = sha256.Sum256(want)
	attrs, err := upload.Commit(ctx, request)
	if err != nil {
		_ = upload.Cleanup(ctx)
		t.Fatalf("Complete(concurrent) error = %v", err)
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
	upload, err := store.Begin(ctx, key, multipart.Options{ContentType: SegmentContentType})
	if err != nil {
		return err
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte(body)))
	if err != nil {
		_ = upload.Cleanup(ctx)
		return err
	}
	request := multipart.NewCommitRequest([]multipart.Receipt{receipt})
	request.ObjectSHA256 = sha256.Sum256([]byte(body))
	if _, err := upload.Commit(ctx, request); err != nil {
		_ = upload.Cleanup(ctx)
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
