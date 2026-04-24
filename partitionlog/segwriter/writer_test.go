package segwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/segblock"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestWriterEndToEndCodecNone(t *testing.T) {
	t.Parallel()

	records := makeWriterRecords(32, 100, 1_000, 31)
	sink := NewMemorySink("memory://none")
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 128
	opts.PartSize = 96

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range records {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	object := sink.Bytes()
	decoded := decodeSegmentForTest(t, object)
	assertRecordsEqual(t, decoded.records, records)
	if result.Metadata.SizeBytes != uint64(len(object)) {
		t.Fatalf("metadata SizeBytes = %d, want %d", result.Metadata.SizeBytes, len(object))
	}
	if result.Trailer != decoded.trailer {
		t.Fatalf("result trailer = %+v, decoded trailer = %+v", result.Trailer, decoded.trailer)
	}
	if decoded.trailer.Codec != segformat.CodecNone {
		t.Fatalf("codec = %v, want %v", decoded.trailer.Codec, segformat.CodecNone)
	}
	if decoded.trailer.BlockCount < 2 {
		t.Fatalf("block_count = %d, want multiple blocks", decoded.trailer.BlockCount)
	}
}

func TestWriterEndToEndZstd(t *testing.T) {
	t.Parallel()

	records := makeWriterRecords(128, 500, 10_000, 256)
	sink := NewMemorySink("memory://zstd")
	opts := testWriterOptions(segformat.CodecZstd)
	opts.TargetBlockSize = 8 << 10
	opts.PartSize = 4 << 10

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range records {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	object := sink.Bytes()
	decoded := decodeSegmentForTest(t, object)
	assertRecordsEqual(t, decoded.records, records)
	if decoded.trailer.Codec != segformat.CodecZstd {
		t.Fatalf("codec = %v, want %v", decoded.trailer.Codec, segformat.CodecZstd)
	}
	if result.Object.SizeBytes != uint64(len(object)) {
		t.Fatalf("object size = %d, want %d", result.Object.SizeBytes, len(object))
	}
	if decoded.trailer.SegmentHash == 0 || decoded.trailer.TrailerHash == 0 {
		t.Fatalf("hashes should be populated: segment=%x trailer=%x", decoded.trailer.SegmentHash, decoded.trailer.TrailerHash)
	}
}

func TestWriterEndToEndCRC32C(t *testing.T) {
	t.Parallel()

	records := makeWriterRecords(64, 700, 20_000, 128)
	sink := NewMemorySink("memory://crc32c")
	opts := testWriterOptions(segformat.CodecNone)
	opts.HashAlgo = segformat.HashCRC32C
	opts.TargetBlockSize = 512
	opts.PartSize = 256

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range records {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	decoded := decodeSegmentForTest(t, sink.Bytes())
	assertRecordsEqual(t, decoded.records, records)
	if decoded.trailer.HashAlgo != segformat.HashCRC32C {
		t.Fatalf("hash_algo = %v, want %v", decoded.trailer.HashAlgo, segformat.HashCRC32C)
	}
	if result.Trailer != decoded.trailer {
		t.Fatalf("result trailer = %+v, decoded trailer = %+v", result.Trailer, decoded.trailer)
	}
}

func TestWriterParallelSealingProducesOrderedSegment(t *testing.T) {
	t.Parallel()

	records := makeWriterRecords(256, 10_000, 1_000_000, 512)
	for i := range records {
		if i%7 == 0 {
			records[i].Value = bytes.Repeat([]byte{byte(i)}, 2048)
		}
	}
	sink := NewMemorySink("memory://parallel")
	opts := testWriterOptions(segformat.CodecZstd)
	opts.TargetBlockSize = 4 << 10
	opts.SealParallelism = 4
	opts.BlockBufferCount = 9
	opts.PartSize = 16 << 10
	opts.UploadParallelism = 2

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range records {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	decoded := decodeSegmentForTest(t, sink.Bytes())
	assertRecordsEqual(t, decoded.records, records)
	if decoded.trailer.BlockCount < 8 {
		t.Fatalf("block_count = %d, want enough blocks to exercise parallel sealing", decoded.trailer.BlockCount)
	}
}

func TestWriterRingBackpressureWhenUploadIsBlocked(t *testing.T) {
	t.Parallel()

	sink := newBlockingSink()
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 32
	opts.PartSize = 16
	opts.SealParallelism = 1
	opts.BlockBufferCount = 2
	opts.UploadParallelism = 1
	opts.UploadQueueSize = 1

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		for _, record := range makeWriterRecords(64, 1, 1, 24) {
			if err := w.Append(context.Background(), record); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	sink.waitForUpload(t)
	select {
	case err := <-done:
		t.Fatalf("Append loop completed before blocked upload was released: %v", err)
	default:
	}

	sink.unblock()
	if err := <-done; err != nil {
		t.Fatalf("Append loop error = %v", err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestWriterEncodeHelper(t *testing.T) {
	t.Parallel()

	records := makeWriterRecords(8, 1, 100, 17)
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 64
	object, meta, err := Encode(context.Background(), records, opts)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded := decodeSegmentForTest(t, object)
	assertRecordsEqual(t, decoded.records, records)
	if meta.RecordCount != uint32(len(records)) {
		t.Fatalf("metadata RecordCount = %d, want %d", meta.RecordCount, len(records))
	}
}

func TestWriterLazyBegin(t *testing.T) {
	t.Parallel()

	sink := NewMemorySink("memory://lazy")
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 1 << 20
	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := sink.BeginCount(); got != 0 {
		t.Fatalf("BeginCount after New = %d, want 0", got)
	}
	if err := w.Append(context.Background(), Record{LSN: 10, TimestampMS: 100, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := sink.BeginCount(); got != 0 {
		t.Fatalf("BeginCount after buffered Append = %d, want 0", got)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := sink.BeginCount(); got != 1 {
		t.Fatalf("BeginCount after Close = %d, want 1", got)
	}
}

func TestWriterRejectsInvalidRecordSequence(t *testing.T) {
	t.Parallel()

	w, err := New(testWriterOptions(segformat.CodecNone), NewMemorySink("memory://invalid"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 10, TimestampMS: 100, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 12, TimestampMS: 101, Value: []byte("b")}); !errors.Is(err, ErrNonContiguousLSN) {
		t.Fatalf("Append(gap) error = %v, want %v", err, ErrNonContiguousLSN)
	}
	if err := w.Append(context.Background(), Record{LSN: 11, TimestampMS: 101, Value: []byte("b")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append after failed append = %v, want %v", err, ErrWriterAborted)
	}

	w, err = New(testWriterOptions(segformat.CodecNone), NewMemorySink("memory://invalid-ts"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 1, TimestampMS: 100, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 2, TimestampMS: 99, Value: []byte("b")}); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("Append(timestamp regression) error = %v, want %v", err, ErrTimestampOrder)
	}
}

func TestWriterRejectsExhaustedLSNRange(t *testing.T) {
	t.Parallel()

	w, err := New(testWriterOptions(segformat.CodecNone), NewMemorySink("memory://lsn-exhausted"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: ^uint64(0), TimestampMS: 100, Value: []byte("a")}); err != nil {
		t.Fatalf("Append(max lsn) error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 0, TimestampMS: 101, Value: []byte("b")}); !errors.Is(err, ErrNonContiguousLSN) {
		t.Fatalf("Append(after max lsn) error = %v, want %v", err, ErrNonContiguousLSN)
	}
}

func TestBlockBufferRejectsTimestampRegression(t *testing.T) {
	t.Parallel()

	var b blockBuffer
	if err := b.Append(Record{LSN: 1, TimestampMS: 10, Value: []byte("a")}, 128); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := b.Append(Record{LSN: 2, TimestampMS: 9, Value: []byte("b")}, 128); !errors.Is(err, ErrTimestampOrder) {
		t.Fatalf("Append(regression) error = %v, want %v", err, ErrTimestampOrder)
	}
}

func TestWriterRejectsEmptyClose(t *testing.T) {
	t.Parallel()

	w, err := New(testWriterOptions(segformat.CodecNone), NewMemorySink("memory://empty"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrEmptySegment) {
		t.Fatalf("Close(empty) error = %v, want %v", err, ErrEmptySegment)
	}
}

func TestWriterRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	opts := testWriterOptions(segformat.CodecNone)
	opts.Codec = segformat.Codec(99)
	if _, err := New(opts, NewMemorySink("memory://bad-codec")); !errors.Is(err, ErrInvalidOptions) || !errors.Is(err, segformat.ErrUnsupportedCodec) {
		t.Fatalf("New(bad codec) error = %v, want %v wrapping %v", err, ErrInvalidOptions, segformat.ErrUnsupportedCodec)
	}

	opts = testWriterOptions(segformat.CodecNone)
	opts.HashAlgo = segformat.HashAlgo(99)
	if _, err := New(opts, NewMemorySink("memory://bad-hash")); !errors.Is(err, ErrInvalidOptions) || !errors.Is(err, segformat.ErrUnsupportedHashAlgo) {
		t.Fatalf("New(bad hash) error = %v, want %v wrapping %v", err, ErrInvalidOptions, segformat.ErrUnsupportedHashAlgo)
	}
}

func TestCheckedBlockCountRejectsIndexLengthOverflow(t *testing.T) {
	t.Parallel()

	got, err := checkedBlockCount(segformat.MaxBlockCount)
	if err != nil {
		t.Fatalf("checkedBlockCount(max) error = %v", err)
	}
	if got != uint32(segformat.MaxBlockCount) {
		t.Fatalf("checkedBlockCount(max) = %d, want %d", got, segformat.MaxBlockCount)
	}

	if _, err := checkedBlockCount(segformat.MaxBlockCount + 1); !errors.Is(err, segformat.ErrInvalidSegment) {
		t.Fatalf("checkedBlockCount(overflow) error = %v, want %v", err, segformat.ErrInvalidSegment)
	}
}

func TestWriterFillsDefaultIdentityMetadata(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions(3)
	opts.Codec = segformat.CodecNone
	opts.HashAlgo = segformat.HashXXH64
	opts.CreatedUnixMS = 0
	opts.SegmentUUID = [16]byte{}
	before := time.Now().UTC().UnixMilli()

	w, err := New(opts, NewMemorySink("memory://default-identity"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 1, TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if result.Metadata.SegmentUUID == ([16]byte{}) {
		t.Fatal("SegmentUUID was not auto-filled")
	}
	if result.Trailer.CreatedUnixMS < before {
		t.Fatalf("CreatedUnixMS = %d, want >= %d", result.Trailer.CreatedUnixMS, before)
	}
}

func TestWriterAbortIsIdempotentAndTerminal(t *testing.T) {
	t.Parallel()

	w, err := New(testWriterOptions(segformat.CodecNone), NewMemorySink("memory://abort"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 1, TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("second Abort() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 2, TimestampMS: 2, Value: []byte("b")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append after Abort = %v, want %v", err, ErrWriterAborted)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Close after Abort = %v, want %v", err, ErrWriterAborted)
	}
}

func TestWriterAbortsTxnOnUploadFailure(t *testing.T) {
	t.Parallel()

	sink := &failingSink{failErr: errors.New("upload failed")}
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 64
	opts.PartSize = 32
	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range makeWriterRecords(4, 1, 1, 24) {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, sink.failErr) {
		t.Fatalf("Close() error = %v, want %v", err, sink.failErr)
	}
	if got := sink.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
	if err := w.Append(context.Background(), Record{LSN: 5, TimestampMS: 5, Value: []byte("x")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append after failed Close = %v, want %v", err, ErrWriterAborted)
	}
}

func TestWriterCloseAfterAsyncUploadErrorDrainsAndAborts(t *testing.T) {
	t.Parallel()

	before := runtime.NumGoroutine()
	sink := &failingSink{failErr: errors.New("upload failed async")}
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 32
	opts.PartSize = 16
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 1
	opts.UploadQueueSize = 1
	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 1, TimestampMS: 1, Value: bytes.Repeat([]byte("a"), 24)}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 2, TimestampMS: 2, Value: bytes.Repeat([]byte("b"), 24)}); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}

	waitForWriterErr(t, w)
	if _, err := w.Close(context.Background()); !errors.Is(err, sink.failErr) {
		t.Fatalf("Close() error = %v, want %v", err, sink.failErr)
	}
	if got := sink.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
	assertNoGoroutineLeak(t, before, 4)
}

func TestWriterCloseContextCancelsLazyBegin(t *testing.T) {
	t.Parallel()

	sink := newBlockingBeginSink()
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 1 << 20
	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := w.Append(context.Background(), Record{LSN: 1, TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := w.Close(ctx)
		done <- err
	}()
	sink.waitStarted(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want %v", err, context.Canceled)
	}
}

func TestWriterFailureCleanupUsesBestEffortAbortContext(t *testing.T) {
	t.Parallel()

	sink := newCleanupAwareSink()
	opts := testWriterOptions(segformat.CodecNone)
	opts.TargetBlockSize = 32
	opts.PartSize = 16

	w, err := New(opts, sink)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, record := range makeWriterRecords(2, 1, 1, 24) {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := w.Close(ctx)
		done <- err
	}()

	sink.waitCompleteStarted(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want %v", err, context.Canceled)
	}
	sink.waitAbort(t)
	if got := sink.abortCtxErr(); got != nil {
		t.Fatalf("Abort() ctx.Err() = %v, want nil cleanup context", got)
	}
}

type decodedSegment struct {
	preamble segformat.FilePreamble
	trailer  segformat.Trailer
	index    []segformat.BlockIndexEntry
	records  []Record
}

func decodeSegmentForTest(t *testing.T, object []byte) decodedSegment {
	t.Helper()
	if len(object) < segformat.FilePreambleSize+segformat.TrailerSize {
		t.Fatalf("object too small: %d", len(object))
	}
	preamble, err := segformat.ParseFilePreamble(object[:segformat.FilePreambleSize])
	if err != nil {
		t.Fatalf("ParseFilePreamble() error = %v", err)
	}
	trailerBytes := object[len(object)-segformat.TrailerSize:]
	trailer, err := segformat.ParseTrailer(trailerBytes, uint64(len(object)))
	if err != nil {
		t.Fatalf("ParseTrailer() error = %v", err)
	}
	if err := segformat.ValidatePreambleTrailer(preamble, trailer); err != nil {
		t.Fatalf("ValidatePreambleTrailer() error = %v", err)
	}
	indexEnd := trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength)
	indexBytes := object[trailer.BlockIndexOffset:indexEnd]
	_, index, err := segformat.ParseBlockIndex(indexBytes, trailer.HashAlgo)
	if err != nil {
		t.Fatalf("ParseBlockIndex() error = %v", err)
	}
	if err := segformat.ValidateIndex(index, trailer); err != nil {
		t.Fatalf("ValidateIndex() error = %v", err)
	}
	segmentHash, err := segformat.HashBytes(trailer.HashAlgo, object[:indexEnd])
	if err != nil {
		t.Fatalf("HashBytes(segment) error = %v", err)
	}
	if segmentHash != trailer.SegmentHash {
		t.Fatalf("segment hash = %x, want %x", segmentHash, trailer.SegmentHash)
	}

	var records []Record
	for _, entry := range index {
		blockStart := entry.BlockOffset
		blockEnd := blockStart + uint64(segformat.BlockPreambleSize) + uint64(entry.StoredSize)
		blockPreamble, err := segformat.ParseBlockPreamble(object[blockStart : blockStart+segformat.BlockPreambleSize])
		if err != nil {
			t.Fatalf("ParseBlockPreamble() error = %v", err)
		}
		if err := segformat.MatchBlockIndexEntry(blockPreamble, entry); err != nil {
			t.Fatalf("MatchBlockIndexEntry() error = %v", err)
		}
		stored := object[blockStart+segformat.BlockPreambleSize : blockEnd]
		raw, err := segblock.Open(trailer.Codec, trailer.HashAlgo, blockPreamble, stored)
		if err != nil {
			t.Fatalf("segblock.Open() error = %v", err)
		}
		blockRecords, err := segformat.DecodeRawBlock(raw, blockPreamble)
		if err != nil {
			t.Fatalf("DecodeRawBlock() error = %v", err)
		}
		for _, record := range blockRecords {
			records = append(records, Record{
				LSN:         record.LSN,
				TimestampMS: record.TimestampMS,
				Value:       append([]byte(nil), record.Value...),
			})
		}
	}
	return decodedSegment{
		preamble: preamble,
		trailer:  trailer,
		index:    index,
		records:  records,
	}
}

func makeWriterRecords(count int, baseLSN uint64, baseTS int64, valueSize int) []Record {
	records := make([]Record, count)
	for i := range records {
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte('a' + (i+j)%26)
		}
		records[i] = Record{
			LSN:         baseLSN + uint64(i),
			TimestampMS: baseTS + int64(i),
			Value:       value,
		}
	}
	return records
}

func assertRecordsEqual(t *testing.T, got []Record, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].LSN != want[i].LSN || got[i].TimestampMS != want[i].TimestampMS || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("record[%d] = {lsn:%d ts:%d value:%q}, want {lsn:%d ts:%d value:%q}",
				i, got[i].LSN, got[i].TimestampMS, got[i].Value,
				want[i].LSN, want[i].TimestampMS, want[i].Value)
		}
	}
}

func waitForWriterErr(t *testing.T, w *Writer) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if w.getFirstErr() != nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for writer error")
		case <-ticker.C:
		}
	}
}

func assertNoGoroutineLeak(t *testing.T, before int, slack int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		runtime.GC()
		if got := runtime.NumGoroutine(); got <= before+slack {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("goroutine count did not settle: got=%d before=%d slack=%d", runtime.NumGoroutine(), before, slack)
		case <-ticker.C:
		}
	}
}

func testWriterOptions(codec segformat.Codec) Options {
	opts := DefaultOptions(3)
	opts.Codec = codec
	opts.HashAlgo = segformat.HashXXH64
	opts.SegmentUUID = [16]byte{1, 2, 3}
	opts.WriterTag = [16]byte{4, 5, 6}
	opts.CreatedUnixMS = 1_776_263_000_000
	return opts
}

type failingSink struct {
	mu      sync.Mutex
	failErr error
	aborts  int
}

type blockingSink struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingBeginSink struct {
	started chan struct{}
	once    sync.Once
}

func newBlockingBeginSink() *blockingBeginSink {
	return &blockingBeginSink{started: make(chan struct{})}
}

func (s *blockingBeginSink) Begin(ctx context.Context, _ Plan) (Txn, error) {
	s.once.Do(func() {
		close(s.started)
	})
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *blockingBeginSink) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sink.Begin")
	}
}

func newBlockingSink() *blockingSink {
	return &blockingSink{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingSink) Begin(context.Context, Plan) (Txn, error) {
	return &blockingTxn{sink: s}, nil
}

func (s *blockingSink) waitForUpload(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upload to start")
	}
}

func (s *blockingSink) unblock() {
	close(s.release)
}

type blockingTxn struct {
	sink *blockingSink
	mu   sync.Mutex
	size uint64
}

func (t *blockingTxn) UploadPart(ctx context.Context, part Part) (PartReceipt, error) {
	t.sink.once.Do(func() {
		close(t.sink.started)
	})
	select {
	case <-t.sink.release:
	case <-ctx.Done():
		return PartReceipt{}, ctx.Err()
	}
	t.mu.Lock()
	t.size += uint64(len(part.Bytes))
	t.mu.Unlock()
	return PartReceipt{Number: part.Number, Token: fmt.Sprintf("blocked-%d", part.Number)}, nil
}

func (t *blockingTxn) Complete(context.Context, []PartReceipt) (CommittedObject, error) {
	t.mu.Lock()
	size := t.size
	t.mu.Unlock()
	return CommittedObject{URI: "blocking://segment", SizeBytes: size, Token: "complete"}, nil
}

func (t *blockingTxn) Abort(context.Context) error {
	return nil
}

func (s *failingSink) Begin(context.Context, Plan) (Txn, error) {
	return &failingTxn{sink: s}, nil
}

func (s *failingSink) abortCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborts
}

type failingTxn struct {
	sink *failingSink
}

func (t *failingTxn) UploadPart(context.Context, Part) (PartReceipt, error) {
	return PartReceipt{}, t.sink.failErr
}

func (t *failingTxn) Complete(context.Context, []PartReceipt) (CommittedObject, error) {
	return CommittedObject{}, fmt.Errorf("complete should not be called")
}

func (t *failingTxn) Abort(context.Context) error {
	t.sink.mu.Lock()
	t.sink.aborts++
	t.sink.mu.Unlock()
	return nil
}

type cleanupAwareSink struct {
	txn *cleanupAwareTxn
}

func newCleanupAwareSink() *cleanupAwareSink {
	return &cleanupAwareSink{
		txn: &cleanupAwareTxn{
			completeStarted: make(chan struct{}),
			abortCalled:     make(chan struct{}),
		},
	}
}

func (s *cleanupAwareSink) Begin(context.Context, Plan) (Txn, error) {
	return s.txn, nil
}

func (s *cleanupAwareSink) waitCompleteStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.txn.completeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for txn.Complete")
	}
}

func (s *cleanupAwareSink) waitAbort(t *testing.T) {
	t.Helper()
	select {
	case <-s.txn.abortCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for txn.Abort")
	}
}

func (s *cleanupAwareSink) abortCtxErr() error {
	s.txn.mu.Lock()
	defer s.txn.mu.Unlock()
	return s.txn.abortErr
}

type cleanupAwareTxn struct {
	mu              sync.Mutex
	size            uint64
	completeStarted chan struct{}
	abortCalled     chan struct{}
	completeOnce    sync.Once
	abortOnce       sync.Once
	abortErr        error
}

func (t *cleanupAwareTxn) UploadPart(_ context.Context, part Part) (PartReceipt, error) {
	t.mu.Lock()
	t.size += uint64(len(part.Bytes))
	t.mu.Unlock()
	return PartReceipt{Number: part.Number, Token: fmt.Sprintf("cleanup-%d", part.Number)}, nil
}

func (t *cleanupAwareTxn) Complete(ctx context.Context, _ []PartReceipt) (CommittedObject, error) {
	t.completeOnce.Do(func() {
		close(t.completeStarted)
	})
	<-ctx.Done()
	return CommittedObject{}, ctx.Err()
}

func (t *cleanupAwareTxn) Abort(ctx context.Context) error {
	t.mu.Lock()
	t.abortErr = ctx.Err()
	t.mu.Unlock()
	t.abortOnce.Do(func() {
		close(t.abortCalled)
	})
	return nil
}
