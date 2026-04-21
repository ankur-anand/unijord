package segwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

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
	var appendErr error
	for _, record := range makeWriterRecords(4, 1, 1, 24) {
		appendErr = w.Append(context.Background(), record)
		if appendErr != nil {
			break
		}
	}
	if !errors.Is(appendErr, sink.failErr) {
		t.Fatalf("Append() upload error = %v, want %v", appendErr, sink.failErr)
	}
	if got := sink.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
	if err := w.Append(context.Background(), Record{LSN: 5, TimestampMS: 5, Value: []byte("x")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append after failed Close = %v, want %v", err, ErrWriterAborted)
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
