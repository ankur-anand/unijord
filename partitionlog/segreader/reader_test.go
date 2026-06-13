package segreader

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

func TestOpenReadAllCodecNone(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 48, 100, 1_000, 31)
	reader := openFixture(t, fixture, DefaultOptions())

	records, err := reader.Read(context.Background(), fixture.ref.BaseLSN, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	assertRecordsEqual(t, records, fixture.records)
	if reader.Trailer().BlockCount < 2 {
		t.Fatalf("block_count = %d, want multiple blocks", reader.Trailer().BlockCount)
	}
}

func TestOpenReadAllZstd(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecZstd, segformat.HashXXH64, 96, 500, 10_000, 256)
	reader := openFixture(t, fixture, DefaultOptions())

	records, err := reader.Read(context.Background(), fixture.ref.BaseLSN, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	assertRecordsEqual(t, records, fixture.records)
	if reader.Trailer().Codec != segformat.CodecZstd {
		t.Fatalf("codec = %v, want %v", reader.Trailer().Codec, segformat.CodecZstd)
	}
}

func TestOpenReadAllCRC32C(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashCRC32C, 64, 700, 20_000, 64)
	reader := openFixture(t, fixture, DefaultOptions())

	records, err := reader.Read(context.Background(), fixture.ref.BaseLSN, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	assertRecordsEqual(t, records, fixture.records)
	if reader.Trailer().HashAlgo != segformat.HashCRC32C {
		t.Fatalf("hash_algo = %v, want %v", reader.Trailer().HashAlgo, segformat.HashCRC32C)
	}
}

func TestReadFromMiddleAndLimit(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 40, 10_000, 30_000, 37)
	reader := openFixture(t, fixture, DefaultOptions())

	records, err := reader.Read(context.Background(), 10_017, 5)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	assertRecordsEqual(t, records, fixture.records[17:22])
}

func TestReadClampsBeforeBaseAndStopsPastEnd(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 8, 100, 1_000, 12)
	reader := openFixture(t, fixture, DefaultOptions())

	records, err := reader.Read(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("Read(before base) error = %v", err)
	}
	assertRecordsEqual(t, records, fixture.records[:2])

	records, err = reader.Read(context.Background(), fixture.ref.LastLSN+1, 0)
	if err != nil {
		t.Fatalf("Read(past end) error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("Read(past end) returned %d records, want 0", len(records))
	}
}

func TestScannerStreamsBlockByBlock(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 33, 1_000, 1_000_000, 64)
	reader := openFixture(t, fixture, DefaultOptions())
	scanner, err := reader.Scan(context.Background(), 1_009)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	defer scanner.Close()

	var records []Record
	for {
		record, ok, err := scanner.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if !ok {
			break
		}
		records = append(records, record)
	}
	assertRecordsEqual(t, records, fixture.records[9:])
}

func TestFindLSNByTimestamp(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 40, 1_000, 50_000, 64)
	reader := openFixture(t, fixture, DefaultOptions())

	lsn, ok, err := reader.FindLSNByTimestamp(context.Background(), 49_000)
	if err != nil {
		t.Fatalf("FindLSNByTimestamp(before min) error = %v", err)
	}
	if !ok || lsn != 1_000 {
		t.Fatalf("FindLSNByTimestamp(before min) = %d ok=%v, want 1000 true", lsn, ok)
	}

	lsn, ok, err = reader.FindLSNByTimestamp(context.Background(), 50_017)
	if err != nil {
		t.Fatalf("FindLSNByTimestamp(mid) error = %v", err)
	}
	if !ok || lsn != 1_017 {
		t.Fatalf("FindLSNByTimestamp(mid) = %d ok=%v, want 1017 true", lsn, ok)
	}

	_, ok, err = reader.FindLSNByTimestamp(context.Background(), 99_000)
	if err != nil {
		t.Fatalf("FindLSNByTimestamp(after max) error = %v", err)
	}
	if ok {
		t.Fatal("FindLSNByTimestamp(after max) ok=true, want false")
	}
}

func TestOpenRejectsCorruptTrailer(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 8, 1, 1, 16)
	object := append([]byte(nil), fixture.object...)
	object[len(object)-1] ^= 0xff
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: object})

	_, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Open() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestOpenRejectsCorruptIndex(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 32, 1, 1, 32)
	object := append([]byte(nil), fixture.object...)
	object[fixture.ref.BlockIndexOffset+segformat.IndexPreambleSize] ^= 0xff
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: object})

	_, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Open() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestReadRejectsCorruptBlock(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 16, 1, 1, 24)
	object := append([]byte(nil), fixture.object...)
	blockOffset := segformat.FilePreambleSize + segformat.BlockPreambleSize
	object[blockOffset] ^= 0xff
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: object})

	reader, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	_, err = reader.Read(context.Background(), fixture.ref.BaseLSN, 1)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Read() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestOpenValidateSegmentHashCatchesBodyCorruption(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 16, 1, 1, 24)
	object := append([]byte(nil), fixture.object...)
	blockOffset := segformat.FilePreambleSize + segformat.BlockPreambleSize
	object[blockOffset] ^= 0xff
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: object})

	opts := DefaultOptions()
	opts.ValidateSegmentHash = true
	_, err := Open(context.Background(), store, fixture.ref, opts)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Open() error = %v, want %v", err, ErrCorruptData)
	}
}

func TestOpenDoesNotFetchWholeObjectByDefault(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 24, 1, 1, 48)
	store := newCountingStore(newMemoryStore(map[string][]byte{fixture.ref.URI: fixture.object}))

	reader, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if got := store.reads(); len(got) != 3 {
		t.Fatalf("Open() read count = %d, want 3: %+v", len(got), got)
	}
	for _, read := range store.reads() {
		if read.off == 0 && read.n == uint64(len(fixture.object)) {
			t.Fatalf("Open() fetched whole object: %+v", read)
		}
	}

	if _, err := reader.Read(context.Background(), fixture.ref.BaseLSN, 1); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := store.reads(); len(got) != 4 {
		t.Fatalf("after one block read count = %d, want 4: %+v", len(got), got)
	}
}

func TestOpenRejectsSegmentRefMismatch(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 8, 1, 1, 16)
	fixture.ref.Partition++
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: fixture.object})

	_, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("Open() error = %v, want %v", err, ErrInvalidSegment)
	}
}

type segmentFixture struct {
	ref     pmeta.SegmentRef
	object  []byte
	records []Record
}

func buildSegment(t *testing.T, codec segformat.Codec, hashAlgo segformat.HashAlgo, count int, baseLSN uint64, baseTS int64, valueSize int) segmentFixture {
	t.Helper()
	writerRecords := makeWriterRecords(count, baseLSN, baseTS, valueSize)

	uri := "memory://segreader-test"
	sink := segwriter.NewMemorySink(uri)
	opts := segwriter.DefaultOptions(7)
	opts.Codec = codec
	opts.HashAlgo = hashAlgo
	opts.TargetBlockSize = 256
	opts.PartSize = 128
	opts.SealParallelism = 2
	opts.BlockBufferCount = 5
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	opts.SegmentUUID = [16]byte{1, 2, 3, 4}
	opts.WriterTag = [16]byte{5, 6, 7, 8}
	opts.CreatedUnixMS = 1_776_263_000_000

	w, err := segwriter.New(opts, sink)
	if err != nil {
		t.Fatalf("segwriter.New() error = %v", err)
	}
	for _, record := range writerRecords {
		if err := w.Append(context.Background(), record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	result, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	object := sink.Bytes()
	ref := pmeta.SegmentRef{
		URI:              result.Object.URI,
		Partition:        result.Metadata.Partition,
		WriterEpoch:      1,
		SegmentUUID:      result.Metadata.SegmentUUID,
		WriterTag:        opts.WriterTag,
		BaseLSN:          result.Metadata.BaseLSN,
		LastLSN:          result.Metadata.LastLSN,
		MinTimestampMS:   result.Metadata.MinTimestampMS,
		MaxTimestampMS:   result.Metadata.MaxTimestampMS,
		RecordCount:      result.Metadata.RecordCount,
		BlockCount:       result.Metadata.BlockCount,
		SizeBytes:        result.Object.SizeBytes,
		BlockIndexOffset: result.Metadata.BlockIndexOffset,
		BlockIndexLength: result.Metadata.BlockIndexLength,
		Codec:            result.Metadata.Codec,
		HashAlgo:         result.Metadata.HashAlgo,
		SegmentHash:      result.Metadata.SegmentHash,
		TrailerHash:      result.Metadata.TrailerHash,
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("SegmentRef.Validate() error = %v", err)
	}
	return segmentFixture{
		ref:     ref,
		object:  object,
		records: convertRecords(result.Metadata.Partition, writerRecords),
	}
}

func openFixture(t *testing.T, fixture segmentFixture, opts Options) *Reader {
	t.Helper()
	store := newMemoryStore(map[string][]byte{fixture.ref.URI: fixture.object})
	reader, err := Open(context.Background(), store, fixture.ref, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return reader
}

func makeWriterRecords(count int, baseLSN uint64, baseTS int64, valueSize int) []segwriter.Record {
	records := make([]segwriter.Record, count)
	for i := range records {
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte('a' + (i+j)%26)
		}
		records[i] = segwriter.Record{
			LSN:         baseLSN + uint64(i),
			TimestampMS: baseTS + int64(i),
			Value:       value,
		}
	}
	return records
}

func convertRecords(partition uint32, records []segwriter.Record) []Record {
	out := make([]Record, len(records))
	for i, record := range records {
		out[i] = Record{
			Partition:   partition,
			LSN:         record.LSN,
			TimestampMS: record.TimestampMS,
			Headers:     segformat.CloneHeaders(record.Headers),
			Value:       append([]byte(nil), record.Value...),
		}
	}
	return out
}

func assertRecordsEqual(t *testing.T, got []Record, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Partition != want[i].Partition ||
			got[i].LSN != want[i].LSN ||
			got[i].TimestampMS != want[i].TimestampMS ||
			!headersEqual(got[i].Headers, want[i].Headers) ||
			!bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("record[%d] = {partition:%d lsn:%d ts:%d headers:%v value:%q}, want {partition:%d lsn:%d ts:%d headers:%v value:%q}",
				i, got[i].Partition, got[i].LSN, got[i].TimestampMS, got[i].Headers, got[i].Value,
				want[i].Partition, want[i].LSN, want[i].TimestampMS, want[i].Headers, want[i].Value)
		}
	}
}

func headersEqual(a []segformat.Header, b []segformat.Header) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

type rangeRead struct {
	off uint64
	n   uint64
}

type countingStore struct {
	inner *memoryStore
	mu    sync.Mutex
	log   []rangeRead
}

func newCountingStore(inner *memoryStore) *countingStore {
	return &countingStore{inner: inner}
}

func (s *countingStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	s.mu.Lock()
	s.log = append(s.log, rangeRead{off: off, n: n})
	s.mu.Unlock()
	return s.inner.ReadAt(ctx, uri, off, n)
}

func (s *countingStore) reads() []rangeRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rangeRead(nil), s.log...)
}
