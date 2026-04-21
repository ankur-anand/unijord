package segmentio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Partition: 7, LSN: 101, TimestampMS: 1_777_000_000_000, Value: []byte("alpha")},
		{Partition: 7, LSN: 102, TimestampMS: 1_777_000_000_100, Value: []byte("beta")},
		{Partition: 7, LSN: 103, TimestampMS: 1_777_000_000_200, Value: []byte("gamma")},
		{Partition: 7, LSN: 104, TimestampMS: 1_777_000_000_300, Value: []byte("delta")},
	}

	encoded, meta, err := Encode(records, WriterOptions{
		Codec:           CodecZstd,
		TargetBlockSize: 32,
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	reader, err := OpenBytes(encoded)
	if err != nil {
		t.Fatalf("OpenBytes() error = %v", err)
	}

	if got, want := reader.Metadata(), meta; got != want {
		t.Fatalf("Metadata() mismatch\ngot  %#v\nwant %#v", got, want)
	}

	got, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	assertRecordsEqual(t, got, records)
}

func TestReadFromLSN(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Partition: 3, LSN: 10, TimestampMS: 100, Value: []byte("a")},
		{Partition: 3, LSN: 11, TimestampMS: 101, Value: []byte("b")},
		{Partition: 3, LSN: 12, TimestampMS: 102, Value: []byte("c")},
		{Partition: 3, LSN: 13, TimestampMS: 103, Value: []byte("d")},
	}

	encoded, _, err := Encode(records, WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 14,
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	reader, err := OpenBytes(encoded)
	if err != nil {
		t.Fatalf("OpenBytes() error = %v", err)
	}

	got, err := reader.ReadFromLSN(12)
	if err != nil {
		t.Fatalf("ReadFromLSN() error = %v", err)
	}
	assertRecordsEqual(t, got, records[2:])
}

func TestSegmentWriterMatchesEncode(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Partition: 9, LSN: 501, TimestampMS: 1_777_100_000_000, Value: []byte("alpha")},
		{Partition: 9, LSN: 502, TimestampMS: 1_777_100_000_100, Value: bytes.Repeat([]byte("b"), 64)},
		{Partition: 9, LSN: 503, TimestampMS: 1_777_100_000_200, Value: []byte("gamma")},
	}

	opts := WriterOptions{
		Codec:           CodecZstd,
		TargetBlockSize: 128,
	}

	wantBytes, wantMeta, err := Encode(records, opts)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	sink := newBufferSegmentSink()
	writer, err := NewSegmentWriter(opts, sink)
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}

	for _, record := range records {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	result, err := writer.Finish(context.Background())
	if err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	if !bytes.Equal(sink.Bytes(), wantBytes) {
		t.Fatal("SegmentWriter bytes do not match Encode()")
	}
	if result.Metadata != wantMeta {
		t.Fatalf("Finish() metadata mismatch\ngot  %#v\nwant %#v", result.Metadata, wantMeta)
	}
}

func TestSegmentWriterFlushReusesRawBlockBuffer(t *testing.T) {
	t.Parallel()

	writer, err := NewSegmentWriter(WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 1024,
		TempDir:         t.TempDir(),
	}, newBufferSegmentSink())
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}
	defer func() {
		_ = writer.Abort(context.Background())
	}()

	if err := writer.Append(Record{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	capBefore := cap(writer.current.raw)
	if capBefore == 0 {
		t.Fatal("current raw buffer capacity = 0, want non-zero")
	}
	if err := writer.flushCurrentBlock(); err != nil {
		t.Fatalf("flushCurrentBlock() error = %v", err)
	}
	if got := cap(writer.current.raw); got != capBefore {
		t.Fatalf("raw buffer capacity after flush = %d want %d", got, capBefore)
	}
	if err := writer.Append(Record{Partition: 1, LSN: 2, TimestampMS: 11, Value: []byte("beta")}); err != nil {
		t.Fatalf("Append() after flush error = %v", err)
	}
}

func TestAssemblerRejectsOutOfOrderBlocks(t *testing.T) {
	t.Parallel()

	_, err := assembleSegment(4, CodecNone, []blockDescriptor{
		descriptorForTest(1, 3, 4, 2, 12, 13),
		descriptorForTest(0, 1, 2, 2, 10, 11),
	})
	if !errors.Is(err, ErrBlockIndexMismatch) {
		t.Fatalf("assembleSegment() error = %v, want %v", err, ErrBlockIndexMismatch)
	}
}

func TestAssemblerRejectsLSNGap(t *testing.T) {
	t.Parallel()

	_, err := assembleSegment(4, CodecNone, []blockDescriptor{
		descriptorForTest(0, 1, 2, 2, 10, 11),
		descriptorForTest(1, 4, 5, 2, 12, 13),
	})
	if !errors.Is(err, ErrBlockIndexMismatch) {
		t.Fatalf("assembleSegment() error = %v, want %v", err, ErrBlockIndexMismatch)
	}
}

func TestEncodeRejectsInvalidRecordSequences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []Record
		want    error
	}{
		{
			name: "mixed partition",
			records: []Record{
				{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("a")},
				{Partition: 2, LSN: 2, TimestampMS: 11, Value: []byte("b")},
			},
			want: ErrMixedPartition,
		},
		{
			name: "non contiguous lsn",
			records: []Record{
				{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("a")},
				{Partition: 1, LSN: 3, TimestampMS: 11, Value: []byte("b")},
			},
			want: ErrNonContiguousLSN,
		},
		{
			name: "timestamp regression",
			records: []Record{
				{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("a")},
				{Partition: 1, LSN: 2, TimestampMS: 9, Value: []byte("b")},
			},
			want: ErrTimestampOrder,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := Encode(tt.records, WriterOptions{Codec: CodecNone})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Encode() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEncodeRejectsOversizedRecord(t *testing.T) {
	t.Parallel()

	_, _, err := Encode([]Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: bytes.Repeat([]byte("a"), MaxRecordValueLen+1)},
	}, WriterOptions{Codec: CodecNone})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Encode() error = %v, want %v", err, ErrRecordTooLarge)
	}
}

func TestSegmentWriterRejectsEmptyFinish(t *testing.T) {
	t.Parallel()

	writer, err := NewSegmentWriter(WriterOptions{Codec: CodecNone}, newBufferSegmentSink())
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}
	if _, err := writer.Finish(context.Background()); !errors.Is(err, ErrEmptySegment) {
		t.Fatalf("Finish() error = %v, want %v", err, ErrEmptySegment)
	}
}

func TestSegmentWriterRejectsSinkThatDoesNotConsumePart(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sink := &nonConsumingSink{}
	writer, err := NewSegmentWriter(WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 24,
		UploadPartSize:  32,
		TempDir:         tempDir,
	}, sink)
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}

	for _, record := range []Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")},
		{Partition: 1, LSN: 2, TimestampMS: 11, Value: []byte("beta")},
		{Partition: 1, LSN: 3, TimestampMS: 12, Value: []byte("gamma")},
	} {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if _, err := writer.Finish(context.Background()); err == nil {
		t.Fatal("Finish() error = nil, want non-nil")
	}
	if !sink.abortCalled {
		t.Fatal("sink.abortCalled = false, want true")
	}
	if err := writer.Append(Record{Partition: 1, LSN: 4, TimestampMS: 13, Value: []byte("delta")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append() after failed finish error = %v, want %v", err, ErrWriterAborted)
	}
	assertDirEmpty(t, tempDir)
}

func TestSegmentWriterRemovesTempPayloadsOnSuccess(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	writer, err := NewSegmentWriter(WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 24,
		TempDir:         tempDir,
	}, newBufferSegmentSink())
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}

	for _, record := range []Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")},
		{Partition: 1, LSN: 2, TimestampMS: 11, Value: []byte("beta")},
		{Partition: 1, LSN: 3, TimestampMS: 12, Value: []byte("gamma")},
	} {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if _, err := writer.Finish(context.Background()); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	assertDirEmpty(t, tempDir)
}

func TestSegmentWriterRemovesTempPayloadsOnAbort(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	writer, err := NewSegmentWriter(WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 24,
		TempDir:         tempDir,
	}, newBufferSegmentSink())
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}

	for _, record := range []Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")},
		{Partition: 1, LSN: 2, TimestampMS: 11, Value: []byte("beta")},
	} {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := writer.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	assertDirEmpty(t, tempDir)
}

func TestSegmentWriterAbortsTransactionOnUploadFailure(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	sink := &failingSink{failUploadPart: 2}
	writer, err := NewSegmentWriter(WriterOptions{
		Codec:           CodecNone,
		TargetBlockSize: 24,
		UploadPartSize:  32,
		TempDir:         tempDir,
	}, sink)
	if err != nil {
		t.Fatalf("NewSegmentWriter() error = %v", err)
	}

	for _, record := range []Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")},
		{Partition: 1, LSN: 2, TimestampMS: 11, Value: []byte("beta")},
		{Partition: 1, LSN: 3, TimestampMS: 12, Value: []byte("gamma")},
	} {
		if err := writer.Append(record); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if _, err := writer.Finish(context.Background()); err == nil {
		t.Fatal("Finish() error = nil, want non-nil")
	}
	if !sink.abortCalled {
		t.Fatal("sink.abortCalled = false, want true")
	}
	if err := writer.Append(Record{Partition: 1, LSN: 4, TimestampMS: 13, Value: []byte("delta")}); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append() after failed finish error = %v, want %v", err, ErrWriterAborted)
	}
	assertDirEmpty(t, tempDir)
}

func TestOpenRejectsTruncatedSegment(t *testing.T) {
	t.Parallel()

	encoded, _, err := Encode([]Record{
		{Partition: 1, LSN: 1, TimestampMS: 10, Value: []byte("alpha")},
	}, WriterOptions{Codec: CodecNone})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	if _, err := OpenBytes(encoded[:len(encoded)-1]); err == nil {
		t.Fatal("OpenBytes(truncated) error = nil, want non-nil")
	}
}

func descriptorForTest(sequence uint32, baseLSN uint64, lastLSN uint64, recordCount uint32, minTS int64, maxTS int64) blockDescriptor {
	rawSize := uint32(recordCount * 16)
	if rawSize == 0 {
		rawSize = 16
	}
	return blockDescriptor{
		Sequence:       sequence,
		BaseLSN:        baseLSN,
		LastLSN:        lastLSN,
		RecordCount:    recordCount,
		MinTimestampMS: minTS,
		MaxTimestampMS: maxTS,
		RawSize:        rawSize,
		StoredSize:     rawSize,
		CRC32C:         1,
		Codec:          CodecNone,
		Payload:        &testPayload{data: bytes.Repeat([]byte("x"), int(rawSize))},
	}
}

type testPayload struct {
	data      []byte
	destroyed bool
}

func (p *testPayload) Size() uint32 {
	return uint32(len(p.data))
}

func (p *testPayload) Open() (io.ReadCloser, error) {
	if p.destroyed {
		return nil, errPayloadDestroyed
	}
	return io.NopCloser(bytes.NewReader(p.data)), nil
}

func (p *testPayload) Destroy() error {
	p.destroyed = true
	return nil
}

func assertRecordsEqual(t *testing.T, got []Record, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("record count = %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Partition != want[i].Partition ||
			got[i].LSN != want[i].LSN ||
			got[i].TimestampMS != want[i].TimestampMS ||
			!bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("record[%d] mismatch\ngot  %#v\nwant %#v", i, got[i], want[i])
		}
	}
}

func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", dir, err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, filepath.Join(dir, entry.Name()))
		}
		t.Fatalf("dir %s not empty: %v", dir, names)
	}
}

type failingSink struct {
	failUploadPart int32
	abortCalled    bool
}

func (s *failingSink) Begin(_ context.Context, _ AssemblyPlan) (SegmentTxn, error) {
	return &failingTxn{sink: s}, nil
}

type failingTxn struct {
	sink *failingSink
}

func (t *failingTxn) UploadPart(_ context.Context, part UploadPart) (PartReceipt, error) {
	if _, err := io.Copy(io.Discard, part.Body); err != nil {
		return PartReceipt{}, err
	}
	if part.PartNumber == t.sink.failUploadPart {
		return PartReceipt{}, fmt.Errorf("forced upload failure")
	}
	return PartReceipt{PartNumber: part.PartNumber, ETag: "ok"}, nil
}

func (t *failingTxn) Complete(_ context.Context, _ []PartReceipt) (CommittedObject, error) {
	return CommittedObject{ObjectID: "failed", ETag: "failed"}, nil
}

func (t *failingTxn) Abort(_ context.Context) error {
	t.sink.abortCalled = true
	return nil
}

type nonConsumingSink struct {
	abortCalled bool
}

func (s *nonConsumingSink) Begin(_ context.Context, _ AssemblyPlan) (SegmentTxn, error) {
	return &nonConsumingTxn{sink: s}, nil
}

type nonConsumingTxn struct {
	sink *nonConsumingSink
}

func (t *nonConsumingTxn) UploadPart(_ context.Context, part UploadPart) (PartReceipt, error) {
	return PartReceipt{PartNumber: part.PartNumber, ETag: "ok"}, nil
}

func (t *nonConsumingTxn) Complete(_ context.Context, _ []PartReceipt) (CommittedObject, error) {
	return CommittedObject{}, nil
}

func (t *nonConsumingTxn) Abort(_ context.Context) error {
	t.sink.abortCalled = true
	return nil
}
