package segmentio

import (
	"context"
	"fmt"
	"io"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	for _, cfg := range benchmarkConfigs() {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			fixture := newBenchmarkFixture(b, cfg.name, cfg.codec, cfg.records, cfg.valueSize)
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.encoded)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				encoded, _, err := Encode(fixture.records, fixture.opts)
				if err != nil {
					b.Fatalf("Encode() error = %v", err)
				}
				if len(encoded) == 0 {
					b.Fatal("Encode() produced empty segment")
				}
			}
		})
	}
}

func BenchmarkReadAll(b *testing.B) {
	for _, cfg := range benchmarkConfigs() {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			fixture := newBenchmarkFixture(b, cfg.name, cfg.codec, cfg.records, cfg.valueSize)
			reader, err := OpenBytes(fixture.encoded)
			if err != nil {
				b.Fatalf("OpenBytes() error = %v", err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.encoded)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				records, err := reader.ReadAll()
				if err != nil {
					b.Fatalf("ReadAll() error = %v", err)
				}
				if len(records) != len(fixture.records) {
					b.Fatalf("ReadAll() records=%d want=%d", len(records), len(fixture.records))
				}
			}
		})
	}
}

func BenchmarkReadFromLSN(b *testing.B) {
	for _, cfg := range benchmarkConfigs() {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			fixture := newBenchmarkFixture(b, cfg.name, cfg.codec, cfg.records, cfg.valueSize)
			reader, err := OpenBytes(fixture.encoded)
			if err != nil {
				b.Fatalf("OpenBytes() error = %v", err)
			}
			fromLSN := fixture.records[len(fixture.records)/2].LSN

			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.encoded) / 2))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				records, err := reader.ReadFromLSN(fromLSN)
				if err != nil {
					b.Fatalf("ReadFromLSN() error = %v", err)
				}
				if len(records) == 0 {
					b.Fatal("ReadFromLSN() returned no records")
				}
			}
		})
	}
}

func BenchmarkSealRawBlock(b *testing.B) {
	for _, codec := range []Codec{CodecNone, CodecZstd} {
		codec := codec
		b.Run(codec.String(), func(b *testing.B) {
			records := makeBenchmarkRecords(0, 0, 4096, 256)
			raw := make([]byte, 0, 4096*(256+12))
			for _, record := range records {
				raw = appendRawRecord(raw, record.TimestampMS, record.Value)
			}

			block := rawBlock{
				Sequence:       0,
				BaseLSN:        records[0].LSN,
				LastLSN:        records[len(records)-1].LSN,
				RecordCount:    uint32(len(records)),
				MinTimestampMS: records[0].TimestampMS,
				MaxTimestampMS: records[len(records)-1].TimestampMS,
				RawBytes:       raw,
			}

			tempDir := b.TempDir()
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				descriptor, err := sealRawBlock(block, codec, tempDir)
				if err != nil {
					b.Fatalf("sealRawBlock() error = %v", err)
				}
				if err := descriptor.Payload.Destroy(); err != nil {
					b.Fatalf("Destroy() error = %v", err)
				}
			}
		})
	}
}

func BenchmarkSegmentWriterFinishDiscardSink(b *testing.B) {
	for _, cfg := range benchmarkConfigs() {
		cfg := cfg
		b.Run(cfg.name, func(b *testing.B) {
			fixture := newBenchmarkFixture(b, cfg.name, cfg.codec, cfg.records, cfg.valueSize)
			tempDir := b.TempDir()
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.encoded)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				writer, err := NewSegmentWriter(WriterOptions{
					Codec:           cfg.codec,
					TargetBlockSize: DefaultTargetBlockSize,
					UploadPartSize:  DefaultUploadPartSize,
					TempDir:         tempDir,
				}, discardSegmentSink{})
				if err != nil {
					b.Fatalf("NewSegmentWriter() error = %v", err)
				}
				for _, record := range fixture.records {
					if err := writer.Append(record); err != nil {
						b.Fatalf("Append() error = %v", err)
					}
				}
				if _, err := writer.Finish(context.Background()); err != nil {
					b.Fatalf("Finish() error = %v", err)
				}
			}
		})
	}
}

type benchmarkConfig struct {
	name      string
	codec     Codec
	records   int
	valueSize int
}

func benchmarkConfigs() []benchmarkConfig {
	return []benchmarkConfig{
		{name: "none_4k_x_256b", codec: CodecNone, records: 4096, valueSize: 256},
		{name: "zstd_4k_x_256b", codec: CodecZstd, records: 4096, valueSize: 256},
		{name: "none_16k_x_128b", codec: CodecNone, records: 16 * 1024, valueSize: 128},
		{name: "zstd_16k_x_128b", codec: CodecZstd, records: 16 * 1024, valueSize: 128},
	}
}

type benchmarkFixture struct {
	records []Record
	opts    WriterOptions
	encoded []byte
}

func newBenchmarkFixture(tb testing.TB, name string, codec Codec, recordCount int, valueSize int) benchmarkFixture {
	tb.Helper()

	records := makeBenchmarkRecords(1, 0, recordCount, valueSize)
	opts := WriterOptions{
		Codec:           codec,
		TargetBlockSize: DefaultTargetBlockSize,
	}
	encoded, _, err := Encode(records, opts)
	if err != nil {
		tb.Fatalf("Encode(%s) error = %v", name, err)
	}
	return benchmarkFixture{
		records: records,
		opts:    opts,
		encoded: encoded,
	}
}

func makeBenchmarkRecords(partition uint32, baseLSN uint64, recordCount int, valueSize int) []Record {
	records := make([]Record, 0, recordCount)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	for i := 0; i < recordCount; i++ {
		payload := append([]byte(nil), value...)
		records = append(records, Record{
			Partition:   partition,
			LSN:         baseLSN + uint64(i),
			TimestampMS: 1_700_000_000_000 + int64(i),
			Value:       payload,
		})
	}
	return records
}

type discardSegmentSink struct{}

func (discardSegmentSink) Begin(_ context.Context, plan AssemblyPlan) (SegmentTxn, error) {
	return discardSegmentTxn{totalSize: plan.TotalSize}, nil
}

type discardSegmentTxn struct {
	totalSize uint64
}

func (discardSegmentTxn) UploadPart(_ context.Context, part UploadPart) (PartReceipt, error) {
	n, err := io.Copy(io.Discard, part.Body)
	if err != nil {
		return PartReceipt{}, err
	}
	if n != part.SizeBytes {
		return PartReceipt{}, fmt.Errorf("discard sink short read: got=%d want=%d", n, part.SizeBytes)
	}
	return PartReceipt{
		PartNumber: part.PartNumber,
		ETag:       fmt.Sprintf("discard-%d", part.PartNumber),
	}, nil
}

func (t discardSegmentTxn) Complete(_ context.Context, _ []PartReceipt) (CommittedObject, error) {
	return CommittedObject{
		ObjectID:  "discard://segment",
		SizeBytes: t.totalSize,
		ETag:      "discard",
	}, nil
}

func (discardSegmentTxn) Abort(_ context.Context) error {
	return nil
}
