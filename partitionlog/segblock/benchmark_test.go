package segblock

import (
	"fmt"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

var (
	benchSealed Sealed
	benchRaw    []byte
)

func BenchmarkSeal(b *testing.B) {
	for _, tc := range []struct {
		name      string
		codec     segformat.Codec
		records   int
		valueSize int
	}{
		{name: "none_4k_x_128b", codec: segformat.CodecNone, records: 4 * 1024, valueSize: 128},
		{name: "zstd_4k_x_128b", codec: segformat.CodecZstd, records: 4 * 1024, valueSize: 128},
		{name: "zstd_16k_x_128b", codec: segformat.CodecZstd, records: 16 * 1024, valueSize: 128},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw, meta := makeBenchRawBlock(b, tc.records, tc.valueSize)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sealed, err := Seal(tc.codec, segformat.HashXXH64, raw, meta)
				if err != nil {
					b.Fatal(err)
				}
				benchSealed = sealed
			}
		})
	}
}

func BenchmarkOpen(b *testing.B) {
	for _, tc := range []struct {
		name      string
		codec     segformat.Codec
		records   int
		valueSize int
	}{
		{name: "none_4k_x_128b", codec: segformat.CodecNone, records: 4 * 1024, valueSize: 128},
		{name: "zstd_4k_x_128b", codec: segformat.CodecZstd, records: 4 * 1024, valueSize: 128},
		{name: "zstd_16k_x_128b", codec: segformat.CodecZstd, records: 16 * 1024, valueSize: 128},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw, meta := makeBenchRawBlock(b, tc.records, tc.valueSize)
			sealed, err := Seal(tc.codec, segformat.HashXXH64, raw, meta)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				opened, err := Open(tc.codec, segformat.HashXXH64, sealed.Preamble, sealed.Stored)
				if err != nil {
					b.Fatal(err)
				}
				benchRaw = opened
			}
		})
	}
}

func makeBenchRawBlock(b *testing.B, records, valueSize int) ([]byte, Meta) {
	b.Helper()
	rawRecords := make([]segformat.RawRecord, records)
	for i := range rawRecords {
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i + j)
		}
		rawRecords[i] = segformat.RawRecord{
			TimestampMS: 1_000 + int64(i),
			Value:       value,
		}
	}
	raw, err := segformat.EncodeRawBlock(rawRecords)
	if err != nil {
		b.Fatal(err)
	}
	return raw, Meta{
		BaseLSN:        100,
		RecordCount:    uint32(records),
		MinTimestampMS: 1_000,
		MaxTimestampMS: 1_000 + int64(records-1),
	}
}

func BenchmarkSealOpenStoredSize(b *testing.B) {
	for _, records := range []int{4 * 1024, 16 * 1024} {
		b.Run(fmt.Sprintf("zstd_%d_x_128b", records), func(b *testing.B) {
			raw, meta := makeBenchRawBlock(b, records, 128)
			sealed, err := Seal(segformat.CodecZstd, segformat.HashXXH64, raw, meta)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(sealed.Stored))/float64(len(raw)), "stored/raw")
		})
	}
}
