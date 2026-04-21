package segformat

import (
	"fmt"
	"testing"
)

var (
	benchBytes        []byte
	benchRecords      []Record
	benchTrailer      Trailer
	benchIndexEntries []BlockIndexEntry
	benchIndexPream   IndexPreamble
	benchHash         uint64
)

func BenchmarkRawBlockEncode(b *testing.B) {
	for _, tc := range []struct {
		name      string
		records   int
		valueSize int
	}{
		{name: "4k_x_128b", records: 4 * 1024, valueSize: 128},
		{name: "16k_x_128b", records: 16 * 1024, valueSize: 128},
		{name: "4k_x_1k", records: 4 * 1024, valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			records := makeRawRecords(tc.records, tc.valueSize)
			rawSize := tc.records * (RecordHeaderSize + tc.valueSize)
			b.SetBytes(int64(rawSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw, err := EncodeRawBlock(records)
				if err != nil {
					b.Fatal(err)
				}
				benchBytes = raw
			}
		})
	}
}

func BenchmarkRawBlockDecode(b *testing.B) {
	for _, tc := range []struct {
		name      string
		records   int
		valueSize int
	}{
		{name: "4k_x_128b", records: 4 * 1024, valueSize: 128},
		{name: "16k_x_128b", records: 16 * 1024, valueSize: 128},
		{name: "4k_x_1k", records: 4 * 1024, valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw, err := EncodeRawBlock(makeRawRecords(tc.records, tc.valueSize))
			if err != nil {
				b.Fatal(err)
			}
			block := BlockPreamble{
				StoredSize:     uint32(len(raw)),
				RawSize:        uint32(len(raw)),
				RecordCount:    uint32(tc.records),
				BaseLSN:        100,
				MinTimestampMS: 1_000,
				MaxTimestampMS: 1_000 + int64(tc.records-1),
				BlockHash:      1,
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				records, err := DecodeRawBlock(raw, block)
				if err != nil {
					b.Fatal(err)
				}
				benchRecords = records
			}
		})
	}
}

func BenchmarkBlockIndex(b *testing.B) {
	for _, count := range []int{1024, 16 * 1024, 64 * 1024} {
		entries, trailer := makeIndexFixture(count)
		indexBytes, _, err := MarshalBlockIndex(entries, trailer)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("validate_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ValidateIndex(entries, trailer); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("marshal_%d", count), func(b *testing.B) {
			b.SetBytes(int64(len(indexBytes)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bytes, preamble, err := MarshalBlockIndex(entries, trailer)
				if err != nil {
					b.Fatal(err)
				}
				benchBytes = bytes
				benchIndexPream = preamble
			}
		})
		b.Run(fmt.Sprintf("parse_%d", count), func(b *testing.B) {
			b.SetBytes(int64(len(indexBytes)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				preamble, parsed, err := ParseBlockIndex(indexBytes, trailer.HashAlgo)
				if err != nil {
					b.Fatal(err)
				}
				benchIndexPream = preamble
				benchIndexEntries = parsed
			}
		})
	}
}

func BenchmarkTrailer(b *testing.B) {
	_, trailer := makeIndexFixture(1024)
	trailerBytes, err := trailer.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bytes, err := trailer.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			benchBytes = bytes
		}
	})
	b.Run("parse", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parsed, err := ParseTrailer(trailerBytes, trailer.TotalSize)
			if err != nil {
				b.Fatal(err)
			}
			benchTrailer = parsed
		}
	})
}

func BenchmarkHashBytes(b *testing.B) {
	for _, size := range []int{4 << 10, 1 << 20, 8 << 20} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i)
		}
		for _, algo := range []HashAlgo{HashCRC32C, HashXXH64} {
			b.Run(fmt.Sprintf("%s_%d", algo, size), func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sum, err := HashBytes(algo, data)
					if err != nil {
						b.Fatal(err)
					}
					benchHash = sum
				}
			})
		}
	}
}

func makeRawRecords(count, valueSize int) []RawRecord {
	records := make([]RawRecord, count)
	for i := range records {
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i + j)
		}
		records[i] = RawRecord{
			TimestampMS: 1_000 + int64(i),
			Value:       value,
		}
	}
	return records
}

func makeIndexFixture(count int) ([]BlockIndexEntry, Trailer) {
	entries := make([]BlockIndexEntry, count)
	offset := uint64(FilePreambleSize)
	baseLSN := uint64(10_000)
	recordCount := uint32(8)
	for i := range entries {
		minTS := int64(1_000 + i*int(recordCount))
		entries[i] = BlockIndexEntry{
			BlockOffset:    offset,
			StoredSize:     128,
			RawSize:        128,
			RecordCount:    recordCount,
			BaseLSN:        baseLSN,
			MinTimestampMS: minTS,
			MaxTimestampMS: minTS + int64(recordCount-1),
			BlockHash:      uint64(i + 1),
		}
		offset += BlockPreambleSize + uint64(entries[i].StoredSize)
		baseLSN += uint64(recordCount)
	}
	trailer := Trailer{
		Partition:        1,
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          entries[0].BaseLSN,
		LastLSN:          entries[len(entries)-1].BaseLSN + uint64(recordCount) - 1,
		MinTimestampMS:   entries[0].MinTimestampMS,
		MaxTimestampMS:   entries[len(entries)-1].MaxTimestampMS,
		RecordCount:      uint32(count) * recordCount,
		BlockCount:       uint32(count),
		BlockIndexOffset: offset,
		BlockIndexLength: uint32(IndexPreambleSize + count*BlockIndexEntrySize),
		TotalSize:        offset + uint64(IndexPreambleSize+count*BlockIndexEntrySize) + TrailerSize,
	}
	return entries, trailer
}
