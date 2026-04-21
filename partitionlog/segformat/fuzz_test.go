package segformat

import "testing"

func FuzzParseFilePreamble(f *testing.F) {
	valid, err := (FilePreamble{
		Codec:        CodecNone,
		HashAlgo:     HashXXH64,
		RecordFormat: RecordFormatV1,
	}).MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(make([]byte, FilePreambleSize))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseFilePreamble(b)
	})
}

func FuzzParseBlockPreamble(f *testing.F) {
	valid, err := (BlockPreamble{
		StoredSize:     RecordHeaderSize,
		RawSize:        RecordHeaderSize,
		RecordCount:    1,
		MinTimestampMS: 1,
		MaxTimestampMS: 1,
	}).MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(make([]byte, BlockPreambleSize))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseBlockPreamble(b)
	})
}

func FuzzParseIndexPreamble(f *testing.F) {
	valid, err := (IndexPreamble{
		EntrySize:  BlockIndexEntrySize,
		EntryCount: 1,
	}).MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(make([]byte, IndexPreambleSize))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseIndexPreamble(b)
	})
}

func FuzzParseBlockIndexEntry(f *testing.F) {
	valid, err := (BlockIndexEntry{
		BlockOffset:    FilePreambleSize,
		StoredSize:     1,
		RawSize:        12,
		RecordCount:    1,
		MinTimestampMS: 1,
		MaxTimestampMS: 1,
	}).MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(make([]byte, BlockIndexEntrySize))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseBlockIndexEntry(b)
	})
}

func FuzzParseTrailer(f *testing.F) {
	trailer := Trailer{
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          1,
		LastLSN:          1,
		MinTimestampMS:   1,
		MaxTimestampMS:   1,
		RecordCount:      1,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + BlockPreambleSize + 1,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	valid, err := trailer.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(make([]byte, TrailerSize))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseTrailer(b, 0)
	})
}

func FuzzDecodeRawBlock(f *testing.F) {
	raw, err := EncodeRawBlock([]RawRecord{{TimestampMS: 1, Value: []byte("x")}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxRawBlockSize {
			t.Skip()
		}
		block := BlockPreamble{
			StoredSize:     uint32(len(b)),
			RawSize:        uint32(len(b)),
			RecordCount:    recordCountHint(len(b)),
			MinTimestampMS: 0,
			MaxTimestampMS: 1<<63 - 1,
			BlockHash:      1,
		}
		_, _ = DecodeRawBlock(b, block)
	})
}

func FuzzParseBlockIndex(f *testing.F) {
	entries := []BlockIndexEntry{
		{
			BlockOffset:    FilePreambleSize,
			StoredSize:     1,
			RawSize:        12,
			RecordCount:    1,
			BaseLSN:        1,
			MinTimestampMS: 1,
			MaxTimestampMS: 1,
		},
	}
	trailer := Trailer{
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          1,
		LastLSN:          1,
		MinTimestampMS:   1,
		MaxTimestampMS:   1,
		RecordCount:      1,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + BlockPreambleSize + 1,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	valid, _, err := MarshalBlockIndex(entries, trailer)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = ParseBlockIndex(b, HashXXH64)
	})
}

func recordCountHint(rawSize int) uint32 {
	if rawSize < RecordHeaderSize {
		return 1
	}
	return uint32(rawSize / RecordHeaderSize)
}
