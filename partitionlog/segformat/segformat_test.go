package segformat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestFilePreambleMarshalParse(t *testing.T) {
	var uuid [16]byte
	var writer [16]byte
	copy(uuid[:], "segment-uuid-001")
	copy(writer[:], "writer-tag-0001")

	in := FilePreamble{
		Partition:    7,
		Codec:        CodecZstd,
		HashAlgo:     HashXXH64,
		RecordFormat: RecordFormatV1,
		BaseLSN:      42,
		SegmentUUID:  uuid,
		WriterTag:    writer,
	}
	buf, err := in.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if len(buf) != FilePreambleSize {
		t.Fatalf("len(buf) = %d, want %d", len(buf), FilePreambleSize)
	}
	if string(buf[0:4]) != "PLSG" {
		t.Fatalf("magic = %q", buf[0:4])
	}
	if got := binary.BigEndian.Uint16(buf[4:6]); got != Version {
		t.Fatalf("version = %d, want %d", got, Version)
	}
	if got := binary.BigEndian.Uint16(buf[6:8]); got != FilePreambleSize {
		t.Fatalf("preamble_len = %d, want %d", got, FilePreambleSize)
	}
	if got := binary.BigEndian.Uint32(buf[12:16]); got != 7 {
		t.Fatalf("partition = %d, want 7", got)
	}
	if got := Codec(binary.BigEndian.Uint16(buf[16:18])); got != CodecZstd {
		t.Fatalf("codec = %v, want %v", got, CodecZstd)
	}
	if got := HashAlgo(binary.BigEndian.Uint16(buf[18:20])); got != HashXXH64 {
		t.Fatalf("hash = %v, want %v", got, HashXXH64)
	}
	if got := binary.BigEndian.Uint64(buf[24:32]); got != 42 {
		t.Fatalf("base_lsn = %d, want 42", got)
	}

	out, err := ParseFilePreamble(buf)
	if err != nil {
		t.Fatalf("ParseFilePreamble() error = %v", err)
	}
	if out != in {
		t.Fatalf("parsed preamble = %+v, want %+v", out, in)
	}
}

func TestBlockPreambleMarshalParse(t *testing.T) {
	in := BlockPreamble{
		StoredSize:     123,
		RawSize:        456,
		RecordCount:    3,
		BaseLSN:        100,
		MinTimestampMS: 10,
		MaxTimestampMS: 12,
		BlockHash:      0xfeedbeef,
	}
	buf, err := in.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if string(buf[0:4]) != "PLBK" {
		t.Fatalf("magic = %q", buf[0:4])
	}
	if got := binary.BigEndian.Uint32(buf[8:12]); got != in.StoredSize {
		t.Fatalf("stored_size = %d, want %d", got, in.StoredSize)
	}
	if got := binary.BigEndian.Uint64(buf[24:32]); got != in.BaseLSN {
		t.Fatalf("base_lsn = %d, want %d", got, in.BaseLSN)
	}
	if got := binary.BigEndian.Uint64(buf[48:56]); got != in.BlockHash {
		t.Fatalf("block_hash = %x, want %x", got, in.BlockHash)
	}

	out, err := ParseBlockPreamble(buf)
	if err != nil {
		t.Fatalf("ParseBlockPreamble() error = %v", err)
	}
	if out != in {
		t.Fatalf("parsed block preamble = %+v, want %+v", out, in)
	}
}

func TestTrailerMarshalParseVerifiesHash(t *testing.T) {
	var uuid [16]byte
	var writer [16]byte
	copy(uuid[:], "segment-uuid-001")
	copy(writer[:], "writer-tag-0001")

	indexOffset := uint64(FilePreambleSize + BlockPreambleSize + 128)
	indexLength := uint32(IndexPreambleSize + BlockIndexEntrySize)
	in := Trailer{
		Partition:        2,
		Codec:            CodecZstd,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          100,
		LastLSN:          102,
		MinTimestampMS:   10,
		MaxTimestampMS:   12,
		RecordCount:      3,
		BlockCount:       1,
		BlockIndexOffset: indexOffset,
		BlockIndexLength: indexLength,
		TotalSize:        indexOffset + uint64(indexLength) + TrailerSize,
		CreatedUnixMS:    1770000000000,
		SegmentUUID:      uuid,
		WriterTag:        writer,
		SegmentHash:      0xabc123,
	}
	buf, out, err := MarshalTrailer(in)
	if err != nil {
		t.Fatalf("MarshalTrailer() error = %v", err)
	}
	if string(buf[0:4]) != "PLFT" {
		t.Fatalf("magic = %q", buf[0:4])
	}
	if got := binary.BigEndian.Uint64(buf[128:136]); got != in.SegmentHash {
		t.Fatalf("segment_hash = %x, want %x", got, in.SegmentHash)
	}
	if out.TrailerHash == 0 {
		t.Fatal("trailer_hash = 0, want non-zero")
	}

	parsed, err := ParseTrailer(buf, in.TotalSize)
	if err != nil {
		t.Fatalf("ParseTrailer() error = %v", err)
	}
	if parsed != out {
		t.Fatalf("parsed trailer = %+v, want %+v", parsed, out)
	}
	in.TrailerHash = out.TrailerHash
	if out != in {
		t.Fatalf("parsed trailer = %+v, want %+v", out, in)
	}

	buf[88] ^= 0x1
	if _, err := ParseTrailer(buf, in.TotalSize); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("ParseTrailer(corrupt) error = %v, want %v", err, ErrIntegrityMismatch)
	}
}

func TestRawBlockEncodeDecode(t *testing.T) {
	raw, err := EncodeRawBlock([]RawRecord{
		{TimestampMS: 10, Value: []byte("alpha")},
		{TimestampMS: 11, Value: []byte("beta")},
	})
	if err != nil {
		t.Fatalf("EncodeRawBlock() error = %v", err)
	}
	block := BlockPreamble{
		StoredSize:     uint32(len(raw)),
		RawSize:        uint32(len(raw)),
		RecordCount:    2,
		BaseLSN:        50,
		MinTimestampMS: 10,
		MaxTimestampMS: 11,
		BlockHash:      1,
	}
	records, err := DecodeRawBlock(raw, block)
	if err != nil {
		t.Fatalf("DecodeRawBlock() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if records[0].LSN != 50 || records[0].TimestampMS != 10 || string(records[0].Value) != "alpha" {
		t.Fatalf("record[0] = %+v", records[0])
	}
	if records[1].LSN != 51 || records[1].TimestampMS != 11 || string(records[1].Value) != "beta" {
		t.Fatalf("record[1] = %+v", records[1])
	}
	raw[len(raw)-1] = '!'
	if string(records[1].Value) != "bet!" {
		t.Fatal("decoded value should alias raw block bytes")
	}
}

func TestRecordCloneDetachesValue(t *testing.T) {
	in := Record{LSN: 7, TimestampMS: 99, Value: []byte("alpha")}
	out := in.Clone()
	if out.LSN != in.LSN || out.TimestampMS != in.TimestampMS || !bytes.Equal(out.Value, in.Value) {
		t.Fatalf("Clone() = %+v, want %+v", out, in)
	}

	in.Value[0] = 'z'
	if string(out.Value) != "alpha" {
		t.Fatalf("cloned value = %q, want detached copy", out.Value)
	}

	empty := Record{LSN: 8, TimestampMS: 100}
	if cloned := empty.Clone(); cloned.Value != nil {
		t.Fatalf("empty Clone().Value = %v, want nil", cloned.Value)
	}
}

func TestBlockIndexMarshalParseValidate(t *testing.T) {
	entries := []BlockIndexEntry{
		{
			BlockOffset:    FilePreambleSize,
			StoredSize:     100,
			RawSize:        200,
			RecordCount:    2,
			BaseLSN:        10,
			MinTimestampMS: 1000,
			MaxTimestampMS: 1001,
			BlockHash:      0x1,
		},
		{
			BlockOffset:    FilePreambleSize + BlockPreambleSize + 100,
			StoredSize:     101,
			RawSize:        201,
			RecordCount:    3,
			BaseLSN:        12,
			MinTimestampMS: 1001,
			MaxTimestampMS: 1004,
			BlockHash:      0x2,
		},
	}
	indexOffset := entries[1].BlockOffset + BlockPreambleSize + uint64(entries[1].StoredSize)
	trailer := Trailer{
		Codec:            CodecZstd,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          10,
		LastLSN:          14,
		MinTimestampMS:   1000,
		MaxTimestampMS:   1004,
		RecordCount:      5,
		BlockCount:       2,
		BlockIndexOffset: indexOffset,
		BlockIndexLength: IndexPreambleSize + 2*BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	indexBytes, preamble, err := MarshalBlockIndex(entries, trailer)
	if err != nil {
		t.Fatalf("MarshalBlockIndex() error = %v", err)
	}
	if preamble.EntryCount != 2 {
		t.Fatalf("EntryCount = %d, want 2", preamble.EntryCount)
	}
	parsedPreamble, parsedEntries, err := ParseBlockIndex(indexBytes, HashXXH64)
	if err != nil {
		t.Fatalf("ParseBlockIndex() error = %v", err)
	}
	if parsedPreamble != preamble {
		t.Fatalf("parsed preamble = %+v, want %+v", parsedPreamble, preamble)
	}
	for i := range entries {
		if parsedEntries[i] != entries[i] {
			t.Fatalf("entry[%d] = %+v, want %+v", i, parsedEntries[i], entries[i])
		}
	}

	if err := ValidateIndex(parsedEntries, trailer); err != nil {
		t.Fatalf("ValidateIndex() error = %v", err)
	}

	parsedEntries[1].BlockOffset++
	if err := ValidateIndex(parsedEntries, trailer); !errors.Is(err, ErrBlockIndexMismatch) {
		t.Fatalf("ValidateIndex(gap) error = %v, want %v", err, ErrBlockIndexMismatch)
	}
}

func TestReservedFieldsAreRejected(t *testing.T) {
	preamble := FilePreamble{
		Codec:        CodecNone,
		HashAlgo:     HashCRC32C,
		RecordFormat: RecordFormatV1,
	}
	buf, err := preamble.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	buf[22] = 1
	if _, err := ParseFilePreamble(buf); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("ParseFilePreamble(reserved) error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestDecodeRawBlockRejectsImpossibleRecordCountBeforeAllocation(t *testing.T) {
	block := BlockPreamble{
		StoredSize:     RecordHeaderSize,
		RawSize:        RecordHeaderSize,
		RecordCount:    2,
		BaseLSN:        1,
		MinTimestampMS: 10,
		MaxTimestampMS: 10,
		BlockHash:      1,
	}
	if _, err := DecodeRawBlock(make([]byte, RecordHeaderSize), block); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("DecodeRawBlock() error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestBlockPreambleRejectsLSNOverflow(t *testing.T) {
	block := BlockPreamble{
		StoredSize:     RecordHeaderSize,
		RawSize:        RecordHeaderSize,
		RecordCount:    2,
		BaseLSN:        ^uint64(0) - 1,
		MinTimestampMS: 10,
		MaxTimestampMS: 10,
		BlockHash:      1,
	}
	if err := block.Validate(); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("BlockPreamble.Validate() error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestDecodeRawBlockRejectsLSNOverflow(t *testing.T) {
	raw, err := EncodeRawBlock([]RawRecord{
		{TimestampMS: 10, Value: []byte("a")},
		{TimestampMS: 10, Value: []byte("b")},
	})
	if err != nil {
		t.Fatalf("EncodeRawBlock() error = %v", err)
	}
	block := BlockPreamble{
		StoredSize:     uint32(len(raw)),
		RawSize:        uint32(len(raw)),
		RecordCount:    2,
		BaseLSN:        ^uint64(0) - 1,
		MinTimestampMS: 10,
		MaxTimestampMS: 10,
		BlockHash:      1,
	}
	if _, err := DecodeRawBlock(raw, block); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("DecodeRawBlock() error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestBlockIndexEntryRejectsLSNOverflow(t *testing.T) {
	entry := BlockIndexEntry{
		BlockOffset:    FilePreambleSize,
		StoredSize:     RecordHeaderSize,
		RawSize:        RecordHeaderSize,
		RecordCount:    2,
		BaseLSN:        ^uint64(0) - 1,
		MinTimestampMS: 10,
		MaxTimestampMS: 10,
		BlockHash:      1,
	}
	if err := entry.Validate(); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("BlockIndexEntry.Validate() error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestValidateIndexRejectsShiftedFirstBlockOffset(t *testing.T) {
	entries := []BlockIndexEntry{
		{
			BlockOffset:    FilePreambleSize + 1,
			StoredSize:     100,
			RawSize:        120,
			RecordCount:    1,
			BaseLSN:        10,
			MinTimestampMS: 100,
			MaxTimestampMS: 100,
			BlockHash:      1,
		},
	}
	trailer := Trailer{
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          10,
		LastLSN:          10,
		MinTimestampMS:   100,
		MaxTimestampMS:   100,
		RecordCount:      1,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + 1 + BlockPreambleSize + 100,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	if err := ValidateIndex(entries, trailer); !errors.Is(err, ErrBlockIndexMismatch) {
		t.Fatalf("ValidateIndex() error = %v, want %v", err, ErrBlockIndexMismatch)
	}
}

func TestValidateIndexRejectsTrailerIndexOffsetMismatch(t *testing.T) {
	entries := []BlockIndexEntry{
		{
			BlockOffset:    FilePreambleSize,
			StoredSize:     100,
			RawSize:        120,
			RecordCount:    1,
			BaseLSN:        10,
			MinTimestampMS: 100,
			MaxTimestampMS: 100,
			BlockHash:      1,
		},
	}
	trailer := Trailer{
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          10,
		LastLSN:          10,
		MinTimestampMS:   100,
		MaxTimestampMS:   100,
		RecordCount:      1,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + BlockPreambleSize + 101,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	if err := ValidateIndex(entries, trailer); !errors.Is(err, ErrBlockIndexMismatch) {
		t.Fatalf("ValidateIndex() error = %v, want %v", err, ErrBlockIndexMismatch)
	}
}

func TestValidateIndexRejectsLSNOverflow(t *testing.T) {
	entries := []BlockIndexEntry{
		{
			BlockOffset:    FilePreambleSize,
			StoredSize:     100,
			RawSize:        120,
			RecordCount:    2,
			BaseLSN:        ^uint64(0) - 1,
			MinTimestampMS: 100,
			MaxTimestampMS: 100,
			BlockHash:      1,
		},
	}
	trailer := Trailer{
		Codec:            CodecNone,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          ^uint64(0) - 1,
		LastLSN:          ^uint64(0),
		MinTimestampMS:   100,
		MaxTimestampMS:   100,
		RecordCount:      2,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + BlockPreambleSize + 100,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	if err := ValidateIndex(entries, trailer); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("ValidateIndex() error = %v, want %v", err, ErrInvalidSegment)
	}
}

func TestValidatePreambleTrailer(t *testing.T) {
	var uuid [16]byte
	var writer [16]byte
	copy(uuid[:], "segment-uuid-001")
	copy(writer[:], "writer-tag-0001")
	preamble := FilePreamble{
		Partition:    1,
		Codec:        CodecZstd,
		HashAlgo:     HashXXH64,
		RecordFormat: RecordFormatV1,
		BaseLSN:      10,
		SegmentUUID:  uuid,
		WriterTag:    writer,
	}
	trailer := Trailer{
		Partition:        1,
		Codec:            CodecZstd,
		HashAlgo:         HashXXH64,
		RecordFormat:     RecordFormatV1,
		BaseLSN:          10,
		LastLSN:          10,
		MinTimestampMS:   100,
		MaxTimestampMS:   100,
		RecordCount:      1,
		BlockCount:       1,
		BlockIndexOffset: FilePreambleSize + BlockPreambleSize + 100,
		BlockIndexLength: IndexPreambleSize + BlockIndexEntrySize,
		SegmentUUID:      uuid,
		WriterTag:        writer,
	}
	trailer.TotalSize = trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength) + TrailerSize
	if err := ValidatePreambleTrailer(preamble, trailer); err != nil {
		t.Fatalf("ValidatePreambleTrailer() error = %v", err)
	}
	trailer.Codec = CodecNone
	if err := ValidatePreambleTrailer(preamble, trailer); !errors.Is(err, ErrInvalidSegment) {
		t.Fatalf("ValidatePreambleTrailer(mismatch) error = %v, want %v", err, ErrInvalidSegment)
	}
}
