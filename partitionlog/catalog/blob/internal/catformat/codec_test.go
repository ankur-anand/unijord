package catformat

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestLeafPageRoundTripAndWireOffsets(t *testing.T) {
	page := testLeafPage()
	encoded, id, err := MarshalLeafPage(page)
	if err != nil {
		t.Fatalf("MarshalLeafPage() error = %v", err)
	}
	if got, want := len(encoded), PageHeaderSize+2*LeafEntrySize+TrailerSize; got != want {
		t.Fatalf("encoded size = %d, want %d", got, want)
	}
	if got := string(encoded[:4]); got != "PLCL" {
		t.Fatalf("magic = %q, want PLCL", got)
	}
	if got := binary.BigEndian.Uint16(encoded[4:6]); got != Version {
		t.Fatalf("version = %d, want %d", got, Version)
	}
	if got := binary.BigEndian.Uint32(encoded[24:28]); got != LeafEntrySize {
		t.Fatalf("entry_size = %d, want %d", got, LeafEntrySize)
	}
	if got := binary.BigEndian.Uint64(encoded[PageHeaderSize+80 : PageHeaderSize+88]); got != page.Entries[0].WriterEpoch {
		t.Fatalf("leaf writer_epoch = %d, want %d", got, page.Entries[0].WriterEpoch)
	}
	wantSum := sha256.Sum256(encoded[:len(encoded)-TrailerSize])
	if got := id; got != [16]byte(wantSum[:16]) {
		t.Fatalf("page ID = %x, want %x", got, wantSum[:16])
	}
	parsed, parsedID, err := ParseLeafPage(encoded)
	if err != nil {
		t.Fatalf("ParseLeafPage() error = %v", err)
	}
	if parsedID != id {
		t.Fatalf("parsed page ID = %x, want %x", parsedID, id)
	}
	if !reflect.DeepEqual(parsed, page) {
		t.Fatalf("parsed page differs:\n got: %#v\nwant: %#v", parsed, page)
	}
}

func TestIndexPageRoundTripAndWireOffsets(t *testing.T) {
	page := testIndexPage()
	encoded, id, err := MarshalIndexPage(page)
	if err != nil {
		t.Fatalf("MarshalIndexPage() error = %v", err)
	}
	if got, want := len(encoded), PageHeaderSize+2*IndexEntrySize+TrailerSize; got != want {
		t.Fatalf("encoded size = %d, want %d", got, want)
	}
	if got := string(encoded[:4]); got != "PLCX" {
		t.Fatalf("magic = %q, want PLCX", got)
	}
	if got := binary.BigEndian.Uint32(encoded[24:28]); got != IndexEntrySize {
		t.Fatalf("entry_size = %d, want %d", got, IndexEntrySize)
	}
	if got := binary.BigEndian.Uint64(encoded[PageHeaderSize+64 : PageHeaderSize+72]); got != page.Entries[0].SegmentCount {
		t.Fatalf("segment_count = %d, want %d", got, page.Entries[0].SegmentCount)
	}
	parsed, parsedID, err := ParseIndexPage(encoded)
	if err != nil {
		t.Fatalf("ParseIndexPage() error = %v", err)
	}
	if parsedID != id {
		t.Fatalf("parsed page ID = %x, want %x", parsedID, id)
	}
	if !reflect.DeepEqual(parsed, page) {
		t.Fatalf("parsed page differs:\n got: %#v\nwant: %#v", parsed, page)
	}
}

func TestHeadRoundTrip(t *testing.T) {
	head := testHead()
	encoded, err := MarshalHead(head)
	if err != nil {
		t.Fatalf("MarshalHead() error = %v", err)
	}
	wantSize := HeadHeaderSize + LeafEntrySize + LeafEntrySize + SectionHeaderSize + IndexEntrySize + TrailerSize
	if got := len(encoded); got != wantSize {
		t.Fatalf("encoded size = %d, want %d", got, wantSize)
	}
	if got := string(encoded[:4]); got != "PLCH" {
		t.Fatalf("magic = %q, want PLCH", got)
	}
	if got := string(encoded[HeadHeaderSize+LeafEntrySize+LeafEntrySize : HeadHeaderSize+LeafEntrySize+LeafEntrySize+4]); got != "PLCS" {
		t.Fatalf("section magic = %q, want PLCS", got)
	}
	parsed, err := ParseHead(encoded)
	if err != nil {
		t.Fatalf("ParseHead() error = %v", err)
	}
	if !reflect.DeepEqual(parsed, head) {
		t.Fatalf("parsed head differs:\n got: %#v\nwant: %#v", parsed, head)
	}
}

func TestEmptyHeadRoundTrip(t *testing.T) {
	head := testEmptyHead()
	encoded, err := MarshalHead(head)
	if err != nil {
		t.Fatalf("MarshalHead() error = %v", err)
	}
	parsed, err := ParseHead(encoded)
	if err != nil {
		t.Fatalf("ParseHead() error = %v", err)
	}
	if !reflect.DeepEqual(parsed, head) {
		t.Fatalf("parsed head differs:\n got: %#v\nwant: %#v", parsed, head)
	}
}

func TestParsersDetectBodyAndTrailerCorruption(t *testing.T) {
	encoded, _, err := MarshalLeafPage(testLeafPage())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		offset int
	}{
		{name: "body", offset: PageHeaderSize + 64},
		{name: "trailer", offset: len(encoded) - 16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			corrupt[tc.offset] ^= 0x80
			_, _, err := ParseLeafPage(corrupt)
			if !errors.Is(err, ErrIntegrityMismatch) {
				t.Fatalf("ParseLeafPage() error = %v, want ErrIntegrityMismatch", err)
			}
		})
	}
}

func TestParserRejectsValidlyHashedReservedBytes(t *testing.T) {
	encoded, _, err := MarshalLeafPage(testLeafPage())
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(nil), encoded[:len(encoded)-TrailerSize]...)
	body[28] = 1
	encoded, err = appendTrailer(body, testLeafPage().Header.HashAlgo)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ParseLeafPage(encoded)
	if !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("ParseLeafPage() error = %v, want ErrInvalidObject", err)
	}
}

func TestParserRejectsUnknownVersionBeforeTrustingCounts(t *testing.T) {
	encoded, _, err := MarshalLeafPage(testLeafPage())
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(nil), encoded[:len(encoded)-TrailerSize]...)
	binary.BigEndian.PutUint16(body[4:6], Version+1)
	binary.BigEndian.PutUint32(body[20:24], ^uint32(0))
	encoded, err = appendTrailer(body, testLeafPage().Header.HashAlgo)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ParseLeafPage(encoded)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("ParseLeafPage() error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestMarshalRejectsSemanticInconsistency(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*LeafPage)
	}{
		{
			name: "gap",
			mutate: func(page *LeafPage) {
				page.Entries[1].BaseLSN++
				page.Entries[1].LastLSN++
			},
		},
		{
			name: "timestamp regression",
			mutate: func(page *LeafPage) {
				page.Entries[1].MinTimestampMS = page.Entries[0].MaxTimestampMS - 1
			},
		},
		{
			name: "writer epoch regression",
			mutate: func(page *LeafPage) {
				page.Entries[1].WriterEpoch = page.Entries[0].WriterEpoch - 1
			},
		},
		{
			name: "zero writer tag",
			mutate: func(page *LeafPage) {
				page.Entries[0].WriterTag = [16]byte{}
			},
		},
		{
			name: "wrong subtree count",
			mutate: func(page *LeafPage) {
				page.Header.SegmentCount++
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			page := testLeafPage()
			tc.mutate(&page)
			if _, _, err := MarshalLeafPage(page); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("MarshalLeafPage() error = %v, want ErrInvalidObject", err)
			}
		})
	}
}

func TestHeadRejectsBrokenRootCoverage(t *testing.T) {
	head := testHead()
	head.Sections[0].Entries[0].SeqHi--
	if _, err := MarshalHead(head); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("MarshalHead() error = %v, want ErrInvalidObject", err)
	}
}

func FuzzParseLeafPage(f *testing.F) {
	encoded, _, err := MarshalLeafPage(testLeafPage())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("PLCL"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ParseLeafPage(data)
	})
}

func FuzzParseIndexPage(f *testing.F) {
	encoded, _, err := MarshalIndexPage(testIndexPage())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("PLCX"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ParseIndexPage(data)
	})
}

func FuzzParseHead(f *testing.F) {
	encoded, err := MarshalHead(testHead())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("PLCH"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseHead(data)
	})
}

func testLeafPage() LeafPage {
	entries := []LeafEntry{
		testLeafEntry(0, 1, 100, 101, 7, 0x11),
		testLeafEntry(2, 3, 101, 104, 8, 0x22),
	}
	return LeafPage{
		Header: PageHeader{
			Partition:      7,
			Level:          0,
			HashAlgo:       segformat.HashXXH64,
			EntryCount:     2,
			EntrySize:      LeafEntrySize,
			SeqLo:          0,
			SeqHi:          3,
			MinTimestampMS: 100,
			MaxTimestampMS: 104,
			Generation:     9,
			WriterEpoch:    8,
			StreamKey:      filled32(0xa1),
			SegmentCount:   2,
		},
		Entries: entries,
	}
}

func testIndexPage() IndexPage {
	entries := []IndexEntry{
		testIndexEntry(0, 0, 3, 100, 104, 7, 2, 0x31),
		testIndexEntry(0, 4, 8, 104, 110, 8, 3, 0x41),
	}
	return IndexPage{
		Header: PageHeader{
			Partition:      7,
			Level:          1,
			HashAlgo:       segformat.HashCRC32C,
			EntryCount:     2,
			EntrySize:      IndexEntrySize,
			SeqLo:          0,
			SeqHi:          8,
			MinTimestampMS: 100,
			MaxTimestampMS: 110,
			Generation:     9,
			WriterEpoch:    8,
			StreamKey:      filled32(0xa1),
			SegmentCount:   5,
		},
		Entries: entries,
	}
}

func testHead() Head {
	active := testLeafEntry(2, 3, 101, 104, 8, 0x22)
	return Head{
		Header: HeadHeader{
			Flags:                 FlagHasLastSegment,
			Partition:             7,
			HashAlgo:              segformat.HashXXH64,
			SegmentLayoutVersion:  SegmentLayoutV1,
			LeafLimit:             4,
			IndexLimit:            4,
			ActiveCount:           1,
			LevelCount:            1,
			NextLSN:               4,
			OldestLSN:             0,
			AppliedRetentionLSN:   0,
			WriterEpoch:           8,
			Generation:            9,
			SegmentCount:          2,
			ReachableSegmentCount: 2,
			WriterID:              filled16(0xb1),
			StreamKey:             filled32(0xa1),
			DataRootKey:           filled32(0xc1),
		},
		LastSegment: active,
		Active:      []LeafEntry{active},
		Sections: []OpenIndexSection{{
			Level:   1,
			Entries: []IndexEntry{testIndexEntry(0, 0, 1, 100, 101, 7, 1, 0x31)},
		}},
	}
}

func testEmptyHead() Head {
	return Head{Header: HeadHeader{
		Partition:            7,
		HashAlgo:             segformat.HashCRC32C,
		SegmentLayoutVersion: SegmentLayoutV1,
		LeafLimit:            4,
		IndexLimit:           4,
		StreamKey:            filled32(0xa1),
		DataRootKey:          filled32(0xc1),
	}}
}

func testLeafEntry(base, last uint64, minTS, maxTS int64, epoch uint64, marker byte) LeafEntry {
	return LeafEntry{
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   minTS,
		MaxTimestampMS:   maxTS,
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        4096 + base,
		BlockIndexOffset: 2048,
		BlockIndexLength: 128,
		Codec:            segformat.CodecZstd,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      0x0102030405060708 + base,
		TrailerHash:      0x1112131415161718 + base,
		WriterEpoch:      epoch,
		SegmentUUID:      filled16(marker),
		WriterTag:        filled16(byte(epoch)),
	}
}

func testIndexEntry(level uint8, lo, hi uint64, minTS, maxTS int64, generation, segments uint64, marker byte) IndexEntry {
	return IndexEntry{
		Level:          level,
		EntryCount:     uint32(segments),
		SeqLo:          lo,
		SeqHi:          hi,
		MinTimestampMS: minTS,
		MaxTimestampMS: maxTS,
		Generation:     generation,
		PageID:         filled16(marker),
		SegmentCount:   segments,
	}
}

func filled16(value byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = value
	}
	return out
}

func filled32(value byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = value
	}
	return out
}
