package segblock

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestSealOpenRoundTripCodecNone(t *testing.T) {
	raw := []byte("raw block bytes")
	sealed, err := Seal(segformat.CodecNone, segformat.HashXXH64, raw, Meta{
		BaseLSN:        10,
		RecordCount:    1,
		MinTimestampMS: 100,
		MaxTimestampMS: 100,
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if !bytes.Equal(sealed.Stored, raw) {
		t.Fatalf("stored = %q, want raw", sealed.Stored)
	}
	raw[0] = 'X'
	if bytes.Equal(sealed.Stored, raw) {
		t.Fatal("Seal() should detach stored bytes from caller raw buffer")
	}

	opened, err := Open(segformat.CodecNone, segformat.HashXXH64, sealed.Preamble, sealed.Stored)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if !bytes.Equal(opened, []byte("raw block bytes")) {
		t.Fatalf("opened = %q", opened)
	}
	sealed.Stored[0] = 'Y'
	if bytes.Equal(opened, sealed.Stored) {
		t.Fatal("Open(codec none) should detach raw bytes from stored buffer")
	}
}

func TestSealOpenRoundTripZstd(t *testing.T) {
	raw, err := segformat.EncodeRawBlock([]segformat.RawRecord{
		{TimestampMS: 100, Value: bytes.Repeat([]byte("a"), 256)},
		{TimestampMS: 101, Value: bytes.Repeat([]byte("b"), 256)},
	})
	if err != nil {
		t.Fatalf("EncodeRawBlock() error = %v", err)
	}
	sealed, err := Seal(segformat.CodecZstd, segformat.HashXXH64, raw, Meta{
		BaseLSN:        20,
		RecordCount:    2,
		MinTimestampMS: 100,
		MaxTimestampMS: 101,
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if len(sealed.Stored) >= len(raw) {
		t.Fatalf("zstd stored size = %d, raw size = %d; want compression for fixture", len(sealed.Stored), len(raw))
	}
	opened, err := Open(segformat.CodecZstd, segformat.HashXXH64, sealed.Preamble, sealed.Stored)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if !bytes.Equal(opened, raw) {
		t.Fatal("opened raw bytes mismatch")
	}
	records, err := segformat.DecodeRawBlock(opened, sealed.Preamble)
	if err != nil {
		t.Fatalf("DecodeRawBlock() error = %v", err)
	}
	if len(records) != 2 || records[0].LSN != 20 || records[1].LSN != 21 {
		t.Fatalf("records = %+v", records)
	}
}

func TestOpenRejectsCorruptStoredBytes(t *testing.T) {
	raw := []byte("raw block bytes")
	sealed, err := Seal(segformat.CodecNone, segformat.HashCRC32C, raw, Meta{
		BaseLSN:        10,
		RecordCount:    1,
		MinTimestampMS: 100,
		MaxTimestampMS: 100,
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	sealed.Stored[0] ^= 1
	if _, err := Open(segformat.CodecNone, segformat.HashCRC32C, sealed.Preamble, sealed.Stored); !errors.Is(err, segformat.ErrIntegrityMismatch) {
		t.Fatalf("Open(corrupt) error = %v, want %v", err, segformat.ErrIntegrityMismatch)
	}
}

func TestSealRejectsInvalidMeta(t *testing.T) {
	_, err := Seal(segformat.CodecNone, segformat.HashXXH64, make([]byte, segformat.RecordHeaderSize), Meta{
		BaseLSN:        1,
		RecordCount:    2,
		MinTimestampMS: 10,
		MaxTimestampMS: 10,
	})
	if !errors.Is(err, segformat.ErrInvalidSegment) {
		t.Fatalf("Seal() error = %v, want %v", err, segformat.ErrInvalidSegment)
	}
}
