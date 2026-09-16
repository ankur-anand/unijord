package runfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestTrailerCompatibilityVector(t *testing.T) {
	vector := loadOuterFramingVector(t)
	expected := decodeHex(t, vector.Trailer.ExpectedHex)
	trailer, err := UnmarshalTrailerForObject(expected, vector.ObjectSize)
	if err != nil {
		t.Fatal(err)
	}
	if trailer.DirectoryOffset != 4408 || trailer.DirectoryLength != 459 || trailer.ObjectSize != 5027 || trailer.RegionCount != 3 {
		t.Fatalf("decoded trailer fields=%+v", trailer)
	}
	encoded, err := MarshalTrailer(trailer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, expected) {
		t.Fatal("trailer does not reproduce frozen compatibility bytes")
	}
	if got := encodeHex(encoded[trailerCRCOffset:trailerCRCEnd]); got != vector.Trailer.ExpectedCRC32C {
		t.Fatalf("trailer CRC-32C=%s, want %s", got, vector.Trailer.ExpectedCRC32C)
	}
	if got := sha256Hex(encoded); got != vector.Trailer.ExpectedSHA256 {
		t.Fatalf("trailer SHA-256=%s, want %s", got, vector.Trailer.ExpectedSHA256)
	}
}

func TestTrailerBoundaryRoundTrip(t *testing.T) {
	directoryOffset := MaxRunObjectBytes - MaxDirectoryBytes - TrailerBytes
	trailer := Trailer{
		DirectoryOffset: directoryOffset,
		DirectoryLength: MaxDirectoryBytes,
		ObjectSize:      MaxRunObjectBytes,
		RegionCount:     MaxRegionCount,
	}
	fillBytes(trailer.DirectoryHash[:], 0xff)
	fillBytes(trailer.PayloadHash[:], 0xfe)
	fillBytes(trailer.RunID[:], 0xfd)

	encoded, err := trailer.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalTrailerForObject(encoded, MaxRunObjectBytes)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != trailer {
		t.Fatalf("round trip=%+v, want %+v", decoded, trailer)
	}

	minimum := Trailer{
		DirectoryOffset: PreambleBytes,
		DirectoryLength: 1,
		ObjectSize:      PreambleBytes + 1 + TrailerBytes,
		RegionCount:     MinRegionCount,
	}
	minimum.DirectoryHash[0] = 1
	minimum.PayloadHash[0] = 1
	minimum.RunID[0] = 1
	encoded, err = MarshalTrailer(minimum)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = UnmarshalTrailer(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != minimum {
		t.Fatalf("minimum round trip=%+v, want %+v", decoded, minimum)
	}
}

func TestTrailerRejectsFixedReservedAndCRCFields(t *testing.T) {
	valid := decodeHex(t, loadOuterFramingVector(t).Trailer.ExpectedHex)
	tests := []struct {
		name       string
		want       error
		rewriteCRC bool
		mutate     func([]byte)
	}{
		{name: "magic", want: ErrCorruptRun, rewriteCRC: true, mutate: func(data []byte) { data[0] ^= 1 }},
		{name: "version", want: ErrUnsupportedRunVersion, rewriteCRC: true, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[4:6], 2) }},
		{name: "trailer bytes", want: ErrCorruptRun, rewriteCRC: true, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[6:8], 159) }},
		{name: "flags", want: ErrCorruptRun, rewriteCRC: true, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[8:12], 1) }},
		{name: "checksum", want: ErrCorruptRun, mutate: func(data []byte) { data[12] ^= 1 }},
		{name: "algorithm before checksum", want: ErrCorruptRun, mutate: func(data []byte) { data[42] = 2 }},
		{name: "hash algorithm", want: ErrUnsupportedRunVersion, rewriteCRC: true, mutate: func(data []byte) { data[42] = 2 }},
		{name: "reserved0", want: ErrCorruptRun, rewriteCRC: true, mutate: func(data []byte) { data[43] = 1 }},
		{name: "reserved1", want: ErrCorruptRun, rewriteCRC: true, mutate: func(data []byte) { data[128] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if test.rewriteCRC {
				rewriteTrailerCRC(t, data)
			}
			if _, err := UnmarshalTrailer(data); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
	for _, data := range [][]byte{valid[:len(valid)-1], append(bytes.Clone(valid), 0)} {
		if _, err := UnmarshalTrailer(data); !errors.Is(err, ErrCorruptRun) {
			t.Fatalf("length %d error=%v, want corruption", len(data), err)
		}
	}
}

func TestTrailerCRCRejectsEverySingleBitMutation(t *testing.T) {
	valid := decodeHex(t, loadOuterFramingVector(t).Trailer.ExpectedHex)
	for offset := range valid {
		for bit := uint(0); bit < 8; bit++ {
			data := bytes.Clone(valid)
			data[offset] ^= 1 << bit
			if _, err := UnmarshalTrailer(data); err == nil {
				t.Fatalf("accepted single-bit mutation at byte=%d bit=%d", offset, bit)
			}
		}
	}
}

func TestTrailerRejectsInvalidArithmeticAndBounds(t *testing.T) {
	valid := decodeHex(t, loadOuterFramingVector(t).Trailer.ExpectedHex)
	tests := []struct {
		name   string
		want   error
		mutate func([]byte)
	}{
		{name: "directory before preamble", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[16:24], 120) }},
		{name: "unaligned directory", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[16:24], 4409) }},
		{name: "zero directory length", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[24:32], 0) }},
		{name: "large directory", want: ErrRunTooLarge, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[24:32], MaxDirectoryBytes+1) }},
		{name: "few regions", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[40:42], MinRegionCount-1) }},
		{name: "many regions", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[40:42], MaxRegionCount+1) }},
		{name: "framing mismatch", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[32:40], 5028) }},
		{name: "directory overflow", want: ErrCorruptRun, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[16:24], math.MaxUint64-7)
			binary.BigEndian.PutUint64(data[24:32], 16)
		}},
		{name: "oversized object", want: ErrRunTooLarge, mutate: func(data []byte) {
			binary.BigEndian.PutUint64(data[16:24], MaxRunObjectBytes)
			binary.BigEndian.PutUint64(data[24:32], 8)
			binary.BigEndian.PutUint64(data[32:40], MaxRunObjectBytes+8+TrailerBytes)
		}},
		{name: "zero run ID", want: ErrCorruptRun, mutate: func(data []byte) { clear(data[112:128]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			rewriteTrailerCRC(t, data)
			if _, err := UnmarshalTrailer(data); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestTrailerMarshalRejectsInvalidInput(t *testing.T) {
	valid, err := UnmarshalTrailer(decodeHex(t, loadOuterFramingVector(t).Trailer.ExpectedHex))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		want   error
		mutate func(*Trailer)
	}{
		{name: "directory before preamble", want: ErrInvalidRun, mutate: func(value *Trailer) { value.DirectoryOffset = 120 }},
		{name: "unaligned directory", want: ErrInvalidRun, mutate: func(value *Trailer) { value.DirectoryOffset++ }},
		{name: "zero directory length", want: ErrInvalidRun, mutate: func(value *Trailer) { value.DirectoryLength = 0 }},
		{name: "large directory", want: ErrRunTooLarge, mutate: func(value *Trailer) { value.DirectoryLength = MaxDirectoryBytes + 1 }},
		{name: "few regions", want: ErrInvalidRun, mutate: func(value *Trailer) { value.RegionCount = MinRegionCount - 1 }},
		{name: "many regions", want: ErrInvalidRun, mutate: func(value *Trailer) { value.RegionCount = MaxRegionCount + 1 }},
		{name: "framing mismatch", want: ErrInvalidRun, mutate: func(value *Trailer) { value.ObjectSize++ }},
		{name: "directory overflow", want: ErrInvalidRun, mutate: func(value *Trailer) {
			value.DirectoryOffset = math.MaxUint64 - 7
			value.DirectoryLength = 16
		}},
		{name: "oversized", want: ErrRunTooLarge, mutate: func(value *Trailer) {
			value.DirectoryOffset = MaxRunObjectBytes
			value.DirectoryLength = 8
			value.ObjectSize = MaxRunObjectBytes + 8 + TrailerBytes
		}},
		{name: "zero run ID", want: ErrInvalidRun, mutate: func(value *Trailer) { value.RunID = [RunIDBytes]byte{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if _, err := MarshalTrailer(candidate); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestTrailerObjectSizeBinding(t *testing.T) {
	vector := loadOuterFramingVector(t)
	data := decodeHex(t, vector.Trailer.ExpectedHex)
	trailer, err := UnmarshalTrailer(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := trailer.ValidateObjectSize(vector.ObjectSize + 1); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("mismatch error=%v, want corruption", err)
	}
	if _, err := UnmarshalTrailerForObject(data, MaxRunObjectBytes+1); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("oversized actual error=%v, want too large", err)
	}
}

func TestTrailerDecodeOwnsDataAndIsAtomic(t *testing.T) {
	data := decodeHex(t, loadOuterFramingVector(t).Trailer.ExpectedHex)
	decoded, err := UnmarshalTrailer(data)
	if err != nil {
		t.Fatal(err)
	}
	wantDirectoryHash := decoded.DirectoryHash
	wantPayloadHash := decoded.PayloadHash
	wantRunID := decoded.RunID
	clear(data[48:128])
	if decoded.DirectoryHash != wantDirectoryHash || decoded.PayloadHash != wantPayloadHash || decoded.RunID != wantRunID {
		t.Fatal("decoded trailer aliases caller input")
	}

	target := decoded
	want := target
	if err := target.UnmarshalBinary(make([]byte, TrailerBytes)); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corruption", err)
	}
	if target != want {
		t.Fatal("failed UnmarshalBinary changed receiver")
	}
	var nilTarget *Trailer
	if err := nilTarget.UnmarshalBinary(data); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("nil receiver error=%v, want invalid run", err)
	}
}

func rewriteTrailerCRC(t *testing.T, data []byte) {
	t.Helper()
	checksum, ok := trailerCRC32C(data)
	if !ok {
		t.Fatalf("cannot checksum trailer length %d", len(data))
	}
	binary.BigEndian.PutUint32(data[trailerCRCOffset:trailerCRCEnd], checksum)
}
