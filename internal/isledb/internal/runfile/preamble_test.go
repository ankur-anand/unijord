package runfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestPreambleCompatibilityVector(t *testing.T) {
	vector := loadOuterFramingVector(t)
	expected := decodeHex(t, vector.Preamble.ExpectedHex)
	p, err := UnmarshalPreamble(expected)
	if err != nil {
		t.Fatal(err)
	}
	if p.CreatorRole != CreatorRoleWriterFlush || p.Shard != 7 || p.CreatorEpoch != 9 || p.SeqLo != 10 || p.SeqHi != 20 {
		t.Fatalf("decoded preamble fields=%+v", p)
	}
	encoded, err := MarshalPreamble(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, expected) {
		t.Fatal("preamble does not reproduce frozen compatibility bytes")
	}
	if got := sha256Hex(encoded); got != vector.Preamble.ExpectedSHA256 {
		t.Fatalf("preamble SHA-256=%s, want %s", got, vector.Preamble.ExpectedSHA256)
	}
}

func TestPreambleBoundaryRoundTrip(t *testing.T) {
	maximum := Preamble{
		CreatorRole:  CreatorRoleCompactionOutput,
		Shard:        math.MaxUint32,
		CreatorEpoch: math.MaxUint64,
		SeqLo:        0,
		SeqHi:        math.MaxUint64,
	}
	fillBytes(maximum.NamespaceHash[:], 0xff)
	fillBytes(maximum.RunID[:], 0xfe)
	fillBytes(maximum.PublicationHash[:], 0xfd)

	encoded, err := maximum.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalPreamble(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != maximum {
		t.Fatalf("maximum round trip=%+v, want %+v", decoded, maximum)
	}

	encoded[0] ^= 0xff
	again, err := maximum.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(again[:4]) != PreambleMagic {
		t.Fatal("mutating encoded bytes changed source value")
	}

	minimum := Preamble{
		CreatorRole:  CreatorRoleWriterFlush,
		CreatorEpoch: 1,
	}
	minimum.NamespaceHash[0] = 1
	minimum.RunID[0] = 1
	minimum.PublicationHash[0] = 1
	encoded, err = MarshalPreamble(minimum)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = UnmarshalPreamble(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != minimum {
		t.Fatalf("minimum round trip=%+v, want %+v", decoded, minimum)
	}
}

func TestPreambleRejectsFixedAndReservedFields(t *testing.T) {
	valid := decodeHex(t, loadOuterFramingVector(t).Preamble.ExpectedHex)
	tests := []struct {
		name   string
		want   error
		mutate func([]byte)
	}{
		{name: "magic", want: ErrCorruptRun, mutate: func(data []byte) { data[0] ^= 1 }},
		{name: "version", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[4:6], 2) }},
		{name: "header bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[6:8], 127) }},
		{name: "flags", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[8:12], 1) }},
		{name: "creator role zero", want: ErrCorruptRun, mutate: func(data []byte) { data[12] = 0 }},
		{name: "creator role unknown", want: ErrCorruptRun, mutate: func(data []byte) { data[12] = 3 }},
		{name: "hash algorithm", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { data[13] = 2 }},
		{name: "reserved0", want: ErrCorruptRun, mutate: func(data []byte) { data[14] = 1 }},
		{name: "reserved1", want: ErrCorruptRun, mutate: func(data []byte) { data[20] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if _, err := UnmarshalPreamble(data); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
	for _, data := range [][]byte{valid[:len(valid)-1], append(bytes.Clone(valid), 0)} {
		if _, err := UnmarshalPreamble(data); !errors.Is(err, ErrCorruptRun) {
			t.Fatalf("length %d error=%v, want corruption", len(data), err)
		}
	}
}

func TestPreambleRejectsInvalidSemanticFields(t *testing.T) {
	validBytes := decodeHex(t, loadOuterFramingVector(t).Preamble.ExpectedHex)
	valid, err := UnmarshalPreamble(validBytes)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Preamble)
	}{
		{name: "role", mutate: func(p *Preamble) { p.CreatorRole = 0 }},
		{name: "epoch", mutate: func(p *Preamble) { p.CreatorEpoch = 0 }},
		{name: "sequence", mutate: func(p *Preamble) { p.SeqLo, p.SeqHi = 2, 1 }},
		{name: "namespace", mutate: func(p *Preamble) { p.NamespaceHash = [SHA256Bytes]byte{} }},
		{name: "run ID", mutate: func(p *Preamble) { p.RunID = [RunIDBytes]byte{} }},
		{name: "publication", mutate: func(p *Preamble) { p.PublicationHash = [SHA256Bytes]byte{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if _, err := MarshalPreamble(candidate); !errors.Is(err, ErrInvalidRun) {
				t.Fatalf("error=%v, want invalid run", err)
			}
		})
	}
}

func TestPreambleDecodeOwnsDataAndIsAtomic(t *testing.T) {
	data := decodeHex(t, loadOuterFramingVector(t).Preamble.ExpectedHex)
	decoded, err := UnmarshalPreamble(data)
	if err != nil {
		t.Fatal(err)
	}
	wantNamespace := decoded.NamespaceHash
	wantRunID := decoded.RunID
	wantPublication := decoded.PublicationHash
	clear(data[48:128])
	if decoded.NamespaceHash != wantNamespace || decoded.RunID != wantRunID || decoded.PublicationHash != wantPublication {
		t.Fatal("decoded preamble aliases caller input")
	}

	target := decoded
	want := target
	if err := target.UnmarshalBinary(make([]byte, PreambleBytes)); !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corruption", err)
	}
	if target != want {
		t.Fatal("failed UnmarshalBinary changed receiver")
	}
	var nilTarget *Preamble
	if err := nilTarget.UnmarshalBinary(data); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("nil receiver error=%v, want invalid run", err)
	}
}
