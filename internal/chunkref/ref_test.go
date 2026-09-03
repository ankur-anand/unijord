package chunkref

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/namespaceid"
	"github.com/ankur-anand/unijord/internal/record"
	"github.com/ankur-anand/unijord/internal/ujtc"
)

func TestFromMetadataDecodeAndJSONRoundTrip(t *testing.T) {
	records := []record.Record{
		{TimelineKey: []byte("a"), TimelineLSN: 3, TimestampMS: 10, Value: []byte("a3")},
		{TimelineKey: []byte("b"), TimelineLSN: 0, TimestampMS: 11, Value: []byte("b0")},
		{TimelineKey: []byte("a"), TimelineLSN: 4, TimestampMS: 12, Value: []byte("a4")},
	}
	body, metadata, err := ujtc.Marshal(ujtc.Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 7, WriterEpoch: 2, Sequence: 19}, records)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := FromMetadata("shards/7/chunk-19.ujtc", metadata)
	if err != nil {
		t.Fatal(err)
	}
	if ref.FormatVersion != ujtc.Version || ref.SizeBytes != uint64(len(body)) || ref.TimelineCount != 2 {
		t.Fatalf("ref = %+v", ref)
	}
	decodedMetadata, decoded, err := Decode(ref, body)
	if err != nil {
		t.Fatal(err)
	}
	if decodedMetadata != metadata || len(decoded) != len(records) {
		t.Fatalf("metadata=%+v records=%d", decodedMetadata, len(decoded))
	}

	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Ref
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !Same(ref, roundTrip) {
		t.Fatalf("JSON round trip = %+v, want %+v", roundTrip, ref)
	}

	body[0] ^= 0xff
	if _, _, err := Decode(ref, body); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Decode(corrupt) error = %v, want ErrMismatch", err)
	}
}

func TestValidateRejectsIncompleteReference(t *testing.T) {
	_, metadata, err := ujtc.Marshal(ujtc.Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), WriterEpoch: 1}, []record.Record{
		{TimelineKey: []byte("a"), TimestampMS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := FromMetadata("chunk.ujtc", metadata)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Ref){
		"key":             func(ref *Ref) { ref.Key = "" },
		"version":         func(ref *Ref) { ref.FormatVersion++ },
		"namespace":       func(ref *Ref) { ref.NamespaceHash = [32]byte{} },
		"epoch":           func(ref *Ref) { ref.WriterEpoch = 0 },
		"records":         func(ref *Ref) { ref.RecordCount = 0 },
		"timelines":       func(ref *Ref) { ref.TimelineCount = ref.RecordCount + 1 },
		"size":            func(ref *Ref) { ref.SizeBytes = 1 },
		"timestamps":      func(ref *Ref) { ref.MinTimestampMS, ref.MaxTimestampMS = 2, 1 },
		"complete SHA256": func(ref *Ref) { ref.SHA256 = [32]byte{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := Validate(candidate); !errors.Is(err, ErrInvalidRef) {
				t.Fatalf("Validate() error = %v, want ErrInvalidRef", err)
			}
		})
	}
}

func TestDecodeRejectsMetadataSubstitution(t *testing.T) {
	body, metadata, err := ujtc.Marshal(ujtc.Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 1, WriterEpoch: 2, Sequence: 3}, []record.Record{
		{TimelineKey: []byte("a"), TimestampMS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := FromMetadata("chunk.ujtc", metadata)
	if err != nil {
		t.Fatal(err)
	}
	ref.Sequence++
	if _, _, err := Decode(ref, bytes.Clone(body)); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Decode(substituted ref) error = %v, want ErrMismatch", err)
	}
	ref.Sequence--
	ref.SHA256[0] ^= 0xff
	if _, _, err := Decode(ref, bytes.Clone(body)); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Decode(substituted hash) error = %v, want ErrMismatch", err)
	}
}

func TestDecodeRejectsNamespaceSubstitution(t *testing.T) {
	body, metadata, err := ujtc.Marshal(ujtc.Identity{
		NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 1, WriterEpoch: 2, Sequence: 3,
	}, []record.Record{{TimelineKey: []byte("a"), TimestampMS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := FromMetadata("chunk.ujtc", metadata)
	if err != nil {
		t.Fatal(err)
	}
	ref.NamespaceHash = namespaceid.Sum([]byte("tenant-b"))
	if _, _, err := Decode(ref, bytes.Clone(body)); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Decode(cross-namespace ref) error = %v, want ErrMismatch", err)
	}
}

func TestVersion1ReferenceVector(t *testing.T) {
	body, metadata, err := ujtc.Marshal(ujtc.Identity{
		NamespaceHash: namespaceid.Sum([]byte("tenant-a")),
		Shard:         17,
		WriterEpoch:   3,
		Sequence:      9,
	}, []record.Record{
		{
			TimelineKey: []byte("run-a"), TimelineLSN: 0, TimestampMS: -5,
			Headers: []record.Header{{Key: []byte("kind"), Value: []byte("open")}},
			Value:   []byte("a0"),
		},
		{TimelineKey: []byte("run-b"), TimelineLSN: 7, TimestampMS: 11, Value: []byte("b7")},
		{TimelineKey: []byte("run-a"), TimelineLSN: 1, TimestampMS: 12, Value: []byte("a1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := FromMetadata("chunks/example-v1.ujtc", metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := Ref{
		Key:            "chunks/example-v1.ujtc",
		FormatVersion:  1,
		NamespaceHash:  namespaceid.Sum([]byte("tenant-a")),
		Shard:          17,
		WriterEpoch:    3,
		Sequence:       9,
		RecordCount:    3,
		TimelineCount:  2,
		SizeBytes:      261,
		MinTimestampMS: -5,
		MaxTimestampMS: 12,
		SHA256:         [32]byte{0x05, 0x7c, 0x84, 0x16, 0x48, 0x71, 0xb5, 0x90, 0x0b, 0x99, 0xbb, 0xd2, 0xac, 0xf0, 0xea, 0x14, 0xed, 0x03, 0xf5, 0x32, 0x62, 0x6b, 0x7a, 0xc2, 0x3d, 0x5f, 0xa8, 0xe2, 0xb8, 0x75, 0x7a, 0x57},
	}
	if !Same(ref, want) || uint64(len(body)) != want.SizeBytes {
		t.Fatalf("reference vector=%+v size=%d want=%+v", ref, len(body), want)
	}
}
