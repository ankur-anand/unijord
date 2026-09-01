package chunkfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/ankur-anand/unijord/internal/namespaceid"
	"github.com/ankur-anand/unijord/internal/record"
	"github.com/cespare/xxhash/v2"
)

func TestRoundTripAndMetadata(t *testing.T) {
	identity := testIdentity(17, 3, 9)
	records := fixtureRecords()

	body, metadata, err := Marshal(identity, records)
	if err != nil {
		t.Fatal(err)
	}
	decodedMetadata, decoded, err := Unmarshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if decodedMetadata != metadata {
		t.Fatalf("metadata = %+v, want %+v", decodedMetadata, metadata)
	}
	if !reflect.DeepEqual(decoded, records) {
		t.Fatalf("records = %#v, want %#v", decoded, records)
	}
	if metadata.Identity != identity || metadata.RecordCount != 3 || metadata.TimelineCount != 2 ||
		metadata.MinTimestamp != -5 || metadata.MaxTimestamp != 12 || metadata.ObjectHash != sha256.Sum256(body) {
		t.Fatalf("metadata = %+v", metadata)
	}
	if len(body) > int(MaxObjectBytes) || !bytes.Equal(body[80:112], identity.NamespaceHash[:]) || !allZero(body[112:120]) {
		t.Fatalf("object bytes=%d namespace=%x reserved_zero=%v", len(body), body[80:112], allZero(body[112:120]))
	}
}

func TestUnmarshalOwnsReturnedBytes(t *testing.T) {
	body, _, err := Marshal(testIdentity(0, 1, 0), fixtureRecords())
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := Unmarshal(body)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneRecords(decoded)
	for i := range body {
		body[i] ^= 0xff
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatal("decoded records alias the mutable input object")
	}
}

func TestRejectsCorruptionAndNonCanonicalFields(t *testing.T) {
	body, _, err := Marshal(testIdentity(17, 3, 9), fixtureRecords())
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func([]byte){
		"body hash": func(corrupt []byte) {
			corrupt[len(corrupt)-1] ^= 0xff
		},
		"reserved header": func(corrupt []byte) {
			corrupt[12] = 1
			fixHeaderHash(corrupt)
		},
		"reserved namespace suffix": func(corrupt []byte) {
			corrupt[112] = 1
			fixHeaderHash(corrupt)
		},
		"zero namespace hash": func(corrupt []byte) {
			clear(corrupt[80:112])
			fixHeaderHash(corrupt)
		},
		"timeline count": func(corrupt []byte) {
			binary.BigEndian.PutUint32(corrupt[36:40], 3)
			fixHeaderHash(corrupt)
		},
		"object size": func(corrupt []byte) {
			binary.BigEndian.PutUint64(corrupt[72:80], uint64(len(corrupt)-1))
			fixHeaderHash(corrupt)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(body)
			mutate(corrupt)
			if _, _, err := Unmarshal(corrupt); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Unmarshal() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestMarshalRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		records  []record.Record
		want     error
	}{
		{name: "zero namespace", identity: Identity{WriterEpoch: 1}, records: fixtureRecords(), want: ErrInvalid},
		{name: "zero epoch", identity: Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a"))}, records: fixtureRecords(), want: ErrInvalid},
		{name: "empty", identity: testIdentity(0, 1, 0), want: ErrNoRecords},
		{
			name:     "timeline gap",
			identity: testIdentity(0, 1, 0),
			records: []record.Record{
				{TimelineKey: []byte("run"), TimelineLSN: 1, TimestampMS: 1},
				{TimelineKey: []byte("run"), TimelineLSN: 3, TimestampMS: 2},
			},
			want: ErrInvalid,
		},
		{
			name:     "timestamp regression",
			identity: testIdentity(0, 1, 0),
			records: []record.Record{
				{TimelineKey: []byte("run"), TimelineLSN: 1, TimestampMS: 2},
				{TimelineKey: []byte("run"), TimelineLSN: 2, TimestampMS: 1},
			},
			want: ErrInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := Marshal(test.identity, test.records); !errors.Is(err, test.want) {
				t.Fatalf("Marshal() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestVersion1CompatibilityVector(t *testing.T) {
	body, metadata, err := Marshal(testIdentity(17, 3, 9), fixtureRecords())
	if err != nil {
		t.Fatal(err)
	}
	const wantSize = 261
	const wantSHA256 = "057c84164871b5900b99bbd2acf0ea14ed03f532626b7ac23d5fa8e2b8757a57"
	if len(body) != wantSize || hex.EncodeToString(metadata.ObjectHash[:]) != wantSHA256 {
		t.Fatalf("fixture size=%d sha256=%x", len(body), metadata.ObjectHash)
	}
}

func FuzzUnmarshal(f *testing.F) {
	seed, _, err := Marshal(testIdentity(17, 3, 9), fixtureRecords())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("UJTC"))
	f.Fuzz(func(t *testing.T, input []byte) {
		metadata, records, err := Unmarshal(input)
		if err != nil {
			return
		}
		reencoded, reencodedMetadata, err := Marshal(metadata.Identity, records)
		if err != nil {
			t.Fatalf("valid decode cannot be re-encoded: %v", err)
		}
		if !bytes.Equal(reencoded, input) || reencodedMetadata != metadata {
			t.Fatal("accepted object is not canonical")
		}
	})
}

func fixtureRecords() []record.Record {
	return []record.Record{
		{
			TimelineKey: []byte("run-a"), TimelineLSN: 0, TimestampMS: -5,
			Headers: []record.Header{{Key: []byte("kind"), Value: []byte("open")}},
			Value:   []byte("a0"),
		},
		{TimelineKey: []byte("run-b"), TimelineLSN: 7, TimestampMS: 11, Headers: []record.Header{}, Value: []byte("b7")},
		{TimelineKey: []byte("run-a"), TimelineLSN: 1, TimestampMS: 12, Headers: []record.Header{}, Value: []byte("a1")},
	}
}

func testIdentity(shard uint32, epoch, sequence uint64) Identity {
	return Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: shard, WriterEpoch: epoch, Sequence: sequence}
}

func cloneRecords(records []record.Record) []record.Record {
	cloned := make([]record.Record, len(records))
	for i, item := range records {
		cloned[i] = record.Record{
			TimelineKey: bytes.Clone(item.TimelineKey), TimelineLSN: item.TimelineLSN,
			TimestampMS: item.TimestampMS, Value: bytes.Clone(item.Value),
			Headers: make([]record.Header, len(item.Headers)),
		}
		for j, header := range item.Headers {
			cloned[i].Headers[j] = record.Header{Key: bytes.Clone(header.Key), Value: bytes.Clone(header.Value)}
		}
	}
	return cloned
}

func fixHeaderHash(buf []byte) {
	binary.BigEndian.PutUint64(buf[120:128], xxhash.Sum64(buf[:120]))
}
