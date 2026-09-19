package runcontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
)

func namespace() (ns [32]byte) {
	for i := range ns {
		ns[i] = byte(i)
	}
	return
}
func keyFixture(kind uint8, timeline []byte, lsn uint64) Key {
	return Key{kind, namespace(), 7, timeline, lsn}
}
func vectorInputs() map[string]any {
	maxKey := keyFixture(Events, make([]byte, 512), math.MaxUint64-1)
	maxKey.Shard = math.MaxUint32
	return map[string]any{
		"event-key-binary": keyFixture(Events, []byte{0, 255, 128}, 1), "head-key-prefix": keyFixture(Heads, []byte{'a', 0}, 0), "event-key-max": maxKey,
		"event-null": Event{Kind: Append, LeaderEpoch: -1}, "event-empty": Event{Kind: Append, LeaderEpoch: -1, Payload: []byte{}},
		"event-binary": Event{Kind: Append, Offset: 100, LeaderEpoch: 3, TimestampPresent: true, Timestamp: math.MinInt64, Payload: []byte{0, 255}, Headers: []Header{{[]byte{}, nil}, {[]byte("x"), []byte{}}, {[]byte("x"), []byte{128}}}, Annotations: []byte{0, 129}},
		"seal-max":     Event{Kind: Seal, Offset: math.MaxInt64 - 1, LeaderEpoch: math.MaxInt32, TimestampPresent: true, Timestamp: math.MaxInt64},
		"head-min":     Head{NextLSN: 2}, "head-max": Head{NextLSN: math.MaxUint64, LastOffset: math.MaxInt64 - 1, Sealed: true, TimestampPresent: true, Timestamp: math.MaxInt64},
	}
}

type logicalVectors struct {
	Vectors []struct {
		Name, Codec string
		Hex         string `json:"encoded_hex"`
		SHA256      string
	}
	Publication struct {
		Preimage         string `json:"preimage_hex"`
		ID               string `json:"publication_id_hex"`
		Hash             string `json:"publication_hash"`
		MutationPreimage string `json:"mutation_preimage_hex"`
		MutationHash     string `json:"mutation_sha256"`
	}
	Maximum struct{ SHA256 string } `json:"maximum_event"`
}

func readVectors(t testing.TB) logicalVectors {
	t.Helper()
	b, err := os.ReadFile("../runfile/testdata/compat/v1/logical-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v logicalVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func hashHex(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func TestIndependentLogicalVectors(t *testing.T) {
	v := readVectors(t)
	inputs := vectorInputs()
	if len(v.Vectors) != len(inputs) {
		t.Fatal("missing vectors")
	}
	for _, c := range v.Vectors {
		t.Run(c.Name, func(t *testing.T) {
			var b []byte
			var err error
			switch in := inputs[c.Name].(type) {
			case Key:
				b, err = EncodeKey(make([]byte, MaxKeyBytes), in)
			case Event:
				var n int
				n, err = EventSize(in)
				if err == nil {
					b, err = EncodeEvent(make([]byte, n), in)
				}
			case Head:
				b, err = EncodeHead(make([]byte, HeadBytes), in)
			default:
				t.Fatal("unknown input")
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, unhex(t, c.Hex)) || hashHex(b) != c.SHA256 {
				t.Fatal("independent oracle mismatch")
			}
			assertRoundTrip(t, c.Codec, b)
		})
	}
	max := Event{Kind: Append, LeaderEpoch: -1, Payload: make([]byte, MaxValueBytes-EventFixedBytes)}
	b, err := EncodeEvent(make([]byte, MaxValueBytes), max)
	if err != nil || hashHex(b) != v.Maximum.SHA256 {
		t.Fatal("maximum value mismatch", err)
	}
	assertRoundTrip(t, "event", b)
	max.Payload = append(max.Payload, 0)
	if _, err := EventSize(max); err == nil {
		t.Fatal("oversized event accepted")
	}
}

func assertRoundTrip(t testing.TB, codec string, b []byte) {
	t.Helper()
	var out []byte
	var err error
	switch codec {
	case "key":
		var v Key
		v, err = DecodeKey(b)
		if err == nil {
			out, err = EncodeKey(make([]byte, MaxKeyBytes), v)
		}
	case "event":
		var v Event
		v, err = DecodeEvent(b)
		if err == nil {
			out, err = EncodeEvent(make([]byte, len(b)), v)
		}
	case "head":
		var v Head
		v, err = DecodeHead(b)
		if err == nil {
			out, err = EncodeHead(make([]byte, HeadBytes), v)
		}
	}
	if err != nil || !bytes.Equal(b, out) {
		t.Fatal("canonical roundtrip", codec, err)
	}
}

func publicationFixture(digest []byte) Publication {
	p := Publication{Namespace: namespace(), Cluster: []byte("cluster"), TopicName: []byte("events"), Partition: 4, Shard: 7, MappingGeneration: 1, WriterEpoch: 9, ExpectedRevision: 20, ExpectedOffset: 100, NextOffset: 106, ExpectedLeaderEpoch: 3, LeaderEpoch: 3, NextSequence: 50, ResultingSequence: 53, RecordCount: 3}
	for i := range p.BindingID {
		p.BindingID[i] = 1
		p.TopicID[i] = 2
		p.OwnerID[i] = 3
		p.RunID[i] = 4
		p.AttemptID[i] = 5
	}
	copy(p.MutationDigest[:], digest)
	return p
}

func TestPublicationAndPositionVectors(t *testing.T) {
	v := readVectors(t)
	// This is a test-only reference of E06's positioning rule, not a runtime actor.
	next := map[byte]uint64{'A': 12, 'B': 5}
	offsets := []uint64{100, 102, 105}
	ids := []byte{'A', 'B', 'A'}
	pre := append([]byte("unijord/kafka/mutations/v1\x00"), 0, 0, 0, 3)
	add := func(k Key, seq uint64, value []byte) {
		key, err := EncodeKey(make([]byte, MaxKeyBytes), k)
		if err != nil {
			t.Fatal(err)
		}
		pre = binary.BigEndian.AppendUint16(pre, uint16(len(key)))
		pre = append(pre, key...)
		pre = binary.BigEndian.AppendUint64(pre, seq)
		pre = binary.BigEndian.AppendUint32(pre, uint32(len(value)))
		pre = append(pre, value...)
	}
	final := map[byte]Head{}
	lastSeq := map[byte]uint64{}
	for i, id := range ids {
		lsn, n, err := AdvanceLSN(next[id])
		if err != nil {
			t.Fatal(err)
		}
		next[id] = n
		e := Event{Kind: Append, Offset: offsets[i], LeaderEpoch: 3, Payload: []byte{byte('a' + i)}}
		if i == 2 {
			e.Kind, e.Payload = Seal, nil
		}
		nbytes, err := EventSize(e)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := EncodeEvent(make([]byte, nbytes), e)
		if err != nil {
			t.Fatal(err)
		}
		add(keyFixture(Events, []byte{id}, lsn), uint64(50+i), ev)
		final[id] = Head{NextLSN: n, LastOffset: offsets[i], Sealed: e.Kind == Seal}
		lastSeq[id] = uint64(50 + i)
	}
	pre = binary.BigEndian.AppendUint32(pre, 2)
	for _, id := range []byte{'A', 'B'} {
		hv, err := EncodeHead(make([]byte, HeadBytes), final[id])
		if err != nil {
			t.Fatal(err)
		}
		add(keyFixture(Heads, []byte{id}, 0), lastSeq[id], hv)
	}
	if !bytes.Equal(pre, unhex(t, v.Publication.MutationPreimage)) || hashHex(pre) != v.Publication.MutationHash {
		t.Fatal("gaps, repeated timeline or seal vector differs")
	}
	p := publicationFixture(unhex(t, v.Publication.MutationHash))
	b, err := MarshalPublication(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, unhex(t, v.Publication.Preimage)) {
		t.Fatal("publication oracle mismatch")
	}
	id, h, err := PublicationHashes(p)
	if err != nil || hex.EncodeToString(id[:]) != v.Publication.ID || hex.EncodeToString(h[:]) != v.Publication.Hash {
		t.Fatal("publication hash mismatch", err)
	}
	decoded, err := UnmarshalPublication(b)
	if err != nil || !reflect.DeepEqual(decoded, p) {
		t.Fatal("publication roundtrip", err)
	}
	for i := range b {
		changed := bytes.Clone(b)
		changed[i] ^= 1
		q, err := UnmarshalPublication(changed)
		if err == nil {
			changedID, _, err := PublicationHashes(q)
			if err != nil || changedID == id {
				t.Fatal("identity field not bound", i, err)
			}
		}
	}
}

func TestCoordinateLimitsAndCanonicalFailures(t *testing.T) {
	hi, next, err := SequenceRange(MaxSequence, 1)
	if err != nil || hi != MaxSequence || next != 1<<56 {
		t.Fatal(hi, next, err)
	}
	for _, pair := range [][2]uint64{{0, 1}, {1, 0}, {MaxSequence, 2}, {math.MaxUint64, 2}, {1, math.MaxUint64}, {1, 1 << 32}, {1 << 56, 1}} {
		if _, _, err := SequenceRange(pair[0], pair[1]); err == nil {
			t.Fatal("overflow", pair)
		}
	}
	if l, n, err := AdvanceLSN(math.MaxUint64 - 1); err != nil || l != math.MaxUint64-1 || n != math.MaxUint64 {
		t.Fatal(l, n, err)
	}
	for _, n := range []uint64{0, math.MaxUint64} {
		if _, _, err := AdvanceLSN(n); err == nil {
			t.Fatal("LSN exhaustion")
		}
	}
	v := readVectors(t)
	for _, c := range v.Vectors {
		b := unhex(t, c.Hex)
		decode := func(b []byte) error {
			switch c.Codec {
			case "key":
				_, err := DecodeKey(b)
				return err
			case "event":
				_, err := DecodeEvent(b)
				return err
			default:
				_, err := DecodeHead(b)
				return err
			}
		}
		for n := 0; n < len(b); n++ {
			if decode(b[:n]) == nil {
				t.Fatalf("%s truncation %d", c.Name, n)
			}
		}
		if decode(append(bytes.Clone(b), 0)) == nil {
			t.Fatal("trailing bytes", c.Name)
		}
		if c.Codec != "key" {
			corrupt := bytes.Clone(b)
			corrupt[7] = 2
			if decode(corrupt) == nil {
				t.Fatal("unknown flags")
			}
		}
	}
	base := publicationFixture(unhex(t, v.Publication.MutationHash))
	for _, mutate := range []func(*Publication){
		func(p *Publication) { p.NextOffset = p.ExpectedOffset }, func(p *Publication) { p.NextOffset = math.MaxUint64 }, func(p *Publication) { p.RecordCount = 7 },
		func(p *Publication) { p.LeaderEpoch = 2 }, func(p *Publication) { p.LeaderEpoch = -1 }, func(p *Publication) { p.ExpectedLeaderEpoch = -2 },
		func(p *Publication) { p.ResultingSequence++ }, func(p *Publication) { p.ExpectedRevision = math.MaxUint64 }, func(p *Publication) { p.Cluster = make([]byte, 257) },
	} {
		p := base
		mutate(&p)
		if _, err := MarshalPublication(p); err == nil {
			t.Fatal("invalid publication accepted", p)
		}
	}
}

func TestTimelineOrderingAndAdmission(t *testing.T) {
	identities := [][]byte{{0}, {0, 0}, {0, 255}, {1}, {'a'}, {'a', 0}, {'a', 0, 1}, {'a', 255}, {255}}
	for _, kind := range []uint8{Events, Heads} {
		var prev []byte
		for _, id := range identities {
			lsn := uint64(0)
			if kind == Events {
				lsn = 1
			}
			b, err := EncodeKey(make([]byte, MaxKeyBytes), keyFixture(kind, id, lsn))
			if err != nil {
				t.Fatal(err)
			}
			if prev != nil && bytes.Compare(prev, b) >= 0 {
				t.Fatal("timeline order differs")
			}
			prev = b
		}
	}
	e := Event{Kind: Append, LeaderEpoch: -1, Headers: make([]Header, MaxHeaders)}
	n, err := EventSize(e)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeEvent(make([]byte, n), e)
	if err != nil {
		t.Fatal(err)
	}
	assertRoundTrip(t, "event", b)
	e.Headers = append(e.Headers, Header{})
	if _, err := EventSize(e); err == nil {
		t.Fatal("header count cap")
	}
	e = Event{Kind: Append, LeaderEpoch: -1, Headers: []Header{{Key: make([]byte, math.MaxUint16)}}}
	n, err = EventSize(e)
	if err != nil {
		t.Fatal(err)
	}
	b, err = EncodeEvent(make([]byte, n), e)
	if err != nil {
		t.Fatal(err)
	}
	assertRoundTrip(t, "event", b)
	e.Headers[0].Key = append(e.Headers[0].Key, 0)
	if _, err := EventSize(e); err == nil {
		t.Fatal("key length cap")
	}
	dst := bytes.Repeat([]byte{0xaa}, 100)
	saved := bytes.Clone(dst)
	if _, err := EncodeEvent(dst, Event{Kind: Append, Payload: make([]byte, 100)}); err == nil || !bytes.Equal(dst, saved) {
		t.Fatal("failed encode wrote destination")
	}
}

func TestMaximumPublicationAndMalformedFields(t *testing.T) {
	p := publicationFixture(bytes.Repeat([]byte{1}, 32))
	p.Cluster, p.TopicName = bytes.Repeat([]byte{255}, 256), bytes.Repeat([]byte{128}, 249)
	p.Partition, p.Shard = math.MaxInt32, math.MaxUint32
	p.MappingGeneration, p.WriterEpoch, p.ExpectedRevision = math.MaxUint64, math.MaxUint64, math.MaxUint64-1
	p.ExpectedOffset, p.NextOffset = math.MaxInt64-1, math.MaxInt64
	p.ExpectedLeaderEpoch, p.LeaderEpoch = math.MaxInt32, math.MaxInt32
	p.NextSequence, p.ResultingSequence, p.RecordCount = MaxSequence, MaxSequence+1, 1
	b, err := MarshalPublication(p)
	if err != nil || len(b) != 763 {
		t.Fatal("maximum publication arithmetic", len(b), err)
	}
	q, err := UnmarshalPublication(b)
	if err != nil || !reflect.DeepEqual(p, q) {
		t.Fatal("maximum publication roundtrip", err)
	}
	for i := 0; i < len(b); i++ {
		if _, err := UnmarshalPublication(b[:i]); err == nil {
			t.Fatal("publication truncation", i)
		}
	}
	if _, err := UnmarshalPublication(append(bytes.Clone(b), 0)); err == nil {
		t.Fatal("publication trailing bytes")
	}
	kb, err := EncodeKey(make([]byte, MaxKeyBytes), keyFixture(Events, []byte{0}, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{2, 3, 41} {
		corrupt := bytes.Clone(kb)
		corrupt[at] = 3
		if _, err := DecodeKey(corrupt); err == nil {
			t.Fatal("invalid key version/kind/escape", at)
		}
	}
	eb, err := EncodeEvent(make([]byte, EventFixedBytes), Event{Kind: Append, LeaderEpoch: -1})
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{4, 6, 8, 16, 24, 28, 32, 34} {
		corrupt := bytes.Clone(eb)
		switch at {
		case 4:
			corrupt[at] = 1
		case 6:
			corrupt[at] = 3
		case 8:
			corrupt[at] = 1
		case 16:
			corrupt[at] = 255
		case 24:
			binary.BigEndian.PutUint32(corrupt[24:28], 0xfffffffe)
		case 28:
			binary.BigEndian.PutUint32(corrupt[28:32], MaxValueBytes)
		case 32:
			binary.BigEndian.PutUint16(corrupt[32:34], MaxHeaders+1)
		case 34:
			binary.BigEndian.PutUint32(corrupt[34:38], math.MaxUint32)
		}
		if _, err := DecodeEvent(corrupt); err == nil {
			t.Fatal("invalid event metadata/length", at)
		}
	}
	hb, err := EncodeHead(make([]byte, HeadBytes), Head{NextLSN: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{4, 6, 8, 16, 24} {
		corrupt := bytes.Clone(hb)
		switch at {
		case 4:
			corrupt[at] = 1
		case 6:
			corrupt[at] = 2
		case 8:
			corrupt[at] = 1
		case 16:
			binary.BigEndian.PutUint64(corrupt[16:24], 1)
		case 24:
			binary.BigEndian.PutUint64(corrupt[24:32], math.MaxInt64)
		}
		if _, err := DecodeHead(corrupt); err == nil {
			t.Fatal("invalid head", at)
		}
	}
}

func FuzzLogicalDecode(f *testing.F) {
	for _, v := range readVectors(f).Vectors {
		f.Add(v.Codec, unhex(f, v.Hex))
	}
	f.Fuzz(func(t *testing.T, codec string, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		var err error
		switch codec {
		case "key":
			if len(b) > MaxKeyBytes {
				return
			}
			_, err = DecodeKey(b)
		case "event":
			_, err = DecodeEvent(b)
		case "head":
			if len(b) > HeadBytes {
				return
			}
			_, err = DecodeHead(b)
		default:
			return
		}
		if err == nil {
			assertRoundTrip(t, codec, b)
		}
	})
}
func FuzzPublicationDecode(f *testing.F) {
	f.Add(unhex(f, readVectors(f).Publication.Preimage))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxPublicationBytes {
			return
		}
		p, err := UnmarshalPublication(b)
		if err != nil {
			return
		}
		out, err := MarshalPublication(p)
		if err != nil || !bytes.Equal(b, out) {
			t.Fatal("noncanonical publication", err)
		}
	})
}
