package runfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

const compatDir = "testdata/compat/v1"

// TestCompatibilityPinnedSSTs materializes the six C01 hashes. It never
// regenerates manifest.json, even when the new complete-run corpus is refreshed.
func TestCompatibilityPinnedSSTs(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(compatDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if sha256Hex(b) != "5d37bfb51a03f7d3a2d3c1204c698e0687b2d1b7bf1ffcc77efca57a719bbe20" {
		t.Fatal("C01-C06 manifest changed")
	}
	var m struct {
		Producer struct {
			Vectors []struct {
				Name        string `json:"name"`
				Generator   string `json:"generator"`
				Compression string `json:"compression_profile"`
				Length      uint64 `json:"expected_length"`
				Hash        string `json:"expected_sha256"`
			} `json:"vectors"`
		} `json:"pebble_reference_producer"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Producer.Vectors) != 6 {
		t.Fatal("missing pinned SST vectors")
	}
	for _, v := range m.Producer.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			compression := TableCompressionSnappy
			if v.Compression == "NoCompression" {
				compression = TableCompressionNone
			}
			if v.Compression == "ZSTD level 3" {
				compression = TableCompressionZstd
			}
			options, err := tableWriterOptions(TableOptions{Compression: compression})
			if err != nil {
				t.Fatal(err)
			}
			entries := make([]Entry, 128)
			timelines := make([][]byte, len(entries))
			for i := range entries {
				prefix, text, fill, n := byte('E'), "event-value/", byte(0x41+i%4), 96
				if v.Generator == "heads" {
					prefix, text, fill, n = 'H', "head-value/", byte(0x61+i%4), 48
				}
				key := binary.BigEndian.AppendUint32([]byte{prefix}, uint32(i))
				value := binary.BigEndian.AppendUint32([]byte(text), uint32(i))
				value = append(value, bytes.Repeat([]byte{fill}, n)...)
				timelines[i] = []byte{byte(i)}
				entries[i] = Entry{Key: key, Value: value, Timeline: timelines[i], TimelineID: TimelineID(i), Seq: uint64(1000 + i)}
			}
			catalog, err := validateCatalog(context.Background(), &sliceTimelineCatalog{timelines: timelines}, 128, nil)
			if err != nil {
				t.Fatal(err)
			}
			source := observedEvents
			if v.Generator == "heads" {
				source = observedHeads
			}
			built, err := buildTableWithin(context.Background(), &sliceEntryIterator{entries: entries}, options, BuildOptions{SeqLo: 1000, SeqHi: 1127, ScratchDir: t.TempDir()}, v.Generator, MaxRunObjectBytes, catalog, source)
			if err != nil {
				t.Fatal(err)
			}
			defer built.cleanup()
			if built.length != v.Length || encodeHex(built.hash[:]) != v.Hash {
				t.Fatalf("pinned SST drift: length=%d hash=%x", built.length, built.hash)
			}
		})
	}
}

type corpusCase struct {
	Name        string
	Count       int
	Compression TableCompression
	Padding     bool
}

var corpusCases = []corpusCase{
	{"smallest", 1, TableCompressionSnappy, false},
	{"opaque-uncompressed", 3, TableCompressionNone, false},
	{"opaque-zstd", 3, TableCompressionZstd, false},
	{"multi-page", 3277, TableCompressionSnappy, false},
	{"padding-every-region", 3, TableCompressionSnappy, true},
}

type corpusRegion struct {
	Kind                    RegionKind
	Offset, Length, Entries uint64
	SHA256                  string
}
type corpusPage struct {
	Index          uint32
	Offset, Length int64
	SHA256, CRC32C string
}
type corpusQuery struct {
	TimelineHex string
	MayContain  bool
}
type corpusVector struct {
	Name, File, SHA256, PayloadSHA256, DirectorySHA256          string
	PublicationIDHex, PublicationSHA256, RunIDHex, NamespaceHex string
	ObjectSize, DirectoryOffset, DirectoryLength                uint64
	Regions                                                     []corpusRegion
	Pages                                                       []corpusPage
	Queries                                                     []corpusQuery
}

func corpusFixture(t testing.TB, c corpusCase) (BuildOptions, BuildInput, [][]byte) {
	t.Helper()
	opts, _, _, _ := benchmarkRunFixture(c.Count, 1)
	publicationID := []byte("e00-compat-v1/" + c.Name)
	preimage := binary.BigEndian.AppendUint32([]byte("unijord/run/publication/v1\x00"), uint32(len(publicationID)))
	opts.PublicationHash = sha256.Sum256(append(preimage, publicationID...))
	runIdentity := sha256.Sum256(append([]byte("e00-run-id/"), publicationID...))
	copy(opts.RunID[:], runIdentity[:16])
	opts.NamespaceHash[0], opts.NamespaceHash[1], opts.NamespaceHash[2] = 0, 0xff, 0x80
	opts.Table.Compression = c.Compression
	events, heads := make([]Entry, c.Count), make([]Entry, c.Count)
	timelines := make([][]byte, c.Count)
	for i := range events {
		timeline := []byte{0}
		if c.Count > 1 {
			timeline = []byte{byte(i >> 8), byte(i), 0xff, 0x80}
		}
		timelines[i] = timeline
		key := runcontract.Key{Kind: runcontract.Events, Namespace: opts.NamespaceHash, Shard: opts.Shard, Timeline: timeline, LSN: 1}
		ek, err := runcontract.EncodeKey(make([]byte, runcontract.MaxKeyBytes), key)
		if err != nil {
			t.Fatal(err)
		}
		key.Kind, key.LSN = runcontract.Heads, 0
		hk, err := runcontract.EncodeKey(make([]byte, runcontract.MaxKeyBytes), key)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := runcontract.EncodeEvent(make([]byte, runcontract.EventFixedBytes), runcontract.Event{Kind: runcontract.Append, Offset: uint64(i), LeaderEpoch: -1})
		if err != nil {
			t.Fatal(err)
		}
		hv, err := runcontract.EncodeHead(make([]byte, runcontract.HeadBytes), runcontract.Head{NextLSN: 2, LastOffset: uint64(i)})
		if err != nil {
			t.Fatal(err)
		}
		events[i] = Entry{Key: ek, Value: ev, Timeline: timeline, TimelineID: TimelineID(i), Seq: uint64(i + 1)}
		heads[i] = Entry{Key: hk, Value: hv, Timeline: timeline, TimelineID: TimelineID(i), Seq: uint64(i + 1)}
	}
	return opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineCatalog{timelines: timelines}}, timelines
}

func corpusExtractor(ref Ref, kind RegionKind, key, value []byte, seq uint64) ([]byte, error) {
	k, err := runcontract.DecodeKey(key)
	if err != nil {
		return nil, err
	}
	if uint16(k.Kind) != uint16(kind) || k.Namespace != ref.NamespaceHash || k.Shard != ref.Shard || seq < ref.SeqLo || seq > ref.SeqHi {
		return nil, fmt.Errorf("logical key/sequence mismatch")
	}
	if kind == RegionKindEventsSST {
		_, err = runcontract.DecodeEvent(value)
	} else {
		_, err = runcontract.DecodeHead(value)
	}
	return k.Timeline, err
}

func paddedCorpus(t testing.TB, object []byte, ref Ref) ([]byte, Ref) {
	t.Helper()
	out := bytes.Clone(object[:PreambleBytes])
	regions := []RegionDescriptor{ref.Events, ref.Heads, ref.TimelineFilter.Region}
	for i := range regions {
		r := &regions[i]
		out = append(out, make([]byte, (8-len(out)%8)%8+8)...)
		start := uint64(len(out))
		out = append(out, object[r.Offset:r.Offset+r.Length]...)
		r.Offset = start
	}
	out = append(out, make([]byte, (8-len(out)%8)%8+8)...)
	d := Directory{DirectoryOffset: uint64(len(out)), MinTimeline: ref.MinTimeline, MaxTimeline: ref.MaxTimeline, Regions: regions}
	db, err := MarshalDirectory(d)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, db...)
	tr := Trailer{DirectoryOffset: d.DirectoryOffset, DirectoryLength: uint64(len(db)), ObjectSize: uint64(len(out) + TrailerBytes), RegionCount: 3, DirectoryHash: sha256.Sum256(db), PayloadHash: sha256.Sum256(out), RunID: ref.RunID}
	tb, err := MarshalTrailer(tr)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, tb...)
	return out, refFromParts(ref.preamble(), d, tr, &ref.TimelineFilter.Header)
}

func TestCompatibilityCompleteRuns(t *testing.T) {
	update := os.Getenv("E00_UPDATE_CORPUS") == "1"
	var vectors []corpusVector
	for _, c := range corpusCases {
		t.Run(c.Name, func(t *testing.T) {
			opts, input, timelines := corpusFixture(t, c)
			opts.ScratchDir = t.TempDir()
			var dst bytes.Buffer
			ref, err := Build(context.Background(), &dst, opts, input)
			if err != nil {
				t.Fatal(err)
			}
			built := dst.Bytes()
			if c.Padding {
				built, ref = paddedCorpus(t, built, ref)
			}
			path := filepath.Join(compatDir, c.Name+".run")
			if update {
				if err := os.WriteFile(path, built, 0644); err != nil {
					t.Fatal(err)
				}
			}
			frozen, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(frozen, built) {
				t.Fatal("complete run byte drift")
			}
			source := &memoryRangeSource{data: frozen}
			recovered, err := Recover(context.Background(), source, c.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !sameRef(ref, recovered) {
				t.Fatal("reference drift")
			}
			if err := Verify(context.Background(), source, c.Name, ref, VerifyComplete, corpusExtractor); err != nil {
				t.Fatal(err)
			}
			// Exact decoded re-encoding of every outer structure, including padding.
			pre, err := UnmarshalPreamble(frozen[:PreambleBytes])
			if err != nil {
				t.Fatal(err)
			}
			pb, err := MarshalPreamble(pre)
			if err != nil || !bytes.Equal(pb, frozen[:PreambleBytes]) {
				t.Fatal("preamble drift", err)
			}
			dir, err := UnmarshalDirectory(frozen[ref.DirectoryOffset:ref.DirectoryOffset+ref.DirectoryLength], ref.DirectoryOffset)
			if err != nil {
				t.Fatal(err)
			}
			db, err := MarshalDirectory(dir)
			if err != nil || !bytes.Equal(db, frozen[ref.DirectoryOffset:ref.DirectoryOffset+ref.DirectoryLength]) {
				t.Fatal("directory drift", err)
			}
			tr, err := UnmarshalTrailer(frozen[len(frozen)-TrailerBytes:])
			if err != nil {
				t.Fatal(err)
			}
			tb, err := MarshalTrailer(tr)
			if err != nil || !bytes.Equal(tb, frozen[len(frozen)-TrailerBytes:]) {
				t.Fatal("trailer drift", err)
			}
			v := corpusVector{Name: c.Name, File: c.Name + ".run", SHA256: sha256Hex(frozen), PayloadSHA256: encodeHex(ref.PayloadHash[:]), DirectorySHA256: encodeHex(ref.DirectoryHash[:]), ObjectSize: ref.ObjectSize, DirectoryOffset: ref.DirectoryOffset, DirectoryLength: ref.DirectoryLength,
				PublicationIDHex: encodeHex([]byte("e00-compat-v1/" + c.Name)), PublicationSHA256: encodeHex(ref.PublicationHash[:]), RunIDHex: encodeHex(ref.RunID[:]), NamespaceHex: encodeHex(ref.NamespaceHash[:])}
			for _, r := range dir.Regions {
				v.Regions = append(v.Regions, corpusRegion{r.Kind, r.Offset, r.Length, r.EntryCount, encodeHex(r.ContentHash[:])})
			}
			fr := *ref.TimelineFilter
			for page := uint32(0); page < fr.Header.PageCount; page++ {
				rel, _ := filterPageRelativeOffset(page)
				n, _ := filterPageDataLength(fr.Header, page)
				off := fr.Region.Offset + rel
				b := frozen[off : off+n+4]
				v.Pages = append(v.Pages, corpusPage{page, int64(off), int64(n + 4), sha256Hex(b), encodeHex(b[len(b)-4:])})
			}
			queries := append(append([][]byte(nil), timelines...), []byte{0xff, 0xff, 0}, []byte{0}, []byte{0, 0})
			for i, q := range queries {
				req, err := PlanFilterPage(fr, q)
				if err != nil {
					t.Fatal(err)
				}
				yes, err := CheckFilterPage(fr, q, frozen[req.Offset:req.Offset+req.Length])
				if err != nil {
					t.Fatal(err)
				}
				if i < len(timelines) && !yes {
					t.Fatal("false negative")
				}
				// Store representative membership answers; check every inserted member.
				if i < 2 || i >= len(timelines)-1 {
					v.Queries = append(v.Queries, corpusQuery{encodeHex(q), yes})
				}
			}
			vectors = append(vectors, v)
		})
	}
	b, err := json.MarshalIndent(vectors, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join(compatDir, "complete-runs.json")
	if update {
		if err := os.WriteFile(path, b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, want) {
		t.Fatal("corpus metadata drift")
	}
}

func TestCompatibilityDuplicateKeysAndContainerLimits(t *testing.T) {
	opts, _, _ := corpusFixture(t, corpusCases[0])
	opts.CreatorRole = CreatorRoleCompactionOutput
	opts.SeqLo, opts.SeqHi = 1, maxPebbleSequence
	opts.ScratchDir = t.TempDir()
	// The generic run container permits retained versions. Foreground logical
	// generation will instead emit unique Events keys and one final Head.
	key := bytes.Repeat([]byte{1}, int(MaxTableKeyBytes))
	events := []Entry{{Key: key, Value: []byte{2}, Timeline: []byte{1}, Seq: maxPebbleSequence}, {Key: key, Value: []byte{1}, Timeline: []byte{1}, Seq: 1}}
	build := func(entries []Entry) error {
		_, err := Build(context.Background(), io.Discard, opts, BuildInput{&sliceEntryIterator{entries: entries}, &sliceEntryIterator{entries: events[:1]}, &sliceTimelineCatalog{timelines: [][]byte{{1}}}})
		return err
	}
	if err := build(events); err != nil {
		t.Fatal("maximum table key / descending duplicate keys", err)
	}
	for _, candidate := range [][]Entry{
		{events[1], events[0]}, {events[0], events[0]},
		{{Key: append(bytes.Clone(key), 1), Value: []byte{1}, Timeline: []byte{1}, Seq: 1}},
		{{Key: key, Value: []byte{1}, Timeline: []byte{1}, Seq: maxPebbleSequence + 1}},
	} {
		if err := build(candidate); err == nil {
			t.Fatal("invalid internal order or limit accepted")
		}
	}
}
