package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testRun(id uint64, level uint32) RunMeta {
	r := RunMeta{ObjectKey: "runs/" + string(rune('a'+id)), ObjectSize: 896, FormatVersion: 1, NamespaceHash: [32]byte{1}, Shard: 7, CreatorRole: 1, CreatorEpoch: 1, SeqLo: id, SeqHi: id, PublicationHash: [32]byte{2}, MinTimeline: []byte{0, 1}, MaxTimeline: []byte{0, 2}, DirectoryOffset: 280, DirectoryLength: 456, DirectoryHash: [32]byte{3}, PayloadHash: [32]byte{4}, CreatedAt: time.Unix(1700000000, 123).UTC(), Level: level}
	binary.BigEndian.PutUint64(r.ID[8:], id)
	r.Events = TableMeta{Kind: 1, Flags: 1, Encoding: 1, Offset: 128, Length: 1, EntryCount: 1, SeqLo: id, SeqHi: id, MinKey: []byte{1}, MaxKey: []byte{1}, ContentHash: [32]byte{5}}
	r.Heads = r.Events.Clone()
	r.Heads.Kind = 2
	r.Heads.Offset = 136
	r.Heads.ContentHash[0] = 6
	r.TimelineFilter = &TimelineFilterMeta{Region: TableMeta{Kind: 3, Encoding: 1, Offset: 144, Length: 132, EntryCount: 1, ContentHash: [32]byte{7}}, Algorithm: 1, Probes: 6, BitsPerKey: 10, KeyCount: 1, LineCount: 1, PageCount: 1, LineBytes: 64, LinesPerPage: 64, PageDataBytes: 4096}
	return r
}
func testRunManifest(runs ...RunMeta) *RunManifest {
	return &RunManifest{Version: RunManifestVersion, NamespaceHash: [32]byte{1}, Shard: 7, Revision: 1, NextSequence: MaxRunSequence + 1, WriterFence: &RunWriterFence{Epoch: 1, OwnerID: [16]byte{1}}, L0Runs: runs}
}
func fixRunDirectory(r *RunMeta) {
	r.DirectoryLength = int64(320 + len(r.MinTimeline) + len(r.MaxTimeline) + len(r.Events.MinKey) + len(r.Events.MaxKey) + len(r.Heads.MinKey) + len(r.Heads.MaxKey))
	if r.TimelineFilter != nil {
		r.DirectoryLength += 128
	}
	r.ObjectSize = r.DirectoryOffset + r.DirectoryLength + 160
}
func mustRunClone(t *testing.T, m *RunManifest) *RunManifest {
	t.Helper()
	c, err := m.Clone()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRunManifestValidAndObjectLimits(t *testing.T) {
	empty := testRunManifest()
	empty.NextSequence = 1
	empty.WriterFence = nil
	if err := empty.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int64{896, 1 << 40, 5 << 40} {
		r := testRun(1, 0)
		r.ObjectSize = size
		r.DirectoryOffset = size - r.DirectoryLength - 160
		m := testRunManifest(r)
		if err := m.Validate(); err != nil {
			t.Fatalf("%d: %v", size, err)
		}
		data, err := EncodeRunCheckpoint(m)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > 1024 {
			t.Fatal("object-sized metadata")
		}
		decoded, err := DecodeRunCheckpoint(data)
		if err != nil || !m.Equal(decoded) {
			t.Fatalf("roundtrip %v", err)
		}
	}
	r := testRun(1, 0)
	r.TimelineFilter = nil
	r.MinTimeline = []byte{0}
	r.MaxTimeline = []byte{0}
	r.DirectoryOffset = 144
	fixRunDirectory(&r)
	if r.ObjectSize != 630 || r.Validate() != nil {
		t.Fatalf("minimum geometry: %d %v", r.ObjectSize, r.Validate())
	}
	for _, seq := range []uint64{1, MaxRunSequence} {
		r := testRun(seq, 0)
		if err := testRunManifest(r).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int64{0, 1, 613, 5<<40 + 1, math.MaxInt64, -1} {
		r := testRun(1, 0)
		r.ObjectSize = size
		if r.Validate() == nil {
			t.Fatalf("accepted size %d", size)
		}
	}
}

func TestRunManifestInvalidFields(t *testing.T) {
	cases := map[string]func(*RunManifest){
		"version": func(m *RunManifest) { m.Version++ }, "namespace": func(m *RunManifest) { m.NamespaceHash = [32]byte{} },
		"revision": func(m *RunManifest) { m.Revision = 0 }, "next-zero": func(m *RunManifest) { m.NextSequence = 0 }, "next-overflow": func(m *RunManifest) { m.NextSequence = MaxRunSequence + 2 },
		"next-live": func(m *RunManifest) { m.NextSequence = 1 }, "fence-epoch": func(m *RunManifest) { m.WriterFence.Epoch = 0 }, "fence-owner": func(m *RunManifest) { m.WriterFence.OwnerID = [16]byte{} }, "fence-missing": func(m *RunManifest) { m.WriterFence = nil },
		"id": func(m *RunManifest) { m.L0Runs[0].ID = [16]byte{} }, "key-empty": func(m *RunManifest) { m.L0Runs[0].ObjectKey = "" }, "key-big": func(m *RunManifest) { m.L0Runs[0].ObjectKey = strings.Repeat("x", 4097) },
		"run-version": func(m *RunManifest) { m.L0Runs[0].FormatVersion = 2 }, "run-namespace": func(m *RunManifest) { m.L0Runs[0].NamespaceHash = [32]byte{} }, "namespace-binding": func(m *RunManifest) { m.L0Runs[0].NamespaceHash[1] = 2 }, "shard": func(m *RunManifest) { m.L0Runs[0].Shard++ },
		"role-zero": func(m *RunManifest) { m.L0Runs[0].CreatorRole = 0 }, "role-future": func(m *RunManifest) { m.L0Runs[0].CreatorRole = 3 }, "epoch": func(m *RunManifest) { m.L0Runs[0].CreatorEpoch = 0 }, "future-epoch": func(m *RunManifest) { m.L0Runs[0].CreatorEpoch++ },
		"seq-zero": func(m *RunManifest) { m.L0Runs[0].SeqLo = 0 }, "seq-reversed": func(m *RunManifest) { m.L0Runs[0].SeqLo = 2 }, "seq-overflow": func(m *RunManifest) { m.L0Runs[0].SeqHi = MaxRunSequence + 1 },
		"publication": func(m *RunManifest) { m.L0Runs[0].PublicationHash = [32]byte{} }, "directory-hash": func(m *RunManifest) { m.L0Runs[0].DirectoryHash = [32]byte{} }, "payload-hash": func(m *RunManifest) { m.L0Runs[0].PayloadHash = [32]byte{} },
		"time-zero": func(m *RunManifest) { m.L0Runs[0].CreatedAt = time.Time{} }, "time-range": func(m *RunManifest) { m.L0Runs[0].CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"timeline-empty": func(m *RunManifest) { m.L0Runs[0].MinTimeline = nil }, "timeline-big": func(m *RunManifest) { m.L0Runs[0].MaxTimeline = bytes.Repeat([]byte{255}, 513) }, "timeline-reversed": func(m *RunManifest) { m.L0Runs[0].MinTimeline = []byte{255} },
		"directory-negative": func(m *RunManifest) { m.L0Runs[0].DirectoryOffset = -8 }, "directory-overflow": func(m *RunManifest) { m.L0Runs[0].DirectoryOffset = math.MaxInt64 - 7 }, "directory-align": func(m *RunManifest) { m.L0Runs[0].DirectoryOffset++ }, "directory-length": func(m *RunManifest) { m.L0Runs[0].DirectoryLength++ }, "directory-too-big": func(m *RunManifest) { m.L0Runs[0].DirectoryLength = 2<<20 + 1 },
		"level-mismatch": func(m *RunManifest) { m.L0Runs[0].Level = 1 }, "level-big": func(m *RunManifest) { m.L0Runs[0].Level = 65 },
		"events-missing": func(m *RunManifest) { m.L0Runs[0].Events = TableMeta{} }, "heads-missing": func(m *RunManifest) { m.L0Runs[0].Heads = TableMeta{} },
		"table-kind": func(m *RunManifest) { m.L0Runs[0].Events.Kind = 2 }, "table-flags": func(m *RunManifest) { m.L0Runs[0].Events.Flags = 0 }, "table-encoding": func(m *RunManifest) { m.L0Runs[0].Events.Encoding = 2 },
		"table-offset-negative": func(m *RunManifest) { m.L0Runs[0].Events.Offset = -8 }, "table-length-negative": func(m *RunManifest) { m.L0Runs[0].Events.Length = -1 }, "table-length-zero": func(m *RunManifest) { m.L0Runs[0].Events.Length = 0 }, "table-overflow": func(m *RunManifest) { m.L0Runs[0].Events.Length = math.MaxInt64 }, "table-offset-overflow": func(m *RunManifest) { m.L0Runs[0].Events.Offset = math.MaxInt64 - 7 },
		"table-offset-alignment": func(m *RunManifest) { m.L0Runs[0].Events.Offset++ }, "table-preamble-overlap": func(m *RunManifest) { m.L0Runs[0].Events.Offset = 120 }, "table-table-overlap": func(m *RunManifest) { m.L0Runs[0].Events.Length = 9 }, "table-filter-overlap": func(m *RunManifest) { m.L0Runs[0].Heads.Length = 9 }, "table-directory-overlap": func(m *RunManifest) { m.L0Runs[0].Heads.Length = 150 },
		"table-count-zero": func(m *RunManifest) { m.L0Runs[0].Events.EntryCount = 0 }, "table-seq-out": func(m *RunManifest) { m.L0Runs[0].Heads.SeqHi++ }, "table-seq-reversed": func(m *RunManifest) { m.L0Runs[0].Heads.SeqLo++ }, "table-seq-zero": func(m *RunManifest) { m.L0Runs[0].Events.SeqLo = 0 }, "table-count-heads": func(m *RunManifest) { m.L0Runs[0].Heads.EntryCount++ }, "table-count-events": func(m *RunManifest) { m.L0Runs[0].Events.EntryCount++ },
		"table-hash": func(m *RunManifest) { m.L0Runs[0].Events.ContentHash = [32]byte{} }, "table-min-empty": func(m *RunManifest) { m.L0Runs[0].Events.MinKey = nil }, "table-max-big": func(m *RunManifest) { m.L0Runs[0].Events.MaxKey = make([]byte, 65528) }, "table-key-reversed": func(m *RunManifest) { m.L0Runs[0].Events.MinKey = []byte{2} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := testRunManifest(testRun(1, 0))
			mutate(m)
			if m.Validate() == nil {
				t.Fatal("accepted")
			}
			if _, err := EncodeRunCheckpoint(m); err == nil {
				t.Fatal("encoded")
			}
		})
	}
}

func TestRunManifestFilterGeometry(t *testing.T) {
	for _, count := range []uint64{1, 51, 52, 3276, 3277, 1<<32 - 1} {
		r := testRun(1, 0)
		r.CreatorRole = 2
		r.Events.EntryCount = count
		r.Heads.EntryCount = count
		f := r.TimelineFilter
		f.KeyCount = count
		f.Region.EntryCount = count
		f.LineCount = uint32((count*10 + 511) / 512)
		f.PageCount = (f.LineCount + 63) / 64
		f.Region.Length = 64 + int64(f.LineCount)*64 + int64(f.PageCount)*4
		r.DirectoryOffset = (f.Region.Offset + f.Region.Length + 7) &^ 7
		fixRunDirectory(&r)
		if err := r.Validate(); err != nil {
			t.Fatalf("count %d: %v", count, err)
		}
		// Every geometry scalar is checked at both immediate neighbours.
		for _, field := range []string{"Algorithm", "Probes", "BitsPerKey", "KeyCount", "LineCount", "PageCount", "LineBytes", "LinesPerPage", "PageDataBytes"} {
			for _, delta := range []int64{-1, 1} {
				c := r.Clone()
				v := reflect.ValueOf(c.TimelineFilter).Elem().FieldByName(field)
				v.SetUint(uint64(int64(v.Uint()) + delta))
				validNeighbour := field == "BitsPerKey" && delta == -1 && count <= 51
				if (c.Validate() == nil) != validNeighbour {
					t.Fatalf("accepted %s %+d", field, delta)
				}
			}
		}
		for _, delta := range []int64{-1, 1} {
			c := r.Clone()
			c.TimelineFilter.Region.Length += delta
			if c.Validate() == nil {
				t.Fatal("filter region length")
			}
		}
	}
	for name, mutate := range map[string]func(*TimelineFilterMeta){
		"kind": func(f *TimelineFilterMeta) { f.Region.Kind++ }, "flags": func(f *TimelineFilterMeta) { f.Region.Flags = 1 }, "encoding": func(f *TimelineFilterMeta) { f.Region.Encoding++ }, "sequence": func(f *TimelineFilterMeta) { f.Region.SeqLo = 1 }, "upper": func(f *TimelineFilterMeta) { f.Region.SeqHi = 1 }, "key": func(f *TimelineFilterMeta) { f.Region.MinKey = []byte{1} }, "maxkey": func(f *TimelineFilterMeta) { f.Region.MaxKey = []byte{1} }, "count": func(f *TimelineFilterMeta) { f.Region.EntryCount++ }, "hash": func(f *TimelineFilterMeta) { f.Region.ContentHash = [32]byte{} }, "overlap": func(f *TimelineFilterMeta) { f.Region.Offset = 136 }, "outside": func(f *TimelineFilterMeta) { f.Region.Offset = 280 }, "negative": func(f *TimelineFilterMeta) { f.Region.Length = -1 }, "multiply": func(f *TimelineFilterMeta) { f.KeyCount = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			r := testRun(1, 0)
			mutate(r.TimelineFilter)
			if r.Validate() == nil {
				t.Fatal("accepted")
			}
		})
	}
	// Largest legal line count, short final CRC page, and true multiplication
	// overflow reach the arithmetic after descriptor/count validation.
	r := testRun(1, 0)
	r.CreatorRole = 2
	n := uint64(math.MaxUint32) * 512
	f := r.TimelineFilter
	f.BitsPerKey = 1
	f.Probes = 1
	f.KeyCount = n
	f.Region.EntryCount = n
	r.Events.EntryCount = n
	r.Heads.EntryCount = n
	f.LineCount = math.MaxUint32
	f.PageCount = 67108864
	f.Region.Length = 275146342400
	r.DirectoryOffset = (f.Region.Offset + f.Region.Length + 7) &^ 7
	fixRunDirectory(&r)
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	f.KeyCount++
	f.Region.EntryCount++
	r.Events.EntryCount++
	r.Heads.EntryCount++
	if r.Validate() == nil {
		t.Fatal("line overflow")
	}
	f.KeyCount = math.MaxUint64
	f.Region.EntryCount = f.KeyCount
	r.Events.EntryCount = f.KeyCount
	r.Heads.EntryCount = f.KeyCount
	f.BitsPerKey = 32
	if r.Validate() == nil {
		t.Fatal("multiplication overflow")
	}
}

func TestRunManifestLevelsAndUniqueness(t *testing.T) {
	m := testRunManifest(testRun(1, 0), testRun(2, 0))
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, across := range []bool{false, true} {
		for _, key := range []bool{false, true} {
			a, b := testRun(1, 0), testRun(2, 0)
			if key {
				b.ObjectKey = a.ObjectKey
			} else {
				b.ID = a.ID
			}
			m := testRunManifest(a, b)
			if across {
				b.Level = 1
				m.L0Runs = m.L0Runs[:1]
				m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{b}}}
			}
			if m.Validate() == nil {
				t.Fatal("duplicate accepted")
			}
		}
	}
	for _, c := range []struct {
		name                   string
		amin, amax, bmin, bmax []byte
		valid                  bool
	}{
		{"binary", []byte{0}, []byte{0, 255}, []byte{1}, []byte{255}, true},
		{"prefix", []byte{0}, []byte{0}, []byte{0, 0}, []byte{0, 255}, true},
		{"touch", []byte{0}, []byte{1}, []byte{1}, []byte{2}, false},
		{"equal", []byte{1}, []byte{1}, []byte{1}, []byte{1}, false},
		{"reverse", []byte{2}, []byte{3}, []byte{0}, []byte{1}, false},
		{"overlap", []byte{0}, []byte{2}, []byte{1}, []byte{3}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, b := testRun(1, 1), testRun(2, 1)
			a.MinTimeline, a.MaxTimeline = c.amin, c.amax
			b.MinTimeline, b.MaxTimeline = c.bmin, c.bmax
			fixRunDirectory(&a)
			fixRunDirectory(&b)
			m := testRunManifest()
			m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{a, b}}}
			if (m.Validate() == nil) != c.valid {
				t.Fatal(m.Validate())
			}
		})
	}
	for _, levels := range [][]RunLevel{{{Number: 0}}, {{Number: 1}, {Number: 1}}, {{Number: 2}, {Number: 1}}, {{Number: 65}}, {{Number: 1, Runs: []RunMeta{testRun(1, 0)}}}} {
		m := testRunManifest()
		m.Levels = levels
		if m.Validate() == nil {
			t.Fatal("levels accepted")
		}
	}
}

func TestRunManifestCloneOwnership(t *testing.T) {
	r := testRun(1, 0)
	shared := []byte{0, 1}
	r.MinTimeline = shared
	r.MaxTimeline = shared
	fixRunDirectory(&r)
	m := testRunManifest(r)
	m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{testRun(2, 1)}}}
	c := mustRunClone(t, m)
	if !m.Equal(c) {
		t.Fatal("clone differs")
	}
	var check func(reflect.Value, reflect.Value)
	check = func(a, b reflect.Value) {
		if a.Type() == reflect.TypeOf(time.Time{}) {
			return
		}
		switch a.Kind() {
		case reflect.Pointer:
			if !a.IsNil() {
				if a.Pointer() == b.Pointer() {
					t.Fatal("pointer alias")
				}
				check(a.Elem(), b.Elem())
			}
		case reflect.Struct:
			for i := 0; i < a.NumField(); i++ {
				if a.Type().Field(i).IsExported() {
					check(a.Field(i), b.Field(i))
				}
			}
		case reflect.Slice:
			if a.Len() > 0 {
				if a.Pointer() == b.Pointer() {
					t.Fatal("slice alias")
				}
				if a.Type().Elem().Kind() != reflect.Uint8 {
					for i := 0; i < a.Len(); i++ {
						check(a.Index(i), b.Index(i))
					}
				}
			}
		}
	}
	check(reflect.ValueOf(m), reflect.ValueOf(c))
	c.L0Runs[0].MinTimeline[0] = 255
	if m.L0Runs[0].MinTimeline[0] != 0 || c.L0Runs[0].MaxTimeline[0] != 0 {
		t.Fatal("aliased bounds")
	}
	// Even noncanonical filter descriptor slices are independently cloned by
	// the scalar clone helpers; validation still rejects their persistence.
	f := testRun(1, 0).TimelineFilter
	f.Region.MinKey = shared
	f.Region.MaxKey = shared
	fc := f.Clone()
	fc.Region.MinKey[0] = 3
	if shared[0] != 0 || fc.Region.MaxKey[0] != 0 {
		t.Fatal("filter clone")
	}
	if m.IndexBytes() != 0 || c.IndexBytes() != 8 || !c.indexed {
		t.Fatal("clone indexes")
	}
}

// A reflection-driven field audit deliberately includes invalid mutations:
// equality must compare every persisted field, not just currently valid values.
func TestRunManifestEqualityEveryField(t *testing.T) {
	m := testRunManifest(testRun(1, 0))
	m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{testRun(2, 1)}}}
	var audit func(reflect.Value, string)
	c := mustRunClone(t, m)
	audit = func(v reflect.Value, path string) {
		if v.Type() == reflect.TypeOf(time.Time{}) {
			old := v.Interface().(time.Time)
			v.Set(reflect.ValueOf(old.Add(time.Nanosecond)))
			if m.Equal(c) {
				t.Fatal(path)
			}
			v.Set(reflect.ValueOf(old))
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					audit(v.Field(i), path+"."+v.Type().Field(i).Name)
				}
			}
		case reflect.Pointer:
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
				if m.Equal(c) {
					t.Fatal(path)
				}
				v.SetZero()
				return
			}
			old := reflect.New(v.Type().Elem())
			old.Elem().Set(v.Elem())
			v.SetZero()
			if m.Equal(c) {
				t.Fatal(path)
			}
			v.Set(old)
			audit(v.Elem(), path)
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				audit(v.Index(i), path)
			}
		case reflect.String:
			old := v.String()
			v.SetString(old + "x")
			if m.Equal(c) {
				t.Fatal(path)
			}
			v.SetString(old)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			old := v.Uint()
			v.SetUint(old ^ 1)
			if m.Equal(c) {
				t.Fatal(path)
			}
			v.SetUint(old)
		case reflect.Int64:
			old := v.Int()
			v.SetInt(old ^ 1)
			if m.Equal(c) {
				t.Fatal(path)
			}
			v.SetInt(old)
		default:
			t.Fatalf("unhandled persisted type %s %v", path, v.Kind())
		}
	}
	audit(reflect.ValueOf(c).Elem(), "manifest")
	c.l0Order = nil
	c.l0Max = nil
	c.indexed = false
	if !m.Equal(c) {
		t.Fatal("index affected equality")
	}
	c.L0Runs = append(c.L0Runs, testRun(3, 0))
	if m.Equal(c) {
		t.Fatal("collection length")
	}
}

func TestRunManifestIndexes(t *testing.T) {
	m := testRunManifest(testRun(2, 0), testRun(1, 0), testRun(3, 0))
	a, b := testRun(4, 1), testRun(5, 1)
	b.MinTimeline = []byte{1}
	b.MaxTimeline = []byte{255}
	fixRunDirectory(&b)
	m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{a, b}}}
	if _, _, err := m.L0Candidates([]byte{0, 1}, nil); err == nil {
		t.Fatal("unbuilt")
	}
	if err := m.BuildIndexes(); err != nil {
		t.Fatal(err)
	}
	for _, q := range [][]byte{{0, 1}, {0, 2}, {0}, {0, 3}, {255}} {
		got, _, err := m.L0Candidates(q, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if bytes.Compare(q, []byte{0, 1}) >= 0 && bytes.Compare(q, []byte{0, 2}) <= 0 {
			want = 3
		}
		if len(got) != want {
			t.Fatal("candidate count")
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].SeqHi <= got[i].SeqHi {
				t.Fatal("newest order")
			}
		}
	}
	for _, q := range [][]byte{{0, 1}, {0, 2}, {0, 3}, {1}, {255}, {255, 0}} {
		got, _, err := m.LevelCandidate(1, q)
		if err != nil {
			t.Fatal(err)
		}
		var want *RunMeta
		for i := range m.Levels[0].Runs {
			r := &m.Levels[0].Runs[i]
			if bytes.Compare(r.MinTimeline, q) <= 0 && bytes.Compare(q, r.MaxTimeline) <= 0 {
				want = r
			}
		}
		if got != want {
			t.Fatal("binary lookup")
		}
	}
	if got, _, err := m.LevelCandidate(2, []byte{1}); err != nil || got != nil {
		t.Fatal("missing level")
	}
	before, _ := EncodeRunCheckpoint(m)
	m.l0Order = nil
	m.l0Max = nil
	m.indexed = false
	after, _ := EncodeRunCheckpoint(m)
	if !bytes.Equal(before, after) {
		t.Fatal("persisted index")
	}
	d, err := DecodeRunCheckpoint(before)
	if err != nil || !d.indexed {
		t.Fatal(err)
	}
	got, _, _ := d.L0Candidates([]byte{0, 1}, nil)
	if len(got) != 3 || got[0] != &d.L0Runs[2] {
		t.Fatal("decode index")
	}
	before[60] ^= 1
	if d.NamespaceHash != m.NamespaceHash {
		t.Fatal("decoded input alias")
	}
}

func TestRunManifestCodecStrictness(t *testing.T) {
	m := testRunManifest(testRun(1, 0))
	data, err := EncodeRunCheckpoint(m)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < runEnvelopeBytes; i++ {
		b := bytes.Clone(data)
		b[i] ^= 1
		if _, err := DecodeRunCheckpoint(b); err == nil {
			t.Fatalf("header byte %d", i)
		}
	}
	for i := 0; i < len(data); i++ {
		if _, err := DecodeRunCheckpoint(data[:i]); err == nil {
			t.Fatalf("truncation %d", i)
		}
	}
	for _, b := range [][]byte{append(bytes.Clone(data), 0), runEnvelope(append(bytes.Clone(data[48:]), 0), runCheckpointKind), []byte(`{"L0SSTs":[],"L0Runs":[]}`), []byte(`{"version":1,"unknown":true}`)} {
		if _, err := DecodeRunCheckpoint(b); err == nil {
			t.Fatal("trailing/mixed/unknown")
		}
	}
	old, err := EncodeSnapshot(&Manifest{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRunCheckpoint(old); !errors.Is(err, ErrUnsupportedRunFormat) {
		t.Fatal("old snapshot", err)
	}
	old, err = EncodeCommitPage(&CommitPage{LayoutVersion: LayoutVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRunPage(old); !errors.Is(err, ErrUnsupportedRunFormat) {
		t.Fatal("old page", err)
	}
	// Rehash payload mutations so they reach allocation/field checks.
	for _, change := range []func([]byte){
		func(b []byte) { b[52] = 2 }, // fence presence
		func(b []byte) { binary.BigEndian.PutUint32(b[86:90], MaxManifestRuns+1) },
		func(b []byte) { binary.BigEndian.PutUint32(b[107:111], math.MaxUint32) }, // object-key length, after source-presence byte
	} {
		b := bytes.Clone(data[48:])
		change(b)
		if _, err := DecodeRunCheckpoint(runEnvelope(b, runCheckpointKind)); err == nil {
			t.Fatal("malformed field")
		}
	}
	for _, kind := range []byte{runPageKind, runLogKind, 255} {
		if _, err := DecodeRunCheckpoint(runEnvelope(data[48:], kind)); err == nil {
			t.Fatal("kind")
		}
	}
	empty := testRunManifest()
	empty.WriterFence = nil
	empty.NextSequence = 1
	b, _ := EncodeRunCheckpoint(empty)
	bad := bytes.Clone(b[48:])
	binary.BigEndian.PutUint32(bad[66:70], MaxRunLevels+1)
	if _, err := DecodeRunCheckpoint(runEnvelope(bad, 1)); err == nil {
		t.Fatal("level count")
	}
	for _, limit := range []uint64{0, MaxRunCheckpointBytes + 1, math.MaxUint64} {
		bad := bytes.Clone(b)
		binary.BigEndian.PutUint64(bad[8:16], limit)
		if _, err := DecodeRunCheckpoint(bad); err == nil {
			t.Fatal("raw size")
		}
	}
}

func TestRunManifestAdmissionBounds(t *testing.T) {
	m := testRunManifest()
	m.Levels = make([]RunLevel, MaxRunLevels+1)
	if !errors.Is(m.Validate(), ErrRunManifestLimit) {
		t.Fatal("levels")
	}
	m.Levels = nil
	m.L0Runs = make([]RunMeta, MaxManifestRuns+1)
	if !errors.Is(m.Validate(), ErrRunManifestLimit) {
		t.Fatal("runs")
	}
	// Shared in-memory oversized aggregate: rejection must precede encoder
	// allocation, without constructing half a GiB of input bytes.
	r := testRun(1, 0)
	key := bytes.Repeat([]byte{1}, 65527)
	r.Events.MinKey = key
	r.Events.MaxKey = key
	r.Heads.MinKey = key
	r.Heads.MaxKey = key
	fixRunDirectory(&r)
	m.L0Runs = make([]RunMeta, 2100)
	for i := range m.L0Runs {
		m.L0Runs[i] = r
	}
	if !errors.Is(m.Validate(), ErrRunManifestLimit) {
		t.Fatal("aggregate raw bytes", m.Validate())
	}
	if _, err := EncodeRunCheckpoint(m); !errors.Is(err, ErrRunManifestLimit) {
		t.Fatal(err)
	}
}

func TestRunManifestLogReplayAndPages(t *testing.T) {
	m := testRunManifest()
	m.NextSequence = 1
	e := RunLogEntry{Op: RunLogAdd, Revision: 2, NextSequence: 2, AddRuns: []RunMeta{testRun(1, 0)}}
	data, err := EncodeRunLogEntry(&e)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DecodeRunLogEntry(data)
	encodedAgain, encodeErr := EncodeRunLogEntry(d)
	if err != nil || encodeErr != nil || !bytes.Equal(data, encodedAgain) {
		t.Fatal("log roundtrip", err)
	}
	n, err := ApplyRunLogEntry(m, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.L0Runs) != 0 || n.Revision != 2 || !n.indexed {
		t.Fatal("replay state")
	}
	d.AddRuns[0].MinTimeline[0] = 255
	if n.L0Runs[0].MinTimeline[0] != 0 {
		t.Fatal("replay alias")
	}
	out := testRun(2, 1)
	out.CreatorRole = 2
	out.SeqLo = 1
	out.SeqHi = 1
	out.Events.SeqLo = 1
	out.Events.SeqHi = 1
	out.Heads.SeqLo = 1
	out.Heads.SeqHi = 1
	c := RunLogEntry{Op: RunLogCompaction, Revision: 3, NextSequence: 2, SourceLevel: 0, DestinationLevel: 1, RemoveRunIDs: [][16]byte{n.L0Runs[0].ID}, AddRuns: []RunMeta{out}}
	compacted, err := ApplyRunLogEntry(n, &c)
	if err != nil {
		t.Fatal(err)
	}
	if len(compacted.L0Runs) != 0 || len(compacted.Levels[0].Runs) != 1 {
		t.Fatal("compaction")
	}
	remove := RunLogEntry{Op: RunLogRemove, Revision: 4, NextSequence: 2, RemoveRunIDs: [][16]byte{out.ID}}
	removed, err := ApplyRunLogEntry(compacted, &remove)
	if err != nil || len(removed.Levels[0].Runs) != 0 {
		t.Fatal(err)
	}
	page := &RunPage{SeqLo: 2, SeqHi: 4, Count: 3, Entries: []RunLogEntry{e, c, remove}}
	b, err := EncodeRunPage(page)
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeRunPage(b)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := EncodeRunPage(p)
	if !bytes.Equal(b, again) {
		t.Fatal("page deterministic")
	}
	replayed, err := ReplayRunPage(m, p)
	if err != nil || !removed.Equal(replayed) {
		t.Fatal("page replay", err)
	}
	index := &RunPage{Level: 1, SeqLo: 2, SeqHi: 4, Count: 3, Children: []RunPageRef{{ObjectKey: "pages/a", EncodedBytes: uint64(len(b)), Hash: sha256.Sum256(b), SeqLo: 2, SeqHi: 4, Count: 3}}}
	b, err = EncodeRunPage(index)
	if err != nil {
		t.Fatal(err)
	}
	p, err = DecodeRunPage(b)
	if err != nil || !reflect.DeepEqual(index, p) {
		t.Fatal("index page", err)
	}
	if _, err := ReplayRunPage(m, index); err == nil {
		t.Fatal("replayed index")
	}
	// Moving an unchanged complete object preserves its creator and identity.
	move := c
	move.AddRuns = []RunMeta{n.L0Runs[0].Clone()}
	move.AddRuns[0].Level = 1
	if _, err := ApplyRunLogEntry(n, &move); err != nil {
		t.Fatal("trivial move", err)
	}
}

func TestRunManifestReplayRejectsAtomically(t *testing.T) {
	m := testRunManifest(testRun(1, 0))
	m.NextSequence = 2
	out := testRun(2, 1)
	out.CreatorRole = 2
	out.SeqLo = 1
	out.SeqHi = 1
	out.Events.SeqLo = 1
	out.Events.SeqHi = 1
	out.Heads.SeqLo = 1
	out.Heads.SeqHi = 1
	base := RunLogEntry{Op: RunLogCompaction, Revision: 2, NextSequence: 2, SourceLevel: 0, DestinationLevel: 1, RemoveRunIDs: [][16]byte{m.L0Runs[0].ID}, AddRuns: []RunMeta{out}}
	for name, mutate := range map[string]func(*RunLogEntry){
		"op": func(e *RunLogEntry) { e.Op = 4 }, "zero-revision": func(e *RunLogEntry) { e.Revision = 0 }, "skip-revision": func(e *RunLogEntry) { e.Revision = 3 }, "old-revision": func(e *RunLogEntry) { e.Revision = 1 }, "cursor": func(e *RunLogEntry) { e.NextSequence++ }, "adjacency": func(e *RunLogEntry) { e.DestinationLevel = 2 }, "source": func(e *RunLogEntry) { e.SourceLevel = 1; e.DestinationLevel = 2; e.AddRuns[0].Level = 2 }, "nonlive": func(e *RunLogEntry) { e.RemoveRunIDs[0][0] = 9 }, "empty-input": func(e *RunLogEntry) { e.RemoveRunIDs = nil }, "duplicate": func(e *RunLogEntry) { e.RemoveRunIDs = append(e.RemoveRunIDs, e.RemoveRunIDs[0]) }, "zero-id": func(e *RunLogEntry) { e.RemoveRunIDs[0] = [16]byte{} }, "empty-output": func(e *RunLogEntry) { e.AddRuns = nil }, "role": func(e *RunLogEntry) { e.AddRuns[0].CreatorRole = 1 }, "reused-id": func(e *RunLogEntry) { e.AddRuns[0].ID = m.L0Runs[0].ID }, "reused-key": func(e *RunLogEntry) { e.AddRuns[0].ObjectKey = m.L0Runs[0].ObjectKey }, "binding": func(e *RunLogEntry) { e.AddRuns[0].Shard++ },
	} {
		t.Run(name, func(t *testing.T) {
			before := mustRunClone(t, m)
			e := base
			e.RemoveRunIDs = append([][16]byte(nil), base.RemoveRunIDs...)
			e.AddRuns = cloneRuns(base.AddRuns)
			mutate(&e)
			if _, err := ApplyRunLogEntry(m, &e); err == nil {
				t.Fatal("accepted")
			}
			if !m.Equal(before) {
				t.Fatal("mutated input")
			}
		})
	}
	// Untouched destination overlap must reject the whole replay.
	m.Levels = []RunLevel{{Number: 1, Runs: []RunMeta{testRun(3, 1)}}}
	m.NextSequence = 4
	base.NextSequence = 4
	if _, err := ApplyRunLogEntry(m, &base); err == nil {
		t.Fatal("destination overlap")
	}
	m.Revision = math.MaxUint64
	base.Revision = 0
	if _, err := ApplyRunLogEntry(m, &base); err == nil {
		t.Fatal("revision overflow")
	}
}

func TestRunManifestPageBounds(t *testing.T) {
	e := RunLogEntry{Op: RunLogRemove, Revision: 1, NextSequence: 1, RemoveRunIDs: [][16]byte{{1}}}
	for name, mutate := range map[string]func(*RunPage){
		"level": func(p *RunPage) { p.Level = 17 }, "count-zero": func(p *RunPage) { p.Count = 0 }, "count-over": func(p *RunPage) { p.Count = MaxRunPageCount + 1 }, "seq-zero": func(p *RunPage) { p.SeqLo = 0 }, "seq-reverse": func(p *RunPage) { p.SeqHi = 0 }, "seq-overflow": func(p *RunPage) { p.SeqLo = math.MaxUint64; p.SeqHi = 1 }, "seq-gap": func(p *RunPage) { p.SeqHi = 2 }, "entries": func(p *RunPage) { p.Entries = make([]RunLogEntry, MaxRunPageEntries+1) }, "mixed": func(p *RunPage) { p.Children = []RunPageRef{{}} }, "revision": func(p *RunPage) { p.Entries[0].Revision = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			p := &RunPage{SeqLo: 1, SeqHi: 1, Count: 1, Entries: []RunLogEntry{e}}
			mutate(p)
			if p.Validate() == nil {
				t.Fatal("accepted")
			}
		})
	}
	ref := RunPageRef{ObjectKey: "p", EncodedBytes: 100, Hash: [32]byte{1}, SeqLo: 1, SeqHi: 1, Count: 1}
	for name, mutate := range map[string]func(*RunPageRef){"level": func(r *RunPageRef) { r.Level = 1 }, "key": func(r *RunPageRef) { r.ObjectKey = "" }, "size": func(r *RunPageRef) { r.EncodedBytes = MaxRunPageBytes + 49 }, "hash": func(r *RunPageRef) { r.Hash = [32]byte{} }, "count": func(r *RunPageRef) { r.Count++ }, "range": func(r *RunPageRef) { r.SeqLo = 2 }} {
		t.Run(name, func(t *testing.T) {
			p := &RunPage{Level: 1, SeqLo: 1, SeqHi: 1, Count: 1, Children: []RunPageRef{ref}}
			mutate(&p.Children[0])
			if p.Validate() == nil {
				t.Fatal("accepted")
			}
		})
	}
	p := &RunPage{SeqLo: 1, SeqHi: 1, Count: 1, Entries: []RunLogEntry{e}}
	b, _ := EncodeRunPage(p)
	for i := 0; i < 48; i++ {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if _, err := DecodeRunPage(bad); err == nil {
			t.Fatal("header", i)
		}
	}
	for _, off := range []int{0, 17, 21} {
		bad := bytes.Clone(b[48:])
		bad[off] = 255
		if _, err := DecodeRunPage(runEnvelope(bad, 2)); err == nil {
			t.Fatal("payload", off)
		}
	}
	if _, err := DecodeRunPage(runEnvelope(append(bytes.Clone(b[48:]), 0), 2)); err == nil {
		t.Fatal("page trailing")
	}
	// A maximum leaf and maximum aggregate count are legal independently.
	p.Entries = make([]RunLogEntry, MaxRunPageEntries)
	for i := range p.Entries {
		p.Entries[i] = e
		p.Entries[i].Revision = uint64(i + 1)
	}
	p.SeqHi = MaxRunPageEntries
	p.Count = MaxRunPageEntries
	if _, err := EncodeRunPage(p); err != nil {
		t.Fatal(err)
	}
	ref.Level = MaxRunPageLevel - 1
	ref.Count = MaxRunPageCount
	ref.SeqHi = MaxRunPageCount
	if err := (&RunPage{Level: MaxRunPageLevel, SeqLo: 1, SeqHi: MaxRunPageCount, Count: MaxRunPageCount, Children: []RunPageRef{ref}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRunManifestFrozenVectors(t *testing.T) {
	empty := testRunManifest()
	empty.WriterFence = nil
	empty.NextSequence = 1
	for name, m := range map[string]*RunManifest{"empty": empty, "one": testRunManifest(testRun(1, 0))} {
		b, err := EncodeRunCheckpoint(m)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(b)
		want := map[string]string{"empty": "a7a53a6826927cce32e0a442a2617469cefc668e11823f15a9611e39efbe7ede", "one": "17ebeb121008a1867ae5667bf2ecd08506085bdd185fcd79cf4e926dd39f6500"}[name]
		if hex.EncodeToString(hash[:]) != want {
			t.Errorf("%s bytes=%d sha256=%x", name, len(b), hash)
		}
		d, err := DecodeRunCheckpoint(b)
		if err != nil || !m.Equal(d) {
			t.Fatal(err)
		}
		again, _ := EncodeRunCheckpoint(d)
		if !bytes.Equal(b, again) {
			t.Fatal("determinism")
		}
	}
	e := &RunLogEntry{Op: RunLogAdd, Revision: 2, NextSequence: 2, AddRuns: []RunMeta{testRun(1, 0)}}
	log, _ := EncodeRunLogEntry(e)
	leaf, _ := EncodeRunPage(&RunPage{SeqLo: 2, SeqHi: 2, Count: 1, Entries: []RunLogEntry{*e}})
	index, _ := EncodeRunPage(&RunPage{Level: 1, SeqLo: 2, SeqHi: 2, Count: 1, Children: []RunPageRef{{ObjectKey: "page/one", EncodedBytes: uint64(len(leaf)), Hash: sha256.Sum256(leaf), SeqLo: 2, SeqHi: 2, Count: 1}}})
	for name, data := range map[string][]byte{"log": log, "leaf": leaf, "index": index} {
		hash := sha256.Sum256(data)
		want := map[string]string{"log": "85d372bb2ba420168405e5509bda3e9abc5b8bb9f16e93456ee214b242b1b94c", "leaf": "b21ececc9b6e920ac94765d0874fd1ebf2b27e1ba2dace489144ff41e21ae57d", "index": "a1c3a3a81ca135e231db41d5d3a2e314eea5816858e1fa112a144debd3025fd8"}[name]
		if hex.EncodeToString(hash[:]) != want {
			t.Errorf("%s bytes=%d sha256=%x", name, len(data), hash)
		}
	}
}

func TestRunManifestLookupScaleAndBoundaries(t *testing.T) {
	m := benchmarkRunManifest(1000, true)
	for i := range m.Levels {
		for j := range m.Levels[i].Runs {
			r := &m.Levels[i].Runs[j]
			got, stats, err := m.LevelCandidate(r.Level, r.MinTimeline)
			if err != nil || got != r || stats.Comparisons > 14 {
				t.Fatalf("binary search: %v %+v", err, stats)
			}
		}
	}
	l0 := testRunManifest()
	for i := 1; i <= 256; i++ {
		r := testRun(uint64(i), 0)
		r.MinTimeline = []byte{byte(i - 1)}
		r.MaxTimeline = []byte{byte(min(i+7, 255))}
		fixRunDirectory(&r)
		l0.L0Runs = append(l0.L0Runs, r)
	}
	if err := l0.BuildIndexes(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		q := []byte{byte(i)}
		got, _, err := l0.L0Candidates(q, nil)
		if err != nil {
			t.Fatal(err)
		}
		var want []*RunMeta
		for j := len(l0.L0Runs) - 1; j >= 0; j-- {
			r := &l0.L0Runs[j]
			if bytes.Compare(r.MinTimeline, q) <= 0 && bytes.Compare(r.MaxTimeline, q) >= 0 {
				want = append(want, r)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("interval search at %x", q)
		}
	}
	a, b := testRun(1, 0), testRun(2, 0)
	b.SeqLo = a.SeqLo
	b.SeqHi = a.SeqHi
	b.Events.SeqLo = 1
	b.Events.SeqHi = 1
	b.Heads.SeqLo = 1
	b.Heads.SeqHi = 1
	m = testRunManifest(b, a)
	if err := m.BuildIndexes(); err != nil {
		t.Fatal(err)
	}
	got, _, _ := m.L0Candidates([]byte{0, 1}, nil)
	if got[0].ID != a.ID {
		t.Fatal("sequence tie must use exact ID")
	}
	var nilManifest *RunManifest
	if nilManifest.BuildIndexes() == nil {
		t.Fatal("nil index")
	}
}

func TestRunManifestPageAggregateAndCoverage(t *testing.T) {
	r := testRun(1, 0)
	key := bytes.Repeat([]byte{1}, 32768)
	r.Events.MinKey = key
	r.Events.MaxKey = key
	r.Heads.MinKey = key
	r.Heads.MaxKey = key
	fixRunDirectory(&r)
	p := &RunPage{SeqLo: 1, SeqHi: 300, Count: 300, Entries: make([]RunLogEntry, 300)}
	for i := range p.Entries {
		p.Entries[i] = RunLogEntry{Op: RunLogAdd, Revision: uint64(i + 1), NextSequence: 2, AddRuns: []RunMeta{r}}
	}
	if _, err := EncodeRunPage(p); !errors.Is(err, ErrRunManifestLimit) {
		t.Fatal("aggregate page bytes", err)
	}
	e := RunLogEntry{Op: RunLogCompaction, Revision: 1, NextSequence: 2, SourceLevel: 0, DestinationLevel: 1, RemoveRunIDs: make([][16]byte, 1025), AddRuns: []RunMeta{r}}
	if !errors.Is(e.Validate(), ErrRunManifestLimit) {
		t.Fatal("operation count")
	}
	first := RunPageRef{ObjectKey: "a", EncodedBytes: 100, Hash: [32]byte{1}, SeqLo: 1, SeqHi: 1, Count: 1}
	second := first
	second.ObjectKey = "b"
	second.SeqLo = 2
	second.SeqHi = 2
	index := &RunPage{Level: 1, SeqLo: 1, SeqHi: 2, Count: 2, Children: []RunPageRef{first, second}}
	if err := index.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, start := range []uint64{1, 3} {
		index.Children[1].SeqLo = start
		index.Children[1].SeqHi = start
		if index.Validate() == nil {
			t.Fatal("index gap/overlap")
		}
	}
	index.Children[1] = second
	index.Children[1].ObjectKey = "a"
	if index.Validate() == nil {
		t.Fatal("duplicate child key")
	}
}

func TestRunManifestTimeCanonicalization(t *testing.T) {
	for _, instant := range []time.Time{
		time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(2026, 9, 19, 12, 0, 0, 1, time.FixedZone("offset", 14*3600)),
	} {
		m := testRunManifest(testRun(1, 0))
		m.L0Runs[0].CreatedAt = instant
		data, err := EncodeRunCheckpoint(m)
		if err != nil {
			t.Fatal(err)
		}
		d, err := DecodeRunCheckpoint(data)
		if err != nil || !m.Equal(d) {
			t.Fatal("instant roundtrip", err)
		}
	}
	for _, instant := range []time.Time{
		time.Date(1, 1, 1, 0, 0, 1, 0, time.FixedZone("east", 3600)),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("west", -3600)),
	} {
		m := testRunManifest(testRun(1, 0))
		m.L0Runs[0].CreatedAt = instant
		if _, err := EncodeRunCheckpoint(m); err == nil {
			t.Fatal("local-year admitted out-of-range UTC instant")
		}
	}
	// Wall-clock fields alone are persisted; a process-local monotonic reading
	// and location must not change the equality or encoded identity.
	a := testRun(1, 0)
	a.CreatedAt = time.Now()
	b := a.Clone()
	b.CreatedAt = time.Unix(a.CreatedAt.Unix(), int64(a.CreatedAt.Nanosecond())).In(time.FixedZone("offset", 3600))
	if !a.Equal(b) {
		t.Fatal("nonpersisted clock state affects equality")
	}
	x, _ := EncodeRunCheckpoint(testRunManifest(a))
	y, _ := EncodeRunCheckpoint(testRunManifest(b))
	if !bytes.Equal(x, y) {
		t.Fatal("nonpersisted clock state affects encoding")
	}
}
