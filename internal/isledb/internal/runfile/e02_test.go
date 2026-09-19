package runfile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
)

// Kept only for the historical test-only timeline-set comparison baseline.
func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}

type e02Catalog struct {
	count int
	get   func(TimelineID) []byte
}

func (c e02Catalog) Len() int                      { return c.count }
func (c e02Catalog) Timeline(id TimelineID) []byte { return c.get(id) }

func TestE02CatalogMasks(t *testing.T) {
	c := &sliceTimelineCatalog{timelines: [][]byte{{0, 255}, {1}}}
	for _, order := range [][3]uint8{
		{1, 2, 4}, {1, 4, 2}, {2, 1, 4}, {2, 4, 1}, {4, 1, 2}, {4, 2, 1},
	} {
		v, err := validateCatalog(context.Background(), c, 2, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, source := range order {
			for _, id := range []TimelineID{1, 0} {
				if err := v.observe(id, c.Timeline(id), source); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Repeated events share one identity and one state byte.
		if err := v.observe(0, c.Timeline(0), observedEvents); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(v.masks, []uint8{7, 7}) {
			t.Fatal(v.masks)
		}
		if err := v.complete(); err != nil {
			t.Fatal(err)
		}
		for _, source := range []uint8{observedHeads, observedFilter, 0, observedAll} {
			if err := v.observe(0, c.Timeline(0), source); !errors.Is(err, ErrInvalidRun) {
				t.Fatal(source, err)
			}
		}
	}
	for _, missing := range []uint8{observedEvents, observedHeads, observedFilter} {
		v, err := validateCatalog(context.Background(), c, 2, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, source := range []uint8{observedEvents, observedHeads, observedFilter} {
			for _, id := range []TimelineID{0, 1} {
				if id == 1 && source == missing {
					continue
				}
				if err := v.observe(id, c.Timeline(id), source); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := v.complete(); !errors.Is(err, ErrInvalidRun) {
			t.Fatal(missing, err)
		}
	}
}

func TestE02CatalogAdmissionAndExactIdentity(t *testing.T) {
	noLookup := func(TimelineID) []byte { t.Fatal("lookup before admission"); return nil }
	maximumID := ^uint32(0)
	for _, c := range []e02Catalog{{-1, noLookup}, {0, noLookup}, {3, noLookup}, {int(maximumID), noLookup}} {
		if _, err := validateCatalog(context.Background(), c, 2, nil); err == nil {
			t.Fatal("unbounded/invalid count accepted", c.count)
		}
	}
	if _, err := validateCatalog(context.Background(), e02Catalog{1, noLookup}, 0, nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatal(err)
	}
	if uint64(^uint(0)>>1) > uint64(^uint32(0)) {
		tooMany := uint64(^uint32(0)) + 1
		if _, err := validateCatalog(context.Background(), e02Catalog{int(tooMany), noLookup}, ^uint32(0), nil); !errors.Is(err, ErrRunTooLarge) {
			t.Fatal("cardinality wraps TimelineID before admission", err)
		}
	}
	for _, timelines := range [][][]byte{
		{{1}, nil, {2}}, // sparse ID space
		{{1}, {2}, {1}}, // duplicate exact identity, nonadjacent IDs
		{bytes.Repeat([]byte{1}, int(MaxTimelineBytes+1))},
	} {
		if _, err := validateCatalog(context.Background(), &sliceTimelineCatalog{timelines: timelines}, 3, nil); err == nil {
			t.Fatal("invalid catalog accepted")
		}
	}
	c := &sliceTimelineCatalog{timelines: [][]byte{{255}, {0}, bytes.Repeat([]byte{128}, int(MaxTimelineBytes))}}
	v, err := validateCatalog(context.Background(), c, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(v.order, []TimelineID{1, 2, 0}) {
		t.Fatal(v.order)
	}
	for _, id := range []TimelineID{3, ^TimelineID(0)} {
		if err := v.observe(id, []byte{0}, observedEvents); !errors.Is(err, ErrInvalidRun) {
			t.Fatal(err)
		}
	}
	// A deliberately broken hash-based upstream lookup routes both exact
	// identities to ID 0. Equal hashes never establish membership here.
	hash := func([]byte) uint64 { return 7 }
	if hash(c.Timeline(0)) != hash(c.Timeline(1)) {
		t.Fatal("not a collision")
	}
	if err := v.observe(0, c.Timeline(1), observedEvents); !errors.Is(err, ErrInvalidRun) {
		t.Fatal("collision merged identities", err)
	}
	if v.masks[0] != 0 {
		t.Fatal("invalid observation changed mask")
	}
	// The largest admitted ID is valid; the uint32 maximum above is rejected
	// without a lookup or an allocation based on the supplied entry ID.
	if err := v.observe(2, bytes.Clone(c.Timeline(2)), observedEvents); err != nil {
		t.Fatal(err)
	}
}

func TestE02PrepareValidationCleanup(t *testing.T) {
	for _, mode := range []string{"nil-catalog", "limit", "missing-event", "missing-head", "extra-catalog", "duplicate-head", "wrong-id", "max-id", "wrong-bytes", "duplicate-catalog", "sparse-catalog"} {
		t.Run(mode, func(t *testing.T) {
			opts, input := validBuildFixture(TableCompressionNone)
			opts.ScratchDir = t.TempDir()
			e := &preparedClosingEntries{sliceEntryIterator: input.Events.(*sliceEntryIterator)}
			h := &preparedClosingEntries{sliceEntryIterator: input.Heads.(*sliceEntryIterator)}
			c := input.Timelines.(*sliceTimelineCatalog)
			input.Events, input.Heads = e, h
			switch mode {
			case "nil-catalog":
				input.Timelines = nil
			case "limit":
				opts.MaxTimelines = 1
			case "missing-event":
				e.entries = e.entries[:2]
			case "missing-head":
				h.entries = h.entries[:1]
			case "extra-catalog":
				c.timelines = append(c.timelines, []byte("unused"))
				opts.MaxTimelines = 3
			case "duplicate-head":
				dup := h.entries[0]
				dup.Key = []byte("timeline-a|head-3")
				h.entries = []Entry{h.entries[0], dup, h.entries[1]}
			case "wrong-id":
				e.entries[0].TimelineID = 1
			case "max-id":
				e.entries[0].TimelineID = ^TimelineID(0)
			case "wrong-bytes":
				h.entries[0].Timeline = []byte("wrong")
			case "duplicate-catalog":
				c.timelines[1] = bytes.Clone(c.timelines[0])
			case "sparse-catalog":
				c.timelines[0] = nil
			}
			p, err := Prepare(context.Background(), opts, input)
			if err == nil || p != nil {
				t.Fatalf("prepared=%v err=%v", p, err)
			}
			if e.closes != 1 || h.closes != 1 {
				t.Fatal("iterator cleanup", e.closes, h.closes)
			}
			requireEmptyScratch(t, opts.ScratchDir)
		})
	}
}

func TestE02CatalogOrderDoesNotChangeRun(t *testing.T) {
	for _, c := range corpusCases {
		t.Run(c.Name, func(t *testing.T) {
			opts, input, timelines := corpusFixture(t, c)
			opts.ScratchDir = t.TempDir()
			var before, after bytes.Buffer
			if _, err := Build(context.Background(), &before, opts, input); err != nil {
				t.Fatal(err)
			}
			_, input, timelines = corpusFixture(t, c)
			slices.Reverse(timelines)
			for _, it := range []EntryIterator{input.Events, input.Heads} {
				for i := range it.(*sliceEntryIterator).entries {
					it.(*sliceEntryIterator).entries[i].TimelineID = TimelineID(len(timelines) - 1 - i)
				}
			}
			if _, err := Build(context.Background(), &after, opts, input); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before.Bytes(), after.Bytes()) {
				t.Fatal("catalog numbering changed run bytes")
			}
			requireEmptyScratch(t, opts.ScratchDir)
		})
	}
}

// Reuses both key and value buffers, including when Next returns false and
// Close runs. This catches retaining the final iterator key as max metadata.
type e02ReusingEntries struct {
	entries    []Entry
	index      int
	key, value []byte
	current    Entry
	closes     int
}

func (it *e02ReusingEntries) Next() bool {
	clear(it.key)
	clear(it.value)
	if it.index == len(it.entries) {
		return false
	}
	e := it.entries[it.index]
	it.index++
	it.key = append(it.key[:0], e.Key...)
	it.value = append(it.value[:0], e.Value...)
	e.Key, e.Value = it.key, it.value
	it.current = e
	return true
}
func (it *e02ReusingEntries) Entry() Entry { return it.current }
func (it *e02ReusingEntries) Err() error   { return nil }
func (it *e02ReusingEntries) Close() error { clear(it.key); clear(it.value); it.closes++; return nil }

type e02CopyCounts struct {
	payload, timeline, key            uint64
	entries, filters, metadata, grows uint64
	maxKeyCapacity                    int
}

func sameStorage(a, b []byte) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}
func (s *e02CopyCounts) instrument(c TimelineCatalog) *buildInstrumentation {
	return &buildInstrumentation{
		entry: func(entry Entry, key, value, timeline []byte) {
			s.entries++
			if !sameStorage(entry.Key, key) {
				s.key++
			}
			if !sameStorage(entry.Value, value) {
				s.payload++
			}
			if !sameStorage(c.Timeline(entry.TimelineID), timeline) {
				s.timeline++
			}
		},
		filter: func(id TimelineID, timeline []byte) {
			s.filters++
			if !sameStorage(c.Timeline(id), timeline) {
				s.timeline++
			}
		},
		keyBuffer:   func(_, capacity int) { s.grows++; s.maxKeyCapacity = max(s.maxKeyCapacity, capacity) },
		metadataKey: func() { s.metadata++ },
	}
}
func (s *e02CopyCounts) check(t testing.TB, entries, timelines int) {
	t.Helper()
	if s.payload != 0 || s.timeline != 0 || s.key != 0 || s.entries != uint64(entries) || s.filters != uint64(timelines) || s.metadata != 4 || s.maxKeyCapacity > int(MaxTableKeyBytes) {
		t.Fatalf("copy/ownership instrumentation: %+v", s)
	}
}

func TestE02CopyInstrumentationDetectsCopies(t *testing.T) {
	c := &sliceTimelineCatalog{timelines: [][]byte{{1}}}
	s := &e02CopyCounts{}
	a := s.instrument(c)
	e := Entry{Key: []byte{2}, Value: []byte{3}, Timeline: c.Timeline(0)}
	a.entry(e, bytes.Clone(e.Key), bytes.Clone(e.Value), bytes.Clone(e.Timeline))
	a.filter(0, bytes.Clone(e.Timeline))
	if s.payload != 1 || s.key != 1 || s.timeline != 2 {
		t.Fatal("copy sensors failed", s)
	}
}

func TestE02ReusableKeysAndOwnedBounds(t *testing.T) {
	for _, compression := range []TableCompression{TableCompressionNone, TableCompressionSnappy, TableCompressionZstd} {
		t.Run(compressionName(compression), func(t *testing.T) {
			opts, _ := validBuildFixture(compression)
			opts.MaxTimelines = 2
			opts.SeqLo, opts.SeqHi = 1, 4
			opts.ScratchDir = t.TempDir()
			timelines := [][]byte{{0, 255, 128}, bytes.Repeat([]byte{255}, int(MaxTimelineBytes))}
			c := &sliceTimelineCatalog{timelines: timelines}
			// Grow and shrink the scratch key across a maximum-sized key. Equal
			// user keys remain legal for Events only in descending sequence order.
			events := []Entry{
				{Key: []byte{1}, Value: []byte{3}, Timeline: timelines[0], Seq: 4},
				{Key: bytes.Repeat([]byte{2}, int(MaxTableKeyBytes)), Value: []byte{4}, Timeline: timelines[0], Seq: 3},
				{Key: []byte{3}, Value: []byte{5}, Timeline: timelines[1], TimelineID: 1, Seq: 2},
				{Key: []byte{3}, Value: []byte{6}, Timeline: timelines[1], TimelineID: 1, Seq: 1},
			}
			heads := []Entry{events[0], events[3]}
			var expected bytes.Buffer
			want, err := Build(context.Background(), &expected, opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, c})
			if err != nil {
				t.Fatal(err)
			}
			e, h := &e02ReusingEntries{entries: events}, &e02ReusingEntries{entries: heads}
			counts := &e02CopyCounts{}
			p, err := prepare(context.Background(), opts, BuildInput{e, h, c}, counts.instrument(c))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			counts.check(t, 6, 2)
			if counts.grows != 3 {
				t.Fatal("previous-key storage grew per entry", counts.grows)
			}
			if e.closes != 1 || h.closes != 1 {
				t.Fatal("iterators not closed")
			}
			// Ownership can be released immediately on return, before any write.
			for _, list := range [][]Entry{events, heads} {
				for _, entry := range list {
					clear(entry.Key)
					clear(entry.Value)
				}
			}
			for _, timeline := range timelines {
				clear(timeline)
			}
			for range 3 {
				var actual bytes.Buffer
				if err := p.WriteTo(context.Background(), &actual); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(expected.Bytes(), actual.Bytes()) || !sameRef(want, p.Ref()) {
					t.Fatal("borrowed data retained past Prepare")
				}
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			requireEmptyScratch(t, opts.ScratchDir)
		})
	}
}

func TestE02ReusableKeysRejectInvalidOrder(t *testing.T) {
	for _, mode := range []string{"unsorted", "equal-sequence", "ascending-sequence"} {
		t.Run(mode, func(t *testing.T) {
			opts, input := validBuildFixture(TableCompressionNone)
			opts.ScratchDir = t.TempDir()
			entries := input.Events.(*sliceEntryIterator).entries
			switch mode {
			case "unsorted":
				entries[0], entries[1] = entries[1], entries[0]
			case "equal-sequence":
				entries[1] = entries[0]
			case "ascending-sequence":
				entries[1].Key = entries[0].Key
			}
			it := &e02ReusingEntries{entries: entries}
			input.Events = it
			if p, err := Prepare(context.Background(), opts, input); !errors.Is(err, ErrInvalidRun) || p != nil {
				t.Fatal(p, err)
			}
			if it.closes != 1 {
				t.Fatal("not closed")
			}
			requireEmptyScratch(t, opts.ScratchDir)
		})
	}
}

func TestE02CatalogMutationViolation(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionNone)
	opts.ScratchDir = t.TempDir()
	c := input.Timelines.(*sliceTimelineCatalog)
	// Mutation at a deterministic iterator boundary violates the stable mapping;
	// entries still carry the original exact bytes, so validation must fail.
	input.Events = &preparedClosingEntries{sliceEntryIterator: input.Events.(*sliceEntryIterator), onNext: func() { c.timelines[0][0] ^= 1 }}
	if p, err := Prepare(context.Background(), opts, input); !errors.Is(err, ErrInvalidRun) || p != nil {
		t.Fatal(p, err)
	}
	requireEmptyScratch(t, opts.ScratchDir)
}

func TestE02CatalogLifetimeAfterPrepare(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionNone)
	opts.ScratchDir = t.TempDir()
	p, err := Prepare(context.Background(), opts, input)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	want := p.Ref()
	var baseline bytes.Buffer
	if err := p.WriteTo(context.Background(), &baseline); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 100 {
			for _, timeline := range input.Timelines.(*sliceTimelineCatalog).timelines {
				for j := range timeline {
					timeline[j]++
				}
			}
			for _, it := range []EntryIterator{input.Events, input.Heads} {
				for _, entry := range it.(*sliceEntryIterator).entries {
					clear(entry.Key)
					clear(entry.Value)
					clear(entry.Timeline)
				}
			}
		}
	}()
	close(start)
	for range 3 {
		var out bytes.Buffer
		if err := p.WriteTo(context.Background(), &out); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), baseline.Bytes()) || !sameRef(want, p.Ref()) {
			t.Fatal("input retained")
		}
	}
	wg.Wait()
}

// Opt-in negative race probe, run in its own go test -race process. Its expected
// race failure is evidence that mutating borrowed catalog bytes during Prepare
// violates the contract. It is never enabled in the normal passing suite.
func TestE02CatalogRaceViolationProbe(t *testing.T) {
	if os.Getenv("RUNFILE_E02_RACE_PROBE") != "1" {
		t.Skip("opt-in expected race failure")
	}
	opts, input := validBuildFixture(TableCompressionNone)
	opts.ScratchDir = t.TempDir()
	start, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	input.Events = &preparedClosingEntries{sliceEntryIterator: input.Events.(*sliceEntryIterator), onNext: func() { once.Do(func() { close(start) }) }}
	go func() { defer close(done); <-start; input.Timelines.(*sliceTimelineCatalog).timelines[0][0] ^= 1 }()
	p, _ := Prepare(context.Background(), opts, input)
	if p != nil {
		_ = p.Close()
	}
	<-done
	requireEmptyScratch(t, opts.ScratchDir)
}
