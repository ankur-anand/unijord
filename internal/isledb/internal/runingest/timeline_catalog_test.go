package runingest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

func testCatalog(t testing.TB, limit uint32, hard uint64) *timelineCatalog {
	t.Helper()
	c, err := newTimelineCatalog(timelineCatalogConfig{hard, limit, 1024, 512})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func internTimeline(t testing.TB, v catalogView, src []byte) timelineID {
	t.Helper()
	id, err := v.Intern(src)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func checkTimeline(t testing.TB, v catalogView, id timelineID, want []byte) {
	t.Helper()
	value, err := v.Timeline(id)
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := value.Equal(want); err != nil || !equal {
		t.Fatalf("id %d: equal=%t, %v", id, equal, err)
	}
	if n, err := value.Len(); err != nil || n != len(want) {
		t.Fatal(n, err)
	}
	var out [runcontract.MaxTimelineBytes]byte
	n, err := value.CopyTo(out[:])
	if err != nil || !bytes.Equal(out[:n], want) {
		t.Fatal(n, err)
	}
	out[0] ^= 0xff
	if equal, err := value.Equal(want); err != nil || !equal {
		t.Fatal("mutable alias", err)
	}
}

func checkCatalogAccounting(t testing.TB, c *timelineCatalog) {
	t.Helper()
	checkArenaAccounting(t, c.arena)
	s := c.Stats()
	if n, err := c.View().Len(); err != nil || uint64(n) != s.DistinctTimelines {
		t.Fatal(n, err, s.DistinctTimelines)
	}
	var copied, arenaCapacity uint64
	for _, m := range c.metadata {
		copied += m.ref.length
	}
	for _, b := range c.arena.normal {
		arenaCapacity += uint64(cap(b.data))
	}
	for _, b := range c.arena.large {
		arenaCapacity += uint64(cap(b.data))
	}
	metadata := uint64(cap(c.metadata)) * uint64(reflect.TypeFor[timelineMetadata]().Size())
	table := uint64(cap(c.slots)) * uint64(reflect.TypeFor[timelineHashSlot]().Size())
	projection := uint64(cap(c.order)) * 4
	descriptors := uint64(cap(c.arena.normal)+cap(c.arena.large)) * arenaBlockBytes
	if s.DistinctTimelines != uint64(len(c.metadata)) || s.CopiedTimelineBytes != copied ||
		s.CopiedTimelineBytes != c.arena.stats.CopyBytes || c.arena.stats.CopyCount != uint64(len(c.metadata)) ||
		s.ArenaReservedBytes != arenaCapacity || s.ArenaMetadataBytes != descriptors ||
		s.StateIDMetadataBytes != metadata || s.TableReservedBytes != table || s.ProjectionBytes != projection ||
		s.FixedBytes != timelineFixedBytes || s.ChargedBytes != arenaCapacity+descriptors+metadata+table+projection+timelineFixedBytes ||
		s.HighWater < s.ChargedBytes || s.HighWater > c.config.HardLimit {
		t.Fatalf("catalog accounting: %+v, copied=%d backing=%d/%d/%d/%d/%d", s, copied, arenaCapacity, descriptors, metadata, table, projection)
	}
	if len(c.slots) != 0 && (len(c.slots)&(len(c.slots)-1) != 0 || len(c.metadata) > len(c.slots)-len(c.slots)/4) {
		t.Fatal("table invariant")
	}
}

func TestTimelineCatalogIdentityAndSingleCopy(t *testing.T) {
	c := testCatalog(t, 10000, 8<<20)
	v := c.View()
	var copies, copyBytes uint64
	c.arena.hooks.copy = func(dst, src []byte) (int, error) {
		copies++
		copyBytes += uint64(len(src))
		return copy(dst, src), nil
	}
	src := []byte{0, 0xff, 0, 7}
	want := bytes.Clone(src)
	id := internTimeline(t, v, src)
	if id != 0 {
		t.Fatal(id)
	}
	src[0] = 99
	checkTimeline(t, v, id, want)
	for range 100 {
		if got := internTimeline(t, v, want); got != id {
			t.Fatal(got)
		}
	}
	if copies != 1 || copyBytes != uint64(len(want)) {
		t.Fatal(copies, copyBytes)
	}
	ref := c.metadata[id].ref
	ptr := &c.bytes(id)[0]
	for i := 1; i < 10000; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if got := internTimeline(t, v, key[:]); got != timelineID(i) {
			t.Fatal(got, i)
		}
		if i&(i-1) == 0 {
			checkCatalogAccounting(t, c)
		}
	}
	if c.metadata[id].ref != ref || &c.bytes(id)[0] != ptr {
		t.Fatal("timeline moved during metadata growth")
	}
	if got := internTimeline(t, v, want); got != id {
		t.Fatal(got)
	}
	checkTimeline(t, v, id, want)
	if s := c.Stats(); s.TableGrowths < 2 || s.Insertions != 10101 || s.CopiedTimelineBytes != 4+9999*8 {
		t.Fatal(s)
	}
	if copies != 10000 || copyBytes != 4+9999*8 {
		t.Fatal(copies, copyBytes)
	}
	checkCatalogAccounting(t, c)
}

func TestTimelineCatalogRawLengthContract(t *testing.T) {
	c := testCatalog(t, 100, 1<<20)
	v := c.View()
	for _, src := range [][]byte{nil, {}, make([]byte, 513)} {
		before := c.Stats()
		if id, err := v.Intern(src); id != 0 || !errors.Is(err, errTimelineEncoding) {
			t.Fatal(id, err)
		}
		if _, found, err := v.Lookup(src); found || !errors.Is(err, errTimelineEncoding) {
			t.Fatal(found, err)
		}
		if c.Stats() != before {
			t.Fatal("invalid input published")
		}
	}
	// Every nonempty <=512-byte opaque value is legal; malformed key escapes
	// and invalid UTF-8 are NOT malformed raw timeline identities.
	for _, src := range [][]byte{{0}, {0xff}, {0, 0}, {0, 0xff}, {0xff, 0xfe, 0x80}, {0, 1, 0}, bytes.Repeat([]byte{0xff}, 512)} {
		id := internTimeline(t, v, src)
		checkTimeline(t, v, id, src)
	}
	checkCatalogAccounting(t, c)
}

func TestTimelineCatalogCollisionsWraparoundAndFullProbe(t *testing.T) {
	c := testCatalog(t, 1000, 1<<20)
	c.hooks.hash = func([]byte) uint64 { return 7 }
	v := c.View()
	for i := 0; i < 500; i++ {
		var src [2]byte
		binary.BigEndian.PutUint16(src[:], uint16(i))
		if id := internTimeline(t, v, src[:]); id != timelineID(i) {
			t.Fatal(id)
		}
		if i == 1 && (!c.slots[7].occupied || !c.slots[0].occupied) {
			t.Fatal("no wraparound")
		}
	}
	for i := 0; i < 500; i++ {
		var src [2]byte
		binary.BigEndian.PutUint16(src[:], uint16(i))
		if id, ok, err := v.Lookup(src[:]); err != nil || !ok || id != timelineID(i) {
			t.Fatal(id, ok, err)
		}
		if id := internTimeline(t, v, src[:]); id != timelineID(i) {
			t.Fatal(id)
		}
	}
	if _, ok, err := v.Lookup([]byte{0xff, 0xff}); err != nil || ok {
		t.Fatal(ok, err)
	}
	if s := c.Stats(); s.MaxProbe != 501 || s.RehashProbes == 0 {
		t.Fatal(s)
	}
	checkCatalogAccounting(t, c)
	// Production grows below full capacity, but the primitive must terminate
	// even on a completely full collision table. No giant allocations needed.
	full := make([]timelineHashSlot, 8)
	for i := range full {
		full[i] = timelineHashSlot{7, timelineID(i), true}
	}
	if index, found, n := c.probe(full, 7, []byte{0xff, 0xff}); index != -1 || found || n != 8 {
		t.Fatal(index, found, n)
	}
	if _, found, n := c.probe(full, 7, []byte{0, 7}); !found || n > 8 {
		t.Fatal(found, n)
	}
}

func TestTimelineCatalogCountAndArithmeticBoundaries(t *testing.T) {
	c := testCatalog(t, 3, 1<<20)
	v := c.View()
	for i := 0; i < 3; i++ {
		if id := internTimeline(t, v, []byte{byte(i)}); id != timelineID(i) {
			t.Fatal(id)
		}
	}
	before := c.Stats()
	if id, err := v.Intern([]byte{3}); id != 0 || !errors.Is(err, errCatalogLimit) {
		t.Fatal(id, err)
	}
	if c.Stats() != before {
		t.Fatal("count failure mutated catalog")
	}
	if id := internTimeline(t, v, []byte{1}); id != 1 {
		t.Fatal(id)
	}
	if id, err := catalogNextID(math.MaxUint32-1, math.MaxUint32); err != nil || id != math.MaxUint32-1 {
		t.Fatal(id, err)
	}
	for _, n := range []uint64{math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
		if _, err := catalogNextID(n, math.MaxUint32); err == nil {
			t.Fatal(n)
		}
	}
	for _, args := range [][4]uint64{{math.MaxUint64, 0, math.MaxUint64, 16}, {1 << 33, 0, 1 << 33, math.MaxUint64}, {11, 0, 10, 4}} {
		if _, _, err := catalogCapacity(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatal(args)
		}
	}
	for _, cfg := range []timelineCatalogConfig{{1, 1, 64, 64}, {1 << 20, 0, 64, 64}, {1 << 20, 1, 0, 64}, {1 << 20, 1, 64, 65}, {timelineFixedBytes, 1, 64, 64}} {
		if c, err := newTimelineCatalog(cfg); err == nil || c != nil {
			t.Fatal(cfg, err)
		}
	}
	if catalogSaturatingAdd(math.MaxUint64, 1) != math.MaxUint64 {
		t.Fatal("metric wrapped")
	}
	checkCatalogAccounting(t, c)
}

func TestTimelineCatalogHardBoundBeforeGrowth(t *testing.T) {
	for _, shape := range []string{"initial", "table", "metadata", "arena", "combined"} {
		t.Run(shape, func(t *testing.T) {
			c := testCatalog(t, 100, 1<<20)
			v := c.View()
			src := []byte{99}
			switch shape {
			case "table":
				for i := 0; i < 6; i++ {
					internTimeline(t, v, []byte{byte(i)})
				}
			case "metadata":
				internTimeline(t, v, []byte{0})
			case "arena":
				src = bytes.Repeat([]byte{99}, 512)
				internTimeline(t, v, bytes.Repeat([]byte{1}, 512))
				internTimeline(t, v, bytes.Repeat([]byte{2}, 512))
			case "combined":
				for i := 0; i < 6; i++ {
					internTimeline(t, v, bytes.Repeat([]byte{byte(i)}, 512))
				}
				src = bytes.Repeat([]byte{99}, 512)
			}
			p, err := c.planInsert(len(src))
			if err != nil {
				t.Fatal(err)
			}
			if p.peak <= c.stats.ChargedBytes {
				t.Fatal("fixture has no growth")
			}
			c.config.HardLimit = p.peak - 1
			before := c.Stats()
			called := false
			c.hooks.slots = func(n int) ([]timelineHashSlot, error) { called = true; return make([]timelineHashSlot, n), nil }
			c.hooks.metadata = func(n int) ([]timelineMetadata, error) { called = true; return make([]timelineMetadata, n), nil }
			c.arena.hooks.bytes = func(n int) ([]byte, error) { called = true; return make([]byte, n), nil }
			if id, err := v.Intern(src); id != 0 || !errors.Is(err, errCatalogLimit) {
				t.Fatal(id, err)
			}
			if called || c.Stats() != before {
				t.Fatal("allocated or mutated before admission")
			}
			c.config.HardLimit = p.peak
			internTimeline(t, v, src)
			if c.stats.HighWater != max(before.HighWater, p.peak) {
				t.Fatal(c.stats, p)
			}
			// The test temporarily lowers the next admission budget. Production
			// configuration is immutable; historical high-water does not shrink.
			c.config.HardLimit = 1 << 20
			checkCatalogAccounting(t, c)
		})
	}
}

func TestTimelineCatalogAllocationFailureAtomicity(t *testing.T) {
	injected := errors.New("catalog allocation fault")
	for _, location := range []string{"slots", "metadata", "arena-blocks", "arena-bytes", "copy", "short-slots", "extra-slots", "short-metadata", "extra-metadata"} {
		for _, stage := range []int{0, 1, 6, 8} {
			t.Run(location+"/"+string(rune('0'+stage)), func(t *testing.T) {
				c := testCatalog(t, 100, 1<<20)
				v := c.View()
				for i := 0; i < stage; i++ {
					internTimeline(t, v, bytes.Repeat([]byte{byte(i)}, 512))
				}
				p, err := c.planInsert(512)
				if err != nil {
					t.Fatal(err)
				}
				if (location == "slots" || location == "short-slots" || location == "extra-slots") && p.slots == 0 {
					return
				}
				if (location == "metadata" || location == "short-metadata" || location == "extra-metadata") && p.metadata == 0 {
					return
				}
				ap, _ := c.arena.planAppend(512, false)
				if location == "arena-blocks" && ap.descriptors == 0 {
					return
				}
				if location == "arena-bytes" && ap.allocation == 0 {
					return
				}
				switch location {
				case "slots":
					c.hooks.slots = func(int) ([]timelineHashSlot, error) { return nil, injected }
				case "metadata":
					c.hooks.metadata = func(int) ([]timelineMetadata, error) { return nil, injected }
				case "short-slots":
					c.hooks.slots = func(n int) ([]timelineHashSlot, error) { return make([]timelineHashSlot, n-1), nil }
				case "extra-slots":
					c.hooks.slots = func(n int) ([]timelineHashSlot, error) { return make([]timelineHashSlot, n, n+1), nil }
				case "short-metadata":
					c.hooks.metadata = func(n int) ([]timelineMetadata, error) { return make([]timelineMetadata, n-1), nil }
				case "extra-metadata":
					c.hooks.metadata = func(n int) ([]timelineMetadata, error) { return make([]timelineMetadata, n, n+1), nil }
				case "arena-blocks":
					c.arena.hooks.blocks = func(int) ([]arenaBlock, error) { return nil, injected }
				case "arena-bytes":
					c.arena.hooks.bytes = func(int) ([]byte, error) { return nil, injected }
				case "copy":
					c.arena.hooks.copy = func(dst, src []byte) (int, error) { return copy(dst[:1], src), injected }
				}
				before, arenaBefore := c.Stats(), c.arena.Stats()
				slots := slices.Clone(c.slots)
				metadata := slices.Clone(c.metadata)
				if id, err := v.Intern(bytes.Repeat([]byte{99}, 512)); id != 0 || err == nil {
					t.Fatal(id, err)
				}
				if c.Stats() != before || c.arena.Stats() != arenaBefore || !slices.Equal(c.slots, slots) || !slices.Equal(c.metadata, metadata) {
					t.Fatal("failed insertion published partial entry")
				}
				for i := 0; i < stage; i++ {
					checkTimeline(t, v, timelineID(i), bytes.Repeat([]byte{byte(i)}, 512))
				}
				c.hooks = timelineCatalogHooks{}
				c.arena.hooks = arenaHooks{}
				if id := internTimeline(t, v, bytes.Repeat([]byte{99}, 512)); id != timelineID(stage) {
					t.Fatal(id)
				}
				checkCatalogAccounting(t, c)
			})
		}
	}
}

func TestTimelineCatalogPublicationAfterCopy(t *testing.T) {
	c := testCatalog(t, 100, 1<<20)
	v := c.View()
	internTimeline(t, v, []byte{1})
	before := c.Stats()
	old := c.metadata[0]
	c.arena.hooks.copy = func(dst, src []byte) (int, error) {
		if c.stats != before || len(c.metadata) != 1 || c.metadata[0] != old {
			t.Fatal("published before copy")
		}
		return copy(dst, src), nil
	}
	internTimeline(t, v, []byte{2})
	checkCatalogAccounting(t, c)
}

func TestTimelineCatalogSortedIDsAndProjectionAdmission(t *testing.T) {
	values := [][]byte{{0xff}, {0, 0xff}, {1, 0}, {0}, {1}, {0, 0}, {0xff, 0}, {1, 0xff}, {0, 1}, {1, 1}}
	want := slices.Clone(values)
	slices.SortFunc(want, bytes.Compare)
	for _, collision := range []bool{false, true} {
		c := testCatalog(t, 100, 1<<20)
		if collision {
			c.hooks.hash = func([]byte) uint64 { return 0 }
		}
		v := c.View()
		for _, src := range values {
			internTimeline(t, v, src)
		}
		refs := slices.Clone(c.metadata)
		copyBefore := c.arena.Stats().CopyBytes
		before := c.Stats()
		limit := c.config.HardLimit
		c.config.HardLimit = before.ChargedBytes
		c.hooks.projection = func(int) ([]timelineID, error) { t.Fatal("projection allocated before admission"); return nil, nil }
		if _, err := v.SortedIDs(); !errors.Is(err, errCatalogLimit) {
			t.Fatal(err)
		}
		if c.Stats() != before {
			t.Fatal("projection rejection changed state")
		}
		c.config.HardLimit = limit
		for _, bad := range []int{0, 1, 2} {
			c.hooks.projection = func(n int) ([]timelineID, error) {
				if bad == 0 {
					return nil, errCatalogAllocation
				}
				if bad == 1 {
					return make([]timelineID, n-1), nil
				}
				return make([]timelineID, n, n+1), nil
			}
			if _, err := v.SortedIDs(); err == nil {
				t.Fatal("invalid projection allocation")
			}
			if c.Stats() != before {
				t.Fatal("failed projection published")
			}
		}
		c.hooks.projection = nil
		p, err := v.SortedIDs()
		if err != nil {
			t.Fatal(err)
		}
		for i, src := range want {
			id, err := p.ID(i)
			if err != nil {
				t.Fatal(err)
			}
			checkTimeline(t, v, id, src)
		}
		if n, err := p.Len(); err != nil || n != len(values) {
			t.Fatal(n, err)
		}
		if _, err := p.ID(-1); err == nil {
			t.Fatal("negative projection index")
		}
		if _, err := p.ID(len(values)); err == nil {
			t.Fatal("projection index overflow")
		}
		if c.arena.Stats().CopyBytes != copyBefore || !slices.Equal(c.metadata, refs) {
			t.Fatal("sort moved bytes or metadata")
		}
		internTimeline(t, v, values[0])
		if _, err := p.ID(0); err != nil {
			t.Fatal("duplicate invalidated projection")
		}
		internTimeline(t, v, []byte{2})
		if _, err := p.ID(0); err == nil {
			t.Fatal("old projection survived insertion")
		}
		checkCatalogAccounting(t, c)
	}
}

func TestTimelineCatalogResetReuseIsolationAndStaleHandles(t *testing.T) {
	a := testCatalog(t, 100, 1<<20)
	b := testCatalog(t, 100, 1<<20)
	other := b.View()
	internTimeline(t, other, []byte{99})
	var first *byte
	for cycle := 0; cycle < 20; cycle++ {
		v := a.View()
		id := internTimeline(t, v, []byte{7})
		value, _ := v.Timeline(id)
		p, _ := v.SortedIDs()
		if cycle == 0 {
			first = &a.bytes(id)[0]
		} else if first != &a.bytes(id)[0] {
			t.Fatal("normal storage not reused")
		}
		a.metadata[id].state = timelineBatchState{1, 2, 3, 4, true, true}
		before := a.Stats()
		if err := a.Reset(); err != nil {
			t.Fatal(err)
		}
		if _, err := v.Intern([]byte{1}); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, _, err := v.Lookup([]byte{7}); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := v.Len(); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := v.Timeline(id); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := value.Len(); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := value.Equal([]byte{7}); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := value.CopyTo(make([]byte, 512)); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := p.Len(); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if _, err := p.ID(0); !errors.Is(err, errCatalogIdentity) {
			t.Fatal(err)
		}
		if a.Stats().ChargedBytes != before.ChargedBytes || a.Stats().HighWater != before.HighWater {
			t.Fatal("lost retained capacity charge")
		}
		for _, m := range a.metadata[:cap(a.metadata)] {
			if m != (timelineMetadata{}) {
				t.Fatal("state retained")
			}
		}
		checkTimeline(t, other, 0, []byte{99})
		checkCatalogAccounting(t, a)
	}
	// Large timeline blocks obey E03's release policy, independently of tables.
	a.arena.config.LargeThreshold = 8
	large := bytes.Repeat([]byte{1}, 512)
	largeID := internTimeline(t, a.View(), large)
	checkTimeline(t, a.View(), largeID, large)
	if got, found, err := a.View().Lookup(large); err != nil || !found || got != largeID {
		t.Fatal(got, found, err)
	}
	before := a.Stats()
	if err := a.Reset(); err != nil {
		t.Fatal(err)
	}
	if a.Stats().ChargedBytes != before.ChargedBytes-512 {
		t.Fatal("large block retained")
	}
	checkCatalogAccounting(t, a)
}

func TestTimelineCatalogProjectionGrowthOverlap(t *testing.T) {
	c := testCatalog(t, 100, 1<<20)
	v := c.View()
	for i := range 4 {
		internTimeline(t, v, []byte{byte(i)})
	}
	p, err := v.SortedIDs()
	if err != nil {
		t.Fatal(err)
	}
	internTimeline(t, v, []byte{4})
	before := c.Stats()
	// Retained four-ID backing remains charged while eight-ID replacement is
	// allocated. Charging just the net delta would incorrectly admit this.
	peak := before.ChargedBytes + 8*timelineIDBytes
	c.config.HardLimit = peak - 1
	c.hooks.projection = func(int) ([]timelineID, error) { t.Fatal("unreserved replacement"); return nil, nil }
	if _, err := v.SortedIDs(); !errors.Is(err, errCatalogLimit) {
		t.Fatal(err)
	}
	if c.Stats() != before {
		t.Fatal("projection failure changed state")
	}
	c.config.HardLimit = peak
	c.hooks.projection = nil
	if _, err := v.SortedIDs(); err != nil {
		t.Fatal(err)
	}
	if s := c.Stats(); s.ChargedBytes != before.ChargedBytes+4*timelineIDBytes || s.HighWater != max(before.HighWater, peak) {
		t.Fatal(s)
	}
	if _, err := p.ID(0); err == nil {
		t.Fatal("old projection survived replacement")
	}
	c.config.HardLimit = 1 << 20
	checkCatalogAccounting(t, c)
}

func TestTimelineCatalogBorrowedHashAndMetricSaturation(t *testing.T) {
	c := testCatalog(t, 10, 1<<20)
	v := c.View()
	src := []byte{0, 0xff, 1}
	hashed := false
	c.hooks.hash = func(b []byte) uint64 {
		if &b[0] != &src[0] {
			t.Fatal("hash did not see borrowed input")
		}
		hashed = true
		return math.MaxUint64
	}
	c.arena.hooks.copy = func(dst, b []byte) (int, error) {
		if !hashed {
			t.Fatal("copied before hashing")
		}
		return copy(dst, b), nil
	}
	id := internTimeline(t, v, src)
	if c.slots[7].id != id || !c.slots[7].occupied {
		t.Fatal("maximum hash route")
	}
	c.stats.Probes, c.stats.ProbeOperations, c.stats.Insertions = math.MaxUint64, math.MaxUint64, math.MaxUint64
	if got := internTimeline(t, v, src); got != id {
		t.Fatal(got)
	}
	if c.stats.Probes != math.MaxUint64 || c.stats.ProbeOperations != math.MaxUint64 || c.stats.Insertions != math.MaxUint64 {
		t.Fatal("metrics wrapped")
	}
}

func TestTimelineCatalogInvalidHandlesAndResetExhaustion(t *testing.T) {
	var c timelineCatalog
	v := c.View()
	if _, err := v.Intern([]byte{1}); err == nil {
		t.Fatal("zero catalog admitted")
	}
	if err := c.Reset(); err == nil {
		t.Fatal("zero catalog reset")
	}
	if _, err := (catalogView{}).Intern([]byte{1}); err == nil {
		t.Fatal("zero view")
	}
	if _, _, err := (catalogView{}).Lookup([]byte{1}); err == nil {
		t.Fatal("zero lookup")
	}
	if _, err := (catalogView{}).Len(); err == nil {
		t.Fatal("zero length")
	}
	if _, err := (catalogView{}).Timeline(0); err == nil {
		t.Fatal("zero timeline")
	}
	if _, err := (catalogView{}).SortedIDs(); err == nil {
		t.Fatal("zero projection")
	}
	if _, err := (timelineValue{}).Len(); err == nil {
		t.Fatal("zero value")
	}
	if _, err := (timelineProjection{}).Len(); err == nil {
		t.Fatal("zero projection length")
	}
	if _, err := (timelineProjection{}).ID(0); err == nil {
		t.Fatal("zero projection ID")
	}
	a := testCatalog(t, 10, 1<<20)
	a.generation = math.MaxUint64
	v = a.View()
	internTimeline(t, v, []byte{1, 2})
	before := a.Stats()
	if err := a.Reset(); err == nil || a.Stats() != before {
		t.Fatal("generation wrapped")
	}
	checkTimeline(t, v, 0, []byte{1, 2})
	if _, err := v.Timeline(math.MaxUint32); err == nil {
		t.Fatal("invalid ID")
	}
	value, _ := v.Timeline(0)
	dst := []byte{99}
	if n, err := value.CopyTo(dst); n != 0 || err == nil || dst[0] != 99 {
		t.Fatal(n, err, dst)
	}
	a.generation = 1
	a.arena.generation = math.MaxUint64
	before = a.Stats()
	if err := a.Reset(); err == nil || a.Stats() != before || a.generation != 1 {
		t.Fatal("partial reset")
	}
}

func TestTimelineCatalogSteadyStateAllocations(t *testing.T) {
	c := testCatalog(t, 1024, 1<<20)
	keys := make([][8]byte, 1024)
	for i := range keys {
		binary.BigEndian.PutUint64(keys[i][:], uint64(i))
	}
	cycle := func() {
		if err := c.Reset(); err != nil {
			panic(err)
		}
		v := c.View()
		for i := range keys {
			if _, err := v.Intern(keys[i][:]); err != nil {
				panic(err)
			}
		}
		if _, err := v.SortedIDs(); err != nil {
			panic(err)
		}
	}
	cycle()
	if n := testing.AllocsPerRun(20, cycle); n != 0 {
		t.Fatalf("reset/intern/sort reuse allocs=%g", n)
	}
	v := c.View()
	key := make([]byte, 8)
	value, _ := v.Timeline(0)
	var dst [512]byte
	if n := testing.AllocsPerRun(100, func() { v.Intern(key); v.Lookup(key); value.Equal(key); value.CopyTo(dst[:]); v.SortedIDs() }); n != 0 {
		t.Fatalf("hot lookup allocs=%g", n)
	}
}

func TestTimelineCatalogConcurrentReadersAndOwner(t *testing.T) {
	c := testCatalog(t, 1024, 1<<20)
	v := c.View()
	internTimeline(t, v, []byte{7})
	value, _ := v.Timeline(0)
	var readers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				if equal, err := value.Equal([]byte{7}); err != nil || !equal {
					t.Error(equal, err)
				}
			}
		}()
	}
	close(start)
	for i := 0; i < 100; i++ {
		internTimeline(t, v, []byte{0, byte(i)})
	}
	readers.Wait()
	// Deterministic lock test using the private scalar-reader seam. No sleeps.
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- value.inspect(func(b []byte) error {
			close(entered)
			<-release
			if !bytes.Equal(b, []byte{7}) {
				t.Error("read mutated")
			}
			return nil
		})
	}()
	<-entered
	if c.mu.TryLock() {
		c.mu.Unlock()
		t.Fatal("reader did not exclude reset")
	}
	resetDone := make(chan error, 1)
	go func() { resetDone <- c.Reset() }()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-resetDone; err != nil {
		t.Fatal(err)
	}
	if _, err := value.Equal([]byte{7}); err == nil {
		t.Fatal("reader survived reset")
	}
}

// Exact byte-key reference model, deliberately linear and independent of hash
// routing. No string conversion or hash-as-identity even in the oracle.
func runCatalogModel(t *testing.T, operations, source []byte, collision bool) {
	t.Helper()
	c := testCatalog(t, 64, 32<<10)
	if collision {
		c.hooks.hash = func([]byte) uint64 { return 7 }
	}
	v := c.View()
	var model [][]byte
	for step, op := range operations {
		if op == 255 {
			old := v
			projection, _ := v.SortedIDs()
			if err := c.Reset(); err != nil {
				t.Fatal(err)
			}
			v = c.View()
			model = nil
			if _, err := old.Len(); err == nil {
				t.Fatal("model stale")
			}
			if _, err := projection.Len(); err == nil {
				t.Fatal("model projection stale")
			}
			continue
		}
		n := int(op) % 33
		var src []byte
		if len(source) > 0 {
			start := step % len(source)
			n = min(n, len(source)-start)
			src = source[start : start+n]
		}
		want := -1
		for i, key := range model {
			if bytes.Equal(key, src) {
				want = i
				break
			}
		}
		before := c.Stats()
		id, err := v.Intern(src)
		if err != nil {
			if len(src) > 0 && !errors.Is(err, errCatalogLimit) {
				t.Fatal(err)
			}
			if c.Stats() != before {
				t.Fatal("model rejection changed state")
			}
			continue
		}
		if want < 0 {
			want = len(model)
			model = append(model, bytes.Clone(src))
		}
		if id != timelineID(want) {
			t.Fatal(id, want)
		}
		for i, key := range model {
			if id, ok, err := v.Lookup(key); err != nil || !ok || id != timelineID(i) {
				t.Fatal(id, ok, err)
			}
			checkTimeline(t, v, timelineID(i), key)
		}
		p, err := v.SortedIDs()
		if err != nil {
			t.Fatal(err)
		}
		wantOrder := make([]int, len(model))
		for i := range wantOrder {
			wantOrder[i] = i
		}
		slices.SortFunc(wantOrder, func(a, b int) int { return bytes.Compare(model[a], model[b]) })
		for i, want := range wantOrder {
			if got, err := p.ID(i); err != nil || got != timelineID(want) {
				t.Fatal(got, want, err)
			}
		}
		checkCatalogAccounting(t, c)
	}
}

func TestTimelineCatalogRandomizedModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xe04, 0x1234))
	source := make([]byte, 512)
	ops := make([]byte, 128)
	for range 30 {
		for i := range source {
			source[i] = byte(rng.Uint32())
		}
		for i := range ops {
			ops[i] = byte(rng.Uint32())
		}
		runCatalogModel(t, ops, source, false)
		runCatalogModel(t, ops, source, true)
	}
}
