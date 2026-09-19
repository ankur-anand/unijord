package runingest

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

func testArena(t testing.TB, slab, threshold, hard uint64) *arena {
	t.Helper()
	a, err := newArena(arenaConfig{slab, threshold, runcontract.MaxValueBytes, hard})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func appendArena(t testing.TB, a *arena, src []byte) arenaRef {
	t.Helper()
	r, err := a.Append(src)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func checkArenaValue(t testing.TB, a *arena, r arenaRef, want []byte) {
	t.Helper()
	err := a.WithBytes(r, func(got []byte) error {
		if !bytes.Equal(got, want) || (got == nil) != (want == nil) || cap(got) != len(got) {
			t.Fatalf("value length/nil/cap = %d/%t/%d, want %d/%t", len(got), got == nil, cap(got), len(want), want == nil)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func checkArenaAccounting(t testing.TB, a *arena) {
	t.Helper()
	s := a.Stats()
	var normal, large uint64
	for _, b := range a.normal {
		normal += uint64(cap(b.data))
	}
	for _, b := range a.large {
		large += uint64(cap(b.data))
	}
	desc := uint64(cap(a.normal)+cap(a.large)) * arenaBlockBytes
	if normal != s.NormalBytes || large != s.LargeBytes || desc != s.DescriptorBytes ||
		s.ChargedBytes != normal+large+desc || s.HighWater < s.ChargedBytes ||
		s.HighWater > a.config.HardLimit || s.CopyBytes != s.LogicalBytes {
		t.Fatalf("accounting = %+v, backing = %d/%d/%d", s, normal, large, desc)
	}
}

func TestArenaLazyEmpty(t *testing.T) {
	a := testArena(t, 64, 64, 1)
	if a.Stats() != (arenaStats{}) || a.normal != nil || a.large != nil {
		t.Fatal("eager allocation")
	}
	null := appendArena(t, a, nil)
	empty := appendArena(t, a, []byte{})
	if null == empty || null == (arenaRef{}) || empty == (arenaRef{}) {
		t.Fatal("empty distinctions lost")
	}
	checkArenaValue(t, a, null, nil)
	checkArenaValue(t, a, empty, []byte{})
	if s := a.Stats(); s.ChargedBytes != 0 || s.CopyCount != 2 || s.CopyBytes != 0 {
		t.Fatal(s)
	}
	checkArenaAccounting(t, a)
}

func TestArenaUninitialized(t *testing.T) {
	var a arena
	for _, src := range [][]byte{nil, {}, {1}} {
		if r, err := a.Append(src); err == nil || r != (arenaRef{}) {
			t.Fatalf("uninitialized append = %+v, %v", r, err)
		}
	}
	if err := a.Reset(); err == nil || a.generation != 0 || a.stats != (arenaStats{}) {
		t.Fatal("reset accepted an uninitialized arena")
	}
}

func TestArenaBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name          string
		threshold     uint64
		sizes         []int
		normal, large uint64
	}{
		{"exact", 64, []int{64}, 64, 0},
		{"over", 64, []int{65}, 0, 65},
		{"multi", 64, []int{64, 64, 64}, 192, 0},
		{"fragment", 64, []int{40, 25, 39, 1}, 192, 0},
		{"threshold", 32, []int{31, 32, 33}, 64, 33},
		{"mixed", 64, []int{17, 100, 47, 65}, 64, 165},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testArena(t, 64, tc.threshold, 4096)
			var total uint64
			var refs []arenaRef
			var values [][]byte
			for i, size := range tc.sizes {
				src := bytes.Repeat([]byte{byte(i + 1)}, size)
				refs = append(refs, appendArena(t, a, src))
				values = append(values, src)
				total += uint64(size)
				checkArenaAccounting(t, a)
			}
			for i, r := range refs {
				checkArenaValue(t, a, r, values[i])
			}
			s := a.Stats()
			if s.NormalBytes != tc.normal || s.LargeBytes != tc.large || s.LogicalBytes != total || s.CopyCount != uint64(len(tc.sizes)) {
				t.Fatal(s)
			}
		})
	}
}

func TestArenaMaximumAndInvalidSizes(t *testing.T) {
	a := testArena(t, 64<<10, 64<<10, 2*runcontract.MaxValueBytes)
	src := make([]byte, runcontract.MaxValueBytes)
	src[0], src[len(src)-1] = 1, 2
	r := appendArena(t, a, src)
	checkArenaValue(t, a, r, src)
	before := a.Stats()
	for _, n := range []uint64{runcontract.MaxValueBytes + 1, math.MaxUint64, uint64(^uint(0)>>1) + 1} {
		if _, err := a.planAppend(n, false); !errors.Is(err, errArenaLimit) {
			t.Fatalf("size %d: %v", n, err)
		}
	}
	if got, err := a.Append(make([]byte, runcontract.MaxValueBytes+1)); !errors.Is(err, errArenaLimit) || got != (arenaRef{}) {
		t.Fatal(got, err)
	}
	if a.Stats() != before {
		t.Fatal("failed validation changed state")
	}
	if _, err := a.planAppend(1, true); err == nil {
		t.Fatal("nonempty null")
	}
	c := a.config
	c.MaxFieldBytes = 3
	small, err := newArena(c)
	if err != nil {
		t.Fatal(err)
	}
	appendArena(t, small, []byte{1, 2, 3})
	if _, err := small.Append([]byte{1, 2, 3, 4}); err == nil {
		t.Fatal("configured maximum ignored")
	}
}

func TestArenaInvalidConfiguration(t *testing.T) {
	base := arenaConfig{64, 32, runcontract.MaxValueBytes, 1024}
	for _, change := range []func(*arenaConfig){
		func(c *arenaConfig) { c.SlabBytes = 0 },
		func(c *arenaConfig) { c.SlabBytes = math.MaxUint64 },
		func(c *arenaConfig) { c.LargeThreshold = 0 },
		func(c *arenaConfig) { c.LargeThreshold = 65 },
		func(c *arenaConfig) { c.MaxFieldBytes = 0 },
		func(c *arenaConfig) { c.MaxFieldBytes = runcontract.MaxValueBytes + 1 },
		func(c *arenaConfig) { c.HardLimit = 0 },
	} {
		c := base
		change(&c)
		if a, err := newArena(c); a != nil || !errors.Is(err, errArenaLimit) {
			t.Fatal(c, a, err)
		}
	}
}

func TestArenaCheckedArithmetic(t *testing.T) {
	if n, err := arenaAdd(math.MaxUint64, 0); n != math.MaxUint64 || err != nil {
		t.Fatal(n, err)
	}
	if _, err := arenaAdd(math.MaxUint64, 1); err == nil {
		t.Fatal("add overflow")
	}
	if n, err := arenaMul(math.MaxUint64, 1); n != math.MaxUint64 || err != nil {
		t.Fatal(n, err)
	}
	if _, err := arenaMul(math.MaxUint64, 2); err == nil {
		t.Fatal("multiply overflow")
	}
	maxInt := uint64(^uint(0) >> 1)
	if _, err := arenaInt(maxInt); err != nil {
		t.Fatal(err)
	}
	if _, err := arenaInt(maxInt + 1); err == nil {
		t.Fatal("int overflow")
	}
	for _, v := range [][2]uint64{{2, 1}, {math.MaxUint64, math.MaxUint64}, {maxInt, maxInt}, {maxInt / arenaBlockBytes, maxInt / arenaBlockBytes}, {maxInt/2 + 1, maxInt/2 + 1}} {
		if _, _, err := arenaDescriptorGrowth(v[0], v[1]); err == nil {
			t.Fatalf("descriptor overflow %v", v)
		}
	}
	if n, charge, err := arenaDescriptorGrowth(0, 0); n != 1 || charge != arenaBlockBytes || err != nil {
		t.Fatal(n, charge, err)
	}
	if n, charge, err := arenaDescriptorGrowth(1, 2); n != 0 || charge != 0 || err != nil {
		t.Fatal(n, charge, err)
	}
	for _, field := range []string{"logical", "copy-bytes", "copy-count", "charge", "normal", "large", "descriptor", "index", "used"} {
		t.Run(field, func(t *testing.T) {
			a := testArena(t, 64, 64, math.MaxUint64)
			n := uint64(1)
			switch field {
			case "logical":
				a.stats.LogicalBytes = math.MaxUint64
			case "copy-bytes":
				a.stats.CopyBytes = math.MaxUint64
			case "copy-count":
				a.stats.CopyCount = math.MaxUint64
			case "charge":
				a.stats.ChargedBytes = math.MaxUint64
			case "normal":
				a.stats.NormalBytes = math.MaxUint64
			case "large":
				a.stats.LargeBytes = math.MaxUint64
				n = 65
			case "descriptor":
				a.stats.DescriptorBytes = math.MaxUint64
			case "index":
				a.active = math.MaxUint64
			case "used":
				a.normal = []arenaBlock{{data: make([]byte, 64), used: math.MaxUint64}}
			}
			before := a.stats
			if _, err := a.planAppend(n, false); err == nil {
				t.Fatal("overflow admitted")
			}
			if before != a.stats {
				t.Fatal("planning mutated accounting")
			}
		})
	}
	var ids atomic.Uint64
	ids.Store(math.MaxUint64 - 1)
	if id, err := nextArenaIdentity(&ids); id != math.MaxUint64 || err != nil {
		t.Fatal(id, err)
	}
	if id, err := nextArenaIdentity(&ids); id != 0 || err == nil || ids.Load() != math.MaxUint64 {
		t.Fatal(id, err)
	}
	a := testArena(t, 64, 64, 1024)
	a.generation = math.MaxUint64
	r := appendArena(t, a, []byte{7})
	before := a.Stats()
	if err := a.Reset(); err == nil || a.Stats() != before {
		t.Fatal("generation wrapped")
	}
	checkArenaValue(t, a, r, []byte{7})
}

func TestArenaHardBoundBeforeAllocation(t *testing.T) {
	for _, large := range []bool{false, true} {
		n := 64
		if large {
			n = 65
		}
		a := testArena(t, 64, 64, uint64(n)+arenaBlockBytes-1)
		a.hooks.bytes = func(int) ([]byte, error) { t.Fatal("bytes allocated before admission"); return nil, nil }
		a.hooks.blocks = func(int) ([]arenaBlock, error) { t.Fatal("descriptors allocated before admission"); return nil, nil }
		if r, err := a.Append(make([]byte, n)); r != (arenaRef{}) || !errors.Is(err, errArenaLimit) {
			t.Fatal(r, err)
		}
		if a.Stats() != (arenaStats{}) {
			t.Fatal(a.Stats())
		}
	}
	// Final ownership of two slabs fits, but old+new descriptor overlap does not.
	final := uint64(128) + 2*arenaBlockBytes
	a := testArena(t, 64, 64, final)
	r := appendArena(t, a, bytes.Repeat([]byte{3}, 64))
	before := a.Stats()
	if _, err := a.Append(make([]byte, 64)); !errors.Is(err, errArenaLimit) {
		t.Fatal(err)
	}
	if a.Stats() != before {
		t.Fatal("reservation changed state")
	}
	checkArenaValue(t, a, r, bytes.Repeat([]byte{3}, 64))
	// Equality at the transient peak is admissible.
	b := testArena(t, 64, 64, final+arenaBlockBytes)
	appendArena(t, b, make([]byte, 64))
	appendArena(t, b, make([]byte, 64))
	if s := b.Stats(); s.ChargedBytes != final || s.HighWater != final+arenaBlockBytes {
		t.Fatal(s)
	}
}

func TestArenaAppendFailureAtomicity(t *testing.T) {
	injected := errors.New("injected failure")
	for _, location := range []string{"descriptor", "allocation", "short-allocation", "extra-capacity", "short-descriptor", "descriptor-extra-capacity", "copy-error", "short-copy"} {
		for _, shape := range []string{"existing", "new-normal", "new-large"} {
			t.Run(location+"/"+shape, func(t *testing.T) {
				a := testArena(t, 64, 64, 8192)
				r := appendArena(t, a, []byte{1, 2, 3})
				src := bytes.Repeat([]byte{9}, 17)
				if shape == "new-normal" {
					src = bytes.Repeat([]byte{9}, 64)
				}
				if shape == "new-large" {
					src = bytes.Repeat([]byte{9}, 65)
				}
				if shape == "existing" && location != "copy-error" && location != "short-copy" {
					return
				}
				switch location {
				case "descriptor":
					a.hooks.blocks = func(int) ([]arenaBlock, error) { return nil, injected }
				case "allocation":
					a.hooks.bytes = func(int) ([]byte, error) { return nil, injected }
				case "short-allocation":
					a.hooks.bytes = func(n int) ([]byte, error) { return make([]byte, n-1), nil }
				case "extra-capacity":
					a.hooks.bytes = func(n int) ([]byte, error) { return make([]byte, n, n+1), nil }
				case "short-descriptor":
					a.hooks.blocks = func(n int) ([]arenaBlock, error) { return make([]arenaBlock, n-1), nil }
				case "descriptor-extra-capacity":
					a.hooks.blocks = func(n int) ([]arenaBlock, error) { return make([]arenaBlock, n, n+1), nil }
				case "copy-error":
					a.hooks.copy = func(dst, src []byte) (int, error) { return copy(dst[:1], src), injected }
				case "short-copy":
					a.hooks.copy = func(dst, src []byte) (int, error) { return copy(dst[:1], src), nil }
				}
				before := a.Stats()
				normalPtr := &a.normal[0]
				if got, err := a.Append(src); got != (arenaRef{}) || err == nil {
					t.Fatal(got, err)
				}
				if a.Stats() != before || normalPtr != &a.normal[0] || len(a.normal) != 1 || len(a.large) != 0 || a.active != 0 || a.normal[0].used != 3 {
					t.Fatal("failure published state")
				}
				checkArenaValue(t, a, r, []byte{1, 2, 3})
				// Even a forged span cannot expose a partially copied tail.
				tail := r
				tail.offset = 3
				tail.length = 1
				if err := a.WithBytes(tail, func([]byte) error { t.Fatal("partial tail exposed"); return nil }); err == nil {
					t.Fatal("accepted tail")
				}
				a.hooks = arenaHooks{}
				checkArenaValue(t, a, appendArena(t, a, src), src)
				checkArenaAccounting(t, a)
			})
		}
	}
}

func TestArenaSingleCopyAndFixedBacking(t *testing.T) {
	a := testArena(t, 64, 64, 1<<20)
	var calls, copied uint64
	a.hooks.copy = func(dst, src []byte) (int, error) {
		if len(src) > 0 && &dst[0] == &src[0] {
			t.Fatal("source aliases arena")
		}
		calls++
		n := copy(dst, src)
		copied += uint64(n)
		return n, nil
	}
	src := []byte("borrowed")
	r := appendArena(t, a, src)
	var first *byte
	if err := a.WithBytes(r, func(b []byte) error { first = &b[0]; return nil }); err != nil {
		t.Fatal(err)
	}
	src[0] = 'X'
	for range 1024 {
		appendArena(t, a, make([]byte, 64))
	}
	for range 8 {
		appendArena(t, a, make([]byte, 65))
	}
	if err := a.WithBytes(r, func(b []byte) error {
		if &b[0] != first || &b[0] == &src[0] || string(b) != "borrowed" {
			t.Fatal("relocated or aliased source")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if s := a.Stats(); s.CopyBytes != copied || s.CopyCount != calls || calls != 1033 {
		t.Fatal(s, calls, copied)
	}
	checkArenaAccounting(t, a)
}

func TestArenaPublicationAfterCopy(t *testing.T) {
	for _, size := range []int{17, 64, 65} {
		a := testArena(t, 64, 64, 4096)
		old := appendArena(t, a, []byte{1, 2, 3})
		before := a.Stats()
		calls := 0
		a.hooks.copy = func(dst, src []byte) (int, error) {
			calls++
			n := copy(dst, src)
			if a.stats != before || len(a.normal) != 1 || len(a.large) != 0 || a.normal[0].used != 3 {
				t.Fatal("state published before copy returned")
			}
			return n, nil
		}
		src := bytes.Repeat([]byte{7}, size)
		r := appendArena(t, a, src)
		if calls != 1 {
			t.Fatal(calls)
		}
		src[0] = 9
		checkArenaValue(t, a, r, bytes.Repeat([]byte{7}, size))
		checkArenaValue(t, a, old, []byte{1, 2, 3})
	}
}

func TestArenaFailedLargeAppendAfterReset(t *testing.T) {
	a := testArena(t, 64, 64, 4096)
	appendArena(t, a, make([]byte, 100))
	if err := a.Reset(); err != nil {
		t.Fatal(err)
	}
	first := appendArena(t, a, []byte("normal"))
	before := a.Stats()
	// The retained descriptor capacity can be reused, but a failed large copy
	// must not populate its unpublished slot or charge a new backing block.
	a.hooks.copy = func(dst, src []byte) (int, error) { return copy(dst[:1], src), errArenaCopy }
	if r, err := a.Append(make([]byte, 100)); r != (arenaRef{}) || err == nil {
		t.Fatal(r, err)
	}
	if a.Stats() != before || len(a.large) != 0 || a.large[:cap(a.large)][0].data != nil {
		t.Fatal("failed append retained large storage")
	}
	checkArenaValue(t, a, first, []byte("normal"))
	a.hooks = arenaHooks{}
	r := appendArena(t, a, make([]byte, 100))
	checkArenaValue(t, a, r, make([]byte, 100))
	checkArenaAccounting(t, a)
}

func TestArenaMalformedReferences(t *testing.T) {
	a := testArena(t, 64, 64, 4096)
	r := appendArena(t, a, []byte{1, 2, 3})
	empty := appendArena(t, a, []byte{})
	bad := []arenaRef{{}}
	for _, change := range []func(*arenaRef){
		func(r *arenaRef) { r.owner++ }, func(r *arenaRef) { r.generation++ },
		func(r *arenaRef) { r.index = math.MaxUint64 }, func(r *arenaRef) { r.offset = math.MaxUint64 },
		func(r *arenaRef) { r.length = math.MaxUint64 }, func(r *arenaRef) { r.offset = 2; r.length = 2 },
		func(r *arenaRef) { r.length = 0 }, func(r *arenaRef) { r.kind = 99 },
		func(r *arenaRef) { r.kind = arenaLarge },
	} {
		b := r
		change(&b)
		bad = append(bad, b)
	}
	for _, change := range []func(*arenaRef){func(r *arenaRef) { r.index = 1 }, func(r *arenaRef) { r.offset = 1 }, func(r *arenaRef) { r.length = 1 }} {
		b := empty
		change(&b)
		bad = append(bad, b)
	}
	for _, r := range bad {
		if err := a.WithBytes(r, func([]byte) error { t.Fatal("bad reference callback"); return nil }); !errors.Is(err, errArenaRef) {
			t.Fatal(r, err)
		}
	}
	if err := a.WithBytes(r, nil); !errors.Is(err, errArenaRef) {
		t.Fatal(err)
	}
	want := errors.New("reader error")
	if err := a.WithBytes(r, func([]byte) error { return want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
	b := testArena(t, 64, 64, 4096)
	appendArena(t, b, []byte{9, 9, 9})
	if err := b.WithBytes(r, func([]byte) error { t.Fatal("foreign ref accepted"); return nil }); !errors.Is(err, errArenaRef) {
		t.Fatal(err)
	}
}

func TestArenaResetReuseAndIsolation(t *testing.T) {
	a := testArena(t, 64, 64, 4096)
	b := testArena(t, 64, 64, 4096)
	other := appendArena(t, b, []byte("other"))
	var old []arenaRef
	for _, n := range []int{0, 17, 64, 65, 200} {
		old = append(old, appendArena(t, a, make([]byte, n)))
	}
	old = append(old, appendArena(t, a, nil))
	base := a.Stats()
	first := &a.normal[0].data[0]
	for range 20 {
		if err := a.Reset(); err != nil {
			t.Fatal(err)
		}
		for _, r := range old {
			if err := a.WithBytes(r, func([]byte) error { t.Fatal("stale callback"); return nil }); !errors.Is(err, errArenaRef) {
				t.Fatal(err)
			}
		}
		for _, block := range a.large[:cap(a.large)] {
			if block.data != nil || block.used != 0 {
				t.Fatal("large allocation retained")
			}
		}
		if s := a.Stats(); s.NormalBytes != base.NormalBytes || s.DescriptorBytes != base.DescriptorBytes || s.LargeBytes != 0 || s.CopyCount != 0 || s.LogicalBytes != 0 || s.HighWater != base.HighWater {
			t.Fatal(s)
		}
		checkArenaValue(t, b, other, []byte("other"))
		old = old[:0]
		for _, n := range []int{17, 64, 65, 200} {
			old = append(old, appendArena(t, a, make([]byte, n)))
		}
		if &a.normal[0].data[0] != first {
			t.Fatal("normal storage not reused")
		}
		checkArenaAccounting(t, a)
	}
}

func TestArenaSteadyStateAllocations(t *testing.T) {
	a := testArena(t, 32<<10, 32<<10, 1<<20)
	src := make([]byte, 256)
	cycle := func() {
		if err := a.Reset(); err != nil {
			panic(err)
		}
		for range 1024 {
			if _, err := a.Append(src); err != nil {
				panic(err)
			}
		}
	}
	cycle()
	if got := testing.AllocsPerRun(100, cycle); got != 0 {
		t.Fatalf("normal reuse = %g allocations", got)
	}
	large := make([]byte, (32<<10)+1)
	largeCycle := func() {
		if err := a.Reset(); err != nil {
			panic(err)
		}
		if _, err := a.Append(large); err != nil {
			panic(err)
		}
	}
	largeCycle()
	if got := testing.AllocsPerRun(100, largeCycle); got != 1 {
		t.Fatalf("large reset = %g allocations, want one exact fresh block", got)
	}
}

func TestArenaConcurrentPublicationAndReset(t *testing.T) {
	a := testArena(t, 64, 64, 1<<20)
	r := appendArena(t, a, []byte("published"))
	// Multiple readers observe the same immutable published bytes.
	var readers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				checkArenaValue(t, a, r, []byte("published"))
			}
		}()
	}
	close(start)
	for range 100 {
		appendArena(t, a, []byte("next"))
	}
	readers.Wait()
	for _, operation := range []string{"append", "reset"} {
		t.Run(operation, func(t *testing.T) {
			r := appendArena(t, a, []byte("lease"))
			entered, release, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(readDone)
				if err := a.WithBytes(r, func(b []byte) error {
					close(entered)
					<-release
					if string(b) != "lease" {
						t.Error("mutation while leased")
					}
					return nil
				}); err != nil {
					t.Error(err)
				}
			}()
			<-entered
			// Deterministic lock assertion, without sleeps or scheduler timing.
			if a.mu.TryLock() {
				a.mu.Unlock()
				t.Fatal("read lease did not exclude writer")
			}
			attempt, done := make(chan struct{}), make(chan error, 1)
			go func() {
				close(attempt)
				if operation == "reset" {
					done <- a.Reset()
				} else {
					_, err := a.Append([]byte("later"))
					done <- err
				}
			}()
			<-attempt
			close(release)
			<-readDone
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if operation == "reset" {
				if err := a.WithBytes(r, func([]byte) error { t.Fatal("stale lease"); return nil }); !errors.Is(err, errArenaRef) {
					t.Fatal(err)
				}
			} else {
				checkArenaValue(t, a, r, []byte("lease"))
			}
		})
	}
}

func FuzzArenaAppend(f *testing.F) {
	f.Add([]byte{0, 1, 64, 65, 255}, []byte("source"))
	f.Add([]byte{255, 0, 0, 32}, []byte{})
	f.Fuzz(func(t *testing.T, operations, source []byte) {
		if len(operations) > 128 || len(source) > 512 {
			t.Skip()
		}
		a := testArena(t, 64, 48, 4096)
		type value struct {
			ref  arenaRef
			want []byte
		}
		var live []value
		for _, op := range operations {
			if op == 255 {
				if err := a.Reset(); err != nil {
					t.Fatal(err)
				}
				for _, v := range live {
					if err := a.WithBytes(v.ref, func([]byte) error { t.Fatal("stale"); return nil }); err == nil {
						t.Fatal("reset")
					}
				}
				live = nil
				continue
			}
			n := int(op)
			if n > len(source) {
				n = len(source)
			}
			input := source[:n]
			before := a.Stats()
			r, err := a.Append(input)
			if err != nil {
				if !errors.Is(err, errArenaLimit) || r != (arenaRef{}) || !reflect.DeepEqual(before, a.Stats()) {
					t.Fatal(r, err, a.Stats())
				}
			} else {
				live = append(live, value{r, input})
			}
			for _, v := range live {
				checkArenaValue(t, a, v.ref, v.want)
			}
			checkArenaAccounting(t, a)
		}
	})
}
