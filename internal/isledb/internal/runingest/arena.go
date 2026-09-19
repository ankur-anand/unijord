// Package runingest owns bounded, source-neutral ingestion storage.
package runingest

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

var (
	errArenaLimit      = errors.New("runingest: arena size or capacity limit")
	errArenaRef        = errors.New("runingest: invalid or stale arena reference")
	errArenaAllocation = errors.New("runingest: invalid arena allocation")
	errArenaCopy       = errors.New("runingest: incomplete arena copy")
	arenaIdentities    atomic.Uint64
)

// There is deliberately no default slab size or threshold. E03 benchmarks
// hypotheses; production sizing belongs to E19. MaxFieldBytes may only lower
// the E00 logical-value limit. HardLimit bounds requested backing capacities,
// including descriptor growth overlap, not allocator rounding or process RSS.
type arenaConfig struct {
	SlabBytes, LargeThreshold, MaxFieldBytes, HardLimit uint64
}

const (
	arenaNormal uint8 = iota + 1
	arenaLarge
	arenaEmpty
	arenaNull
)

// arenaRef is a checked span, not a pointer, slice, or persistent identity.
// It is valid only in its arena and generation. Bounds checks also permit a
// subspan of already initialized storage; this is not a security capability.
// A zero reference is invalid, distinct from both null and present empty.
// References are returned by value; caller-owned reference arrays must be
// charged by their owner. The arena retains only block descriptors.
type arenaRef struct {
	owner, generation, index, offset, length uint64
	kind                                     uint8
}

type arenaBlock struct {
	data []byte
	used uint64
}

var arenaBlockBytes = uint64(reflect.TypeFor[arenaBlock]().Size())

// Per-generation admission counters reset; HighWater spans all generations.
// DescriptorBytes includes retained empty descriptor slots. ChargedBytes is
// NormalBytes + LargeBytes + DescriptorBytes. HighWater includes descriptor
// replacement overlap for successful appends. A failed transaction relinquishes
// its private reservation and leaves these published metrics unchanged.
type arenaStats struct {
	LogicalBytes, NormalBytes, LargeBytes, DescriptorBytes uint64
	ChargedBytes, HighWater, CopyBytes, CopyCount          uint64
}

// Allocation/copy hooks are private fault-injection seams. Nil uses make/copy.
// Allocators must return fresh, exclusively owned storage with exact len/cap;
// they must not retain it, alias live storage, or reenter the arena. A recoverable
// allocation error rolls back; Go's process-fatal out-of-memory is not recoverable.
type arenaHooks struct {
	bytes  func(int) ([]byte, error)
	blocks func(int) ([]arenaBlock, error)
	copy   func([]byte, []byte) (int, error)
}

// An arena requires newArena and must not be copied after construction.
// Its zero value rejects Append and Reset. Append, Reset, and Stats are
// synchronized. WithBytes holds a read lease through its callback; callbacks
// must neither mutate/retain the view nor reenter arena methods. Concurrent
// readers may inspect a published value; Reset waits for their leases to end.
// Borrowed input must remain immutable for the duration of Append.
type arena struct {
	mu                   sync.RWMutex
	config               arenaConfig
	identity, generation uint64
	normal, large        []arenaBlock
	active               uint64
	stats                arenaStats
	hooks                arenaHooks
}

func newArena(c arenaConfig) (*arena, error) {
	if c.SlabBytes == 0 || c.SlabBytes > runcontract.MaxValueBytes ||
		c.LargeThreshold == 0 || c.LargeThreshold > c.SlabBytes ||
		c.MaxFieldBytes == 0 || c.MaxFieldBytes > runcontract.MaxValueBytes || c.HardLimit == 0 {
		return nil, errArenaLimit
	}
	id, err := nextArenaIdentity(&arenaIdentities)
	if err != nil {
		return nil, err
	}
	return &arena{config: c, identity: id, generation: 1}, nil
}

func nextArenaIdentity(counter *atomic.Uint64) (uint64, error) {
	for {
		old := counter.Load()
		if old == math.MaxUint64 {
			return 0, errArenaLimit
		}
		if counter.CompareAndSwap(old, old+1) {
			return old + 1, nil
		}
	}
}

func arenaAdd(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, errArenaLimit
	}
	return a + b, nil
}

func arenaMul(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, errArenaLimit
	}
	return a * b, nil
}

func arenaInt(n uint64) (int, error) {
	if n > uint64(^uint(0)>>1) {
		return 0, errArenaLimit
	}
	return int(n), nil
}

// Return the full replacement capacity charge, not just the capacity delta:
// the old and new descriptor arrays coexist until publication succeeds.
func arenaDescriptorGrowth(length, capacity uint64) (int, uint64, error) {
	if length > capacity {
		return 0, 0, errArenaLimit
	}
	if _, err := arenaInt(capacity); err != nil {
		return 0, 0, err
	}
	if length < capacity {
		return 0, 0, nil
	}
	count, err := arenaAdd(length, 1)
	if err != nil {
		return 0, 0, err
	}
	if capacity != 0 {
		count, err = arenaMul(capacity, 2)
		if err != nil {
			return 0, 0, err
		}
	}
	charge, err := arenaMul(count, arenaBlockBytes)
	if err != nil {
		return 0, 0, err
	}
	n, err := arenaInt(count)
	if err != nil {
		return 0, 0, err
	}
	// make([]T, n) also multiplies n by sizeof(T) using machine-sized arithmetic.
	if _, err = arenaInt(charge); err != nil {
		return 0, 0, err
	}
	return n, charge, nil
}

type arenaAppendPlan struct {
	ref         arenaRef
	stats       arenaStats
	allocation  int
	descriptors int
	newBlock    bool
}

// planAppend has no allocation or mutation, including on overflow or pressure.
// Holding mu exclusively reserves its complete checked peak until commit/abort.
func (a *arena) planAppend(n uint64, null bool) (arenaAppendPlan, error) {
	p := arenaAppendPlan{stats: a.stats, ref: arenaRef{owner: a.identity, generation: a.generation, length: n}}
	if a.identity == 0 || a.generation == 0 || n > a.config.MaxFieldBytes || (null && n != 0) {
		return p, errArenaLimit
	}
	var err error
	if p.stats.LogicalBytes, err = arenaAdd(p.stats.LogicalBytes, n); err != nil {
		return p, err
	}
	if p.stats.CopyBytes, err = arenaAdd(p.stats.CopyBytes, n); err != nil {
		return p, err
	}
	if p.stats.CopyCount, err = arenaAdd(p.stats.CopyCount, 1); err != nil {
		return p, err
	}
	if n == 0 {
		p.ref.kind = arenaEmpty
		if null {
			p.ref.kind = arenaNull
		}
		return p, nil
	}
	blocks := a.normal
	p.ref.kind, p.ref.index = arenaNormal, a.active
	allocation := a.config.SlabBytes
	if n > a.config.LargeThreshold {
		blocks, p.ref.kind, p.ref.index = a.large, arenaLarge, uint64(len(a.large))
		allocation = n
	} else if p.ref.index < uint64(len(blocks)) {
		block := blocks[p.ref.index]
		if block.used > uint64(len(block.data)) {
			return p, errArenaLimit
		}
		if n <= uint64(len(block.data))-block.used {
			p.ref.offset = block.used
			return p, nil
		}
		p.ref.index, err = arenaAdd(p.ref.index, 1)
		if err != nil {
			return p, err
		}
	}
	if p.ref.index > uint64(len(blocks)) {
		return p, errArenaLimit
	}
	if p.ref.index < uint64(len(blocks)) {
		// Only normal slabs survive reset; later slabs must still be unused.
		block := blocks[p.ref.index]
		if block.used != 0 || n > uint64(len(block.data)) {
			return p, errArenaLimit
		}
		return p, nil
	}
	p.newBlock = true
	p.allocation, err = arenaInt(allocation)
	if err != nil {
		return p, err
	}
	var descriptorCharge uint64
	p.descriptors, descriptorCharge, err = arenaDescriptorGrowth(uint64(len(blocks)), uint64(cap(blocks)))
	if err != nil {
		return p, err
	}
	extra, err := arenaAdd(allocation, descriptorCharge)
	if err != nil {
		return p, err
	}
	peak, err := arenaAdd(a.stats.ChargedBytes, extra)
	if err != nil || peak > a.config.HardLimit {
		return p, errArenaLimit
	}
	p.stats.ChargedBytes = peak
	if peak > p.stats.HighWater {
		p.stats.HighWater = peak
	}
	if descriptorCharge != 0 {
		oldCharge, err := arenaMul(uint64(cap(blocks)), arenaBlockBytes)
		if err != nil || oldCharge > a.stats.DescriptorBytes {
			return p, errArenaLimit
		}
		p.stats.DescriptorBytes, err = arenaAdd(a.stats.DescriptorBytes-oldCharge, descriptorCharge)
		if err != nil {
			return p, err
		}
		p.stats.ChargedBytes -= oldCharge
	}
	if p.ref.kind == arenaLarge {
		p.stats.LargeBytes, err = arenaAdd(p.stats.LargeBytes, allocation)
	} else {
		p.stats.NormalBytes, err = arenaAdd(p.stats.NormalBytes, allocation)
	}
	return p, err
}

// Append performs exactly one direct copy into final fixed backing storage.
// Empty and null each count as one admitted copy of zero bytes, without a slab.
// Errors return a zero reference and preserve all published state. Bytes written
// by a failed injected copy stay outside every published span and are overwritten
// before a later reference can expose them.
func (a *arena) Append(src []byte) (arenaRef, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, err := a.planAppend(uint64(len(src)), src == nil)
	if err != nil {
		return arenaRef{}, err
	}
	if p.ref.length == 0 {
		a.stats = p.stats
		return p.ref, nil
	}
	blocks := a.normal
	if p.ref.kind == arenaLarge {
		blocks = a.large
	}
	if p.descriptors != 0 {
		var replacement []arenaBlock
		if a.hooks.blocks == nil {
			replacement = make([]arenaBlock, p.descriptors)
		} else {
			replacement, err = a.hooks.blocks(p.descriptors)
		}
		if err != nil {
			return arenaRef{}, err
		}
		if len(replacement) != p.descriptors || cap(replacement) != p.descriptors {
			return arenaRef{}, errArenaAllocation
		}
		copy(replacement, blocks) // descriptors only, never source bytes
		blocks = replacement[:len(blocks)]
	}
	var destination []byte
	if p.newBlock {
		if a.hooks.bytes == nil {
			destination = make([]byte, p.allocation)
		} else {
			destination, err = a.hooks.bytes(p.allocation)
		}
		if err != nil {
			return arenaRef{}, err
		}
		if len(destination) != p.allocation || cap(destination) != p.allocation {
			return arenaRef{}, errArenaAllocation
		}
	} else {
		destination = blocks[p.ref.index].data
	}
	// Validation precedes integer conversion and every slice operation.
	end, err := arenaAdd(p.ref.offset, p.ref.length)
	if err != nil || end > uint64(len(destination)) {
		return arenaRef{}, errArenaLimit
	}
	dst := destination[int(p.ref.offset):int(end):int(end)]
	var copied int
	if a.hooks.copy == nil {
		copied = copy(dst, src)
	} else {
		copied, err = a.hooks.copy(dst, src)
	}
	if err != nil {
		return arenaRef{}, err
	}
	if copied != len(src) {
		return arenaRef{}, errArenaCopy
	}
	// No fallible work follows. Publish descriptors, cursor, and metrics together
	// under the lock, only after the destination is completely initialized.
	if p.newBlock {
		blocks = blocks[:len(blocks)+1]
	}
	blocks[p.ref.index] = arenaBlock{data: destination, used: end}
	if p.ref.kind == arenaLarge {
		a.large = blocks
	} else {
		a.normal, a.active = blocks, p.ref.index
	}
	a.stats = p.stats
	return p.ref, nil
}

// WithBytes lends an immutable, capacity-clipped view for this callback only.
// A nil callback or a malformed/stale/foreign reference fails without a callback.
// Successful null yields nil; present empty yields a non-nil zero-capacity slice.
func (a *arena) WithBytes(r arenaRef, fn func([]byte) error) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if fn == nil || r.owner != a.identity || r.generation != a.generation || r.owner == 0 {
		return errArenaRef
	}
	if r.kind == arenaEmpty || r.kind == arenaNull {
		if r.index != 0 || r.offset != 0 || r.length != 0 {
			return errArenaRef
		}
		if r.kind == arenaNull {
			return fn(nil)
		}
		return fn([]byte{})
	}
	blocks := a.normal
	if r.kind == arenaLarge {
		blocks = a.large
	} else if r.kind != arenaNormal {
		return errArenaRef
	}
	if r.length == 0 || r.index >= uint64(len(blocks)) {
		return errArenaRef
	}
	b := blocks[r.index]
	if b.used > uint64(len(b.data)) || r.offset > b.used || r.length > b.used-r.offset {
		return errArenaRef
	}
	end := r.offset + r.length // proven <= used <= len(data), hence fits int
	return fn(b.data[int(r.offset):int(end):int(end)])
}

// Reset invalidates references before any capacity reuse. Normal data and both
// descriptor arrays stay charged. Large data is dropped, never pooled or reused.
// A generation exhaustion error leaves the entire arena unchanged.
func (a *arena) Reset() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.identity == 0 || a.generation == 0 || a.generation == math.MaxUint64 {
		return errArenaLimit
	}
	a.generation++
	for i := range a.normal {
		a.normal[i].used = 0
	}
	clear(a.large[:cap(a.large)])
	a.large = a.large[:0]
	a.active = 0
	a.stats.ChargedBytes -= a.stats.LargeBytes
	a.stats.LargeBytes, a.stats.LogicalBytes, a.stats.CopyBytes, a.stats.CopyCount = 0, 0, 0, 0
	return nil
}

func (a *arena) Stats() arenaStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.stats
}
