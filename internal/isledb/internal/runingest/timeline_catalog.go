package runingest

import (
	"bytes"
	"errors"
	"hash/maphash"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

var (
	errTimelineEncoding  = errors.New("runingest: timeline must contain 1..512 exact bytes")
	errCatalogLimit      = errors.New("runingest: catalog capacity limit")
	errCatalogIdentity   = errors.New("runingest: invalid or stale catalog identity")
	errCatalogAllocation = errors.New("runingest: invalid catalog allocation")
)

// timelineID is dense insertion order, not a durable or globally unique name.
// Always use it with its originating catalogView. Rebinding an integer to a new
// view names that new generation's timeline, not the old identity. E06 owns the
// adapter to runfile's immutable Prepare input; E04 exposes no mutable slices.
type timelineID uint32

type timelineCatalogConfig struct {
	HardLimit                 uint64
	MaxTimelines              uint32
	SlabBytes, LargeThreshold uint64
}

type timelineHashSlot struct {
	hash     uint64
	id       timelineID
	occupied bool // zero hash and ID zero are both valid
}

// Reserved private state only. Admission, resolution, positioning and sealing
// transitions belong to later candidates, not to this catalog.
type timelineBatchState struct {
	startingHead, nextLSN, finalHead, eventCount uint64
	resolved, sealed                             bool
}

type timelineMetadata struct {
	ref   arenaRef
	state timelineBatchState
}

var (
	timelineSlotBytes     = uint64(reflect.TypeFor[timelineHashSlot]().Size())
	timelineMetadataBytes = uint64(reflect.TypeFor[timelineMetadata]().Size())
	timelineIDBytes       = uint64(reflect.TypeFor[timelineID]().Size())
	// Unlike the standalone arena, the catalog includes its fixed owner and
	// dedicated arena header as well as every requested backing capacity.
	timelineFixedBytes = uint64(reflect.TypeFor[timelineCatalog]().Size() + reflect.TypeFor[arena]().Size())
)

// Metrics describe this generation except HighWater, which spans resets.
// Probe metrics cover successful Intern and valid Lookup calls (including
// misses), separately from rehash work. Failed transactions leave all metrics
// unchanged. Counters saturate rather than wrap. Charges include load-factor
// headroom and old/new allocation overlap, but not allocator rounding, GC lag,
// caller-owned handles/destinations, or process RSS.
type timelineCatalogStats struct {
	DistinctTimelines, Insertions, CopiedTimelineBytes            uint64
	ArenaReservedBytes, ArenaMetadataBytes, TableReservedBytes    uint64
	StateIDMetadataBytes, ProjectionBytes, FixedBytes             uint64
	ChargedBytes, HighWater                                       uint64
	ProbeOperations, Probes, MaxProbe, RehashProbes, TableGrowths uint64
}

// Hooks are test-only. Like arena hooks, allocations must be fresh, exclusively
// owned, exact len/cap, and must not retain storage or reenter. Hash overrides
// must remain fixed for the catalog lifetime; production always uses maphash.
type timelineCatalogHooks struct {
	hash       func([]byte) uint64
	slots      func(int) ([]timelineHashSlot, error)
	metadata   func(int) ([]timelineMetadata, error)
	projection func(int) ([]timelineID, error)
}

// Never copy an initialized catalog. All access is synchronized; borrowed input
// stays immutable through a call. The arena is private and exclusively accessed
// under mu, making its allocation-free E03 plan stable until Append. No lock or
// callback lease escapes, so Reset cannot race a byte reader.
type timelineCatalog struct {
	mu         sync.RWMutex
	config     timelineCatalogConfig
	generation uint64
	seed       maphash.Seed
	arena      *arena
	slots      []timelineHashSlot
	metadata   []timelineMetadata
	order      []timelineID
	stats      timelineCatalogStats
	hooks      timelineCatalogHooks
}

// A view is the namespace for compact IDs. Reset invalidates every old view,
// value and projection, even when the same numeric ID is assigned again.
type catalogView struct {
	catalog    *timelineCatalog
	generation uint64
}
type timelineValue struct {
	view catalogView
	id   timelineID
}
type timelineProjection struct {
	view  catalogView
	count int
}

func newTimelineCatalog(config timelineCatalogConfig) (*timelineCatalog, error) {
	if config.MaxTimelines == 0 || config.HardLimit < timelineFixedBytes {
		return nil, errCatalogLimit
	}
	a, err := newArena(arenaConfig{config.SlabBytes, config.LargeThreshold, runcontract.MaxTimelineBytes, config.HardLimit - timelineFixedBytes})
	if err != nil {
		return nil, err
	}
	return &timelineCatalog{config: config, generation: 1, seed: maphash.MakeSeed(), arena: a,
		stats: timelineCatalogStats{FixedBytes: timelineFixedBytes, ChargedBytes: timelineFixedBytes, HighWater: timelineFixedBytes}}, nil
}

func (c *timelineCatalog) View() catalogView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return catalogView{c, c.generation}
}

func (v catalogView) valid() bool {
	return v.catalog != nil && v.generation != 0 && v.generation == v.catalog.generation
}

func validTimeline(src []byte) error {
	if len(src) == 0 || len(src) > runcontract.MaxTimelineBytes {
		return errTimelineEncoding
	}
	// Raw opaque identities have no escapes, UTF-8 or key framing to decode.
	return nil
}

func (c *timelineCatalog) hash(src []byte) uint64 {
	if c.hooks.hash != nil {
		return c.hooks.hash(src)
	}
	return maphash.Bytes(c.seed, src)
}

// bytes is private to locked catalog operations. Only successful E03 Append
// references enter metadata; neither references nor slices escape this owner.
func (c *timelineCatalog) bytes(id timelineID) []byte {
	r := c.metadata[id].ref
	blocks := c.arena.normal
	if r.kind == arenaLarge {
		blocks = c.arena.large
	}
	// E03 proved offset+length <= used <= len(data) when publishing this ref;
	// catalog locking prevents reset/reuse while these slices are inspected.
	b := blocks[r.index].data
	end := r.offset + r.length
	return b[r.offset:end:end]
}

// Probe at most len(slots) times, including a full table and wraparound. Slots
// are always a power of two. Hash equality only selects an exact comparison.
func (c *timelineCatalog) probe(slots []timelineHashSlot, hash uint64, src []byte) (index int, found bool, probes uint64) {
	if len(slots) == 0 {
		return -1, false, 0
	}
	i, mask := hash&uint64(len(slots)-1), uint64(len(slots)-1)
	for n := 1; n <= len(slots); n++ {
		s := slots[i]
		if !s.occupied {
			return int(i), false, uint64(n)
		}
		if s.hash == hash && bytes.Equal(c.bytes(s.id), src) {
			return int(i), true, uint64(n)
		}
		i = (i + 1) & mask
	}
	return -1, false, uint64(len(slots))
}

func catalogSaturatingAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func (c *timelineCatalog) recordProbe(n, longest uint64) {
	c.stats.ProbeOperations = catalogSaturatingAdd(c.stats.ProbeOperations, 1)
	c.stats.Probes = catalogSaturatingAdd(c.stats.Probes, n)
	c.stats.MaxProbe = max(c.stats.MaxProbe, longest)
}

// Counts follow E00's uint32 count contract: at most MaxUint32 entries, IDs
// [0, MaxUint32). The configured boundary exercises this without huge arrays.
func catalogNextID(count uint64, limit uint32) (timelineID, error) {
	if count >= uint64(limit) {
		return 0, errCatalogLimit
	}
	return timelineID(count), nil
}

// Full replacement capacity (not the delta) is charged before make. The 3/4
// table load and doubling policy are private implementation choices, not E19
// operational defaults. All size conversions and multiplications are checked.
func catalogCapacity(need, old, maximum, width uint64) (int, uint64, error) {
	if need > maximum {
		return 0, 0, errCatalogLimit
	}
	if need <= old {
		return 0, 0, nil
	}
	n := max(uint64(1), old)
	for n < need {
		if n > maximum/2 {
			n = maximum
		} else {
			n *= 2
		}
	}
	charge, err := arenaMul(n, width)
	if err != nil {
		return 0, 0, err
	}
	if _, err = arenaInt(charge); err != nil {
		return 0, 0, err
	}
	i, err := arenaInt(n)
	return i, charge, err
}

type catalogInsertPlan struct {
	slots, metadata                   int
	tableCharge, metadataCharge, peak uint64
}

func (c *timelineCatalog) planInsert(length int) (catalogInsertPlan, error) {
	p := catalogInsertPlan{}
	need := uint64(len(c.metadata)) + 1
	if _, err := catalogNextID(need-1, c.config.MaxTimelines); err != nil {
		return p, err
	}
	// At most ceil(MaxUint32 / .75) slots, rounded up to 2^33.
	tableNeed := uint64(len(c.slots))
	if need > tableNeed-tableNeed/4 {
		tableNeed = max(uint64(8), tableNeed*2)
	}
	var err error
	p.slots, p.tableCharge, err = catalogCapacity(tableNeed, uint64(len(c.slots)), 1<<33, timelineSlotBytes)
	if err != nil {
		return p, err
	}
	p.metadata, p.metadataCharge, err = catalogCapacity(need, uint64(cap(c.metadata)), uint64(c.config.MaxTimelines), timelineMetadataBytes)
	if err != nil {
		return p, err
	}
	ap, err := c.arena.planAppend(uint64(length), false)
	if err != nil {
		return p, err
	}
	// E03's historical high-water cannot describe this transaction. Its plan
	// names the new block and full replacement descriptor capacity explicitly.
	extra, err := arenaMul(uint64(ap.descriptors), arenaBlockBytes)
	if err != nil {
		return p, err
	}
	p.peak = c.stats.ChargedBytes
	for _, charge := range [...]uint64{p.tableCharge, p.metadataCharge, extra, uint64(ap.allocation)} {
		p.peak, err = arenaAdd(p.peak, charge)
		if err != nil || p.peak > c.config.HardLimit {
			return p, errCatalogLimit
		}
	}
	return p, nil
}

func (v catalogView) Intern(src []byte) (timelineID, error) {
	if v.catalog == nil {
		return 0, errCatalogIdentity
	}
	c := v.catalog
	c.mu.Lock()
	defer c.mu.Unlock()
	if !v.valid() {
		return 0, errCatalogIdentity
	}
	if err := validTimeline(src); err != nil {
		return 0, err
	}
	hash := c.hash(src) // borrowed bytes, before any owned byte copy
	index, found, probes := c.probe(c.slots, hash, src)
	longest := probes
	if found {
		c.recordProbe(probes, longest)
		c.stats.Insertions = catalogSaturatingAdd(c.stats.Insertions, 1)
		return c.slots[index].id, nil
	}
	p, err := c.planInsert(len(src))
	if err != nil {
		return 0, err
	}
	// The catalog lock holds this composite reservation until commit/abort.
	// Allocate all fallible metadata BEFORE calling transactional arena.Append.
	slots, metadata := c.slots, c.metadata
	var rehashProbes uint64
	if p.slots != 0 {
		if c.hooks.slots == nil {
			slots = make([]timelineHashSlot, p.slots)
		} else {
			slots, err = c.hooks.slots(p.slots)
		}
		if err != nil {
			return 0, err
		}
		if len(slots) != p.slots || cap(slots) != p.slots {
			return 0, errCatalogAllocation
		}
		clear(slots)
		for _, s := range c.slots {
			if !s.occupied {
				continue
			}
			i, _, n := c.probe(slots, s.hash, c.bytes(s.id))
			if i < 0 {
				return 0, errCatalogLimit
			}
			slots[i] = s
			rehashProbes = catalogSaturatingAdd(rehashProbes, n)
		}
		var n uint64
		index, _, n = c.probe(slots, hash, src)
		probes += n
		longest = max(longest, n)
	}
	if index < 0 {
		return 0, errCatalogLimit
	}
	if p.metadata != 0 {
		if c.hooks.metadata == nil {
			metadata = make([]timelineMetadata, p.metadata)
		} else {
			metadata, err = c.hooks.metadata(p.metadata)
		}
		if err != nil {
			return 0, err
		}
		if len(metadata) != p.metadata || cap(metadata) != p.metadata {
			return 0, errCatalogAllocation
		}
		copy(metadata, c.metadata) // references/state only
		metadata = metadata[:len(c.metadata)]
	}
	ref, err := c.arena.Append(src)
	if err != nil {
		return 0, err
	}
	// No fallible operation follows the one exact byte copy.
	id := timelineID(len(metadata))
	metadata = metadata[:len(metadata)+1]
	metadata[id] = timelineMetadata{ref: ref}
	slots[index] = timelineHashSlot{hash: hash, id: id, occupied: true}
	c.slots, c.metadata, c.order = slots, metadata, c.order[:0]
	c.recordProbe(probes, longest)
	c.stats.Insertions = catalogSaturatingAdd(c.stats.Insertions, 1)
	c.stats.RehashProbes = catalogSaturatingAdd(c.stats.RehashProbes, rehashProbes)
	if p.slots != 0 {
		c.stats.TableGrowths++
	}
	c.refreshCharge(p.peak)
	return id, nil
}

func (v catalogView) Lookup(src []byte) (timelineID, bool, error) {
	if v.catalog == nil {
		return 0, false, errCatalogIdentity
	}
	c := v.catalog
	c.mu.Lock()
	defer c.mu.Unlock()
	if !v.valid() {
		return 0, false, errCatalogIdentity
	}
	if err := validTimeline(src); err != nil {
		return 0, false, err
	}
	i, found, n := c.probe(c.slots, c.hash(src), src)
	c.recordProbe(n, n)
	if !found {
		return 0, false, nil
	}
	return c.slots[i].id, true, nil
}

func (c *timelineCatalog) refreshCharge(peak uint64) {
	a := c.arena.Stats()
	s := &c.stats
	s.DistinctTimelines, s.CopiedTimelineBytes = uint64(len(c.metadata)), a.CopyBytes
	s.ArenaReservedBytes, s.ArenaMetadataBytes = a.NormalBytes+a.LargeBytes, a.DescriptorBytes
	s.TableReservedBytes = uint64(cap(c.slots)) * timelineSlotBytes
	s.StateIDMetadataBytes = uint64(cap(c.metadata)) * timelineMetadataBytes
	s.ProjectionBytes = uint64(cap(c.order)) * timelineIDBytes
	s.ChargedBytes = s.FixedBytes + a.ChargedBytes + s.TableReservedBytes + s.StateIDMetadataBytes + s.ProjectionBytes
	s.HighWater = max(s.HighWater, peak)
}

func (c *timelineCatalog) Stats() timelineCatalogStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (v catalogView) Len() (int, error) {
	if v.catalog == nil {
		return 0, errCatalogIdentity
	}
	v.catalog.mu.RLock()
	defer v.catalog.mu.RUnlock()
	if !v.valid() {
		return 0, errCatalogIdentity
	}
	return len(v.catalog.metadata), nil
}

func (v catalogView) Timeline(id timelineID) (timelineValue, error) {
	if v.catalog == nil {
		return timelineValue{}, errCatalogIdentity
	}
	v.catalog.mu.RLock()
	defer v.catalog.mu.RUnlock()
	if !v.valid() || uint64(id) >= uint64(len(v.catalog.metadata)) {
		return timelineValue{}, errCatalogIdentity
	}
	return timelineValue{v, id}, nil
}

// inspect never exposes its slice outside this file. Public value operations
// return scalars or copy into explicitly caller-owned storage, never an alias.
func (v timelineValue) inspect(fn func([]byte) error) error {
	c := v.view.catalog
	if c == nil {
		return errCatalogIdentity
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !v.view.valid() || uint64(v.id) >= uint64(len(c.metadata)) {
		return errCatalogIdentity
	}
	return fn(c.bytes(v.id))
}

func (v timelineValue) Len() (n int, err error) {
	err = v.inspect(func(b []byte) error { n = len(b); return nil })
	return
}

func (v timelineValue) Equal(src []byte) (equal bool, err error) {
	err = v.inspect(func(b []byte) error { equal = bytes.Equal(b, src); return nil })
	return
}

func (v timelineValue) CopyTo(dst []byte) (n int, err error) {
	err = v.inspect(func(b []byte) error {
		if len(dst) < len(b) {
			return errCatalogLimit
		}
		n = copy(dst, b)
		return nil
	})
	return
}

// SortedIDs sorts only IDs. A projection expires on the next distinct insert
// or reset; duplicates and repeated projection calls leave it valid. It owns
// no backing slice, so an old handle cannot retain uncharged replacement arrays.
func (v catalogView) SortedIDs() (timelineProjection, error) {
	if v.catalog == nil {
		return timelineProjection{}, errCatalogIdentity
	}
	c := v.catalog
	c.mu.Lock()
	defer c.mu.Unlock()
	if !v.valid() {
		return timelineProjection{}, errCatalogIdentity
	}
	if len(c.order) == len(c.metadata) {
		return timelineProjection{v, len(c.order)}, nil
	}
	n, charge, err := catalogCapacity(uint64(len(c.metadata)), uint64(cap(c.order)), uint64(c.config.MaxTimelines), timelineIDBytes)
	if err != nil {
		return timelineProjection{}, err
	}
	peak, err := arenaAdd(c.stats.ChargedBytes, charge)
	if err != nil || peak > c.config.HardLimit {
		return timelineProjection{}, errCatalogLimit
	}
	order := c.order
	if n != 0 {
		if c.hooks.projection == nil {
			order = make([]timelineID, n)
		} else {
			order, err = c.hooks.projection(n)
		}
		if err != nil {
			return timelineProjection{}, err
		}
		if len(order) != n || cap(order) != n {
			return timelineProjection{}, errCatalogAllocation
		}
	}
	order = order[:len(c.metadata)]
	for i := range order {
		order[i] = timelineID(i)
	}
	slices.SortFunc(order, func(a, b timelineID) int { return bytes.Compare(c.bytes(a), c.bytes(b)) })
	c.order = order
	c.refreshCharge(peak)
	return timelineProjection{v, len(order)}, nil
}

func (p timelineProjection) Len() (int, error) {
	c := p.view.catalog
	if c == nil {
		return 0, errCatalogIdentity
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !p.view.valid() || p.count != len(c.metadata) {
		return 0, errCatalogIdentity
	}
	return p.count, nil
}

func (p timelineProjection) ID(index int) (timelineID, error) {
	c := p.view.catalog
	if c == nil {
		return 0, errCatalogIdentity
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !p.view.valid() || p.count != len(c.metadata) || index < 0 || index >= p.count {
		return 0, errCatalogIdentity
	}
	return c.order[index], nil
}

func (c *timelineCatalog) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation == 0 || c.generation == math.MaxUint64 {
		return errCatalogIdentity
	}
	if err := c.arena.Reset(); err != nil {
		return err
	}
	c.generation++
	clear(c.slots)
	clear(c.metadata[:cap(c.metadata)])
	c.metadata, c.order = c.metadata[:0], c.order[:0]
	c.stats = timelineCatalogStats{FixedBytes: timelineFixedBytes, HighWater: c.stats.HighWater}
	c.refreshCharge(c.stats.HighWater)
	return nil
}
