package runingest

import (
	"errors"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

type BatchConfig struct {
	TargetRunBytes, MaxSlotChargedBytes, MaxEventBytes uint64
	MaxRecords, MaxTimelines                           uint32
	MaxResidence                                       time.Duration
	SlabBytes, LargeThreshold, HeadroomBytes           uint64
	ExpectedOffset                                     int64
	ExpectedLeaderEpoch                                int32
	Credits                                            MemoryCredits
}

// SealReason order is also the deterministic precedence for simultaneous
// automatic triggers. Epoch changes are a separate source boundary.
type SealReason uint8

const (
	SealNone SealReason = iota
	SealTargetBytes
	SealChargedBytes
	SealRecordCount
	SealTimelineCount
	SealResidence
	SealControl
	SealRebalance
	SealShutdown
	SealMemoryPressure
	SealScratchPressure
	SealObjectIOPressure
	SealL0Pressure
	SealLeaderEpoch
)

// SealSignals carries only explicit control/pressure observations. It contains
// no polling, rebalance, I/O, or compaction runtime.
type SealSignals uint16

func (r SealReason) Signal() SealSignals { return SealSignals(1) << r }

const externalSealSignals = SealSignals(1<<SealControl | 1<<SealRebalance | 1<<SealShutdown |
	1<<SealMemoryPressure | 1<<SealScratchPressure | 1<<SealObjectIOPressure | 1<<SealL0Pressure)

type AppendDisposition uint8

const (
	AppendInvalid AppendDisposition = iota // permanent source/configured absolute limit
	AppendMutable
	AppendSealed
	AppendSealFirst   // not accepted; caller must explicitly Seal and retry elsewhere
	AppendSingleton   // accepted oversized singleton, sealed within absolute limits
	AppendRetry       // recoverable allocation/credit/encoding failure; no slot change
	AppendUnavailable // already sealed or released
)

type AppendResult struct {
	Disposition AppendDisposition
	Reason      SealReason
	RolledBack  bool // reservation acquired then aborted; diagnostics stay caller-owned
}

// Private fault seams follow the E03/E04 fresh, exclusive allocation contract.
// No hook may retain source/destination views or reenter the slot.
type batchHooks struct {
	records func(int) ([]recordRef, error)
	encode  func([]byte, runcontract.Event) ([]byte, error)
}

// One synchronous append owner; the mutex also permits safe handoff to sealed
// readers and terminal Close. Arenas/catalog never escape as mutable handles.
// There is deliberately no Reset: slot reuse/publication lifecycle is later work.
type batchSlot struct {
	mu         sync.RWMutex
	config     BatchConfig
	payload    *arena
	catalog    *timelineCatalog
	records    []recordRef
	interval   SourceInterval
	accounting BatchAccounting
	started    time.Time
	reason     SealReason
	closed     bool
	sealed     sealedBatch
	hooks      batchHooks
}

// sealedBatch is an immutable handle, invalidated only by terminal Close after
// its readers finish. It owns no backing arrays and exposes no mutable slices.
type sealedBatch struct{ slot *batchSlot }

func newBatchSlot(c BatchConfig) (*batchSlot, error) {
	base, err := arenaAdd(batchFixedBytes, c.HeadroomBytes)
	if err != nil || c.TargetRunBytes == 0 || c.MaxEventBytes < runcontract.EventFixedBytes ||
		c.MaxEventBytes > runcontract.MaxValueBytes || c.MaxRecords == 0 || c.MaxTimelines == 0 ||
		c.MaxResidence <= 0 || c.ExpectedOffset < 0 || c.ExpectedLeaderEpoch < -1 ||
		c.SlabBytes == 0 || c.SlabBytes > runcontract.MaxValueBytes ||
		c.LargeThreshold == 0 || c.LargeThreshold > c.SlabBytes || base >= c.MaxSlotChargedBytes {
		return nil, ErrBatchConfig
	}
	if c.Credits != nil {
		if err := c.Credits.Reserve(base); err != nil {
			return nil, err
		}
	}
	payload, err := newArena(arenaConfig{c.SlabBytes, c.LargeThreshold, c.MaxEventBytes, c.MaxSlotChargedBytes})
	var catalog *timelineCatalog
	if err == nil {
		catalog, err = newTimelineCatalog(timelineCatalogConfig{c.MaxSlotChargedBytes, c.MaxTimelines, c.SlabBytes, c.LargeThreshold})
	}
	if err != nil {
		if c.Credits != nil {
			c.Credits.Release(base)
		}
		return nil, err
	}
	s := &batchSlot{config: c, payload: payload, catalog: catalog,
		interval: SourceInterval{Expected: c.ExpectedOffset, FirstObserved: -1, Next: c.ExpectedOffset,
			ExpectedEpoch: c.ExpectedLeaderEpoch, IntervalEpoch: c.ExpectedLeaderEpoch}}
	s.sealed.slot = s
	s.refreshAccounting(base)
	return s, nil
}

type batchAppendPlan struct {
	payload        arenaAppendPlan
	recordCapacity int
	peak, extra    uint64
	estimate       uint64
	distinct       bool
}

// Called only under the exclusive slot lock. All child storage is private:
// no catalog/arena mutation can invalidate the allocation-free E03/E04 plans.
func (s *batchSlot) plan(r BorrowedRecord, size int) (batchAppendPlan, error) {
	p := batchAppendPlan{}
	_, found, _ := s.catalog.probe(s.catalog.slots, s.catalog.hash(r.Timeline), r.Timeline)
	p.distinct = !found
	var err error
	p.estimate, err = arenaAdd(s.accounting.EstimatedRunBytes,
		estimateRecord(size, r.Timeline, p.distinct, len(s.records) == 0))
	if err != nil {
		return p, err
	}
	p.peak = s.accounting.ChargedBytes
	if p.distinct {
		cp, err := s.catalog.planInsert(len(r.Timeline))
		if err != nil {
			return p, err
		}
		p.peak, err = arenaAdd(p.peak, cp.peak-s.catalog.stats.ChargedBytes)
		if err != nil {
			return p, err
		}
	}
	p.payload, err = s.payload.planAppend(uint64(size), false)
	if err != nil {
		return p, err
	}
	var descriptorCharge uint64
	p.recordCapacity, descriptorCharge, err = catalogCapacity(uint64(len(s.records))+1,
		uint64(cap(s.records)), uint64(s.config.MaxRecords), recordRefBytes)
	if err != nil {
		return p, err
	}
	blockCharge, err := arenaMul(uint64(p.payload.descriptors), arenaBlockBytes)
	if err != nil {
		return p, err
	}
	for _, n := range [...]uint64{descriptorCharge, blockCharge, uint64(p.payload.allocation)} {
		p.peak, err = arenaAdd(p.peak, n)
		if err != nil {
			return p, err
		}
	}
	p.extra = p.peak - s.accounting.ChargedBytes
	return p, nil
}

// Use the same E03/E04 allocation-free planners on empty planning state when a
// record hits a batch boundary. No constructors, identities, hashes, source
// copies or credits are acquired. This distinguishes a seal-and-retry boundary
// from an event that cannot fit even as a singleton under the absolute limit.
func (s *batchSlot) singletonFits(size, timelineLength int) bool {
	payload := arena{config: s.payload.config, identity: 1, generation: 1}
	timelines := arena{config: s.catalog.arena.config, identity: 1, generation: 1}
	catalog := timelineCatalog{config: s.catalog.config, arena: &timelines,
		stats: timelineCatalogStats{ChargedBytes: timelineFixedBytes}}
	ap, err := payload.planAppend(uint64(size), false)
	if err != nil {
		return false
	}
	cp, err := catalog.planInsert(timelineLength)
	if err != nil {
		return false
	}
	peak := batchFixedBytes - timelineFixedBytes
	for _, n := range [...]uint64{s.config.HeadroomBytes, cp.peak, ap.stats.ChargedBytes, recordRefBytes} {
		peak, err = arenaAdd(peak, n)
		if err != nil || peak > s.config.MaxSlotChargedBytes {
			return false
		}
	}
	return true
}

// AppendBorrowed validates and reserves the whole transaction before allocating.
// now is an explicit monotonic residence clock, independent of source timestamp.
// A rejection does not even change the seal decision. AppendSealFirst requires
// the owner to call Seal(result.Reason) before retrying in another empty slot.
func (s *batchSlot) AppendBorrowed(r BorrowedRecord, now time.Time) (AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.payload == nil {
		return AppendResult{Disposition: AppendUnavailable}, ErrBatchClosed
	}
	if s.reason != SealNone {
		return AppendResult{Disposition: AppendUnavailable, Reason: s.reason}, ErrBatchSealed
	}
	size, err := r.validate(s.interval.Next, s.interval.IntervalEpoch, s.config.MaxEventBytes)
	if err != nil {
		return AppendResult{Disposition: AppendInvalid}, err
	}
	if len(s.records) != 0 && now.Before(s.started) {
		return AppendResult{Disposition: AppendInvalid}, ErrBatchClock
	}
	if len(s.records) != 0 && r.LeaderEpoch != s.interval.IntervalEpoch {
		return AppendResult{Disposition: AppendSealFirst, Reason: SealLeaderEpoch}, nil
	}
	p, err := s.plan(r, size)
	if len(s.records) != 0 && (err != nil || p.peak > s.config.MaxSlotChargedBytes || p.estimate > s.config.TargetRunBytes) &&
		!s.singletonFits(size, len(r.Timeline)) {
		return AppendResult{Disposition: AppendInvalid}, ErrBatchLimit
	}
	// Target has precedence over charged capacity when both boundaries apply.
	// plan computes the estimate before any capacity check and never mutates.
	if len(s.records) != 0 && p.estimate > s.config.TargetRunBytes {
		return AppendResult{Disposition: AppendSealFirst, Reason: SealTargetBytes}, nil
	}
	if err != nil || p.peak > s.config.MaxSlotChargedBytes {
		if len(s.records) != 0 {
			return AppendResult{Disposition: AppendSealFirst, Reason: SealChargedBytes}, nil
		}
		return AppendResult{Disposition: AppendInvalid}, errors.Join(ErrBatchLimit, err)
	}
	if len(s.records) != 0 {
		if now.Sub(s.started) >= s.config.MaxResidence {
			return AppendResult{Disposition: AppendSealFirst, Reason: SealResidence}, nil
		}
	}
	if s.config.Credits != nil {
		if err := s.config.Credits.Reserve(p.extra); err != nil {
			return AppendResult{Disposition: AppendRetry}, err
		}
	}
	result, err := s.appendReserved(r, now, p)
	if err != nil {
		if s.config.Credits != nil {
			s.config.Credits.Release(p.extra)
		}
		return AppendResult{Disposition: AppendRetry, RolledBack: true}, err
	}
	// Replacement arrays are now unreachable. Release only their overlap; the
	// final capacity and configured headroom stay charged until terminal Close.
	if s.config.Credits != nil && p.peak > s.accounting.ChargedBytes {
		s.config.Credits.Release(p.peak - s.accounting.ChargedBytes)
	}
	return result, nil
}

func (s *batchSlot) appendReserved(r BorrowedRecord, now time.Time, p batchAppendPlan) (AppendResult, error) {
	records := s.records
	var err error
	if p.recordCapacity != 0 {
		if s.hooks.records == nil {
			records = make([]recordRef, p.recordCapacity)
		} else {
			records, err = s.hooks.records(p.recordCapacity)
		}
		if err != nil {
			return AppendResult{}, err
		}
		if len(records) != p.recordCapacity || cap(records) != p.recordCapacity {
			return AppendResult{}, errArenaAllocation
		}
		copy(records, s.records) // scalar descriptors only
		records = records[:len(s.records)]
	}
	blocks, destination, err := s.stagePayload(p.payload, r.event())
	if err != nil {
		return AppendResult{}, err
	}
	// LAST fallible step: E04 Intern is itself transactional. Encoding before
	// interning avoids needing an E04 delete/undo operation. Neither payload nor
	// record descriptors have been published if catalog insertion fails.
	id, err := s.catalog.View().Intern(r.Timeline)
	if err != nil {
		return AppendResult{}, err
	}
	// No fallible work follows. Publish only initialized spans under slot.mu.
	ap := p.payload
	if ap.newBlock {
		blocks = blocks[:len(blocks)+1]
	}
	blocks[ap.ref.index] = arenaBlock{data: destination, used: ap.ref.offset + ap.ref.length}
	if ap.ref.kind == arenaLarge {
		s.payload.large = blocks
	} else {
		s.payload.normal, s.payload.active = blocks, ap.ref.index
	}
	s.payload.stats = ap.stats
	records = records[:len(records)+1]
	records[len(records)-1] = recordRef{Offset: r.Offset, TimestampMS: r.TimestampMS,
		Value: ap.ref, Timeline: id, LeaderEpoch: r.LeaderEpoch, Flags: r.Flags}
	if len(s.records) == 0 {
		s.started, s.interval.FirstObserved, s.interval.IntervalEpoch = now, r.Offset, r.LeaderEpoch
	}
	if r.Offset > s.interval.Next {
		s.interval.GapCount++
		s.interval.MissingOffsets += uint64(r.Offset - s.interval.Next)
	}
	s.interval.Next = r.Offset + 1 // validated < MaxInt64
	s.records = records
	s.accounting.CanonicalBytes += ap.ref.length
	s.accounting.EstimatedRunBytes = p.estimate
	s.accounting.Copies = addCopies(s.accounting.Copies, r, p.distinct)
	s.refreshAccounting(p.peak)
	s.reason = s.automaticReason(now, 0)
	if len(records) == 1 && p.estimate > s.config.TargetRunBytes {
		return AppendResult{Disposition: AppendSingleton, Reason: s.reason}, nil
	}
	if s.reason != SealNone {
		return AppendResult{Disposition: AppendSealed, Reason: s.reason}, nil
	}
	return AppendResult{Disposition: AppendMutable}, nil
}

// stagePayload uses E03's checked placement plan and allocation hooks, but writes
// the codec directly into uninitialized tail storage instead of copying an
// intermediate value. Only this E05 owner accesses the dedicated payload arena.
// Failed writes can dirty unused tail bytes, never an initialized/published span.
func (s *batchSlot) stagePayload(p arenaAppendPlan, event runcontract.Event) ([]arenaBlock, []byte, error) {
	a := s.payload
	blocks := a.normal
	if p.ref.kind == arenaLarge {
		blocks = a.large
	}
	if p.descriptors != 0 {
		var replacement []arenaBlock
		var err error
		if a.hooks.blocks == nil {
			replacement = make([]arenaBlock, p.descriptors)
		} else {
			replacement, err = a.hooks.blocks(p.descriptors)
		}
		if err != nil {
			return nil, nil, err
		}
		if len(replacement) != p.descriptors || cap(replacement) != p.descriptors {
			return nil, nil, errArenaAllocation
		}
		copy(replacement, blocks)
		blocks = replacement[:len(blocks)]
	}
	var destination []byte
	if p.newBlock {
		var err error
		if a.hooks.bytes == nil {
			destination = make([]byte, p.allocation)
		} else {
			destination, err = a.hooks.bytes(p.allocation)
		}
		if err != nil {
			return nil, nil, err
		}
		if len(destination) != p.allocation || cap(destination) != p.allocation {
			return nil, nil, errArenaAllocation
		}
	} else {
		destination = blocks[p.ref.index].data
	}
	end, err := arenaAdd(p.ref.offset, p.ref.length)
	if err != nil || end > uint64(len(destination)) {
		return nil, nil, errArenaLimit
	}
	dst := destination[int(p.ref.offset):int(end):int(end)]
	encode := s.hooks.encode
	if encode == nil {
		encode = runcontract.EncodeEvent
	}
	out, err := encode(dst, event)
	if err != nil {
		return nil, nil, err
	}
	if len(out) != len(dst) || &out[0] != &dst[0] {
		return nil, nil, errArenaCopy
	}
	return blocks, destination, nil
}

func (s *batchSlot) automaticReason(now time.Time, signals SealSignals) SealReason {
	if len(s.records) == 0 {
		return SealNone
	}
	switch {
	case s.accounting.EstimatedRunBytes >= s.config.TargetRunBytes:
		return SealTargetBytes
	case s.accounting.ChargedBytes >= s.config.MaxSlotChargedBytes:
		return SealChargedBytes
	case uint64(len(s.records)) >= uint64(s.config.MaxRecords):
		return SealRecordCount
	case uint64(len(s.catalog.metadata)) >= uint64(s.config.MaxTimelines):
		return SealTimelineCount
	case now.Sub(s.started) >= s.config.MaxResidence:
		return SealResidence
	}
	for r := SealControl; r <= SealL0Pressure; r++ {
		if signals&r.Signal() != 0 {
			return r
		}
	}
	return SealNone
}

// Seal is explicit and idempotent. Empty slots remain mutable and produce no
// sealed handle, even for timer/control signals. A rejected append's reason can
// be passed here by the owner; the rejected append itself never seals the slot.
func (s *batchSlot) Seal(reason SealReason) (*sealedBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.payload == nil {
		return nil, ErrBatchClosed
	}
	if s.reason != SealNone {
		return &s.sealed, nil
	}
	if reason <= SealNone || reason > SealLeaderEpoch {
		return nil, ErrBatchConfig
	}
	if len(s.records) == 0 {
		return nil, nil
	}
	s.reason = reason
	return &s.sealed, nil
}

// PollSeal evaluates the residence boundary with an injected timestamp plus
// simultaneous control/pressure signals, using the documented precedence.
func (s *batchSlot) PollSeal(now time.Time, signals SealSignals) (*sealedBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.payload == nil {
		return nil, ErrBatchClosed
	}
	if s.reason != SealNone {
		return &s.sealed, nil
	}
	if signals & ^externalSealSignals != 0 {
		return nil, ErrBatchConfig
	}
	if len(s.records) != 0 && now.Before(s.started) {
		return nil, ErrBatchClock
	}
	reason := s.automaticReason(now, signals)
	if reason == SealNone {
		return nil, nil
	}
	s.reason = reason
	return &s.sealed, nil
}

func (s *batchSlot) Accounting() BatchAccounting {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accounting
}

func (s *batchSlot) Interval() SourceInterval {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.interval
}

// Close is terminal disposal, not reuse. It waits for reads and releases each
// retained credit once. Call only after the consumer is finished with the sealed
// input. Future operations/handles fail and cannot retain uncharged arrays.
func (s *batchSlot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.payload, s.catalog, s.records = nil, nil, nil
	if s.config.Credits != nil {
		s.config.Credits.Release(s.accounting.ChargedBytes)
	}
	s.accounting.ChargedBytes = 0
}

func (b *sealedBatch) inspect(fn func(*batchSlot) error) error {
	if b == nil || b.slot == nil {
		return ErrBatchClosed
	}
	s := b.slot
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.reason == SealNone {
		return ErrBatchClosed
	}
	return fn(s)
}

func (b *sealedBatch) Summary() (interval SourceInterval, accounting BatchAccounting, reason SealReason, err error) {
	err = b.inspect(func(s *batchSlot) error {
		interval, accounting, reason = s.interval, s.accounting, s.reason
		return nil
	})
	return
}

func (b *sealedBatch) Len() (n int, err error) {
	err = b.inspect(func(s *batchSlot) error { n = len(s.records); return nil })
	return
}

func (b *sealedBatch) Record(index int) (r recordRef, err error) {
	err = b.inspect(func(s *batchSlot) error {
		if index < 0 || index >= len(s.records) {
			return ErrBatchIndex
		}
		r = s.records[index]
		return nil
	})
	return
}

// CopyValue/CopyTimeline target caller-owned inspection buffers. No mutable
// alias or retained pre-SST copy is exposed. E06's builder adapter is separate.
func (b *sealedBatch) CopyValue(index int, dst []byte) (n int, err error) {
	err = b.inspect(func(s *batchSlot) error {
		if index < 0 || index >= len(s.records) {
			return ErrBatchIndex
		}
		return s.payload.WithBytes(s.records[index].Value, func(src []byte) error {
			if len(dst) < len(src) {
				return ErrBatchLimit
			}
			n = copy(dst, src)
			return nil
		})
	})
	return
}

func (b *sealedBatch) CopyTimeline(id timelineID, dst []byte) (n int, err error) {
	err = b.inspect(func(s *batchSlot) error {
		v, err := s.catalog.View().Timeline(id)
		if err != nil {
			return err
		}
		n, err = v.CopyTo(dst)
		return err
	})
	return
}
