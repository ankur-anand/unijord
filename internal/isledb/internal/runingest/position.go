package runingest

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/ankur-anand/isledb/internal/runcontract"
	"github.com/ankur-anand/isledb/internal/runfile"
)

var (
	ErrResolvedHead   = errors.New("runingest: invalid aligned resolved head")
	ErrTimelineSealed = errors.New("runingest: event after timeline seal")
	ErrPositionBusy   = errors.New("runingest: positioning lease already in use")
	ErrPositionClosed = errors.New("runingest: positioning lease closed")
)

// catalogIdentity binds scalar results without retaining the catalog or arena.
// The arena owner ID is process-unique; Generation rejects catalog resets.
type catalogIdentity struct {
	Owner, Generation uint64
}

func (b *sealedBatch) CatalogIdentity() (identity catalogIdentity, err error) {
	err = b.inspect(func(s *batchSlot) error {
		s.catalog.mu.RLock()
		defer s.catalog.mu.RUnlock()
		identity = catalogIdentity{Owner: s.catalog.arena.identity, Generation: s.catalog.generation}
		return nil
	})
	return
}

// ResolvedHead is source-neutral. Catalog binds ID to both its exact catalog
// and generation; heads[i].ID must equal i. A missing head has Present=false
// and the zero Head. Booleans make invalid wire flag representations impossible;
// a resolver decoding persisted bytes must use runcontract.DecodeHead first.
// The caller owns this slice and must keep it immutable only during positionBatch.
type ResolvedHead struct {
	Catalog catalogIdentity
	ID      timelineID
	Present bool
	Head    runcontract.Head
}

type positionOptions struct {
	Namespace    [32]byte
	Shard        uint32
	NextSequence uint64
}

type positionedHead struct {
	head       runcontract.Head
	lastRecord uint32
}

// Charges are requested capacities, including the owner and embedded iterator
// scratch, not RSS/allocator rounding. SlotHighWater includes prepaid headroom.
type positionAccounting struct {
	OverlayBytes, EventsOrderBytes, HeadsOrderBytes, FixedBytes uint64
	ChargedBytes, HeadroomUsed, NewCredits, SlotHighWater       uint64
	IteratorScratchBytes                                        uint64
}

// Fault seams do not retain arguments or allocate storage. A failure after each
// phase exercises cleanup of the storage actually acquired up to that phase.
type positionHooks struct{ phase func(string) error }

// positionedBatch owns only scalar state and uint32 projections. It pins the
// slot with a read lease and exclusively leases the catalog mutex to serialize
// use of E05's headroom without mutating E03-E05. A second attempt fails busy.
// Do not call catalog methods while this lease is live. Sealed scalar/payload
// inspection remains available. Close this owner before calling slot.Close.
// Never copy an initialized owner. Its methods serialize access; callers must
// still obey EntryIterator's borrowed-slice lifetime (including during Close).
type positionedBatch struct {
	mu                        sync.Mutex
	slot                      *batchSlot
	options                   positionOptions
	lsns                      []uint64
	heads                     []positionedHead
	eventOrder, headOrder     []uint32
	seqHi, nextSequence       uint64
	accounting                positionAccounting
	closing, disposed, opened bool
	input                     positionedInput
}

var positionFixedBytes = uint64(reflect.TypeFor[positionedBatch]().Size())
var positionedHeadBytes = uint64(reflect.TypeFor[positionedHead]().Size())

func positionCharge(records, timelines uint64) (a positionAccounting, err error) {
	if records == 0 || records > math.MaxUint32 || timelines == 0 || timelines > records {
		return a, ErrBatchLimit
	}
	// Every requested allocation must fit int in bytes, not merely in elements.
	sizes := [4]uint64{records * 8, timelines * positionedHeadBytes, records * 4, timelines * 4}
	for _, n := range sizes {
		if _, err = arenaInt(n); err != nil {
			return a, err
		}
	}
	a.OverlayBytes = sizes[0] + sizes[1]
	a.EventsOrderBytes, a.HeadsOrderBytes = sizes[2], sizes[3]
	a.FixedBytes = positionFixedBytes
	a.IteratorScratchBytes = runcontract.MaxKeyBytes + (runcontract.MaxKeyBytes - 8) + runcontract.HeadBytes
	a.ChargedBytes = a.FixedBytes + a.OverlayBytes + a.EventsOrderBytes + a.HeadsOrderBytes
	return a, nil // uint32 counts and fixed scalar widths cannot overflow uint64.
}

func positionBatch(b *sealedBatch, heads []ResolvedHead, options positionOptions) (*positionedBatch, error) {
	return positionBatchWithHooks(b, heads, options, positionHooks{})
}

func positionBatchWithHooks(b *sealedBatch, heads []ResolvedHead, options positionOptions, hooks positionHooks) (result *positionedBatch, err error) {
	if b == nil || b.slot == nil {
		return nil, ErrBatchClosed
	}
	s := b.slot
	// Do not queue a read behind terminal Close while an earlier positioned
	// owner is pinning that Close. The retry must be able to return busy.
	if !s.mu.TryRLock() {
		return nil, ErrPositionBusy
	}
	if s.closed || s.reason == SealNone || s.catalog == nil {
		s.mu.RUnlock()
		return nil, ErrBatchClosed
	}
	if !s.catalog.mu.TryLock() {
		s.mu.RUnlock()
		return nil, ErrPositionBusy
	}
	var reserved uint64
	defer func() {
		if result == nil {
			if reserved != 0 && s.config.Credits != nil {
				s.config.Credits.Release(reserved)
			}
			s.catalog.mu.Unlock()
			s.mu.RUnlock()
		}
	}()
	if options.Namespace == ([32]byte{}) {
		return nil, runcontract.ErrEncoding
	}
	hi, next, err := runcontract.SequenceRange(options.NextSequence, uint64(len(s.records)))
	if err != nil {
		return nil, err
	}
	if len(heads) != len(s.catalog.metadata) {
		return nil, ErrResolvedHead
	}
	// Validate the entire aligned response before reserving or positioning. Strict
	// alignment proves uniqueness and completeness without a second seen array.
	var scratch [runcontract.HeadBytes]byte
	for i, h := range heads {
		if h.Catalog.Owner != s.catalog.arena.identity || h.Catalog.Generation == 0 ||
			h.Catalog.Generation != s.catalog.generation || uint64(h.ID) != uint64(i) {
			return nil, ErrResolvedHead
		}
		if !h.Present {
			if h.Head != (runcontract.Head{}) {
				return nil, ErrResolvedHead
			}
		} else if _, err := runcontract.EncodeHead(scratch[:], h.Head); err != nil {
			return nil, errors.Join(ErrResolvedHead, err)
		}
	}
	a, err := positionCharge(uint64(len(s.records)), uint64(len(heads)))
	if err != nil {
		return nil, err
	}
	a.HeadroomUsed = min(a.ChargedBytes, s.config.HeadroomBytes)
	a.NewCredits = a.ChargedBytes - a.HeadroomUsed
	peak, err := arenaAdd(s.accounting.ChargedBytes, a.NewCredits)
	if err != nil || peak > s.config.MaxSlotChargedBytes {
		return nil, ErrBatchLimit
	}
	a.SlotHighWater = max(peak, s.accounting.HighWater)
	if s.config.Credits != nil && a.NewCredits != 0 {
		if err := s.config.Credits.Reserve(a.NewCredits); err != nil {
			return nil, err
		}
	}
	reserved = a.NewCredits
	fault := func(phase string) error {
		if hooks.phase != nil {
			return hooks.phase(phase)
		}
		return nil
	}
	if err := fault("overlay"); err != nil {
		return nil, err
	}
	p := &positionedBatch{slot: s, options: options, seqHi: hi, nextSequence: next, accounting: a,
		lsns: make([]uint64, len(s.records)), heads: make([]positionedHead, len(heads))}
	if err := p.scan(heads); err != nil {
		return nil, err
	}
	if err := fault("events-projection"); err != nil {
		return nil, err
	}
	p.eventOrder = make([]uint32, len(s.records))
	if err := fault("heads-projection"); err != nil {
		return nil, err
	}
	p.headOrder = make([]uint32, len(heads))
	for i := range p.eventOrder {
		p.eventOrder[i] = uint32(i)
	}
	for i := range p.headOrder {
		p.headOrder[i] = uint32(i)
	}
	if err := fault("events-sort"); err != nil {
		return nil, err
	}
	p.sortEvents()
	if err := fault("heads-sort"); err != nil {
		return nil, err
	}
	p.sortHeads()
	if err := fault("iterators"); err != nil {
		return nil, err
	}
	p.input.owner = p
	p.input.events.input, p.input.heads.input = &p.input, &p.input
	return p, nil
}

func (p *positionedBatch) scan(resolved []ResolvedHead) error {
	for i, r := range resolved {
		p.heads[i].head = r.Head
		if !r.Present {
			p.heads[i].head.NextLSN = 1
		}
	}
	for i, r := range p.slot.records {
		h := &p.heads[r.Timeline]
		if h.head.Sealed {
			return ErrTimelineSealed
		}
		lsn, next, err := runcontract.AdvanceLSN(h.head.NextLSN)
		if err != nil {
			return err
		}
		p.lsns[i] = lsn
		h.head = runcontract.Head{NextLSN: next, LastOffset: uint64(r.Offset),
			TimestampPresent: r.Flags&RecordTimestampPresent != 0, Timestamp: r.TimestampMS,
			Sealed: r.Flags&RecordSeal != 0}
		h.lastRecord = uint32(i)
	}
	return nil
}

func (p *positionedBatch) compareEvents(a, b uint32) int {
	s := p.slot
	if c := bytes.Compare(s.catalog.bytes(s.records[a].Timeline), s.catalog.bytes(s.records[b].Timeline)); c != 0 {
		return c
	}
	if p.lsns[a] < p.lsns[b] {
		return -1
	}
	if p.lsns[a] > p.lsns[b] {
		return 1
	}
	// Sequence = base+record index, so ties use descending original index.
	if a > b {
		return -1
	}
	if a < b {
		return 1
	}
	return 0
}

func (p *positionedBatch) sortEvents() { slices.SortFunc(p.eventOrder, p.compareEvents) }
func (p *positionedBatch) sortHeads() {
	slices.SortFunc(p.headOrder, func(a, b uint32) int {
		return bytes.Compare(p.slot.catalog.bytes(timelineID(a)), p.slot.catalog.bytes(timelineID(b)))
	})
}

// Input opens the single iterator pair. The caller MUST close the returned
// input after runfile.Prepare returns (success or failure), not when its table
// iterators finish: the filter still borrows catalog bytes after Events/Heads.
func (p *positionedBatch) Input() (*positionedInput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil, ErrPositionClosed
	}
	if p.opened {
		return nil, ErrPositionBusy
	}
	p.opened = true
	return &p.input, nil
}

// Close is idempotent. An open input delays disposal until input.Close, keeping
// even a concurrently closing owner safe through Prepare and iterator cleanup.
func (p *positionedBatch) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closing = true
	if !p.opened || p.input.closed {
		p.dispose()
	}
	return nil
}

func (p *positionedBatch) dispose() {
	if p.disposed {
		return
	}
	p.disposed = true
	s := p.slot
	p.slot, p.lsns, p.heads, p.eventOrder, p.headOrder = nil, nil, nil, nil, nil
	if p.accounting.NewCredits != 0 && s.config.Credits != nil {
		s.config.Credits.Release(p.accounting.NewCredits)
	}
	s.catalog.mu.Unlock()
	s.mu.RUnlock()
}

type positionedInput struct {
	owner  *positionedBatch
	closed bool
	events eventIterator
	heads  headIterator
}

func (in *positionedInput) BuildInput() runfile.BuildInput {
	return runfile.BuildInput{Events: &in.events, Heads: &in.heads, Timelines: in}
}

func (in *positionedInput) Len() int {
	p := in.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	if in.closed || p.disposed {
		return 0
	}
	return len(p.heads)
}

func (in *positionedInput) Timeline(id runfile.TimelineID) []byte {
	p := in.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	if in.closed || p.disposed || uint64(id) >= uint64(len(p.heads)) {
		return nil
	}
	return p.slot.catalog.bytes(timelineID(id))
}

// Close ends the catalog lease, and therefore must not race Prepare or use of
// borrowed entry/catalog slices. Owner.Close may safely race those operations.
func (in *positionedInput) Close() error {
	p := in.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	if !in.closed {
		in.closed = true
		in.events.closed, in.heads.closed = true, true
		in.events.entry, in.heads.entry = runfile.Entry{}, runfile.Entry{}
	}
	if p.closing {
		p.dispose()
	}
	return nil
}
