package runingest

import "reflect"

// MemoryCredits is a synchronous process credit gate. Reserve either acquires
// the entire amount or returns an error without acquiring any. Release cannot
// fail. Implementations must not reenter the slot. A nil gate uses only the
// slot's local bound; the caller owns the gate's storage and synchronization.
type MemoryCredits interface {
	Reserve(bytes uint64) error
	Release(bytes uint64)
}

// CopyAccounting counts admitted source fields, including zero-byte nullable
// fields. Canonical framing is generated, not copied source data. HeaderCopies
// counts keys and values individually; TimelineCopies counts distinct values.
type CopyAccounting struct {
	ValueBytes, HeaderBytes, AnnotationBytes, TimelineBytes     uint64
	ValueCopies, HeaderCopies, AnnotationCopies, TimelineCopies uint64
}

type BatchAccounting struct {
	CanonicalBytes, EstimatedRunBytes                  uint64
	ChargedBytes, HighWater, FixedBytes, HeadroomBytes uint64
	PayloadCapacity, PayloadMetadata, CatalogBytes     uint64
	TimelineCapacity, CatalogTable, CatalogMetadata    uint64
	DescriptorCapacity, DescriptorBytes, TimelineCount uint64
	Copies                                             CopyAccounting
}

var (
	recordRefBytes = uint64(reflect.TypeFor[recordRef]().Size())
	// Includes all retained accounting/control metadata and the embedded sealed
	// handle. The catalog charge separately includes its arena and owner header.
	batchFixedBytes = uint64(reflect.TypeFor[batchSlot]().Size()+reflect.TypeFor[arena]().Size()) + timelineFixedBytes
)

// A private uncompressed sizing heuristic, not a persisted length or a memory
// bound: fixed run allowance + canonical values + exact escaped key lengths,
// internal-key trailers, and one Heads/filter allowance per distinct timeline.
// Compression, blocks, indexes and padding make actual output differ. E19 owns
// calibration; E05 never builds keys, copies payloads for sizing, or emits SSTs.
func estimateRecord(size int, timeline []byte, distinct, first bool) uint64 {
	escaped := uint64(len(timeline))
	for _, b := range timeline {
		if b == 0 {
			escaped++
		}
	}
	n := uint64(size) + 50 + escaped + 8
	if distinct {
		n += 42 + escaped + 32 + 8 + 16
	}
	if first {
		n += 512
	}
	return n
}

func (s *batchSlot) refreshAccounting(peak uint64) {
	p, c := s.payload.stats, s.catalog.stats
	a := &s.accounting
	a.FixedBytes, a.HeadroomBytes = batchFixedBytes, s.config.HeadroomBytes
	a.PayloadCapacity, a.PayloadMetadata = p.NormalBytes+p.LargeBytes, p.DescriptorBytes
	a.CatalogBytes = c.ChargedBytes
	a.TimelineCapacity, a.CatalogTable = c.ArenaReservedBytes, c.TableReservedBytes
	a.CatalogMetadata = c.ArenaMetadataBytes + c.StateIDMetadataBytes + c.ProjectionBytes
	a.DescriptorCapacity = uint64(cap(s.records))
	a.DescriptorBytes = a.DescriptorCapacity * recordRefBytes
	a.TimelineCount = uint64(len(s.catalog.metadata))
	a.ChargedBytes = batchFixedBytes - timelineFixedBytes + s.config.HeadroomBytes + p.ChargedBytes + c.ChargedBytes + a.DescriptorBytes
	a.HighWater = max(a.HighWater, peak, a.ChargedBytes)
}

func addCopies(c CopyAccounting, r BorrowedRecord, distinct bool) CopyAccounting {
	c.ValueBytes += uint64(len(r.Value))
	c.ValueCopies++
	c.AnnotationBytes += uint64(len(r.Annotations))
	c.AnnotationCopies++
	for _, h := range r.Headers {
		c.HeaderBytes += uint64(len(h.Key)) + uint64(len(h.Value))
		c.HeaderCopies += 2
	}
	if distinct {
		c.TimelineBytes += uint64(len(r.Timeline))
		c.TimelineCopies++
	}
	// E00 bounds every record at 16 MiB and count at uint32, so each total is
	// below 2^56 bytes / 2^45 fields. No uint64 counter can wrap in one slot.
	return c
}
