package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"math"
)

const (
	RunManifestVersion           = 2
	MaxRunObjectBytes     int64  = 5 << 40
	MaxRunSequence        uint64 = 1<<56 - 1
	MaxManifestRuns              = 262144
	MaxRunLevels                 = 64
	MaxRunObjectKeyBytes         = 4096
	MaxRunCheckpointBytes        = 512 << 20
	MaxRunIndexBytes             = 2 << 20
	MaxRunPageBytes              = 32 << 20
	MaxRunPageEntries            = 1024
	MaxRunPageLevel              = 16
	MaxRunPageCount              = 1048576
	MaxRunOperationRuns          = 1024
)

var (
	ErrInvalidRunManifest   = errors.New("invalid run manifest")
	ErrUnsupportedRunFormat = errors.New("unsupported run manifest format")
	ErrRunManifestLimit     = errors.New("run manifest resource limit")
)

func runInvalid(field string) error { return fmt.Errorf("%w: %s", ErrInvalidRunManifest, field) }

// Validate does not inspect object bytes or allocate from ObjectSize. The first
// pass admits counts and exact encoded bytes before uniqueness maps/indexes.
func (m *RunManifest) Validate() error {
	if m == nil {
		return runInvalid("nil checkpoint")
	}
	if m.Version != RunManifestVersion {
		return ErrUnsupportedRunFormat
	}
	if m.NamespaceHash == [32]byte{} || m.Revision == 0 || m.NextSequence == 0 || m.NextSequence > MaxRunSequence+1 {
		return runInvalid("checkpoint identity/sequence")
	}
	if m.WriterFence != nil && (m.WriterFence.Epoch == 0 || m.WriterFence.OwnerID == [16]byte{}) {
		return runInvalid("writer fence")
	}
	if err := m.Source.validate(); err != nil {
		return err
	}
	if err := m.Receipts.validate(); err != nil {
		return err
	}
	if m.Source != nil {
		if m.Source.Identity.Namespace != m.NamespaceHash || m.Source.Identity.Shard != m.Shard || m.WriterFence == nil || m.WriterFence.Epoch != m.Source.Epoch || m.WriterFence.OwnerID != m.Source.OwnerID {
			return runInvalid("source/fence divergence")
		}
		if (m.Receipts.Count == 0 && (m.NextSequence != 1 || len(m.L0Runs) != 0 || len(m.Levels) != 0)) || m.Receipts.Count > m.NextSequence-1 {
			return runInvalid("source/receipt/sequence divergence")
		}
	} else if m.Receipts.Count != 0 {
		return runInvalid("receipts without source")
	}
	if len(m.Levels) > MaxRunLevels || len(m.L0Runs) > MaxManifestRuns || uint64(len(m.L0Runs))*8 > MaxRunIndexBytes {
		return ErrRunManifestLimit
	}
	count, size := len(m.L0Runs), uint64(62)+frontierWireSize(m.Receipts)
	if m.Source != nil {
		size += uint64(124 + len(m.Source.Identity.Cluster) + len(m.Source.Identity.TopicName))
	}
	if m.WriterFence != nil {
		size += 24
	}
	checkRuns := func(runs []RunMeta, level uint32) error {
		for i := range runs {
			r := &runs[i]
			if err := r.Validate(); err != nil {
				return err
			}
			if r.Level != level || r.NamespaceHash != m.NamespaceHash || r.Shard != m.Shard || r.SeqHi >= m.NextSequence {
				return runInvalid("run binding/level/sequence")
			}
			if r.Source != nil && (m.Source == nil || !r.Source.Equal(m.Source.Identity)) {
				return runInvalid("run/source identity")
			}
			if m.Source != nil && r.CreatorRole == 1 && r.Source == nil {
				return runInvalid("Kafka writer run without source")
			}
			if m.WriterFence == nil || (r.CreatorRole == 1 && r.CreatorEpoch > m.WriterFence.Epoch) {
				return runInvalid("run writer epoch")
			}
			if level > 0 && i > 0 && bytes.Compare(runs[i-1].MaxTimeline, r.MinTimeline) >= 0 {
				return runInvalid("lower-level order/overlap")
			}
			n := runWireSize(r)
			if n > MaxRunCheckpointBytes-size {
				return ErrRunManifestLimit
			}
			size += n
		}
		return nil
	}
	if err := checkRuns(m.L0Runs, 0); err != nil {
		return err
	}
	for i := range m.Levels {
		l := &m.Levels[i]
		if l.Number == 0 || l.Number > MaxRunLevels || (i > 0 && m.Levels[i-1].Number >= l.Number) {
			return runInvalid("level number/order")
		}
		if len(l.Runs) > MaxManifestRuns-count || size > MaxRunCheckpointBytes-8 {
			return ErrRunManifestLimit
		}
		count += len(l.Runs)
		size += 8
		if err := checkRuns(l.Runs, l.Number); err != nil {
			return err
		}
	}
	ids := make(map[[16]byte]struct{}, count)
	keys := make(map[string]struct{}, count)
	unique := func(runs []RunMeta) error {
		for i := range runs {
			r := &runs[i]
			if _, ok := ids[r.ID]; ok {
				return runInvalid("duplicate run ID")
			}
			if _, ok := keys[r.ObjectKey]; ok {
				return runInvalid("duplicate object key")
			}
			ids[r.ID], keys[r.ObjectKey] = struct{}{}, struct{}{}
		}
		return nil
	}
	if err := unique(m.L0Runs); err != nil {
		return err
	}
	for i := range m.Levels {
		if err := unique(m.Levels[i].Runs); err != nil {
			return err
		}
	}
	return nil
}

func validRunBounds(min, max []byte, limit int) bool {
	return len(min) > 0 && len(min) <= limit && len(max) > 0 && len(max) <= limit && bytes.Compare(min, max) <= 0
}

func (r *RunMeta) Validate() error {
	if r == nil {
		return runInvalid("nil run")
	}
	if r.Source != nil {
		if err := r.Source.Validate(); err != nil {
			return err
		}
		if r.Source.Namespace != r.NamespaceHash || r.Source.Shard != r.Shard {
			return runInvalid("RunMeta source binding")
		}
	}
	if r.ID == [16]byte{} || len(r.ObjectKey) == 0 || len(r.ObjectKey) > MaxRunObjectKeyBytes ||
		r.FormatVersion != 1 || r.NamespaceHash == [32]byte{} || r.CreatorEpoch == 0 ||
		(r.CreatorRole != 1 && r.CreatorRole != 2) || r.PublicationHash == [32]byte{} ||
		r.DirectoryHash == [32]byte{} || r.PayloadHash == [32]byte{} || r.Level > MaxRunLevels {
		return runInvalid("run identity/hash")
	}
	if r.SeqLo == 0 || r.SeqLo > r.SeqHi || r.SeqHi > MaxRunSequence {
		return runInvalid("run sequence")
	}
	// Admission uses the encoded UTC instant, not the caller's local calendar
	// year (a zone offset can cross either wire boundary).
	seconds := r.CreatedAt.Unix()
	if r.CreatedAt.IsZero() || seconds < -62135596800 || seconds > 253402300799 {
		return runInvalid("creation time")
	}
	if !validRunBounds(r.MinTimeline, r.MaxTimeline, 512) {
		return runInvalid("timeline bounds")
	}
	// Signed subtraction after positivity checks avoids offset/length wrapping.
	if r.ObjectSize < 128+326+160 || r.ObjectSize > MaxRunObjectBytes ||
		r.DirectoryOffset < 128 || r.DirectoryOffset%8 != 0 || r.DirectoryLength < 326 || r.DirectoryLength > 2<<20 ||
		r.DirectoryOffset > r.ObjectSize-160 || r.DirectoryLength != r.ObjectSize-160-r.DirectoryOffset {
		return runInvalid("object/directory geometry")
	}
	for _, t := range []*TableMeta{&r.Events, &r.Heads} {
		if err := validateRunRegion(t, r.DirectoryOffset); err != nil {
			return err
		}
		if t.Flags != 1 || t.EntryCount == 0 || t.SeqLo < r.SeqLo || t.SeqLo > t.SeqHi || t.SeqHi > r.SeqHi || !validRunBounds(t.MinKey, t.MaxKey, 65527) {
			return runInvalid("table descriptor")
		}
	}
	if r.Events.Kind != 1 || r.Heads.Kind != 2 || r.Events.Offset+r.Events.Length > r.Heads.Offset {
		return runInvalid("table order/overlap")
	}
	if r.Events.SeqLo != r.SeqLo || r.Events.SeqHi != r.SeqHi || r.Heads.EntryCount > r.Events.EntryCount {
		return runInvalid("table counts/sequences")
	}
	if r.CreatorRole == 1 && (r.Events.EntryCount > math.MaxUint32 || r.Events.EntryCount != r.SeqHi-r.SeqLo+1) {
		return runInvalid("foreground sequence count")
	}
	directoryBytes := int64(64 + 2*128 + len(r.MinTimeline) + len(r.MaxTimeline) + len(r.Events.MinKey) + len(r.Events.MaxKey) + len(r.Heads.MinKey) + len(r.Heads.MaxKey))
	if f := r.TimelineFilter; f != nil {
		if err := validateRunRegion(&f.Region, r.DirectoryOffset); err != nil {
			return err
		}
		if f.Region.Kind != 3 || f.Region.Flags != 0 || f.Region.SeqLo != 0 || f.Region.SeqHi != 0 || len(f.Region.MinKey) != 0 || len(f.Region.MaxKey) != 0 ||
			f.Region.Offset < r.Heads.Offset+r.Heads.Length || f.KeyCount == 0 || f.KeyCount != f.Region.EntryCount || f.KeyCount != r.Heads.EntryCount ||
			f.Algorithm != 1 || f.BitsPerKey == 0 || f.BitsPerKey > 32 || f.LineBytes != 64 || f.LinesPerPage != 64 || f.PageDataBytes != 4096 {
			return runInvalid("filter descriptor/geometry")
		}
		if f.KeyCount > math.MaxUint64/uint64(f.BitsPerKey) {
			return runInvalid("filter multiplication")
		}
		bits := f.KeyCount * uint64(f.BitsPerKey)
		lines := bits / 512
		if bits%512 != 0 {
			lines++
		}
		if lines == 0 {
			lines = 1
		}
		if lines > math.MaxUint32 || uint64(f.LineCount) != lines {
			return runInvalid("filter lines")
		}
		pages := lines / 64
		if lines%64 != 0 {
			pages++
		}
		probes := uint8(uint32(f.BitsPerKey) * 69 / 100)
		if probes == 0 {
			probes = 1
		}
		if f.Probes != probes || uint64(f.PageCount) != pages || uint64(f.Region.Length) != 64+lines*64+pages*4 {
			return runInvalid("filter pages/checksum geometry")
		}
		directoryBytes += 128
	}
	if r.DirectoryLength != directoryBytes {
		return runInvalid("canonical directory length")
	}
	return nil
}

func validateRunRegion(t *TableMeta, directory int64) error {
	if t.Encoding != 1 || t.Offset < 128 || t.Offset%8 != 0 || t.Offset > directory || t.Length <= 0 || t.Length > directory-t.Offset || t.ContentHash == [32]byte{} {
		return runInvalid("region containment/encoding/hash")
	}
	return nil
}

// Call only after field length validation. All terms are bounded far below
// MaxUint64, including on 32-bit systems. No object-size term is present.
func runWireSize(r *RunMeta) uint64 {
	n := uint64(401 + len(r.ObjectKey) + len(r.MinTimeline) + len(r.MaxTimeline) + len(r.Events.MinKey) + len(r.Events.MaxKey) + len(r.Heads.MinKey) + len(r.Heads.MaxKey))
	if r.Source != nil {
		n += uint64(88 + len(r.Source.Cluster) + len(r.Source.TopicName))
	}
	if r.TimelineFilter != nil {
		n += 114
	}
	return n
}
