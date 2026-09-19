package manifest

import (
	"bytes"
	"sort"
	"time"
)

// RunManifest is the separate, run-only checkpoint model. After BuildIndexes,
// callers must treat it and every referenced slice as immutable. Edit a clone,
// then rebuild before lookup. No legacy manifest conversion is provided.
// RUN_MANIFEST_FORMAT.md freezes the wire contract and resource limits.
type RunManifest struct {
	Version       uint16
	NamespaceHash [32]byte
	Shard         uint32
	WriterFence   *RunWriterFence
	Source        *KafkaSourceState
	Receipts      ReceiptFrontier
	Revision      uint64
	NextSequence  uint64
	L0Runs        []RunMeta
	Levels        []RunLevel
	l0Order       []uint32
	l0Max         []uint32
	indexed       bool
}

type RunWriterFence struct {
	Epoch   uint64
	OwnerID [16]byte
}

type RunLevel struct {
	Number uint32
	Runs   []RunMeta
}

// TableMeta is the scalar projection of one embedded region descriptor.
// Filter descriptors use the same projection with zero sequences/key bounds.
type TableMeta struct {
	Kind        uint16
	Flags       uint16
	Encoding    uint16
	Offset      int64
	Length      int64
	EntryCount  uint64
	SeqLo       uint64
	SeqHi       uint64
	MinKey      []byte
	MaxKey      []byte
	ContentHash [32]byte
}

type TimelineFilterMeta struct {
	Region        TableMeta
	Algorithm     uint8
	Probes        uint8
	BitsPerKey    uint16
	KeyCount      uint64
	LineCount     uint32
	PageCount     uint32
	LineBytes     uint16
	LinesPerPage  uint16
	PageDataBytes uint32
}

type RunMeta struct {
	Source          *KafkaSourceIdentity
	ID              [16]byte
	ObjectKey       string
	ObjectSize      int64
	FormatVersion   uint16
	NamespaceHash   [32]byte
	Shard           uint32
	CreatorRole     uint8
	CreatorEpoch    uint64
	SeqLo           uint64
	SeqHi           uint64
	PublicationHash [32]byte
	MinTimeline     []byte
	MaxTimeline     []byte
	Events          TableMeta
	Heads           TableMeta
	TimelineFilter  *TimelineFilterMeta
	DirectoryOffset int64
	DirectoryLength int64
	DirectoryHash   [32]byte
	PayloadHash     [32]byte
	CreatedAt       time.Time
	Level           uint32
}

func (t TableMeta) Clone() TableMeta {
	t.MinKey, t.MaxKey = bytes.Clone(t.MinKey), bytes.Clone(t.MaxKey)
	return t
}

func (t TableMeta) Equal(b TableMeta) bool {
	return t.Kind == b.Kind && t.Flags == b.Flags && t.Encoding == b.Encoding &&
		t.Offset == b.Offset && t.Length == b.Length && t.EntryCount == b.EntryCount &&
		t.SeqLo == b.SeqLo && t.SeqHi == b.SeqHi && t.ContentHash == b.ContentHash &&
		bytes.Equal(t.MinKey, b.MinKey) && bytes.Equal(t.MaxKey, b.MaxKey)
}

func (f *TimelineFilterMeta) Clone() *TimelineFilterMeta {
	if f == nil {
		return nil
	}
	c := *f
	c.Region = f.Region.Clone()
	return &c
}

func (f *TimelineFilterMeta) Equal(b *TimelineFilterMeta) bool {
	if f == nil || b == nil {
		return f == b
	}
	return f.Region.Equal(b.Region) && f.Algorithm == b.Algorithm && f.Probes == b.Probes &&
		f.BitsPerKey == b.BitsPerKey && f.KeyCount == b.KeyCount && f.LineCount == b.LineCount &&
		f.PageCount == b.PageCount && f.LineBytes == b.LineBytes &&
		f.LinesPerPage == b.LinesPerPage && f.PageDataBytes == b.PageDataBytes
}

func (r RunMeta) Clone() RunMeta {
	if r.Source != nil {
		s := r.Source.Clone()
		r.Source = &s
	}
	r.MinTimeline, r.MaxTimeline = bytes.Clone(r.MinTimeline), bytes.Clone(r.MaxTimeline)
	r.Events, r.Heads = r.Events.Clone(), r.Heads.Clone()
	r.TimelineFilter = r.TimelineFilter.Clone()
	return r
}

func (r RunMeta) Equal(b RunMeta) bool {
	if r.Source == nil || b.Source == nil {
		if r.Source != b.Source {
			return false
		}
	} else if !r.Source.Equal(*b.Source) {
		return false
	}
	return r.ID == b.ID && r.ObjectKey == b.ObjectKey && r.ObjectSize == b.ObjectSize &&
		r.FormatVersion == b.FormatVersion && r.NamespaceHash == b.NamespaceHash && r.Shard == b.Shard &&
		r.CreatorRole == b.CreatorRole && r.CreatorEpoch == b.CreatorEpoch && r.SeqLo == b.SeqLo &&
		r.SeqHi == b.SeqHi && r.PublicationHash == b.PublicationHash &&
		bytes.Equal(r.MinTimeline, b.MinTimeline) && bytes.Equal(r.MaxTimeline, b.MaxTimeline) &&
		r.Events.Equal(b.Events) && r.Heads.Equal(b.Heads) && r.TimelineFilter.Equal(b.TimelineFilter) &&
		r.DirectoryOffset == b.DirectoryOffset && r.DirectoryLength == b.DirectoryLength &&
		r.DirectoryHash == b.DirectoryHash && r.PayloadHash == b.PayloadHash &&
		r.CreatedAt.Unix() == b.CreatedAt.Unix() && r.CreatedAt.Nanosecond() == b.CreatedAt.Nanosecond() && r.Level == b.Level
}

func (l RunLevel) Clone() RunLevel {
	l.Runs = cloneRuns(l.Runs)
	return l
}

func (l RunLevel) Equal(b RunLevel) bool { return l.Number == b.Number && equalRuns(l.Runs, b.Runs) }

func cloneRuns(runs []RunMeta) []RunMeta {
	if runs == nil {
		return nil
	}
	out := make([]RunMeta, len(runs))
	for i := range runs {
		out[i] = runs[i].Clone()
	}
	return out
}

func equalRuns(a, b []RunMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

// Clone validates admission before allocating and rebuilds indexes over the
// clone's own metadata. Nil and empty collections have identical wire meaning.
func (m *RunManifest) Clone() (*RunManifest, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	c := *m
	c.Source = m.Source.Clone()
	if m.WriterFence != nil {
		f := *m.WriterFence
		c.WriterFence = &f
	}
	c.L0Runs = cloneRuns(m.L0Runs)
	c.Levels = make([]RunLevel, len(m.Levels))
	for i := range m.Levels {
		c.Levels[i] = m.Levels[i].Clone()
	}
	c.buildIndexes()
	return &c, nil
}

func (m *RunManifest) Equal(b *RunManifest) bool {
	if m == nil || b == nil {
		return m == b
	}
	if m.Version != b.Version || m.NamespaceHash != b.NamespaceHash || m.Shard != b.Shard ||
		!m.Source.Equal(b.Source) || m.Receipts != b.Receipts ||
		m.Revision != b.Revision || m.NextSequence != b.NextSequence || len(m.Levels) != len(b.Levels) ||
		!equalRuns(m.L0Runs, b.L0Runs) {
		return false
	}
	if m.WriterFence == nil || b.WriterFence == nil {
		if m.WriterFence != b.WriterFence {
			return false
		}
	} else if *m.WriterFence != *b.WriterFence {
		return false
	}
	for i := range m.Levels {
		if !m.Levels[i].Equal(b.Levels[i]) {
			return false
		}
	}
	return true
}

func (m *RunManifest) BuildIndexes() error {
	if m == nil {
		return runInvalid("nil checkpoint")
	}
	m.indexed = false
	if err := m.Validate(); err != nil {
		return err
	}
	m.buildIndexes()
	return nil
}

// Only integer references are stored: 8 bytes per L0 run. Lower levels already
// form sorted arrays, so their primary storage is also their binary-search index.
func (m *RunManifest) buildIndexes() {
	m.l0Order, m.l0Max = make([]uint32, len(m.L0Runs)), make([]uint32, len(m.L0Runs))
	for i := range m.l0Order {
		m.l0Order[i] = uint32(i)
	}
	sort.Slice(m.l0Order, func(i, j int) bool {
		a, b := &m.L0Runs[m.l0Order[i]], &m.L0Runs[m.l0Order[j]]
		if c := bytes.Compare(a.MinTimeline, b.MinTimeline); c != 0 {
			return c < 0
		}
		return newerRun(a, b)
	})
	var build func(int, int) uint32
	build = func(lo, hi int) uint32 {
		mid := lo + (hi-lo)/2
		best := m.l0Order[mid]
		merge := func(x uint32) {
			if bytes.Compare(m.L0Runs[x].MaxTimeline, m.L0Runs[best].MaxTimeline) > 0 {
				best = x
			}
		}
		if lo < mid {
			merge(build(lo, mid))
		}
		if mid+1 < hi {
			merge(build(mid+1, hi))
		}
		m.l0Max[mid] = best
		return best
	}
	if len(m.L0Runs) > 0 {
		build(0, len(m.L0Runs))
	}
	m.indexed = true
}

func newerRun(a, b *RunMeta) bool {
	if a.SeqHi != b.SeqHi {
		return a.SeqHi > b.SeqHi
	}
	if a.SeqLo != b.SeqLo {
		return a.SeqLo > b.SeqLo
	}
	return bytes.Compare(a.ID[:], b.ID[:]) < 0
}

type RunLookupStats struct{ Comparisons, Candidates uint64 }

func (m *RunManifest) IndexBytes() uint64 { return uint64(len(m.l0Order)+len(m.l0Max)) * 4 }

// L0Candidates appends borrowed pointers, newest sequence first. dst belongs
// to the caller; neither returned metadata nor the indexed manifest may mutate.
func (m *RunManifest) L0Candidates(timeline []byte, dst []*RunMeta) ([]*RunMeta, RunLookupStats, error) {
	var stats RunLookupStats
	if m == nil || !m.indexed || len(timeline) == 0 || len(timeline) > 512 {
		return dst, stats, ErrInvalidRunManifest
	}
	start := len(dst)
	var visit func(int, int)
	visit = func(lo, hi int) {
		if lo == hi {
			return
		}
		mid := lo + (hi-lo)/2
		stats.Comparisons++
		if bytes.Compare(m.L0Runs[m.l0Order[lo]].MinTimeline, timeline) > 0 {
			return
		}
		stats.Comparisons++
		if bytes.Compare(m.L0Runs[m.l0Max[mid]].MaxTimeline, timeline) < 0 {
			return
		}
		visit(lo, mid)
		r := &m.L0Runs[m.l0Order[mid]]
		stats.Comparisons++
		if bytes.Compare(r.MinTimeline, timeline) <= 0 {
			stats.Comparisons++
			if bytes.Compare(r.MaxTimeline, timeline) >= 0 {
				dst = append(dst, r)
			}
			visit(mid+1, hi)
		}
	}
	visit(0, len(m.l0Order))
	added := dst[start:]
	sort.Slice(added, func(i, j int) bool { stats.Comparisons++; return newerRun(added[i], added[j]) })
	stats.Candidates = uint64(len(added))
	return dst, stats, nil
}

func (m *RunManifest) LevelCandidate(level uint32, timeline []byte) (*RunMeta, RunLookupStats, error) {
	var stats RunLookupStats
	if m == nil || !m.indexed || level == 0 || len(timeline) == 0 || len(timeline) > 512 {
		return nil, stats, ErrInvalidRunManifest
	}
	i := sort.Search(len(m.Levels), func(i int) bool { stats.Comparisons++; return m.Levels[i].Number >= level })
	if i == len(m.Levels) || m.Levels[i].Number != level {
		return nil, stats, nil
	}
	runs := m.Levels[i].Runs
	j := sort.Search(len(runs), func(j int) bool { stats.Comparisons++; return bytes.Compare(runs[j].MaxTimeline, timeline) >= 0 })
	if j < len(runs) {
		stats.Comparisons++
		if bytes.Compare(runs[j].MinTimeline, timeline) <= 0 {
			stats.Candidates = 1
			return &runs[j], stats, nil
		}
	}
	return nil, stats, nil
}
