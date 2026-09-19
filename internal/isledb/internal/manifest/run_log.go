package manifest

import (
	"bytes"
	"math"
	"sort"
)

type RunLogOp uint8

const (
	RunLogAdd        RunLogOp = 1
	RunLogRemove     RunLogOp = 2
	RunLogCompaction RunLogOp = 3
)

// RunLogEntry is state replay data only. It is not an authorization, source
// cursor, retry receipt, CAS operation, or Kafka publication API.
type RunLogEntry struct {
	Op               RunLogOp
	Revision         uint64
	NextSequence     uint64
	SourceLevel      uint32
	DestinationLevel uint32
	RemoveRunIDs     [][16]byte
	AddRuns          []RunMeta
}

func (e *RunLogEntry) Validate() error {
	if e == nil || e.Revision == 0 || e.NextSequence == 0 || e.NextSequence > MaxRunSequence+1 {
		return runInvalid("log revision/sequence")
	}
	if len(e.RemoveRunIDs) > MaxRunOperationRuns || len(e.AddRuns) > MaxRunOperationRuns {
		return ErrRunManifestLimit
	}
	switch e.Op {
	case RunLogAdd:
		if len(e.RemoveRunIDs) != 0 || len(e.AddRuns) != 1 || e.SourceLevel != 0 || e.DestinationLevel != 0 || e.AddRuns[0].Level != 0 || e.AddRuns[0].CreatorRole != 1 {
			return runInvalid("add_run shape")
		}
	case RunLogRemove:
		if len(e.RemoveRunIDs) == 0 || len(e.AddRuns) != 0 || e.SourceLevel != 0 || e.DestinationLevel != 0 {
			return runInvalid("remove_runs shape")
		}
	case RunLogCompaction:
		if len(e.RemoveRunIDs) == 0 || len(e.AddRuns) == 0 || e.SourceLevel >= MaxRunLevels || e.DestinationLevel != e.SourceLevel+1 {
			return runInvalid("compaction shape/levels")
		}
	default:
		return runInvalid("unknown log operation")
	}
	for i := range e.AddRuns {
		r := &e.AddRuns[i]
		if err := r.Validate(); err != nil {
			return err
		}
		if r.SeqHi >= e.NextSequence {
			return runInvalid("log run sequence")
		}
		if e.Op == RunLogCompaction && (r.Level != e.DestinationLevel || (i > 0 && bytes.Compare(e.AddRuns[i-1].MaxTimeline, r.MinTimeline) >= 0)) {
			return runInvalid("compaction outputs")
		}
	}
	if e.wireSize() > MaxRunPageBytes {
		return ErrRunManifestLimit
	}
	ids := make(map[[16]byte]bool, len(e.RemoveRunIDs))
	for _, id := range e.RemoveRunIDs {
		if id == [16]byte{} || ids[id] {
			return runInvalid("remove ID")
		}
		ids[id] = true
	}
	clear(ids)
	keys := make(map[string]bool, len(e.AddRuns))
	for i := range e.AddRuns {
		r := &e.AddRuns[i]
		if ids[r.ID] || keys[r.ObjectKey] {
			return runInvalid("duplicate output")
		}
		ids[r.ID] = true
		keys[r.ObjectKey] = true
	}
	return nil
}

func (e *RunLogEntry) wireSize() uint64 {
	n := uint64(33 + 16*len(e.RemoveRunIDs))
	for i := range e.AddRuns {
		n += runWireSize(&e.AddRuns[i])
	}
	return n
}
func (w *runEncoder) entry(e *RunLogEntry) {
	w.u8(uint8(e.Op))
	w.u64(e.Revision)
	w.u64(e.NextSequence)
	w.u32(e.SourceLevel)
	w.u32(e.DestinationLevel)
	w.u32(uint32(len(e.RemoveRunIDs)))
	for _, id := range e.RemoveRunIDs {
		w.data = append(w.data, id[:]...)
	}
	w.runs(e.AddRuns)
}
func (r *runDecoder) entry() RunLogEntry {
	e := RunLogEntry{Op: RunLogOp(r.u8()), Revision: r.u64(), NextSequence: r.u64(), SourceLevel: r.u32(), DestinationLevel: r.u32()}
	n := r.count(MaxRunOperationRuns, 16)
	if r.err != nil {
		return e
	}
	e.RemoveRunIDs = make([][16]byte, n)
	for i := range e.RemoveRunIDs {
		copy(e.RemoveRunIDs[i][:], r.take(16))
	}
	e.AddRuns = r.readRuns(MaxRunOperationRuns)
	return e
}

func EncodeRunLogEntry(e *RunLogEntry) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	w := runEncoder{data: make([]byte, 0, int(e.wireSize()))}
	w.entry(e)
	return runEnvelope(w.data, runLogKind), nil
}
func DecodeRunLogEntry(data []byte) (*RunLogEntry, error) {
	payload, err := openRunEnvelope(data, runLogKind, MaxRunPageBytes)
	if err != nil {
		return nil, err
	}
	r := runDecoder{data: payload}
	e := r.entry()
	if err := r.done(); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

// ApplyRunLogEntry returns a new owned, indexed state or an error without
// mutating either input. Revisions must be contiguous; receipt reconciliation
// and idempotent publication are intentionally outside this replay primitive.
func ApplyRunLogEntry(m *RunManifest, e *RunLogEntry) (*RunManifest, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	if m.Revision == math.MaxUint64 || e.Revision != m.Revision+1 {
		return nil, runInvalid("replay revision")
	}
	if e.Op == RunLogAdd {
		// Source-backed foreground state may advance only through the atomic
		// publication path, which also installs its exact receipt frontier.
		if m.Source != nil {
			return nil, runInvalid("Kafka add requires atomic publication")
		}
		if e.AddRuns[0].SeqLo != m.NextSequence || e.NextSequence != e.AddRuns[0].SeqHi+1 {
			return nil, runInvalid("replay sequence allocation")
		}
	} else if e.NextSequence != m.NextSequence {
		return nil, runInvalid("maintenance changes mutation cursor")
	}
	live := make(map[[16]byte]*RunMeta, len(m.L0Runs))
	for i := range m.L0Runs {
		live[m.L0Runs[i].ID] = &m.L0Runs[i]
	}
	for i := range m.Levels {
		for j := range m.Levels[i].Runs {
			r := &m.Levels[i].Runs[j]
			live[r.ID] = r
		}
	}
	removed := make(map[[16]byte]bool, len(e.RemoveRunIDs))
	hasSource := false
	for _, id := range e.RemoveRunIDs {
		r := live[id]
		if r == nil {
			return nil, runInvalid("remove non-live run")
		}
		if e.Op == RunLogCompaction {
			if r.Level != e.SourceLevel && r.Level != e.DestinationLevel {
				return nil, runInvalid("compaction input level")
			}
			hasSource = hasSource || r.Level == e.SourceLevel
		}
		removed[id] = true
	}
	if e.Op == RunLogCompaction && !hasSource {
		return nil, runInvalid("compaction missing source")
	}
	if len(e.AddRuns) > MaxManifestRuns-(len(live)-len(removed)) {
		return nil, ErrRunManifestLimit
	}
	for i := range e.AddRuns {
		r := &e.AddRuns[i]
		if old := live[r.ID]; old != nil {
			// A complete-object trivial move preserves all persisted identity
			// except its manifest-only level. Changing bytes under an ID is invalid.
			copyOld := *old
			copyOld.Level = r.Level
			if e.Op != RunLogCompaction || !removed[r.ID] || !copyOld.Equal(*r) {
				return nil, runInvalid("reused run identity")
			}
		} else if e.Op == RunLogCompaction && r.CreatorRole != 2 {
			return nil, runInvalid("new compaction creator")
		}
		for _, id := range e.RemoveRunIDs {
			if old := live[id]; old.ObjectKey == r.ObjectKey && old.ID != r.ID {
				return nil, runInvalid("reused immutable object key")
			}
		}
	}
	c := *m
	c.Revision = e.Revision
	c.NextSequence = e.NextSequence
	c.indexed = false
	filter := func(runs []RunMeta) []RunMeta {
		out := make([]RunMeta, 0, len(runs))
		for i := range runs {
			if !removed[runs[i].ID] {
				out = append(out, runs[i])
			}
		}
		return out
	}
	c.L0Runs = filter(m.L0Runs)
	c.Levels = make([]RunLevel, len(m.Levels))
	for i := range m.Levels {
		c.Levels[i] = RunLevel{Number: m.Levels[i].Number, Runs: filter(m.Levels[i].Runs)}
	}
	if e.Op == RunLogAdd {
		c.L0Runs = append(c.L0Runs, e.AddRuns[0])
	} else if e.Op == RunLogCompaction {
		i := sort.Search(len(c.Levels), func(i int) bool { return c.Levels[i].Number >= e.DestinationLevel })
		if i == len(c.Levels) || c.Levels[i].Number != e.DestinationLevel {
			c.Levels = append(c.Levels, RunLevel{})
			copy(c.Levels[i+1:], c.Levels[i:])
			c.Levels[i] = RunLevel{Number: e.DestinationLevel}
		}
		c.Levels[i].Runs = append(c.Levels[i].Runs, e.AddRuns...)
		sort.Slice(c.Levels[i].Runs, func(a, b int) bool {
			return bytes.Compare(c.Levels[i].Runs[a].MinTimeline, c.Levels[i].Runs[b].MinTimeline) < 0
		})
	}
	return c.Clone()
}

// ReplayRunPage only accepts leaf pages. Loading a page tree and storage
// integration belong to later candidates; this operation is atomic in memory.
func ReplayRunPage(m *RunManifest, p *RunPage) (*RunManifest, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if p.Level != 0 {
		return nil, runInvalid("replay index page")
	}
	c := m
	for i := range p.Entries {
		next, err := ApplyRunLogEntry(c, &p.Entries[i])
		if err != nil {
			return nil, err
		}
		c = next
	}
	return c, nil
}
