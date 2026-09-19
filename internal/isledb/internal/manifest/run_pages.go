package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"time"
)

const runEnvelopeBytes = 48
const (
	runCheckpointKind = 1
	runPageKind       = 2
	runLogKind        = 3
)

// The codec is deliberately separate from the legacy object codec. No JSON,
// compression, extension fields, or fallback parsing exists. The E07 prototype
// version 1 is rejected; only current version 2 is admitted.
func runEnvelope(payload []byte, kind byte) []byte {
	out := make([]byte, runEnvelopeBytes+len(payload))
	copy(out, "UJRM")
	binary.BigEndian.PutUint16(out[4:6], RunManifestVersion)
	out[6] = kind
	binary.BigEndian.PutUint64(out[8:16], uint64(len(payload)))
	hash := sha256.Sum256(payload)
	copy(out[16:48], hash[:])
	copy(out[48:], payload)
	return out
}

func openRunEnvelope(data []byte, kind byte, limit uint64) ([]byte, error) {
	if len(data) >= 4 && string(data[:4]) == "ISLM" {
		return nil, ErrUnsupportedRunFormat
	}
	if len(data) < runEnvelopeBytes {
		return nil, runInvalid("truncated envelope")
	}
	if string(data[:4]) != "UJRM" || binary.BigEndian.Uint16(data[4:6]) != RunManifestVersion {
		return nil, ErrUnsupportedRunFormat
	}
	if data[6] != kind || data[7] != 0 {
		return nil, runInvalid("envelope kind/flags")
	}
	n := binary.BigEndian.Uint64(data[8:16])
	if n == 0 || n > limit || n > uint64(math.MaxInt)-runEnvelopeBytes {
		return nil, ErrRunManifestLimit
	}
	if n != uint64(len(data)-runEnvelopeBytes) {
		return nil, runInvalid("envelope length/trailing data")
	}
	payload := data[runEnvelopeBytes:]
	hash := sha256.Sum256(payload)
	if !bytes.Equal(hash[:], data[16:48]) {
		return nil, runInvalid("envelope checksum")
	}
	return payload, nil
}

type runEncoder struct{ data []byte }

func (w *runEncoder) u8(v uint8)    { w.data = append(w.data, v) }
func (w *runEncoder) u16(v uint16)  { w.data = binary.BigEndian.AppendUint16(w.data, v) }
func (w *runEncoder) u32(v uint32)  { w.data = binary.BigEndian.AppendUint32(w.data, v) }
func (w *runEncoder) u64(v uint64)  { w.data = binary.BigEndian.AppendUint64(w.data, v) }
func (w *runEncoder) blob(b []byte) { w.u32(uint32(len(b))); w.data = append(w.data, b...) }
func (w *runEncoder) str(s string)  { w.u32(uint32(len(s))); w.data = append(w.data, s...) }
func (w *runEncoder) table(t *TableMeta) {
	w.u16(t.Kind)
	w.u16(t.Flags)
	w.u16(t.Encoding)
	w.u64(uint64(t.Offset))
	w.u64(uint64(t.Length))
	w.u64(t.EntryCount)
	w.u64(t.SeqLo)
	w.u64(t.SeqHi)
	w.blob(t.MinKey)
	w.blob(t.MaxKey)
	w.data = append(w.data, t.ContentHash[:]...)
}
func (w *runEncoder) run(r *RunMeta) {
	if r.Source == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.sourceIdentity(*r.Source)
	}
	w.data = append(w.data, r.ID[:]...)
	w.str(r.ObjectKey)
	w.u64(uint64(r.ObjectSize))
	w.u16(r.FormatVersion)
	w.data = append(w.data, r.NamespaceHash[:]...)
	w.u32(r.Shard)
	w.u8(r.CreatorRole)
	w.u64(r.CreatorEpoch)
	w.u64(r.SeqLo)
	w.u64(r.SeqHi)
	w.data = append(w.data, r.PublicationHash[:]...)
	w.blob(r.MinTimeline)
	w.blob(r.MaxTimeline)
	w.table(&r.Events)
	w.table(&r.Heads)
	if f := r.TimelineFilter; f != nil {
		w.u8(1)
		w.table(&f.Region)
		w.u8(f.Algorithm)
		w.u8(f.Probes)
		w.u16(f.BitsPerKey)
		w.u64(f.KeyCount)
		w.u32(f.LineCount)
		w.u32(f.PageCount)
		w.u16(f.LineBytes)
		w.u16(f.LinesPerPage)
		w.u32(f.PageDataBytes)
	} else {
		w.u8(0)
	}
	w.u64(uint64(r.DirectoryOffset))
	w.u64(uint64(r.DirectoryLength))
	w.data = append(w.data, r.DirectoryHash[:]...)
	w.data = append(w.data, r.PayloadHash[:]...)
	w.u64(uint64(r.CreatedAt.Unix()))
	w.u32(uint32(r.CreatedAt.Nanosecond()))
	w.u32(r.Level)
}
func (w *runEncoder) runs(runs []RunMeta) {
	w.u32(uint32(len(runs)))
	for i := range runs {
		w.run(&runs[i])
	}
}

type runDecoder struct {
	data []byte
	pos  int
	err  error
	runs int
}

func (r *runDecoder) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.data)-r.pos {
		r.err = runInvalid("truncated payload")
		return nil
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}
func (r *runDecoder) u8() uint8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (r *runDecoder) u16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (r *runDecoder) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (r *runDecoder) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
func (r *runDecoder) blob(limit uint32) []byte {
	n := r.u32()
	if r.err != nil {
		return nil
	}
	if n > limit || uint64(n) > uint64(len(r.data)-r.pos) {
		r.err = ErrRunManifestLimit
		return nil
	}
	return bytes.Clone(r.take(int(n)))
}
func (r *runDecoder) str() string {
	n := r.u32()
	if r.err != nil {
		return ""
	}
	if n > MaxRunObjectKeyBytes || uint64(n) > uint64(len(r.data)-r.pos) {
		r.err = ErrRunManifestLimit
		return ""
	}
	return string(r.take(int(n)))
}
func (r *runDecoder) count(limit int, minimumBytes uint64) int {
	n := r.u32()
	if r.err != nil {
		return 0
	}
	if uint64(n) > uint64(limit) || uint64(n) > uint64(len(r.data)-r.pos)/minimumBytes {
		r.err = ErrRunManifestLimit
		return 0
	}
	return int(n)
}
func (r *runDecoder) table() TableMeta {
	t := TableMeta{Kind: r.u16(), Flags: r.u16(), Encoding: r.u16(), Offset: int64(r.u64()), Length: int64(r.u64()), EntryCount: r.u64(), SeqLo: r.u64(), SeqHi: r.u64()}
	t.MinKey = r.blob(65527)
	t.MaxKey = r.blob(65527)
	copy(t.ContentHash[:], r.take(32))
	return t
}
func (r *runDecoder) run() RunMeta {
	var m RunMeta
	switch r.u8() {
	case 0:
	case 1:
		s := r.sourceIdentity()
		m.Source = &s
	default:
		r.err = runInvalid("run source presence")
	}
	copy(m.ID[:], r.take(16))
	m.ObjectKey = r.str()
	m.ObjectSize = int64(r.u64())
	m.FormatVersion = r.u16()
	copy(m.NamespaceHash[:], r.take(32))
	m.Shard = r.u32()
	m.CreatorRole = r.u8()
	m.CreatorEpoch = r.u64()
	m.SeqLo = r.u64()
	m.SeqHi = r.u64()
	copy(m.PublicationHash[:], r.take(32))
	m.MinTimeline = r.blob(512)
	m.MaxTimeline = r.blob(512)
	m.Events = r.table()
	m.Heads = r.table()
	switch r.u8() {
	case 0:
	case 1:
		f := &TimelineFilterMeta{Region: r.table(), Algorithm: r.u8(), Probes: r.u8(), BitsPerKey: r.u16(), KeyCount: r.u64(), LineCount: r.u32(), PageCount: r.u32(), LineBytes: r.u16(), LinesPerPage: r.u16(), PageDataBytes: r.u32()}
		m.TimelineFilter = f
	default:
		r.err = runInvalid("filter presence")
	}
	m.DirectoryOffset = int64(r.u64())
	m.DirectoryLength = int64(r.u64())
	copy(m.DirectoryHash[:], r.take(32))
	copy(m.PayloadHash[:], r.take(32))
	sec, ns := int64(r.u64()), r.u32()
	if sec < -62135596800 || sec > 253402300799 || ns >= 1e9 {
		r.err = runInvalid("timestamp")
	} else {
		m.CreatedAt = time.Unix(sec, int64(ns)).UTC()
	}
	m.Level = r.u32()
	return m
}
func (r *runDecoder) readRuns(limit int) []RunMeta {
	if remaining := MaxManifestRuns - r.runs; limit > remaining {
		limit = remaining
	}
	n := r.count(limit, 401)
	if r.err != nil {
		return nil
	}
	r.runs += n
	out := make([]RunMeta, n)
	for i := range out {
		out[i] = r.run()
		if r.err != nil {
			return nil
		}
	}
	return out
}
func (r *runDecoder) done() error {
	if r.err != nil {
		return r.err
	}
	if r.pos != len(r.data) {
		return runInvalid("unknown/trailing payload fields")
	}
	return nil
}

func EncodeRunCheckpoint(m *RunManifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	n := uint64(62+8*len(m.Levels)) + frontierWireSize(m.Receipts)
	if m.Source != nil {
		n += uint64(124 + len(m.Source.Identity.Cluster) + len(m.Source.Identity.TopicName))
	}
	if m.WriterFence != nil {
		n += 24
	}
	for i := range m.L0Runs {
		n += runWireSize(&m.L0Runs[i])
	}
	for i := range m.Levels {
		for j := range m.Levels[i].Runs {
			n += runWireSize(&m.Levels[i].Runs[j])
		}
	}
	w := runEncoder{data: make([]byte, 0, int(n))}
	w.data = append(w.data, m.NamespaceHash[:]...)
	w.u32(m.Shard)
	w.u64(m.Revision)
	w.u64(m.NextSequence)
	if m.WriterFence != nil {
		w.u8(1)
		w.u64(m.WriterFence.Epoch)
		w.data = append(w.data, m.WriterFence.OwnerID[:]...)
	} else {
		w.u8(0)
	}
	w.source(m.Source)
	w.frontier(m.Receipts)
	w.runs(m.L0Runs)
	w.u32(uint32(len(m.Levels)))
	for i := range m.Levels {
		w.u32(m.Levels[i].Number)
		w.runs(m.Levels[i].Runs)
	}
	return runEnvelope(w.data, runCheckpointKind), nil
}

func DecodeRunCheckpoint(data []byte) (*RunManifest, error) {
	payload, err := openRunEnvelope(data, runCheckpointKind, MaxRunCheckpointBytes)
	if err != nil {
		return nil, err
	}
	r := runDecoder{data: payload}
	m := &RunManifest{Version: RunManifestVersion}
	copy(m.NamespaceHash[:], r.take(32))
	m.Shard = r.u32()
	m.Revision = r.u64()
	m.NextSequence = r.u64()
	switch r.u8() {
	case 0:
	case 1:
		m.WriterFence = &RunWriterFence{Epoch: r.u64()}
		copy(m.WriterFence.OwnerID[:], r.take(16))
	default:
		r.err = runInvalid("fence presence")
	}
	m.Source = r.source()
	m.Receipts = r.frontier()
	m.L0Runs = r.readRuns(MaxManifestRuns)
	n := r.count(MaxRunLevels, 8)
	if r.err != nil {
		return nil, r.err
	}
	m.Levels = make([]RunLevel, n)
	for i := range m.Levels {
		m.Levels[i].Number = r.u32()
		m.Levels[i].Runs = r.readRuns(MaxManifestRuns)
		if r.err != nil {
			return nil, r.err
		}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	if err := m.BuildIndexes(); err != nil {
		return nil, err
	}
	return m, nil
}

// RunPage contains contiguous manifest revisions, distinct from mutation
// sequences. Leaf pages hold operations; index pages hold only page references.
type RunPage struct {
	Level    uint8
	SeqLo    uint64
	SeqHi    uint64
	Count    uint32
	Entries  []RunLogEntry
	Children []RunPageRef
}
type RunPageRef struct {
	ObjectKey    string
	EncodedBytes uint64
	Hash         [32]byte
	Level        uint8
	SeqLo        uint64
	SeqHi        uint64
	Count        uint32
}

func validRunPageRange(level uint8, lo, hi uint64, count uint32) bool {
	return level <= MaxRunPageLevel && lo > 0 && hi >= lo && count > 0 && count <= MaxRunPageCount && hi-lo == uint64(count)-1
}
func (p *RunPage) Validate() error {
	if p == nil || !validRunPageRange(p.Level, p.SeqLo, p.SeqHi, p.Count) {
		return runInvalid("page range/level/count")
	}
	size := uint64(25)
	if p.Level == 0 {
		if len(p.Children) != 0 || len(p.Entries) == 0 || len(p.Entries) > MaxRunPageEntries || uint64(len(p.Entries)) != uint64(p.Count) {
			return runInvalid("leaf entries")
		}
		runs := 0
		for i := range p.Entries {
			e := &p.Entries[i]
			if e.Revision != p.SeqLo+uint64(i) {
				return runInvalid("leaf revision coverage")
			}
			if err := e.Validate(); err != nil {
				return err
			}
			if len(e.AddRuns) > MaxManifestRuns-runs {
				return ErrRunManifestLimit
			}
			runs += len(e.AddRuns)
			n := e.wireSize()
			if n > MaxRunPageBytes-size {
				return ErrRunManifestLimit
			}
			size += n
		}
	} else {
		if len(p.Entries) != 0 || len(p.Children) == 0 || len(p.Children) > MaxRunPageEntries {
			return runInvalid("index children")
		}
		var count uint64
		keys := make(map[string]bool, len(p.Children))
		for i := range p.Children {
			c := &p.Children[i]
			if !validRunPageRange(c.Level, c.SeqLo, c.SeqHi, c.Count) || c.Level != p.Level-1 || len(c.ObjectKey) == 0 || len(c.ObjectKey) > MaxRunObjectKeyBytes || c.Hash == [32]byte{} || c.EncodedBytes <= runEnvelopeBytes || c.EncodedBytes > MaxRunPageBytes+runEnvelopeBytes || keys[c.ObjectKey] {
				return runInvalid("page reference")
			}
			keys[c.ObjectKey] = true
			if i == 0 {
				if c.SeqLo != p.SeqLo {
					return runInvalid("first child range")
				}
			} else if p.Children[i-1].SeqHi == math.MaxUint64 || c.SeqLo != p.Children[i-1].SeqHi+1 {
				return runInvalid("child range coverage")
			}
			count += uint64(c.Count)
			if count > MaxRunPageCount {
				return ErrRunManifestLimit
			}
			size += uint64(65 + len(c.ObjectKey))
		}
		if count != uint64(p.Count) || p.Children[len(p.Children)-1].SeqHi != p.SeqHi {
			return runInvalid("index range/count")
		}
	}
	if size > MaxRunPageBytes {
		return ErrRunManifestLimit
	}
	return nil
}

func EncodeRunPage(p *RunPage) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	w := runEncoder{}
	w.u8(p.Level)
	w.u64(p.SeqLo)
	w.u64(p.SeqHi)
	w.u32(p.Count)
	if p.Level == 0 {
		w.u32(uint32(len(p.Entries)))
		for i := range p.Entries {
			w.entry(&p.Entries[i])
		}
	} else {
		w.u32(uint32(len(p.Children)))
		for i := range p.Children {
			c := &p.Children[i]
			w.str(c.ObjectKey)
			w.u64(c.EncodedBytes)
			w.data = append(w.data, c.Hash[:]...)
			w.u8(c.Level)
			w.u64(c.SeqLo)
			w.u64(c.SeqHi)
			w.u32(c.Count)
		}
	}
	return runEnvelope(w.data, runPageKind), nil
}

func DecodeRunPage(data []byte) (*RunPage, error) {
	payload, err := openRunEnvelope(data, runPageKind, MaxRunPageBytes)
	if err != nil {
		return nil, err
	}
	r := runDecoder{data: payload}
	p := &RunPage{Level: r.u8(), SeqLo: r.u64(), SeqHi: r.u64(), Count: r.u32()}
	if !validRunPageRange(p.Level, p.SeqLo, p.SeqHi, p.Count) {
		return nil, runInvalid("page header")
	}
	if p.Level == 0 {
		n := r.count(MaxRunPageEntries, 33)
		if r.err != nil {
			return nil, r.err
		}
		p.Entries = make([]RunLogEntry, n)
		for i := range p.Entries {
			p.Entries[i] = r.entry()
			if r.err != nil {
				return nil, r.err
			}
		}
	} else {
		n := r.count(MaxRunPageEntries, 65)
		if r.err != nil {
			return nil, r.err
		}
		p.Children = make([]RunPageRef, n)
		for i := range p.Children {
			c := &p.Children[i]
			c.ObjectKey = r.str()
			c.EncodedBytes = r.u64()
			copy(c.Hash[:], r.take(32))
			c.Level = r.u8()
			c.SeqLo = r.u64()
			c.SeqHi = r.u64()
			c.Count = r.u32()
			if r.err != nil {
				return nil, r.err
			}
		}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}
