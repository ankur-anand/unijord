package manifest

import (
	"bytes"
	"math"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

// KafkaSourceIdentity is one immutable partition-to-shard mapping. Byte strings
// are exact and topic incarnation is supplied by the binding, never its name.
type KafkaSourceIdentity struct {
	BindingID         [16]byte
	Cluster           []byte
	TopicID           [16]byte
	TopicName         []byte
	Partition         uint32
	Namespace         [32]byte
	Shard             uint32
	MappingGeneration uint64
}

type KafkaSourceState struct {
	Identity    KafkaSourceIdentity
	OwnerID     [16]byte
	Epoch       uint64
	NextOffset  uint64
	LeaderEpoch int32 // -1 is unknown; zero is a known epoch.
}

func (s KafkaSourceIdentity) Validate() error {
	if s.BindingID == [16]byte{} || s.TopicID == [16]byte{} || s.Namespace == [32]byte{} || s.MappingGeneration == 0 || s.Partition > math.MaxInt32 || len(s.Cluster) == 0 || len(s.Cluster) > 256 || len(s.TopicName) == 0 || len(s.TopicName) > 249 {
		return runInvalid("Kafka source identity")
	}
	return nil
}
func (s KafkaSourceIdentity) Clone() KafkaSourceIdentity {
	s.Cluster, s.TopicName = bytes.Clone(s.Cluster), bytes.Clone(s.TopicName)
	return s
}
func (s KafkaSourceIdentity) Equal(b KafkaSourceIdentity) bool {
	return s.BindingID == b.BindingID && bytes.Equal(s.Cluster, b.Cluster) && s.TopicID == b.TopicID && bytes.Equal(s.TopicName, b.TopicName) && s.Partition == b.Partition && s.Namespace == b.Namespace && s.Shard == b.Shard && s.MappingGeneration == b.MappingGeneration
}
func (s *KafkaSourceState) Clone() *KafkaSourceState {
	if s == nil {
		return nil
	}
	c := *s
	c.Identity = s.Identity.Clone()
	return &c
}
func (s *KafkaSourceState) Equal(b *KafkaSourceState) bool {
	if s == nil || b == nil {
		return s == b
	}
	return s.Identity.Equal(b.Identity) && s.OwnerID == b.OwnerID && s.Epoch == b.Epoch && s.NextOffset == b.NextOffset && s.LeaderEpoch == b.LeaderEpoch
}
func (s *KafkaSourceState) validate() error {
	if s == nil {
		return nil
	}
	if err := s.Identity.Validate(); err != nil {
		return err
	}
	if s.OwnerID == [16]byte{} || s.Epoch == 0 || s.NextOffset > math.MaxInt64 || s.LeaderEpoch < -1 {
		return runInvalid("Kafka source state")
	}
	return nil
}
func sourceFromPlan(p runcontract.Publication) KafkaSourceIdentity {
	return KafkaSourceIdentity{p.BindingID, p.Cluster, p.TopicID, p.TopicName, p.Partition, p.Namespace, p.Shard, p.MappingGeneration}
}

func (w *runEncoder) sourceIdentity(s KafkaSourceIdentity) {
	w.data = append(w.data, s.BindingID[:]...)
	w.blob(s.Cluster)
	w.data = append(w.data, s.TopicID[:]...)
	w.blob(s.TopicName)
	w.u32(s.Partition)
	w.data = append(w.data, s.Namespace[:]...)
	w.u32(s.Shard)
	w.u64(s.MappingGeneration)
}
func (r *runDecoder) sourceIdentity() KafkaSourceIdentity {
	var s KafkaSourceIdentity
	copy(s.BindingID[:], r.take(16))
	s.Cluster = r.blob(256)
	copy(s.TopicID[:], r.take(16))
	s.TopicName = r.blob(249)
	s.Partition = r.u32()
	copy(s.Namespace[:], r.take(32))
	s.Shard = r.u32()
	s.MappingGeneration = r.u64()
	return s
}
func (w *runEncoder) source(s *KafkaSourceState) {
	if s == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.sourceIdentity(s.Identity)
	w.data = append(w.data, s.OwnerID[:]...)
	w.u64(s.Epoch)
	w.u64(s.NextOffset)
	w.u32(uint32(s.LeaderEpoch))
}
func (r *runDecoder) source() *KafkaSourceState {
	switch r.u8() {
	case 0:
		return nil
	case 1:
		s := &KafkaSourceState{Identity: r.sourceIdentity()}
		copy(s.OwnerID[:], r.take(16))
		s.Epoch = r.u64()
		s.NextOffset = r.u64()
		s.LeaderEpoch = int32(r.u32())
		return s
	default:
		r.err = runInvalid("source presence")
		return nil
	}
}

type ActivateKafkaSourceRequest struct {
	Identity                 KafkaSourceIdentity
	ExpectedManifestRevision uint64
	ExpectedWriterFence      *RunWriterFence
	OwnerID                  [16]byte
	Epoch                    uint64 // zero allocates exactly the current epoch + 1.
	InitialOffset            uint64 // first activation only; takeover must repeat current cursor.
	InitialLeaderEpoch       int32
}

func activateKafkaSource(m *RunManifest, q ActivateKafkaSourceRequest) (*RunManifest, error) {
	if err := q.Identity.Validate(); err != nil {
		return nil, err
	}
	if q.Identity.Namespace != m.NamespaceHash || q.Identity.Shard != m.Shard || (m.Source != nil && !m.Source.Identity.Equal(q.Identity)) {
		return nil, ErrSourceMismatch
	}
	if q.OwnerID == [16]byte{} {
		return nil, runInvalid("activation owner")
	}
	if !equalRunFence(q.ExpectedWriterFence, m.WriterFence) {
		return nil, ErrRunFence
	}
	if q.ExpectedManifestRevision != m.Revision {
		return nil, ErrRunCASConflict
	}
	if m.Revision == math.MaxUint64 {
		return nil, ErrRunCoordinateExhausted
	}
	var epoch uint64 = 1
	if m.WriterFence != nil {
		if m.WriterFence.Epoch == math.MaxUint64 {
			return nil, ErrRunCoordinateExhausted
		}
		epoch = m.WriterFence.Epoch + 1
	}
	if q.Epoch != 0 && q.Epoch != epoch {
		return nil, ErrRunFence
	}
	if q.InitialOffset > math.MaxInt64 || q.InitialLeaderEpoch < -1 {
		return nil, runInvalid("activation cursor")
	}
	if m.Source != nil && (q.InitialOffset != m.Source.NextOffset || q.InitialLeaderEpoch != m.Source.LeaderEpoch) {
		return nil, ErrSourceMismatch
	}
	if m.Source == nil && (m.NextSequence != 1 || len(m.L0Runs) != 0 || len(m.Levels) != 0 || m.Receipts.Count != 0) {
		return nil, runInvalid("first activation requires empty state")
	}
	c, err := m.Clone()
	if err != nil {
		return nil, err
	}
	c.Source = &KafkaSourceState{q.Identity.Clone(), q.OwnerID, epoch, q.InitialOffset, q.InitialLeaderEpoch}
	c.WriterFence = &RunWriterFence{Epoch: epoch, OwnerID: q.OwnerID}
	c.Revision++
	if err = c.BuildIndexes(); err != nil {
		return nil, err
	}
	return c, nil
}
func equalRunFence(a, b *RunWriterFence) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
