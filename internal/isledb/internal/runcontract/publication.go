package runcontract

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
)

const publicationDomain = "unijord/kafka/publication-plan/v1\x00"
const outerPublicationDomain = "unijord/run/publication/v1\x00"
const MaxPublicationBytes = 1024

// Publication is the pre-build identity, not RunMeta or a manifest operation.
// Cluster and TopicName are exact bytes. TopicID is the immutable incarnation
// supplied by the binding loader (never inferred from the topic name).
type Publication struct {
	Namespace           [32]byte
	BindingID           [16]byte
	Cluster             []byte
	TopicID             [16]byte
	TopicName           []byte
	Partition           uint32
	Shard               uint32
	MappingGeneration   uint64
	WriterEpoch         uint64
	OwnerID             [16]byte
	ExpectedRevision    uint64
	ExpectedOffset      uint64
	NextOffset          uint64
	ExpectedLeaderEpoch int32
	LeaderEpoch         int32
	NextSequence        uint64
	ResultingSequence   uint64
	RecordCount         uint32
	RunID               [16]byte
	AttemptID           [16]byte
	MutationDigest      [32]byte
}

func (p Publication) validate() error {
	if p.Namespace == [32]byte{} || p.BindingID == [16]byte{} || p.TopicID == [16]byte{} || p.OwnerID == [16]byte{} || p.RunID == [16]byte{} || p.AttemptID == [16]byte{} || p.MutationDigest == [32]byte{} || p.MappingGeneration == 0 || p.WriterEpoch == 0 || p.ExpectedRevision == math.MaxUint64 {
		return ErrEncoding
	}
	if len(p.Cluster) < 1 || len(p.Cluster) > 256 || len(p.TopicName) < 1 || len(p.TopicName) > 249 || p.Partition > math.MaxInt32 {
		return ErrLimit
	}
	if p.NextOffset > math.MaxInt64 || p.ExpectedOffset >= p.NextOffset || uint64(p.RecordCount) > p.NextOffset-p.ExpectedOffset {
		return ErrEncoding
	}
	if p.ExpectedLeaderEpoch < -1 || p.LeaderEpoch < -1 || (p.ExpectedLeaderEpoch >= 0 && p.LeaderEpoch < p.ExpectedLeaderEpoch) {
		return ErrEncoding
	}
	_, next, err := SequenceRange(p.NextSequence, uint64(p.RecordCount))
	if err != nil {
		return err
	}
	if next != p.ResultingSequence {
		return ErrEncoding
	}
	return nil
}

// MarshalPublication returns the bounded canonical preimage. All integers are
// big endian; signed epochs use their two's-complement uint32 representation.
func MarshalPublication(p Publication) ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, 0, MaxPublicationBytes)
	b = append(b, publicationDomain...)
	b = append(b, p.Namespace[:]...)
	b = append(b, p.BindingID[:]...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.Cluster)))
	b = append(b, p.Cluster...)
	b = append(b, p.TopicID[:]...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.TopicName)))
	b = append(b, p.TopicName...)
	b = binary.BigEndian.AppendUint32(b, p.Partition)
	b = binary.BigEndian.AppendUint32(b, p.Shard)
	b = binary.BigEndian.AppendUint64(b, p.MappingGeneration)
	b = binary.BigEndian.AppendUint64(b, p.WriterEpoch)
	b = append(b, p.OwnerID[:]...)
	b = binary.BigEndian.AppendUint64(b, p.ExpectedRevision)
	b = binary.BigEndian.AppendUint64(b, p.ExpectedOffset)
	b = binary.BigEndian.AppendUint64(b, p.NextOffset)
	b = binary.BigEndian.AppendUint32(b, uint32(p.ExpectedLeaderEpoch))
	b = binary.BigEndian.AppendUint32(b, uint32(p.LeaderEpoch))
	b = binary.BigEndian.AppendUint64(b, p.NextSequence)
	b = binary.BigEndian.AppendUint64(b, p.ResultingSequence)
	b = binary.BigEndian.AppendUint32(b, p.RecordCount)
	b = append(b, p.RunID[:]...)
	b = append(b, p.AttemptID[:]...)
	b = append(b, p.MutationDigest[:]...)
	return b, nil
}

// UnmarshalPublication borrows the two bounded variable strings. It does not
// allocate from counts and rejects extra bytes, including alternate framing.
func UnmarshalPublication(b []byte) (Publication, error) {
	var p Publication
	if len(b) > MaxPublicationBytes || len(b) < len(publicationDomain) || string(b[:len(publicationDomain)]) != publicationDomain {
		return p, ErrEncoding
	}
	b = b[len(publicationDomain):]
	ok := true
	take := func(n int) []byte {
		if !ok || n > len(b) {
			ok = false
			return nil
		}
		v := b[:n:n]
		b = b[n:]
		return v
	}
	u16 := func() uint16 {
		v := take(2)
		if !ok {
			return 0
		}
		return binary.BigEndian.Uint16(v)
	}
	u32 := func() uint32 {
		v := take(4)
		if !ok {
			return 0
		}
		return binary.BigEndian.Uint32(v)
	}
	u64 := func() uint64 {
		v := take(8)
		if !ok {
			return 0
		}
		return binary.BigEndian.Uint64(v)
	}
	copy(p.Namespace[:], take(32))
	copy(p.BindingID[:], take(16))
	n := u16()
	if n == 0 || n > 256 {
		return Publication{}, ErrLimit
	}
	p.Cluster = take(int(n))
	copy(p.TopicID[:], take(16))
	n = u16()
	if n == 0 || n > 249 {
		return Publication{}, ErrLimit
	}
	p.TopicName = take(int(n))
	p.Partition, p.Shard = u32(), u32()
	p.MappingGeneration, p.WriterEpoch = u64(), u64()
	copy(p.OwnerID[:], take(16))
	p.ExpectedRevision = u64()
	p.ExpectedOffset, p.NextOffset = u64(), u64()
	p.ExpectedLeaderEpoch, p.LeaderEpoch = int32(u32()), int32(u32())
	p.NextSequence, p.ResultingSequence = u64(), u64()
	p.RecordCount = u32()
	copy(p.RunID[:], take(16))
	copy(p.AttemptID[:], take(16))
	copy(p.MutationDigest[:], take(32))
	if !ok || len(b) != 0 {
		return Publication{}, ErrEncoding
	}
	if err := p.validate(); err != nil {
		return Publication{}, err
	}
	return p, nil
}

// PublicationHashes preserves SPEC section 6: the 32-byte plan digest is the
// publication_id input to the unchanged outer run publication hash. Neither
// hash depends on the subsequently built object's own payload hash.
func PublicationHashes(p Publication) (id, runHash [32]byte, err error) {
	b, err := MarshalPublication(p)
	if err != nil {
		return id, runHash, err
	}
	id = sha256.Sum256(b)
	h := sha256.New()
	h.Write([]byte(outerPublicationDomain))
	h.Write([]byte{0, 0, 0, 32})
	h.Write(id[:])
	copy(runHash[:], h.Sum(nil))
	return id, runHash, nil
}
