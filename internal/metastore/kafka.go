package metastore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

type KafkaLeaderEpoch struct {
	Value int32
	Valid bool
}

// KafkaPartitionMapping is the durable destination selected when a binding is
// created. The mapping is loaded during activation and revalidated from
// authoritative metastore state on every commit; a partition actor never
// chooses a shard for an individual publication.
type KafkaPartitionMapping struct {
	BindingID         KafkaBindingID
	TopicID           KafkaTopicID
	Partition         int32
	MappingGeneration uint64
	Shard             ShardKey
}

// KafkaSourceExpectation carries the mapping observed during activation. It is
// an optimistic expectation for validation and hashing, not authority to choose
// a destination; CommitKafkaChunk must compare it with durable metastore state.
type KafkaSourceExpectation struct {
	Mapping             KafkaPartitionMapping
	OwnerEpoch          uint64
	ExpectedNextOffset  int64
	ResultingNextOffset int64
	LeaderEpoch         KafkaLeaderEpoch
}

type CommitKafkaChunkRequest struct {
	Source      KafkaSourceExpectation
	Publication ChunkPublication
}

type CommitKafkaChunkResult struct {
	// Heads are the exact post-state acknowledged by this Kafka interval. On
	// exact replay they may be older than the current timeline heads.
	Heads      []TimelineHead
	NextOffset int64
	Replayed   bool
	// Authority fields describe what reconciliation observed. False proves
	// authority loss; true is not a guarantee against a later takeover.
	SourceOwnerActive bool
	WriterFenceActive bool
}

func (r CommitKafkaChunkResult) AuthorityActive() bool {
	return r.SourceOwnerActive && r.WriterFenceActive
}

type KafkaActivationRequest struct {
	BindingID         KafkaBindingID
	TopicID           KafkaTopicID
	ConfigurationHash [32]byte
	Owner             OwnerID
	Partitions        []int32
}

// KafkaPartitionLease is returned by KafkaActivator and must not be assembled
// from independently loaded source and writer state. Connector code derives
// chunk identity and commit expectations from this one value.
type KafkaPartitionLease struct {
	Mapping          KafkaPartitionMapping
	SourceOwnerEpoch uint64
	NextOffset       int64
	LeaderEpoch      KafkaLeaderEpoch
	Writer           ShardState
}

type KafkaCommitter interface {
	// CommitKafkaChunk atomically publishes the chunk and advances the Kafka
	// source cursor. Implementations must select the writer row through the
	// durable source mapping, then require Source.Mapping and Publication.Fence
	// to match it; the request's shard is never authoritative by itself. Exact
	// commit replay precedes source/writer authority checks. A non-exact request
	// checks stale source and writer authority before position, sequence, or
	// content conflicts. After an exact-replay miss, implementations call
	// ValidateNewKafkaChunk with freshly locked durable state. The complete
	// outcome rules are in CONFORMANCE.md.
	CommitKafkaChunk(context.Context, CommitKafkaChunkRequest) (CommitKafkaChunkResult, error)
}

type KafkaActivator interface {
	// ActivateKafkaPartitions returns leases aligned with request.Partitions.
	// Each lease contains the persisted mapping and the writer state claimed for
	// that mapped shard in the same activation transaction.
	ActivateKafkaPartitions(context.Context, KafkaActivationRequest) ([]KafkaPartitionLease, error)
}

func ValidateKafkaPartitionMapping(mapping KafkaPartitionMapping) error {
	if isZero128(mapping.BindingID) || isZero128(mapping.TopicID) || mapping.Partition < 0 ||
		mapping.MappingGeneration == 0 {
		return fmt.Errorf("%w: incomplete Kafka partition mapping", ErrInvalidRequest)
	}
	if err := ValidateShardKey(mapping.Shard); err != nil {
		return fmt.Errorf("%w: invalid Kafka partition mapping shard: %v", ErrInvalidRequest, err)
	}
	return nil
}

// ValidateKafkaPartitionMappings validates one complete mapping generation for
// a topic with partitionCount partitions. Every source partition in
// [0, partitionCount) and every destination shard must appear exactly once,
// and all partitions in the binding target one namespace.
func ValidateKafkaPartitionMappings(partitionCount int32, mappings []KafkaPartitionMapping) error {
	if partitionCount <= 0 || int64(len(mappings)) != int64(partitionCount) {
		return fmt.Errorf("%w: Kafka partition count=%d mappings=%d", ErrInvalidRequest,
			partitionCount, len(mappings))
	}
	first := mappings[0]
	partitions := make(map[int32]struct{}, len(mappings))
	type shardIdentity struct {
		namespaceHash [32]byte
		shard         uint32
	}
	shards := make(map[shardIdentity]Namespace, len(mappings))
	for i, mapping := range mappings {
		if err := ValidateKafkaPartitionMapping(mapping); err != nil {
			return fmt.Errorf("%w: mapping=%d: %v", ErrInvalidRequest, i, err)
		}
		if mapping.BindingID != first.BindingID || mapping.TopicID != first.TopicID ||
			mapping.MappingGeneration != first.MappingGeneration ||
			!mapping.Shard.Namespace.Equal(first.Shard.Namespace) {
			return fmt.Errorf("%w: mappings do not describe one binding generation", ErrInvalidRequest)
		}
		if mapping.Partition >= partitionCount {
			return fmt.Errorf("%w: Kafka partition=%d outside [0,%d)", ErrInvalidRequest,
				mapping.Partition, partitionCount)
		}
		if _, exists := partitions[mapping.Partition]; exists {
			return fmt.Errorf("%w: duplicate Kafka partition mapping", ErrInvalidRequest)
		}
		partitions[mapping.Partition] = struct{}{}

		identity := shardIdentity{namespaceHash: mapping.Shard.Namespace.Hash(), shard: mapping.Shard.Shard}
		if prior, exists := shards[identity]; exists {
			if prior.Equal(mapping.Shard.Namespace) {
				return fmt.Errorf("%w: duplicate Kafka shard mapping", ErrInvalidRequest)
			}
			return fmt.Errorf("%w: namespace digest collision", ErrCorrupt)
		}
		shards[identity] = mapping.Shard.Namespace
	}
	return nil
}

func ValidateKafkaSourceExpectation(source KafkaSourceExpectation) error {
	if err := ValidateKafkaPartitionMapping(source.Mapping); err != nil {
		return err
	}
	if source.OwnerEpoch == 0 || source.ExpectedNextOffset < 0 ||
		source.ResultingNextOffset <= source.ExpectedNextOffset {
		return fmt.Errorf("%w: invalid Kafka source expectation", ErrInvalidRequest)
	}
	if source.LeaderEpoch.Valid && source.LeaderEpoch.Value < 0 {
		return fmt.Errorf("%w: negative Kafka leader epoch", ErrInvalidRequest)
	}
	return nil
}

func ValidateCommitKafkaChunk(request CommitKafkaChunkRequest) error {
	if err := ValidateKafkaSourceExpectation(request.Source); err != nil {
		return err
	}
	if err := ValidateChunkPublication(request.Publication); err != nil {
		return err
	}
	if !SameShardKey(request.Source.Mapping.Shard, request.Publication.Fence.Shard) {
		return fmt.Errorf("%w: publication shard does not match durable Kafka mapping", ErrInvalidRequest)
	}
	return nil
}

func SameKafkaPartitionMapping(a, b KafkaPartitionMapping) bool {
	return a.BindingID == b.BindingID && a.TopicID == b.TopicID && a.Partition == b.Partition &&
		a.MappingGeneration == b.MappingGeneration && SameShardKey(a.Shard, b.Shard)
}

// ValidateNewKafkaChunk applies the Kafka source and writer preconditions for
// a genuinely new operation. durable must be loaded and locked from metastore
// state; a request-assembled lease is not authoritative. An implementation
// first reconciles an exact HashKafkaChunkCommit replay, then calls this helper
// before inserting a ChunkRef or advancing an offset or timeline head.
func ValidateNewKafkaChunk(durable KafkaPartitionLease, request CommitKafkaChunkRequest) error {
	if err := ValidateCommitKafkaChunk(request); err != nil {
		return err
	}
	if err := ValidateKafkaPartitionMapping(durable.Mapping); err != nil {
		return fmt.Errorf("%w: durable Kafka mapping: %v", ErrCorrupt, err)
	}
	if !SameKafkaPartitionMapping(durable.Mapping, request.Source.Mapping) {
		return fmt.Errorf("%w: Kafka partition mapping changed", ErrStaleSource)
	}
	if durable.SourceOwnerEpoch == 0 || durable.NextOffset < 0 {
		return fmt.Errorf("%w: invalid durable Kafka source state", ErrCorrupt)
	}
	if durable.SourceOwnerEpoch != request.Source.OwnerEpoch {
		return fmt.Errorf("%w: Kafka source owner changed", ErrStaleSource)
	}
	if err := ValidateShardState(durable.Writer); err != nil {
		return fmt.Errorf("%w: durable Kafka writer state: %v", ErrCorrupt, err)
	}
	if !SameShardKey(durable.Mapping.Shard, durable.Writer.Fence.Shard) {
		return fmt.Errorf("%w: durable Kafka mapping and writer are cross-wired", ErrCorrupt)
	}
	if !SameWriterFence(durable.Writer.Fence, request.Publication.Fence) {
		return fmt.Errorf("%w: Kafka shard writer changed", ErrStaleWriter)
	}
	if durable.NextOffset != request.Source.ExpectedNextOffset {
		return fmt.Errorf("%w: Kafka offset=%d expected=%d", ErrSourcePosition,
			durable.NextOffset, request.Source.ExpectedNextOffset)
	}
	return ValidateNewChunkPublication(durable.Writer, request.Publication)
}

func ValidateKafkaActivation(request KafkaActivationRequest) error {
	if isZero128(request.BindingID) || isZero128(request.TopicID) ||
		request.ConfigurationHash == ([32]byte{}) || len(request.Partitions) == 0 ||
		len(request.Partitions) > MaxActivationItems {
		return fmt.Errorf("%w: invalid Kafka activation", ErrInvalidRequest)
	}
	if err := ValidateOwnerID(request.Owner); err != nil {
		return err
	}
	seen := make(map[int32]struct{}, len(request.Partitions))
	for _, partition := range request.Partitions {
		if partition < 0 {
			return fmt.Errorf("%w: negative Kafka partition", ErrInvalidRequest)
		}
		if _, exists := seen[partition]; exists {
			return fmt.Errorf("%w: duplicate Kafka partition", ErrInvalidRequest)
		}
		seen[partition] = struct{}{}
	}
	return nil
}

// ValidateKafkaActivationResult proves that each returned source lease is
// paired with the writer state for its persisted mapped shard. Results are
// aligned with request.Partitions.
func ValidateKafkaActivationResult(request KafkaActivationRequest, leases []KafkaPartitionLease) error {
	if err := ValidateKafkaActivation(request); err != nil {
		return err
	}
	if len(leases) != len(request.Partitions) {
		return fmt.Errorf("%w: Kafka leases=%d partitions=%d", ErrCorrupt,
			len(leases), len(request.Partitions))
	}
	var mappingGeneration uint64
	var namespace Namespace
	type shardIdentity struct {
		namespaceHash [32]byte
		shard         uint32
	}
	shards := make(map[shardIdentity]Namespace, len(leases))
	for i, lease := range leases {
		if err := ValidateKafkaPartitionMapping(lease.Mapping); err != nil {
			return fmt.Errorf("%w: Kafka lease=%d mapping: %v", ErrCorrupt, i, err)
		}
		if lease.Mapping.BindingID != request.BindingID || lease.Mapping.TopicID != request.TopicID ||
			lease.Mapping.Partition != request.Partitions[i] || lease.SourceOwnerEpoch == 0 ||
			lease.NextOffset < 0 || (lease.LeaderEpoch.Valid && lease.LeaderEpoch.Value < 0) {
			return fmt.Errorf("%w: Kafka lease=%d source identity", ErrCorrupt, i)
		}
		if err := ValidateShardState(lease.Writer); err != nil {
			return fmt.Errorf("%w: Kafka lease=%d writer: %v", ErrCorrupt, i, err)
		}
		if !SameShardKey(lease.Mapping.Shard, lease.Writer.Fence.Shard) ||
			!lease.Writer.Fence.Owner.Equal(request.Owner) {
			return fmt.Errorf("%w: Kafka lease=%d mapped writer mismatch", ErrCorrupt, i)
		}
		if i == 0 {
			mappingGeneration = lease.Mapping.MappingGeneration
			namespace = lease.Mapping.Shard.Namespace
		} else if lease.Mapping.MappingGeneration != mappingGeneration ||
			!lease.Mapping.Shard.Namespace.Equal(namespace) {
			return fmt.Errorf("%w: Kafka leases span mapping generations or namespaces", ErrCorrupt)
		}
		identity := shardIdentity{
			namespaceHash: lease.Mapping.Shard.Namespace.Hash(), shard: lease.Mapping.Shard.Shard,
		}
		if prior, exists := shards[identity]; exists {
			if prior.Equal(lease.Mapping.Shard.Namespace) {
				return fmt.Errorf("%w: Kafka leases duplicate a mapped shard", ErrCorrupt)
			}
			return fmt.Errorf("%w: Kafka lease namespace digest collision", ErrCorrupt)
		}
		shards[identity] = lease.Mapping.Shard.Namespace
	}
	return nil
}

func HashKafkaChunkCommit(request CommitKafkaChunkRequest) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/kafka-chunk/v1\x00"))
	publicationHash := HashChunkPublication(request.Publication)
	_, _ = h.Write(publicationHash[:])
	_, _ = h.Write(request.Source.Mapping.BindingID[:])
	_, _ = h.Write(request.Source.Mapping.TopicID[:])
	var scalar [8]byte
	binary.BigEndian.PutUint32(scalar[:4], uint32(request.Source.Mapping.Partition))
	_, _ = h.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], request.Source.Mapping.MappingGeneration)
	_, _ = h.Write(scalar[:])
	writeLengthBytes(h, request.Source.Mapping.Shard.Namespace.Bytes())
	binary.BigEndian.PutUint32(scalar[:4], request.Source.Mapping.Shard.Shard)
	_, _ = h.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], request.Source.OwnerEpoch)
	_, _ = h.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(request.Source.ExpectedNextOffset))
	_, _ = h.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(request.Source.ResultingNextOffset))
	_, _ = h.Write(scalar[:])
	if request.Source.LeaderEpoch.Valid {
		_, _ = h.Write([]byte{1})
		binary.BigEndian.PutUint32(scalar[:4], uint32(request.Source.LeaderEpoch.Value))
		_, _ = h.Write(scalar[:4])
	} else {
		_, _ = h.Write([]byte{0})
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}
