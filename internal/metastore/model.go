package metastore

import (
	"fmt"
)

const (
	DefaultListLimit     = 128
	MaxListLimit         = 4096
	MaxHeadLookupKeys    = 4096
	MaxActiveTailChunks  = 4096
	MaxActivationItems   = 4096
	DefaultReadPlanLimit = 128
	MaxReadPlanLimit     = 4096
)

type TimelineState uint8

const (
	TimelineOpen TimelineState = iota + 1
	TimelineSealed
)

type WriterFence struct {
	Shard ShardKey
	Epoch uint64
	Owner OwnerID
}

type ShardState struct {
	Fence              WriterFence
	NextChunkSequence  uint64
	MaterializedBefore uint64
}

type TimelineHead struct {
	Key             TimelineKey
	Shard           uint32
	NextLSN         uint64
	LastTimestampMS int64
	State           TimelineState
	Revision        uint64
}

type shardIdentity struct {
	namespaceHash [32]byte
	shard         uint32
}

type HeadLookup struct {
	Found bool
	Head  TimelineHead
}

func ValidateWriterFence(fence WriterFence) error {
	if err := ValidateShardKey(fence.Shard); err != nil {
		return err
	}
	if fence.Epoch == 0 {
		return fmt.Errorf("%w: writer epoch=%d", ErrInvalidRequest, fence.Epoch)
	}
	if err := ValidateOwnerID(fence.Owner); err != nil {
		return err
	}
	return nil
}

func SameWriterFence(a, b WriterFence) bool {
	return a.Epoch == b.Epoch && SameShardKey(a.Shard, b.Shard) && a.Owner.Equal(b.Owner)
}

// ValidateShardState checks the durable shard counters independently of any
// publication. A backend must validate this state after loading it and before
// using it to authorize a write.
func ValidateShardState(state ShardState) error {
	if err := ValidateWriterFence(state.Fence); err != nil {
		return err
	}
	if state.MaterializedBefore > state.NextChunkSequence {
		return fmt.Errorf("%w: materialized before=%d next chunk=%d", ErrCorrupt,
			state.MaterializedBefore, state.NextChunkSequence)
	}
	return nil
}

func SameShardKey(a, b ShardKey) bool {
	return a.Shard == b.Shard && a.Namespace.Equal(b.Namespace)
}

func validateShardSet(shards []ShardKey, duplicateMessage string) error {
	if len(shards) == 0 || len(shards) > MaxActivationItems {
		return fmt.Errorf("%w: shards=%d", ErrInvalidRequest, len(shards))
	}
	seen := make(map[shardIdentity]Namespace, len(shards))
	namespaces := make(map[[32]byte]Namespace, len(shards))
	for i, shard := range shards {
		if err := ValidateShardKey(shard); err != nil {
			return fmt.Errorf("%w: shard=%d: %v", ErrInvalidRequest, i, err)
		}
		namespaceHash := shard.Namespace.Hash()
		if prior, exists := namespaces[namespaceHash]; exists && !prior.Equal(shard.Namespace) {
			return fmt.Errorf("%w: namespace digest collision", ErrCorrupt)
		}
		namespaces[namespaceHash] = shard.Namespace
		identity := shardIdentity{namespaceHash: namespaceHash, shard: shard.Shard}
		if prior, exists := seen[identity]; exists {
			if prior.Equal(shard.Namespace) {
				return fmt.Errorf("%w: %s", ErrInvalidRequest, duplicateMessage)
			}
			return fmt.Errorf("%w: namespace digest collision", ErrCorrupt)
		}
		seen[identity] = shard.Namespace
	}
	return nil
}

func ValidateTimelineHead(head TimelineHead) error {
	if err := ValidateTimelineKey(head.Key); err != nil {
		return err
	}
	if head.Revision == 0 || (head.State != TimelineOpen && head.State != TimelineSealed) {
		return fmt.Errorf("%w: invalid timeline head", ErrCorrupt)
	}
	return nil
}

func ValidateHeadLookup(keys []TimelineKey) error {
	if len(keys) == 0 || len(keys) > MaxHeadLookupKeys {
		return fmt.Errorf("%w: head lookup keys=%d", ErrInvalidRequest, len(keys))
	}
	for i, key := range keys {
		if err := ValidateTimelineKey(key); err != nil {
			return fmt.Errorf("%w: head lookup key=%d: %v", ErrInvalidRequest, i, err)
		}
	}
	return nil
}

func ValidateHeadLookupResult(key TimelineKey, lookup HeadLookup) error {
	if err := ValidateTimelineKey(key); err != nil {
		return err
	}
	if !lookup.Found {
		if lookup.Head != (TimelineHead{}) {
			return fmt.Errorf("%w: missing lookup contains head state", ErrCorrupt)
		}
		return nil
	}
	if !lookup.Head.Key.Equal(key) {
		return fmt.Errorf("%w: head identity mismatch", ErrCorrupt)
	}
	return ValidateTimelineHead(lookup.Head)
}

func CheckActiveTailCapacity(nextChunkSequence, materializedBefore uint64) error {
	if materializedBefore > nextChunkSequence {
		return fmt.Errorf("%w: materialized before=%d next chunk=%d", ErrCorrupt,
			materializedBefore, nextChunkSequence)
	}
	active := nextChunkSequence - materializedBefore
	if active >= MaxActiveTailChunks {
		return fmt.Errorf("%w: chunks=%d limit=%d", ErrTailFull, active, MaxActiveTailChunks)
	}
	return nil
}
