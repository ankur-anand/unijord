package metastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/ankur-anand/unijord/internal/chunkref"
)

// TimelineMutation is the complete logical effect one UJTC has on one
// timeline. A timeline appears at most once in a ChunkPublication.
type TimelineMutation struct {
	Key              TimelineKey
	ExpectedNextLSN  uint64
	LastLSN          uint64
	FirstTimestampMS int64
	LastTimestampMS  int64
	SealAfterAppend  bool
}

// ChunkPublication is the connector-neutral unit atomically made visible by
// the metastore. Ingress retry state wraps this value in direct.go or kafka.go.
type ChunkPublication struct {
	Fence     WriterFence
	Chunk     chunkref.Ref
	Mutations []TimelineMutation
}

type ChunkPublicationResult struct {
	// Heads are the exact post-state acknowledged by this publication. On an
	// exact replay they may be older than the current timeline heads.
	Heads             []TimelineHead
	Replayed          bool
	WriterFenceActive bool
}

func ValidateChunkPublication(publication ChunkPublication) error {
	if err := ValidateWriterFence(publication.Fence); err != nil {
		return err
	}
	if err := chunkref.Validate(publication.Chunk); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if publication.Chunk.NamespaceHash != publication.Fence.Shard.Namespace.Hash() ||
		publication.Chunk.Shard != publication.Fence.Shard.Shard ||
		publication.Chunk.WriterEpoch != publication.Fence.Epoch {
		return fmt.Errorf("%w: chunk identity does not match writer fence", ErrInvalidRequest)
	}
	if publication.Chunk.Sequence == math.MaxUint64 {
		return fmt.Errorf("%w: chunk sequence exhausted", ErrInvalidRequest)
	}
	return validateChunkMutations(publication.Fence.Shard.Namespace, publication.Chunk, publication.Mutations)
}

// ValidateNewChunkPublication applies the durable shard preconditions for a
// genuinely new publication. Backends must first reconcile an exact retry by
// its canonical commit identity. If no exact retry exists, they call this
// validation after loading and locking the authoritative ShardState and before
// inserting a chunk reference or changing a timeline head.
//
// Fence validation deliberately precedes sequence validation: a stale writer
// must receive ErrStaleWriter even when its old sequence has been reused.
func ValidateNewChunkPublication(state ShardState, publication ChunkPublication) error {
	if err := ValidateChunkPublication(publication); err != nil {
		return err
	}
	if err := ValidateShardState(state); err != nil {
		return err
	}
	if !SameWriterFence(state.Fence, publication.Fence) {
		return fmt.Errorf("%w: writer fence changed", ErrStaleWriter)
	}
	if publication.Chunk.Sequence != state.NextChunkSequence {
		return fmt.Errorf("%w: chunk sequence=%d next=%d", ErrConflict,
			publication.Chunk.Sequence, state.NextChunkSequence)
	}
	return CheckActiveTailCapacity(state.NextChunkSequence, state.MaterializedBefore)
}

func validateChunkMutations(namespace Namespace, chunk chunkref.Ref, mutations []TimelineMutation) error {
	if len(mutations) == 0 || len(mutations) != int(chunk.TimelineCount) {
		return fmt.Errorf("%w: mutations=%d chunk timelines=%d", ErrInvalidRequest,
			len(mutations), chunk.TimelineCount)
	}

	seen := make(map[[32]byte]TimelineKey, len(mutations))
	var records uint64
	minTimestamp := int64(math.MaxInt64)
	maxTimestamp := int64(math.MinInt64)
	for i, mutation := range mutations {
		if err := ValidateTimelineKey(mutation.Key); err != nil {
			return fmt.Errorf("%w: mutation=%d: %v", ErrInvalidRequest, i, err)
		}
		if !mutation.Key.Namespace().Equal(namespace) {
			return fmt.Errorf("%w: mutation=%d namespace mismatch", ErrInvalidRequest, i)
		}
		if mutation.LastLSN < mutation.ExpectedNextLSN || mutation.LastLSN == math.MaxUint64 ||
			mutation.LastTimestampMS < mutation.FirstTimestampMS {
			return fmt.Errorf("%w: mutation=%d range or timestamps", ErrInvalidRequest, i)
		}
		hash := mutation.Key.Hash()
		if prior, ok := seen[hash]; ok {
			if prior.Equal(mutation.Key) {
				return fmt.Errorf("%w: duplicate timeline mutation", ErrInvalidRequest)
			}
			return fmt.Errorf("%w: timeline digest collision", ErrCorrupt)
		}
		seen[hash] = mutation.Key
		span := mutation.LastLSN - mutation.ExpectedNextLSN + 1
		if records > math.MaxUint64-span {
			return fmt.Errorf("%w: record count overflow", ErrInvalidRequest)
		}
		records += span
		minTimestamp = min(minTimestamp, mutation.FirstTimestampMS)
		maxTimestamp = max(maxTimestamp, mutation.LastTimestampMS)
	}
	if records != uint64(chunk.RecordCount) || minTimestamp != chunk.MinTimestampMS ||
		maxTimestamp != chunk.MaxTimestampMS {
		return fmt.Errorf("%w: chunk summary does not match mutations", ErrInvalidRequest)
	}
	return nil
}

// HashChunkPublication returns the canonical identity of the connector-neutral
// publication. Mutation input order does not affect the result.
func HashChunkPublication(publication ChunkPublication) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/chunk-publication/v1\x00"))
	writeWriterFence(h, publication.Fence)
	writeChunkAndMutations(h, publication.Chunk, publication.Mutations)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func writeChunkAndMutations(w byteWriter, ref chunkref.Ref, mutations []TimelineMutation) {
	writeChunkRef(w, ref)

	type orderedMutation struct {
		hash  [32]byte
		index int
	}
	order := make([]orderedMutation, len(mutations))
	for i := range mutations {
		order[i] = orderedMutation{hash: mutations[i].Key.Hash(), index: i}
	}
	slices.SortFunc(order, func(a, b orderedMutation) int {
		return bytes.Compare(a.hash[:], b.hash[:])
	})

	var scalar [8]byte
	for _, item := range order {
		mutation := mutations[item.index]
		writeLengthBytes(w, mutation.Key.Namespace().Bytes())
		writeLengthBytes(w, mutation.Key.Bytes())
		binary.BigEndian.PutUint64(scalar[:], mutation.ExpectedNextLSN)
		_, _ = w.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], mutation.LastLSN)
		_, _ = w.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], uint64(mutation.FirstTimestampMS))
		_, _ = w.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], uint64(mutation.LastTimestampMS))
		_, _ = w.Write(scalar[:])
		if mutation.SealAfterAppend {
			_, _ = w.Write([]byte{1})
		} else {
			_, _ = w.Write([]byte{0})
		}
	}
}

func writeWriterFence(w byteWriter, fence WriterFence) {
	writeLengthBytes(w, fence.Shard.Namespace.Bytes())
	var scalar [8]byte
	binary.BigEndian.PutUint32(scalar[:4], fence.Shard.Shard)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], fence.Epoch)
	_, _ = w.Write(scalar[:])
	writeLengthBytes(w, fence.Owner.Bytes())
}

func writeChunkRef(w byteWriter, ref chunkref.Ref) {
	writeLengthBytes(w, []byte(ref.Key))
	var scalar [8]byte
	binary.BigEndian.PutUint16(scalar[:2], ref.FormatVersion)
	_, _ = w.Write(scalar[:2])
	_, _ = w.Write(ref.NamespaceHash[:])
	binary.BigEndian.PutUint32(scalar[:4], ref.Shard)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], ref.WriterEpoch)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], ref.Sequence)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint32(scalar[:4], ref.RecordCount)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint32(scalar[:4], ref.TimelineCount)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], ref.SizeBytes)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(ref.MinTimestampMS))
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(ref.MaxTimestampMS))
	_, _ = w.Write(scalar[:])
	_, _ = w.Write(ref.SHA256[:])
}
