package metastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

type ProducerPosition struct {
	ProducerID    ProducerID
	IncarnationID ProducerIncarnationID
	Epoch         uint64
	Sequence      uint64
}

// DirectAppendOperation binds one public operation to the exact logical LSN
// range assigned before its group-committed UJTC was built.
type DirectAppendOperation struct {
	Producer        ProducerPosition
	OperationHash   [32]byte
	Timeline        TimelineKey
	FirstLSN        uint64
	LastLSN         uint64
	SealAfterAppend bool
}

type CommitDirectChunkRequest struct {
	Operations  []DirectAppendOperation
	Publication ChunkPublication
}

// DirectReceipt is stable across an exact producer replay. Head is the exact
// acknowledged group post-state, not a current metastore read, and may contain
// another operation from the same atomic group. NextLSN is the exclusive end
// of this operation's own range.
type DirectReceipt struct {
	Producer ProducerPosition
	Timeline TimelineKey
	FirstLSN uint64
	LastLSN  uint64
	NextLSN  uint64
	Head     TimelineHead
	Replayed bool
}

type CommitDirectChunkResult struct {
	// Heads are the exact post-state acknowledged by this commit. They remain
	// stable on replay even after a timeline advances or seals.
	Receipts []DirectReceipt
	Heads    []TimelineHead
	Replayed bool
	// WriterFenceActive reports the authority observed while producing this
	// result. False proves takeover was observed; true is not a lease against a
	// takeover immediately afterward.
	WriterFenceActive bool
}

type CommitDirectSealRequest struct {
	Producer        ProducerPosition
	OperationHash   [32]byte
	Fence           WriterFence
	Timeline        TimelineKey
	ExpectedNextLSN uint64
}

type CommitDirectSealResult struct {
	Producer ProducerPosition
	// Head is the exact post-state acknowledged by this seal. It may be older
	// than the current head when an exact retry is reconciled.
	Head     TimelineHead
	Replayed bool
	// WriterFenceActive has observation semantics, not lease semantics.
	WriterFenceActive bool
}

type DirectShardClaimRequest struct {
	Owner  OwnerID
	Shards []ShardKey
}

type DirectShardLease struct {
	State ShardState
}

type DirectCommitter interface {
	// CommitDirectChunk obeys the commit and reconciliation rules in
	// CONFORMANCE.md. In particular, exact replay precedes authority and head
	// checks, while a genuinely new operation validates future authority first.
	CommitDirectChunk(context.Context, CommitDirectChunkRequest) (CommitDirectChunkResult, error)
	// CommitDirectSeal uses HashDirectSealCommit as its stable operation
	// identity and obeys the same ambiguity and exact-replay rules.
	CommitDirectSeal(context.Context, CommitDirectSealRequest) (CommitDirectSealResult, error)
}

type DirectActivator interface {
	// ClaimDirectShards atomically claims the complete bounded shard set and
	// returns leases aligned with request.Shards. Repeating a claim with the
	// same owner is idempotent. A different owner advances each writer epoch.
	ClaimDirectShards(context.Context, DirectShardClaimRequest) ([]DirectShardLease, error)
}

func ValidateProducerPosition(position ProducerPosition) error {
	if isZero128(position.ProducerID) || isZero128(position.IncarnationID) || position.Epoch == 0 {
		return fmt.Errorf("%w: incomplete producer position", ErrInvalidRequest)
	}
	return nil
}

func ValidateCommitDirectChunk(request CommitDirectChunkRequest) error {
	if err := ValidateChunkPublication(request.Publication); err != nil {
		return err
	}
	if len(request.Operations) == 0 {
		return fmt.Errorf("%w: no direct operations", ErrInvalidRequest)
	}

	mutations := make(map[[32]byte]TimelineMutation, len(request.Publication.Mutations))
	for _, mutation := range request.Publication.Mutations {
		mutations[mutation.Key.Hash()] = mutation
	}
	type operationGroup struct {
		key        TimelineKey
		operations []DirectAppendOperation
	}
	groups := make(map[[32]byte]operationGroup, len(mutations))
	producers := make(map[ProducerID]struct{}, len(request.Operations))
	for i, operation := range request.Operations {
		if err := ValidateProducerPosition(operation.Producer); err != nil {
			return fmt.Errorf("%w: operation=%d: %v", ErrInvalidRequest, i, err)
		}
		if operation.OperationHash == ([32]byte{}) {
			return fmt.Errorf("%w: operation=%d zero operation hash", ErrInvalidRequest, i)
		}
		if _, exists := producers[operation.Producer.ProducerID]; exists {
			return fmt.Errorf("%w: producer appears more than once", ErrInvalidRequest)
		}
		producers[operation.Producer.ProducerID] = struct{}{}
		if err := ValidateTimelineKey(operation.Timeline); err != nil {
			return fmt.Errorf("%w: operation=%d: %v", ErrInvalidRequest, i, err)
		}
		if operation.LastLSN < operation.FirstLSN || operation.LastLSN == math.MaxUint64 {
			return fmt.Errorf("%w: operation=%d invalid LSN range", ErrInvalidRequest, i)
		}
		hash := operation.Timeline.Hash()
		mutation, exists := mutations[hash]
		if !exists || !mutation.Key.Equal(operation.Timeline) {
			return fmt.Errorf("%w: operation=%d has no matching mutation", ErrInvalidRequest, i)
		}
		group := groups[hash]
		if len(group.operations) != 0 && !group.key.Equal(operation.Timeline) {
			return fmt.Errorf("%w: timeline digest collision", ErrCorrupt)
		}
		group.key = operation.Timeline
		group.operations = append(group.operations, operation)
		groups[hash] = group
	}
	if len(groups) != len(mutations) {
		return fmt.Errorf("%w: one or more mutations have no direct operation", ErrInvalidRequest)
	}

	for hash, group := range groups {
		mutation := mutations[hash]
		slices.SortFunc(group.operations, func(a, b DirectAppendOperation) int {
			return compareUint64(a.FirstLSN, b.FirstLSN)
		})
		next := mutation.ExpectedNextLSN
		for i, operation := range group.operations {
			if operation.FirstLSN != next {
				return fmt.Errorf("%w: direct operation coverage is not contiguous", ErrInvalidRequest)
			}
			if operation.SealAfterAppend && i != len(group.operations)-1 {
				return fmt.Errorf("%w: seal operation is not final for timeline", ErrInvalidRequest)
			}
			next = operation.LastLSN + 1
		}
		last := group.operations[len(group.operations)-1]
		if last.LastLSN != mutation.LastLSN || last.SealAfterAppend != mutation.SealAfterAppend {
			return fmt.Errorf("%w: direct operations do not exactly cover mutation", ErrInvalidRequest)
		}
	}
	return nil
}

func ValidateCommitDirectSeal(request CommitDirectSealRequest) error {
	if err := ValidateProducerPosition(request.Producer); err != nil {
		return err
	}
	if request.OperationHash == ([32]byte{}) {
		return fmt.Errorf("%w: zero operation hash", ErrInvalidRequest)
	}
	if err := ValidateWriterFence(request.Fence); err != nil {
		return err
	}
	if err := ValidateTimelineKey(request.Timeline); err != nil {
		return err
	}
	if !request.Timeline.Namespace().Equal(request.Fence.Shard.Namespace) {
		return fmt.Errorf("%w: seal namespace mismatch", ErrInvalidRequest)
	}
	return nil
}

func ValidateDirectShardClaim(request DirectShardClaimRequest) error {
	if err := ValidateOwnerID(request.Owner); err != nil {
		return err
	}
	return validateShardSet(request.Shards, "duplicate shard claim")
}

func ValidateDirectShardClaimResult(request DirectShardClaimRequest, leases []DirectShardLease) error {
	if err := ValidateDirectShardClaim(request); err != nil {
		return err
	}
	if len(leases) != len(request.Shards) {
		return fmt.Errorf("%w: direct leases=%d shards=%d", ErrCorrupt, len(leases), len(request.Shards))
	}
	for i, lease := range leases {
		if err := ValidateShardState(lease.State); err != nil {
			return fmt.Errorf("%w: direct lease=%d: %v", ErrCorrupt, i, err)
		}
		if !SameShardKey(lease.State.Fence.Shard, request.Shards[i]) ||
			!lease.State.Fence.Owner.Equal(request.Owner) {
			return fmt.Errorf("%w: direct lease=%d identity mismatch", ErrCorrupt, i)
		}
	}
	return nil
}

// HashDirectChunkCommit identifies both the physical publication and every
// producer operation it acknowledges. Operation input order is immaterial.
func HashDirectChunkCommit(request CommitDirectChunkRequest) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/direct-chunk/v1\x00"))
	publicationHash := HashChunkPublication(request.Publication)
	_, _ = h.Write(publicationHash[:])
	operations := slices.Clone(request.Operations)
	slices.SortFunc(operations, func(a, b DirectAppendOperation) int {
		return bytes.Compare(a.Producer.ProducerID[:], b.Producer.ProducerID[:])
	})
	for _, operation := range operations {
		writeProducerPosition(h, operation.Producer)
		_, _ = h.Write(operation.OperationHash[:])
		writeLengthBytes(h, operation.Timeline.Namespace().Bytes())
		writeLengthBytes(h, operation.Timeline.Bytes())
		var scalar [8]byte
		binary.BigEndian.PutUint64(scalar[:], operation.FirstLSN)
		_, _ = h.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], operation.LastLSN)
		_, _ = h.Write(scalar[:])
		if operation.SealAfterAppend {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

// HashDirectSealCommit returns the canonical identity used to reconcile an
// ambiguous direct seal. Every authority, producer, operation, timeline, and
// expected-position field participates in the hash.
func HashDirectSealCommit(request CommitDirectSealRequest) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/direct-seal/v1\x00"))
	writeProducerPosition(h, request.Producer)
	_, _ = h.Write(request.OperationHash[:])
	writeWriterFence(h, request.Fence)
	writeLengthBytes(h, request.Timeline.Namespace().Bytes())
	writeLengthBytes(h, request.Timeline.Bytes())
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], request.ExpectedNextLSN)
	_, _ = h.Write(scalar[:])
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func writeProducerPosition(w byteWriter, position ProducerPosition) {
	_, _ = w.Write(position.ProducerID[:])
	_, _ = w.Write(position.IncarnationID[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], position.Epoch)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], position.Sequence)
	_, _ = w.Write(scalar[:])
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
