package metastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/packref"
)

type TimelinePackRange struct {
	Key      TimelineKey
	FirstLSN uint64
	LastLSN  uint64
	Pack     packref.Ref
}

type SourceChunkCoverage struct {
	Chunk     chunkref.Ref
	Mutations []TimelineMutation
}

type PublishPackRequest struct {
	Shard                      ShardKey
	ExpectedMaterializedBefore uint64
	Pack                       packref.Ref
	Ranges                     []TimelinePackRange
	SourceChunks               []SourceChunkCoverage
	DeleteNotBeforeMS          int64
}

type PublishPackResult struct {
	MaterializedBefore uint64
	Replayed           bool
}

type MaterializationPlan struct {
	Shard              ShardKey
	MaterializedBefore uint64
	Chunks             []chunkref.Ref
	More               bool
}

type MaterializerClaimRequest struct {
	Owner  OwnerID
	Shards []ShardKey
}

type MaterializerLease struct {
	Shard              ShardKey
	MaterializedBefore uint64
	NextChunkSequence  uint64
}

type MaterializationStore interface {
	// ClaimMaterializerShards returns leases aligned with request.Shards.
	ClaimMaterializerShards(context.Context, MaterializerClaimRequest) ([]MaterializerLease, error)
	PlanMaterialization(context.Context, ShardKey, int) (MaterializationPlan, error)
	PublishPack(context.Context, PublishPackRequest) (PublishPackResult, error)
}

func ValidatePublishPack(request PublishPackRequest) error {
	if err := ValidateShardKey(request.Shard); err != nil {
		return err
	}
	if request.DeleteNotBeforeMS < 0 {
		return fmt.Errorf("%w: negative delete-not-before timestamp", ErrInvalidRequest)
	}
	if err := packref.Validate(request.Pack); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if request.Pack.NamespaceHash != request.Shard.Namespace.Hash() ||
		request.Pack.Shard != request.Shard.Shard ||
		request.Pack.FirstChunkSequence != request.ExpectedMaterializedBefore {
		return fmt.Errorf("%w: pack identity does not match shard or cursor", ErrInvalidRequest)
	}
	if request.Pack.LastChunkSequence == math.MaxUint64 || len(request.Ranges) == 0 ||
		len(request.Ranges) != int(request.Pack.TimelineCount) {
		return fmt.Errorf("%w: pack ranges=%d timelines=%d", ErrInvalidRequest,
			len(request.Ranges), request.Pack.TimelineCount)
	}

	ranges := make(map[[32]byte]TimelinePackRange, len(request.Ranges))
	var rangeRecords uint64
	for i, item := range request.Ranges {
		if err := ValidateTimelineKey(item.Key); err != nil {
			return fmt.Errorf("%w: range=%d: %v", ErrInvalidRequest, i, err)
		}
		if !item.Key.Namespace().Equal(request.Shard.Namespace) || item.LastLSN < item.FirstLSN ||
			item.LastLSN == math.MaxUint64 {
			return fmt.Errorf("%w: invalid pack range=%d", ErrInvalidRequest, i)
		}
		if item.Pack != (packref.Ref{}) && !packref.Same(item.Pack, request.Pack) {
			return fmt.Errorf("%w: range=%d carries another pack", ErrInvalidRequest, i)
		}
		hash := item.Key.Hash()
		if prior, exists := ranges[hash]; exists {
			if prior.Key.Equal(item.Key) {
				return fmt.Errorf("%w: duplicate timeline pack range", ErrInvalidRequest)
			}
			return fmt.Errorf("%w: timeline digest collision", ErrCorrupt)
		}
		ranges[hash] = item
		span := item.LastLSN - item.FirstLSN + 1
		if rangeRecords > math.MaxUint64-span {
			return fmt.Errorf("%w: pack range count overflow", ErrInvalidRequest)
		}
		rangeRecords += span
	}
	if rangeRecords != uint64(request.Pack.RecordCount) {
		return fmt.Errorf("%w: range records=%d pack records=%d", ErrInvalidRequest,
			rangeRecords, request.Pack.RecordCount)
	}
	return validateSourceCoverage(request, ranges)
}

type coveredTimeline struct {
	key             TimelineKey
	firstLSN        uint64
	lastLSN         uint64
	lastTimestampMS int64
}

func validateSourceCoverage(request PublishPackRequest, ranges map[[32]byte]TimelinePackRange) error {
	wantSources := request.Pack.LastChunkSequence - request.Pack.FirstChunkSequence + 1
	if uint64(len(request.SourceChunks)) != wantSources {
		return fmt.Errorf("%w: source chunks=%d want=%d", ErrInvalidRequest,
			len(request.SourceChunks), wantSources)
	}
	covered := make(map[[32]byte]coveredTimeline, len(ranges))
	var records uint64
	for i, source := range request.SourceChunks {
		wantSequence := request.Pack.FirstChunkSequence + uint64(i)
		if err := chunkref.Validate(source.Chunk); err != nil {
			return fmt.Errorf("%w: source chunk=%d: %v", ErrInvalidRequest, i, err)
		}
		if source.Chunk.NamespaceHash != request.Shard.Namespace.Hash() ||
			source.Chunk.Shard != request.Shard.Shard || source.Chunk.Sequence != wantSequence {
			return fmt.Errorf("%w: source chunk=%d identity or sequence", ErrInvalidRequest, i)
		}
		if err := validateChunkMutations(request.Shard.Namespace, source.Chunk, source.Mutations); err != nil {
			return fmt.Errorf("%w: source chunk=%d coverage: %v", ErrInvalidRequest, i, err)
		}
		if records > math.MaxUint64-uint64(source.Chunk.RecordCount) {
			return fmt.Errorf("%w: source record count overflow", ErrInvalidRequest)
		}
		records += uint64(source.Chunk.RecordCount)
		for _, mutation := range source.Mutations {
			hash := mutation.Key.Hash()
			current, exists := covered[hash]
			if !exists {
				covered[hash] = coveredTimeline{key: mutation.Key, firstLSN: mutation.ExpectedNextLSN,
					lastLSN: mutation.LastLSN, lastTimestampMS: mutation.LastTimestampMS}
				continue
			}
			if !current.key.Equal(mutation.Key) {
				return fmt.Errorf("%w: source timeline digest collision", ErrCorrupt)
			}
			if current.lastLSN == math.MaxUint64 || mutation.ExpectedNextLSN != current.lastLSN+1 ||
				mutation.FirstTimestampMS < current.lastTimestampMS {
				return fmt.Errorf("%w: source coverage is not contiguous", ErrInvalidRequest)
			}
			current.lastLSN = mutation.LastLSN
			current.lastTimestampMS = mutation.LastTimestampMS
			covered[hash] = current
		}
	}
	if records != uint64(request.Pack.RecordCount) || len(covered) != len(ranges) {
		return fmt.Errorf("%w: source coverage records=%d timelines=%d", ErrInvalidRequest,
			records, len(covered))
	}
	for hash, item := range ranges {
		current, exists := covered[hash]
		if !exists || !current.key.Equal(item.Key) || current.firstLSN != item.FirstLSN ||
			current.lastLSN != item.LastLSN {
			return fmt.Errorf("%w: pack range does not exactly match sources", ErrInvalidRequest)
		}
	}
	return nil
}

func ValidateMaterializerClaim(request MaterializerClaimRequest) error {
	if err := ValidateOwnerID(request.Owner); err != nil {
		return err
	}
	return validateShardSet(request.Shards, "duplicate materializer shard")
}

func HashSourceChunkCoverage(source SourceChunkCoverage) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/source-chunk-coverage/v1\x00"))
	writeChunkAndMutations(h, source.Chunk, source.Mutations)
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func HashPackPublication(request PublishPackRequest) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("unijord/metastore/pack-publication/v1\x00"))
	writePackRef(h, request.Pack)
	ranges := slices.Clone(request.Ranges)
	slices.SortFunc(ranges, func(a, b TimelinePackRange) int {
		aHash := a.Key.Hash()
		bHash := b.Key.Hash()
		return bytes.Compare(aHash[:], bHash[:])
	})
	var scalar [8]byte
	for _, item := range ranges {
		writeLengthBytes(h, item.Key.Namespace().Bytes())
		writeLengthBytes(h, item.Key.Bytes())
		binary.BigEndian.PutUint64(scalar[:], item.FirstLSN)
		_, _ = h.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], item.LastLSN)
		_, _ = h.Write(scalar[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func writePackRef(w byteWriter, ref packref.Ref) {
	_, _ = w.Write(ref.ID[:])
	writeLengthBytes(w, []byte(ref.Key))
	var scalar [8]byte
	binary.BigEndian.PutUint16(scalar[:2], ref.FormatVersion)
	_, _ = w.Write(scalar[:2])
	_, _ = w.Write(ref.NamespaceHash[:])
	binary.BigEndian.PutUint32(scalar[:4], ref.Shard)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], ref.FirstChunkSequence)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], ref.LastChunkSequence)
	_, _ = w.Write(scalar[:])
	binary.BigEndian.PutUint32(scalar[:4], ref.RecordCount)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint32(scalar[:4], ref.TimelineCount)
	_, _ = w.Write(scalar[:4])
	binary.BigEndian.PutUint64(scalar[:], ref.SizeBytes)
	_, _ = w.Write(scalar[:])
	_, _ = w.Write(ref.SHA256[:])
}
