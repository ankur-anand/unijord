package postgres

import (
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/metastore"
)

type headWrite struct {
	insert bool
	hash   [32]byte
	head   metastore.TimelineHead
}

type publicationPlan struct {
	heads             []headWrite
	nextChunkSequence uint64
}

// planNewPublication is pure: it validates locked durable state and computes
// the complete logical post-state without issuing SQL or retaining new byte
// slices. Its result remains aligned with publication.Mutations.
func planNewPublication(state metastore.ShardState, publication metastore.ChunkPublication,
	stored map[[32]byte]metastore.TimelineHead,
) (publicationPlan, error) {
	if err := metastore.ValidateNewChunkPublication(state, publication); err != nil {
		return publicationPlan{}, err
	}
	plan := publicationPlan{
		heads:             make([]headWrite, len(publication.Mutations)),
		nextChunkSequence: publication.Chunk.Sequence + 1,
	}
	for i, mutation := range publication.Mutations {
		hash := mutation.Key.Hash()
		head, exists := stored[hash]
		if !exists {
			if mutation.ExpectedNextLSN != 0 {
				return publicationPlan{}, fmt.Errorf("%w: timeline expected=%d durable=0",
					metastore.ErrConflict, mutation.ExpectedNextLSN)
			}
			state := metastore.TimelineOpen
			if mutation.SealAfterAppend {
				state = metastore.TimelineSealed
			}
			head = metastore.TimelineHead{
				Key: mutation.Key, Shard: publication.Chunk.Shard,
				NextLSN: mutation.LastLSN + 1, LastTimestampMS: mutation.LastTimestampMS,
				State: state, Revision: 1,
			}
			plan.heads[i] = headWrite{insert: true, hash: hash, head: head}
			continue
		}

		if err := metastore.ValidateTimelineHead(head); err != nil {
			return publicationPlan{}, fmt.Errorf("%w: invalid stored timeline head: %v",
				metastore.ErrCorrupt, err)
		}
		if !head.Key.Equal(mutation.Key) {
			return publicationPlan{}, fmt.Errorf("%w: timeline digest collision", metastore.ErrCorrupt)
		}
		if head.Shard != publication.Chunk.Shard {
			return publicationPlan{}, fmt.Errorf("%w: timeline belongs to shard=%d",
				metastore.ErrConflict, head.Shard)
		}
		if head.State == metastore.TimelineSealed {
			return publicationPlan{}, metastore.ErrSealed
		}
		if head.NextLSN != mutation.ExpectedNextLSN {
			return publicationPlan{}, fmt.Errorf("%w: timeline expected=%d durable=%d",
				metastore.ErrConflict, mutation.ExpectedNextLSN, head.NextLSN)
		}
		if mutation.FirstTimestampMS < head.LastTimestampMS {
			return publicationPlan{}, fmt.Errorf("%w: timestamp=%d previous=%d",
				metastore.ErrConflict, mutation.FirstTimestampMS, head.LastTimestampMS)
		}
		if head.Revision == math.MaxUint64 {
			return publicationPlan{}, fmt.Errorf("%w: timeline revision exhausted", metastore.ErrCorrupt)
		}

		head.NextLSN = mutation.LastLSN + 1
		head.LastTimestampMS = mutation.LastTimestampMS
		head.Revision++
		if mutation.SealAfterAppend {
			head.State = metastore.TimelineSealed
		}
		plan.heads[i] = headWrite{hash: hash, head: head}
	}
	return plan, nil
}

func headsFromPlan(plan publicationPlan) []metastore.TimelineHead {
	heads := make([]metastore.TimelineHead, len(plan.heads))
	for i := range plan.heads {
		heads[i] = plan.heads[i].head
	}
	return heads
}
