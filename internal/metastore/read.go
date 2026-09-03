package metastore

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/packref"
)

type TimelineCursor struct {
	NamespaceHash [32]byte
	KeyHash       [32]byte
}

type ListTimelinesRequest struct {
	Namespace Namespace
	After     *TimelineCursor
	Limit     int
}

type ListTimelinesResult struct {
	Heads []TimelineHead
	Next  *TimelineCursor
	More  bool
}

type TimelineStorage struct {
	PackCount          uint64
	ActiveShardChunks  uint32
	MaterializedBefore uint64
}

type TimelineDescription struct {
	Head    TimelineHead
	Storage TimelineStorage
}

// ReadPlan is one consistent routing snapshot. Pack ranges and active chunks
// must be returned together so a concurrent fold cannot make both sides miss.
type ReadPlan struct {
	Head               TimelineHead
	MaterializedBefore uint64
	PackRanges         []TimelinePackRange
	MorePackRanges     bool
	// NextPackLSN is the exclusive continuation position for the next read-plan
	// page. It is non-zero exactly when MorePackRanges is true.
	NextPackLSN  uint64
	ActiveChunks []chunkref.Ref
}

type Reader interface {
	Shard(context.Context, ShardKey) (ShardState, error)
	Head(context.Context, TimelineKey) (TimelineHead, error)
	LookupHeads(context.Context, []TimelineKey) ([]HeadLookup, error)
	ListTimelines(context.Context, ListTimelinesRequest) (ListTimelinesResult, error)
	DescribeTimeline(context.Context, TimelineKey) (TimelineDescription, error)
	// ReadPlan returns ranges beginning at or covering fromLSN. If another page
	// exists, MorePackRanges is true and NextPackLSN must advance strictly past
	// fromLSN. Callers continue from NextPackLSN; backends must never return an
	// empty or non-advancing page with MorePackRanges=true.
	ReadPlan(context.Context, TimelineKey, uint64, int) (ReadPlan, error)
}

func ValidateReadPlanRequest(key TimelineKey, fromLSN uint64, limit int) error {
	if err := ValidateTimelineKey(key); err != nil {
		return err
	}
	if limit < 0 || limit > MaxReadPlanLimit {
		return fmt.Errorf("%w: read-plan limit=%d", ErrInvalidRequest, limit)
	}
	return nil
}

func NormalizedReadPlanLimit(limit int) int {
	if limit <= 0 {
		return DefaultReadPlanLimit
	}
	return limit
}

// ValidateReadPlan enforces identity, boundedness, and forward progress at the
// backend boundary. It intentionally does not inspect object contents.
func ValidateReadPlan(key TimelineKey, fromLSN uint64, limit int, plan ReadPlan) error {
	if err := ValidateReadPlanRequest(key, fromLSN, limit); err != nil {
		return err
	}
	if err := ValidateTimelineHead(plan.Head); err != nil {
		return err
	}
	if !plan.Head.Key.Equal(key) {
		return fmt.Errorf("%w: read-plan head identity mismatch", ErrCorrupt)
	}
	if fromLSN > plan.Head.NextLSN {
		return fmt.Errorf("%w: from=%d next=%d", ErrInvalidRequest, fromLSN, plan.Head.NextLSN)
	}
	if len(plan.PackRanges) > NormalizedReadPlanLimit(limit) {
		return fmt.Errorf("%w: read-plan ranges=%d limit=%d", ErrCorrupt,
			len(plan.PackRanges), NormalizedReadPlanLimit(limit))
	}

	var previousLast uint64
	for i, item := range plan.PackRanges {
		if err := ValidateTimelineKey(item.Key); err != nil {
			return fmt.Errorf("%w: read-plan range=%d: %v", ErrCorrupt, i, err)
		}
		if !item.Key.Equal(key) || item.LastLSN < item.FirstLSN ||
			item.LastLSN >= plan.Head.NextLSN {
			return fmt.Errorf("%w: invalid read-plan range=%d", ErrCorrupt, i)
		}
		if err := packref.Validate(item.Pack); err != nil {
			return fmt.Errorf("%w: read-plan range=%d pack: %v", ErrCorrupt, i, err)
		}
		if item.Pack.NamespaceHash != key.Namespace().Hash() || item.Pack.Shard != plan.Head.Shard {
			return fmt.Errorf("%w: read-plan range=%d pack identity", ErrCorrupt, i)
		}
		if i == 0 {
			if item.FirstLSN > fromLSN || item.LastLSN < fromLSN {
				return fmt.Errorf("%w: first range does not reach requested LSN", ErrCorrupt)
			}
		} else if previousLast == math.MaxUint64 || item.FirstLSN != previousLast+1 {
			return fmt.Errorf("%w: pack ranges do not advance contiguously", ErrCorrupt)
		}
		previousLast = item.LastLSN
	}

	if plan.MorePackRanges {
		if len(plan.PackRanges) == 0 || previousLast == math.MaxUint64 ||
			plan.NextPackLSN != previousLast+1 || plan.NextPackLSN <= fromLSN ||
			plan.NextPackLSN >= plan.Head.NextLSN {
			return fmt.Errorf("%w: read-plan page does not make forward progress", ErrCorrupt)
		}
	} else if plan.NextPackLSN != 0 {
		return fmt.Errorf("%w: terminal read-plan page has continuation", ErrCorrupt)
	}

	if len(plan.ActiveChunks) > MaxActiveTailChunks {
		return fmt.Errorf("%w: active chunks=%d limit=%d", ErrCorrupt,
			len(plan.ActiveChunks), MaxActiveTailChunks)
	}
	for i, ref := range plan.ActiveChunks {
		if err := chunkref.Validate(ref); err != nil {
			return fmt.Errorf("%w: active chunk=%d: %v", ErrCorrupt, i, err)
		}
		if plan.MaterializedBefore > math.MaxUint64-uint64(i) ||
			ref.NamespaceHash != key.Namespace().Hash() || ref.Shard != plan.Head.Shard ||
			ref.Sequence != plan.MaterializedBefore+uint64(i) {
			return fmt.Errorf("%w: active chunk=%d identity or sequence", ErrCorrupt, i)
		}
	}
	return nil
}

func ValidateListTimelines(request ListTimelinesRequest) error {
	if err := ValidateNamespace(request.Namespace); err != nil {
		return err
	}
	if request.Limit < 0 || request.Limit > MaxListLimit {
		return fmt.Errorf("%w: list limit=%d", ErrInvalidRequest, request.Limit)
	}
	if request.After != nil && request.After.NamespaceHash != request.Namespace.Hash() {
		return fmt.Errorf("%w: cursor belongs to another namespace", ErrInvalidRequest)
	}
	return nil
}

func NormalizedLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultListLimit
	case limit > MaxListLimit:
		return MaxListLimit
	default:
		return limit
	}
}

func CompareTimelineCursor(a, b TimelineCursor) int {
	if result := bytes.Compare(a.NamespaceHash[:], b.NamespaceHash[:]); result != 0 {
		return result
	}
	return bytes.Compare(a.KeyHash[:], b.KeyHash[:])
}
