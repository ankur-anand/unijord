package runfile

import (
	"bytes"
	"context"
	"slices"
)

const (
	observedEvents uint8 = 1 << iota
	observedHeads
	observedFilter
	observedAll = observedEvents | observedHeads | observedFilter
)

// Validation owns only integer state. The catalog and all timeline bytes are
// borrowed until Prepare returns. Sorting IDs proves exact uniqueness without
// a hash table, timeline clones, string conversions, or per-table sets.
type catalogValidation struct {
	catalog TimelineCatalog
	masks   []uint8
	order   []TimelineID
	audit   *buildInstrumentation
}

func validateCatalog(ctx context.Context, catalog TimelineCatalog, limit uint32, audit *buildInstrumentation) (*catalogValidation, error) {
	if catalog == nil {
		return nil, invalidRunf("nil timeline catalog")
	}
	n := catalog.Len()
	if n <= 0 {
		return nil, invalidRunf("timeline catalog is empty or has negative length")
	}
	if limit == 0 {
		return nil, invalidRunf("MaxTimelines must be positive")
	}
	// Check both the caller's admission limit and integer arithmetic before
	// allocation, conversion to TimelineID, or any catalog lookup.
	if uint64(n) > uint64(limit) || uint64(n) > uint64(^uint(0)>>1)/5 {
		return nil, runTooLargef("catalog length %d exceeds validation limit %d", n, limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v := &catalogValidation{catalog: catalog, masks: make([]uint8, n), order: make([]TimelineID, n), audit: audit}
	for i := range n {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := TimelineID(i)
		timeline := catalog.Timeline(id)
		if len(timeline) == 0 {
			return nil, invalidRunf("catalog ID %d is missing or empty", id)
		}
		if uint64(len(timeline)) > MaxTimelineBytes {
			return nil, runTooLargef("catalog ID %d timeline exceeds %d bytes", id, MaxTimelineBytes)
		}
		v.order[i] = id
	}
	slices.SortFunc(v.order, func(a, b TimelineID) int {
		return bytes.Compare(catalog.Timeline(a), catalog.Timeline(b))
	})
	for i := 1; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if bytes.Equal(v.timeline(v.order[i-1]), v.timeline(v.order[i])) {
			return nil, invalidRunf("catalog IDs %d and %d have the same exact timeline", v.order[i-1], v.order[i])
		}
	}
	return v, nil
}

func (v *catalogValidation) timeline(id TimelineID) []byte {
	return v.catalog.Timeline(id)
}

func (v *catalogValidation) observe(id TimelineID, timeline []byte, source uint8) error {
	if uint64(id) >= uint64(len(v.masks)) {
		return invalidRunf("timeline ID %d outside catalog", id)
	}
	if !bytes.Equal(timeline, v.timeline(id)) {
		return invalidRunf("timeline ID %d exact bytes differ from catalog", id)
	}
	if source != observedEvents && source != observedHeads && source != observedFilter {
		return invalidRunf("invalid timeline observation source %d", source)
	}
	if source != observedEvents && v.masks[id]&source != 0 {
		return invalidRunf("duplicate timeline ID %d for source %03b", id, source)
	}
	v.masks[id] |= source
	return nil
}

func (v *catalogValidation) complete() error {
	for id, mask := range v.masks {
		if mask != observedAll {
			return invalidRunf("timeline ID %d observed mask %03b, require 111", id, mask)
		}
	}
	return nil
}
