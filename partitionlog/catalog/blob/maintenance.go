package blob

import (
	"context"
	"fmt"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catengine"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

// MaintenanceSnapshot is bounded object-catalog state needed by physical
// lifecycle workers. It deliberately excludes page roots and segment history.
type MaintenanceSnapshot struct {
	Head          pmeta.PartitionHead
	Generation    uint64
	MaxIndexLevel uint8
}

// MaintenancePageRequest selects a bounded ordered slice of reachable catalog
// page paths at one level.
type MaintenancePageRequest struct {
	Partition uint32
	Level     uint8
	FromSeqLo uint64
	Limit     int
}

// MaintenancePage contains reachable immutable page paths ordered by seq_lo.
type MaintenancePage struct {
	Paths     []string
	NextSeqLo uint64
	HasMore   bool
}

// ListMaintenanceSegments returns a bounded segment page and the exact head
// snapshot used to resolve it. Lifecycle workers use the pair to avoid mixing
// eligibility from one head generation with topology from another.
func (c *Catalog) ListMaintenanceSegments(ctx context.Context, req csession.ListSegmentsRequest) (MaintenanceSnapshot, pmeta.SegmentPage, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceSnapshot{}, pmeta.SegmentPage{}, err
	}
	head, generation, maxLevel, page, err := c.engine.ListSegmentsSnapshot(ctx, req.Partition, req.FromLSN, req.NormalizedLimit())
	if err != nil {
		return MaintenanceSnapshot{}, pmeta.SegmentPage{}, err
	}
	snapshot := MaintenanceSnapshot{Head: head, Generation: generation, MaxIndexLevel: maxLevel}
	return snapshot, page, nil
}

// ListMaintenancePages returns reachable page paths from one validated head.
func (c *Catalog) ListMaintenancePages(ctx context.Context, req MaintenancePageRequest) (MaintenanceSnapshot, MaintenancePage, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceSnapshot{}, MaintenancePage{}, err
	}
	if req.Level > MaxIndexLevel {
		return MaintenanceSnapshot{}, MaintenancePage{}, fmt.Errorf("%w: page level=%d max=%d", ErrCorruptCatalog, req.Level, MaxIndexLevel)
	}
	limit := req.Limit
	if limit <= 0 {
		limit = csession.DefaultSegmentPageLimit
	} else if limit > csession.MaxSegmentPageLimit {
		limit = csession.MaxSegmentPageLimit
	}
	head, generation, maxLevel, page, err := c.engine.ListPagePathsSnapshot(ctx, req.Partition, req.Level, req.FromSeqLo, limit)
	if err != nil {
		return MaintenanceSnapshot{}, MaintenancePage{}, err
	}
	snapshot := MaintenanceSnapshot{Head: head, Generation: generation, MaxIndexLevel: maxLevel}
	return snapshot, MaintenancePage{Paths: page.Paths, NextSeqLo: page.NextSeqLo, HasMore: page.HasMore}, nil
}

// IsPageReachable checks whether an immutable catalog page is reachable from
// one validated head snapshot. It follows only branches whose ranges can
// contain the candidate page.
func (c *Catalog) IsPageReachable(ctx context.Context, partition uint32, path string) (MaintenanceSnapshot, bool, error) {
	parsed, err := ParsePagePath(c.opts.Prefix, c.opts.StreamID, partition, path)
	if err != nil {
		return MaintenanceSnapshot{}, false, err
	}
	head, generation, maxLevel, reachable, err := c.engine.IsPageReachableSnapshot(ctx, partition, catengine.PageTarget{
		Key: parsed.Key, Level: parsed.Level, SeqLo: parsed.SeqLo, SeqHi: parsed.SeqHi,
	})
	if err != nil {
		return MaintenanceSnapshot{}, false, err
	}
	snapshot := MaintenanceSnapshot{Head: head, Generation: generation, MaxIndexLevel: maxLevel}
	return snapshot, reachable, nil
}

// LoadMaintenanceSnapshot reads and validates the authoritative partition
// head for physical lifecycle decisions.
func (c *Catalog) LoadMaintenanceSnapshot(ctx context.Context, partition uint32) (MaintenanceSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceSnapshot{}, err
	}
	head, generation, maxLevel, err := c.engine.MaintenanceSnapshot(ctx, partition)
	if err != nil {
		return MaintenanceSnapshot{}, err
	}
	return MaintenanceSnapshot{
		Head:          head,
		Generation:    generation,
		MaxIndexLevel: maxLevel,
	}, nil
}
