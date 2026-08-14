package blob

import (
	"context"
	"fmt"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
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
	head, _, err := c.loadHead(ctx, req.Partition)
	if err != nil {
		return MaintenanceSnapshot{}, pmeta.SegmentPage{}, err
	}
	snapshot := MaintenanceSnapshot{
		Head: stateFromHead(head), Generation: head.Generation, MaxIndexLevel: head.MaxIndexLevel,
	}
	page, err := c.listSegmentsInHead(ctx, head, req.FromLSN, req.NormalizedLimit())
	if err != nil {
		return MaintenanceSnapshot{}, pmeta.SegmentPage{}, err
	}
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
	head, _, err := c.loadHead(ctx, req.Partition)
	if err != nil {
		return MaintenanceSnapshot{}, MaintenancePage{}, err
	}
	snapshot := MaintenanceSnapshot{
		Head: stateFromHead(head), Generation: head.Generation, MaxIndexLevel: head.MaxIndexLevel,
	}
	collector := maintenancePageCollector{from: req.FromSeqLo, limit: limit}
	for _, root := range reachableRoots(head) {
		if collector.done() {
			break
		}
		if err := c.collectMaintenancePageRefs(ctx, root, req.Level, head.StreamID, head.Partition, &collector); err != nil {
			return MaintenanceSnapshot{}, MaintenancePage{}, err
		}
	}
	return snapshot, MaintenancePage{Paths: collector.paths, NextSeqLo: collector.nextSeqLo, HasMore: collector.hasMore}, nil
}

func (c *Catalog) collectMaintenancePageRefs(ctx context.Context, ref pageRef, level uint8, streamID string, partition uint32, collector *maintenancePageCollector) error {
	if collector.done() || ref.SeqHi < collector.from || ref.Level < level {
		return nil
	}
	if ref.Level == level {
		collector.add(ref)
		return nil
	}
	page, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return err
	}
	start := firstPageRefAtOrAfter(page.Refs, collector.from)
	for i := start; i < len(page.Refs) && !collector.done(); i++ {
		if err := c.collectMaintenancePageRefs(ctx, page.Refs[i], level, streamID, partition, collector); err != nil {
			return err
		}
	}
	return nil
}

type maintenancePageCollector struct {
	from      uint64
	limit     int
	paths     []string
	nextSeqLo uint64
	hasMore   bool
}

func (c *maintenancePageCollector) add(ref pageRef) {
	if ref.SeqHi < c.from {
		return
	}
	if len(c.paths) < c.limit {
		c.paths = append(c.paths, ref.Path)
		return
	}
	c.nextSeqLo = ref.SeqLo
	c.hasMore = true
}

func (c *maintenancePageCollector) done() bool {
	return c.hasMore
}

// IsPageReachable checks whether an immutable catalog page is reachable from
// one validated head snapshot. It follows only branches whose ranges can
// contain the candidate page.
func (c *Catalog) IsPageReachable(ctx context.Context, partition uint32, path string) (MaintenanceSnapshot, bool, error) {
	parsed, err := ParsePagePath(c.opts.Prefix, c.opts.StreamID, partition, path)
	if err != nil {
		return MaintenanceSnapshot{}, false, err
	}
	head, _, err := c.loadHead(ctx, partition)
	if err != nil {
		return MaintenanceSnapshot{}, false, err
	}
	snapshot := MaintenanceSnapshot{
		Head: stateFromHead(head), Generation: head.Generation, MaxIndexLevel: head.MaxIndexLevel,
	}
	for _, root := range reachableRoots(head) {
		reachable, err := c.pagePathReachable(ctx, root, parsed, c.opts.StreamID, partition)
		if err != nil {
			return MaintenanceSnapshot{}, false, err
		}
		if reachable {
			return snapshot, true, nil
		}
	}
	return snapshot, false, nil
}

func (c *Catalog) pagePathReachable(ctx context.Context, ref pageRef, target PageObjectKey, streamID string, partition uint32) (bool, error) {
	if ref.Path == target.Key {
		return true, nil
	}
	if target.SeqHi < ref.SeqLo || target.SeqLo > ref.SeqHi || target.Level >= ref.Level {
		return false, nil
	}
	if ref.Level == 0 {
		return false, nil
	}
	page, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return false, fmt.Errorf("catalog: traverse reachable page %s: %w", ref.Path, err)
	}
	for _, child := range page.Refs {
		if target.SeqHi < child.SeqLo || target.SeqLo > child.SeqHi {
			continue
		}
		reachable, err := c.pagePathReachable(ctx, child, target, streamID, partition)
		if err != nil || reachable {
			return reachable, err
		}
	}
	return false, nil
}

// LoadMaintenanceSnapshot reads and validates the authoritative partition
// head for physical lifecycle decisions.
func (c *Catalog) LoadMaintenanceSnapshot(ctx context.Context, partition uint32) (MaintenanceSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceSnapshot{}, err
	}
	head, _, err := c.loadHead(ctx, partition)
	if err != nil {
		return MaintenanceSnapshot{}, err
	}
	return MaintenanceSnapshot{
		Head:          stateFromHead(head),
		Generation:    head.Generation,
		MaxIndexLevel: head.MaxIndexLevel,
	}, nil
}
