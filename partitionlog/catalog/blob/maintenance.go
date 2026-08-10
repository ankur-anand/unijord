package blob

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

// MaintenanceSnapshot is bounded object-catalog state needed by physical
// lifecycle workers. It deliberately excludes page roots and segment history.
type MaintenanceSnapshot struct {
	Head          pmeta.PartitionHead
	Generation    uint64
	MaxIndexLevel uint8
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
