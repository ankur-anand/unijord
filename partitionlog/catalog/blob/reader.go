package blob

import (
	"context"
	"sort"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.Reader = (*Catalog)(nil)

func (c *Catalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, err
	}
	head, _, err := c.loadHead(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	return stateFromHead(head), nil
}

func (c *Catalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	head, _, err := c.loadHead(ctx, partition)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	if !head.HasLastSegment || lsn < head.OldestLSN || lsn >= head.NextLSN {
		return pmeta.SegmentRef{}, false, nil
	}

	roots := reachableRoots(head)
	for _, root := range roots {
		if lsn < root.SeqLo || lsn > root.SeqHi {
			continue
		}
		return c.findInPageRef(ctx, root, head.StreamID, head.Partition, lsn)
	}
	return findInSegments(head.ActiveSegments, lsn)
}

func (c *Catalog) ListSegments(ctx context.Context, req csession.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentPage{}, err
	}
	head, _, err := c.loadHead(ctx, req.Partition)
	if err != nil {
		return pmeta.SegmentPage{}, err
	}
	if !head.HasLastSegment || req.FromLSN >= head.NextLSN {
		return pmeta.SegmentPage{}, nil
	}

	collector := segmentCollector{
		from:  req.FromLSN,
		limit: req.NormalizedLimit(),
	}
	for _, root := range reachableRoots(head) {
		if collector.done() {
			break
		}
		if root.SeqHi < req.FromLSN {
			continue
		}
		if err := c.collectFromPageRef(ctx, root, head.StreamID, head.Partition, &collector); err != nil {
			return pmeta.SegmentPage{}, err
		}
	}
	if !collector.done() {
		collector.addSegments(head.ActiveSegments)
	}
	return pmeta.SegmentPage{
		Segments: collector.segments,
		NextLSN:  collector.nextLSN,
		HasMore:  collector.hasMore,
	}, nil
}

func (c *Catalog) findInPageRef(ctx context.Context, ref pageRef, streamID string, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	if ref.Level == 0 {
		leaf, err := c.loadLeaf(ctx, ref, streamID, partition)
		if err != nil {
			return pmeta.SegmentRef{}, false, err
		}
		return findInSegments(leaf.Segments, lsn)
	}
	index, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	i := firstPageRefAtOrAfter(index.Refs, lsn)
	if i == len(index.Refs) || lsn < index.Refs[i].SeqLo || lsn > index.Refs[i].SeqHi {
		return pmeta.SegmentRef{}, false, nil
	}
	return c.findInPageRef(ctx, index.Refs[i], streamID, partition, lsn)
}

func (c *Catalog) collectFromPageRef(ctx context.Context, ref pageRef, streamID string, partition uint32, collector *segmentCollector) error {
	if collector.done() || ref.SeqHi < collector.from {
		return nil
	}
	if ref.Level == 0 {
		leaf, err := c.loadLeaf(ctx, ref, streamID, partition)
		if err != nil {
			return err
		}
		collector.addSegments(leaf.Segments)
		return nil
	}
	index, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return err
	}
	start := firstPageRefAtOrAfter(index.Refs, collector.from)
	for i := start; i < len(index.Refs) && !collector.done(); i++ {
		if err := c.collectFromPageRef(ctx, index.Refs[i], streamID, partition, collector); err != nil {
			return err
		}
	}
	return nil
}

func findInSegments(segments []pmeta.SegmentRef, lsn uint64) (pmeta.SegmentRef, bool, error) {
	i := firstSegmentAtOrAfter(segments, lsn)
	if i == len(segments) {
		return pmeta.SegmentRef{}, false, nil
	}
	segment := segments[i]
	if lsn < segment.BaseLSN || lsn > segment.LastLSN {
		return pmeta.SegmentRef{}, false, nil
	}
	return segment, true, nil
}

func firstSegmentAtOrAfter(segments []pmeta.SegmentRef, lsn uint64) int {
	return sort.Search(len(segments), func(i int) bool {
		return segments[i].LastLSN >= lsn
	})
}

func firstPageRefAtOrAfter(refs []pageRef, lsn uint64) int {
	return sort.Search(len(refs), func(i int) bool {
		return refs[i].SeqHi >= lsn
	})
}

type segmentCollector struct {
	from     uint64
	limit    int
	segments []pmeta.SegmentRef
	nextLSN  uint64
	hasMore  bool
}

func (c *segmentCollector) addSegments(segments []pmeta.SegmentRef) {
	start := firstSegmentAtOrAfter(segments, c.from)
	for i := start; i < len(segments); i++ {
		if !c.addSegment(segments[i]) {
			return
		}
	}
}

func (c *segmentCollector) addSegment(segment pmeta.SegmentRef) bool {
	if segment.LastLSN < c.from {
		return true
	}
	if len(c.segments) < c.limit {
		c.segments = append(c.segments, segment)
		return true
	}
	c.hasMore = true
	c.nextLSN = segment.BaseLSN
	return false
}

func (c *segmentCollector) done() bool {
	return c.hasMore
}
