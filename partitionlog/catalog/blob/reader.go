package blob

import (
	"context"
	"fmt"
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
	return c.findSegmentInHead(ctx, head, lsn)
}

func (c *Catalog) LookupTimestamp(ctx context.Context, req csession.TimestampLookupRequest) (csession.TimestampLookupResult, error) {
	if err := ctx.Err(); err != nil {
		return csession.TimestampLookupResult{}, err
	}
	head, _, err := c.loadHead(ctx, req.Partition)
	if err != nil {
		return csession.TimestampLookupResult{}, err
	}
	result := csession.TimestampLookupResult{Head: stateFromHead(head)}
	if !head.HasLastSegment || head.OldestLSN == head.NextLSN || req.TimestampMS > head.LastSegment.MaxTimestampMS {
		return result, nil
	}

	roots := reachableRoots(head)
	i := firstPageRefAtOrAfterTimestamp(roots, req.TimestampMS)
	if i < len(roots) {
		segment, found, err := c.findTimestampInPageRef(ctx, roots[i], head.StreamID, head.Partition, req.TimestampMS)
		if err != nil {
			return csession.TimestampLookupResult{}, err
		}
		if !found {
			return csession.TimestampLookupResult{}, fmt.Errorf("%w: page range contains timestamp_ms=%d but no segment qualifies", ErrCorruptCatalog, req.TimestampMS)
		}
		result.Segment = segment
		result.Found = true
		return result, nil
	}

	i = firstSegmentAtOrAfterTimestamp(head.ActiveSegments, req.TimestampMS)
	if i == len(head.ActiveSegments) {
		return csession.TimestampLookupResult{}, fmt.Errorf("%w: head range contains timestamp_ms=%d but no segment qualifies", ErrCorruptCatalog, req.TimestampMS)
	}
	result.Segment = head.ActiveSegments[i]
	result.Found = true
	return result, nil
}

func (c *Catalog) findSegmentInHead(ctx context.Context, head headFile, lsn uint64) (pmeta.SegmentRef, bool, error) {
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
	return c.listSegmentsInHead(ctx, head, req.FromLSN, req.NormalizedLimit())
}

func (c *Catalog) listSegmentsInHead(ctx context.Context, head headFile, fromLSN uint64, limit int) (pmeta.SegmentPage, error) {
	if !head.HasLastSegment || fromLSN >= head.NextLSN {
		return pmeta.SegmentPage{}, nil
	}

	collector := segmentCollector{
		from:  fromLSN,
		limit: limit,
	}
	for _, root := range reachableRoots(head) {
		if collector.done() {
			break
		}
		if root.SeqHi < fromLSN {
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

func (c *Catalog) findTimestampInPageRef(ctx context.Context, ref pageRef, streamID string, partition uint32, timestampMS int64) (pmeta.SegmentRef, bool, error) {
	if ref.Level == 0 {
		leaf, err := c.loadLeaf(ctx, ref, streamID, partition)
		if err != nil {
			return pmeta.SegmentRef{}, false, err
		}
		i := firstSegmentAtOrAfterTimestamp(leaf.Segments, timestampMS)
		if i == len(leaf.Segments) {
			return pmeta.SegmentRef{}, false, nil
		}
		return leaf.Segments[i], true, nil
	}
	index, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	i := firstPageRefAtOrAfterTimestamp(index.Refs, timestampMS)
	if i == len(index.Refs) {
		return pmeta.SegmentRef{}, false, nil
	}
	return c.findTimestampInPageRef(ctx, index.Refs[i], streamID, partition, timestampMS)
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

func firstSegmentAtOrAfterTimestamp(segments []pmeta.SegmentRef, timestampMS int64) int {
	return sort.Search(len(segments), func(i int) bool {
		return segments[i].MaxTimestampMS >= timestampMS
	})
}

func firstPageRefAtOrAfterTimestamp(refs []pageRef, timestampMS int64) int {
	return sort.Search(len(refs), func(i int) bool {
		return refs[i].MaxTimestampMS >= timestampMS
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
