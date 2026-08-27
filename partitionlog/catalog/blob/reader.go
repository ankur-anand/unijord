package blob

import (
	"context"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.Reader = (*Catalog)(nil)

func (c *Catalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	return c.engine.LoadPartition(ctx, partition)
}

func (c *Catalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	return c.engine.FindSegment(ctx, partition, lsn)
}

func (c *Catalog) LookupTimestamp(ctx context.Context, req csession.TimestampLookupRequest) (csession.TimestampLookupResult, error) {
	head, segment, found, err := c.engine.LookupTimestampSnapshot(ctx, req.Partition, req.TimestampMS)
	if err != nil {
		return csession.TimestampLookupResult{}, err
	}
	return csession.TimestampLookupResult{Head: head, Segment: segment, Found: found}, nil
}

func (c *Catalog) ListSegments(ctx context.Context, req csession.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return c.engine.ListSegments(ctx, req.Partition, req.FromLSN, req.NormalizedLimit())
}
