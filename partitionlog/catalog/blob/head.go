package blob

import (
	"context"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.WriterManager = (*Catalog)(nil)

func (c *Catalog) InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error) {
	return c.engine.Initialize(ctx, partition, nextLSN)
}

func (c *Catalog) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (csession.WriterSession, error) {
	session, err := c.engine.OpenWriter(ctx, partition, writerID)
	if err != nil {
		return nil, err
	}
	return &writerSession{cat: c, session: session}, nil
}
