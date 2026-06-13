package sink

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

type segmentSink struct {
	store         multipart.Store
	partition     uint32
	key           string
	stagingPrefix string
	contentType   string
}

var _ segwriter.Sink = (*segmentSink)(nil)

func (s *segmentSink) Begin(ctx context.Context, plan segwriter.Plan) (segwriter.Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan.Partition != s.partition {
		return nil, fmt.Errorf("%w: plan partition=%d segment partition=%d", segwriter.ErrInvalidOptions, plan.Partition, s.partition)
	}
	upload, err := s.store.BeginMultipart(ctx, s.key, multipart.Options{
		ContentType:   s.contentType,
		StagingPrefix: s.stagingPrefix,
	})
	if err != nil {
		return nil, mapMultipartSegmentError(err)
	}
	return newSegmentTxn(upload), nil
}
