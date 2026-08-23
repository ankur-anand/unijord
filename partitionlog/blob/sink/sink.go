package sink

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	uploadstream "github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

type segmentSink struct {
	store         multipart.Store
	partition     uint32
	key           string
	stagingPrefix string
	contentType   string
	bufferPool    *uploadstream.BufferPool
}

var _ segwriter.Sink = (*segmentSink)(nil)

func (s *segmentSink) Begin(ctx context.Context, plan segwriter.Plan) (segwriter.Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan.Partition != s.partition {
		return nil, fmt.Errorf("%w: plan partition=%d segment partition=%d", segwriter.ErrInvalidOptions, plan.Partition, s.partition)
	}
	parallelism := plan.UploadParallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	queueSize := plan.UploadQueueSize
	if queueSize <= 0 {
		queueSize = parallelism
	}
	upload, err := uploadstream.BeginMultipartUpload(ctx, s.store, s.key, multipart.Options{
		ContentType:   s.contentType,
		StagingPrefix: s.stagingPrefix,
	}, uploadstream.MultipartOptions{
		PartSize:          plan.PartSize,
		UploadParallelism: parallelism,
		UploadQueueSize:   queueSize,
		UploadLimiter:     plan.UploadLimiter,
		BufferPool:        s.bufferPool,
	})
	if err != nil {
		return nil, mapStreamSegmentError(err)
	}
	return newSegmentTxn(upload), nil
}
