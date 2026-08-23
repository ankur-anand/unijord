package sink

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

type Factory struct {
	store       multipart.Store
	layout      Layout
	contentType string
	bufferPool  *stream.BufferPool
}

var _ plwriter.SinkFactory = (*Factory)(nil)

func New(store multipart.Store, opts Options) (*Factory, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil multipart store", plwriter.ErrInvalidOptions)
	}
	if err := store.Limits().Validate(); err != nil {
		return nil, fmt.Errorf("%w: multipart store limits: %v", plwriter.ErrInvalidOptions, err)
	}
	opts = normalizeOptions(opts)
	return &Factory{
		store:       store,
		layout:      NewLayout(opts.Prefix),
		contentType: opts.ContentType,
		bufferPool:  opts.BufferPool,
	}, nil
}

func (f *Factory) Layout() Layout {
	return f.layout
}

func (f *Factory) NewSegmentSink(ctx context.Context, info plwriter.SegmentInfo) (segwriter.Sink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &segmentSink{
		store:         f.store,
		partition:     info.Partition,
		key:           f.layout.SegmentKey(info),
		stagingPrefix: f.layout.StagingPrefix(info),
		contentType:   f.contentType,
		bufferPool:    f.bufferPool,
	}, nil
}
