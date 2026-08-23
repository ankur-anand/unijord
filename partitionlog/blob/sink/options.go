package sink

import (
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
)

const (
	DefaultPrefix      = "partitionlog"
	DefaultContentType = "application/octet-stream"

	segmentFileSuffix = ".plseg"
)

type Options struct {
	// Prefix groups final segment objects and provider staging objects under one
	// object-store key namespace.
	Prefix string

	// ContentType is attached to committed segment objects when the provider
	// supports object content types.
	ContentType string

	// BufferPool optionally applies one multipart payload-memory bound across
	// all segment uploads created by this factory. Its buffer size must match
	// the writer's configured PartSize.
	BufferPool *stream.BufferPool
}

func normalizeOptions(opts Options) Options {
	opts.Prefix = strings.Trim(opts.Prefix, "/")
	if opts.Prefix == "" {
		opts.Prefix = DefaultPrefix
	}
	if opts.ContentType == "" {
		opts.ContentType = DefaultContentType
	}
	return opts
}
