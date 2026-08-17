package segreader

import (
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const (
	DefaultMaxBlockBytes = uint64(segformat.BlockPreambleSize + segformat.MaxStoredBlockSize)
	DefaultMaxIndexBytes = uint64(64 << 20)
)

type Options struct {
	// ValidateSegmentHash verifies the trailer segment hash during Open. It is
	// off by default because it requires reading the whole segment body in
	// bounded range requests.
	ValidateSegmentHash bool
	MaxBlockBytes       uint64
	// MaxIndexBytes bounds the block-index metadata fetched and held by Open.
	MaxIndexBytes uint64
}

func DefaultOptions() Options {
	return Options{
		MaxBlockBytes: DefaultMaxBlockBytes,
		MaxIndexBytes: DefaultMaxIndexBytes,
	}
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.MaxBlockBytes == 0 {
		opts.MaxBlockBytes = DefaultMaxBlockBytes
	}
	if opts.MaxBlockBytes < segformat.BlockPreambleSize {
		return Options{}, fmt.Errorf("%w: max_block_bytes=%d too small", ErrInvalidOptions, opts.MaxBlockBytes)
	}
	if opts.MaxIndexBytes == 0 {
		opts.MaxIndexBytes = DefaultMaxIndexBytes
	}
	minIndexBytes := uint64(segformat.IndexPreambleSize + segformat.BlockIndexEntrySize)
	if opts.MaxIndexBytes < minIndexBytes {
		return Options{}, fmt.Errorf("%w: max_index_bytes=%d too small", ErrInvalidOptions, opts.MaxIndexBytes)
	}
	return opts, nil
}
