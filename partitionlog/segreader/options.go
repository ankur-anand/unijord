package segreader

import (
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const DefaultMaxBlockBytes = uint64(segformat.BlockPreambleSize + segformat.MaxStoredBlockSize)

type Options struct {
	// ValidateSegmentHash verifies the trailer segment hash during Open. It is
	// off by default because it requires reading the whole segment body.
	ValidateSegmentHash bool
	MaxBlockBytes       uint64
}

func DefaultOptions() Options {
	return Options{MaxBlockBytes: DefaultMaxBlockBytes}
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.MaxBlockBytes == 0 {
		opts.MaxBlockBytes = DefaultMaxBlockBytes
	}
	if opts.MaxBlockBytes < segformat.BlockPreambleSize {
		return Options{}, fmt.Errorf("%w: max_block_bytes=%d too small", ErrInvalidOptions, opts.MaxBlockBytes)
	}
	return opts, nil
}
