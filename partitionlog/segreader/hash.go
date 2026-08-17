package segreader

import (
	"context"
	"fmt"
	"hash"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/cespare/xxhash/v2"
)

const segmentHashChunkBytes = uint64(8 << 20)

func validateSegmentHash(
	ctx context.Context,
	store SegmentStore,
	uri string,
	length uint64,
	algo segformat.HashAlgo,
	want uint64,
) error {
	return validateSegmentHashChunks(ctx, store, uri, length, algo, want, segmentHashChunkBytes)
}

func validateSegmentHashChunks(
	ctx context.Context,
	store SegmentStore,
	uri string,
	length uint64,
	algo segformat.HashAlgo,
	want uint64,
	chunkBytes uint64,
) error {
	if chunkBytes == 0 {
		return fmt.Errorf("%w: hash chunk bytes must be positive", ErrInvalidOptions)
	}
	hasher, sum, err := segmentHasher(algo)
	if err != nil {
		return fmt.Errorf("%w: hash segment: %w", ErrCorruptData, err)
	}
	for off := uint64(0); off < length; {
		n := min(chunkBytes, length-off)
		body, err := readAtExact(ctx, store, uri, off, n)
		if err != nil {
			return err
		}
		if _, err := hasher.Write(body); err != nil {
			return fmt.Errorf("%w: hash segment: %w", ErrCorruptData, err)
		}
		off += n
	}
	if got := sum(); got != want {
		return fmt.Errorf("%w: segment hash got=%x want=%x", ErrCorruptData, got, want)
	}
	return nil
}

func segmentHasher(algo segformat.HashAlgo) (hash.Hash, func() uint64, error) {
	switch algo {
	case segformat.HashCRC32C:
		hasher := segformat.NewCRC32C()
		return hasher, func() uint64 { return uint64(hasher.Sum32()) }, nil
	case segformat.HashXXH64:
		hasher := xxhash.New()
		return hasher, hasher.Sum64, nil
	default:
		return nil, nil, fmt.Errorf("unsupported hash algorithm: %d", algo)
	}
}
