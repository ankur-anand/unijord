package segreader

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestValidateSegmentHashUsesBoundedRanges(t *testing.T) {
	t.Parallel()

	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	for _, algo := range []segformat.HashAlgo{segformat.HashCRC32C, segformat.HashXXH64} {
		algo := algo
		t.Run(algo.String(), func(t *testing.T) {
			t.Parallel()
			want, err := segformat.HashBytes(algo, data)
			if err != nil {
				t.Fatalf("HashBytes() error = %v", err)
			}
			reads := 0
			store := SegmentStoreFunc(func(ctx context.Context, uri string, off, n uint64) ([]byte, error) {
				reads++
				if uri != "segment" || n > 7 {
					t.Fatalf("ReadAt(uri=%q off=%d n=%d)", uri, off, n)
				}
				return append([]byte(nil), data[off:off+n]...), nil
			})
			if err := validateSegmentHashChunks(context.Background(), store, "segment", uint64(len(data)), algo, want, 7); err != nil {
				t.Fatalf("validateSegmentHashChunks() error = %v", err)
			}
			if reads != 6 {
				t.Fatalf("reads = %d, want 6", reads)
			}
		})
	}
}

func TestValidateSegmentHashHonorsCancellationBetweenRanges(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	reads := 0
	store := SegmentStoreFunc(func(context.Context, string, uint64, uint64) ([]byte, error) {
		reads++
		cancel()
		return []byte("abcd"), nil
	})
	err := validateSegmentHashChunks(ctx, store, "segment", 8, segformat.HashXXH64, 0, 4)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("validateSegmentHashChunks() error = %v, want %v", err, context.Canceled)
	}
	if reads != 1 {
		t.Fatalf("reads = %d, want 1", reads)
	}
}
