package segreader

import (
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestNormalizeOptionsDefaultsMaxBlockBytes(t *testing.T) {
	t.Parallel()

	opts, err := normalizeOptions(Options{})
	if err != nil {
		t.Fatalf("normalizeOptions() error = %v", err)
	}
	if opts.MaxBlockBytes != DefaultMaxBlockBytes {
		t.Fatalf("MaxBlockBytes = %d, want %d", opts.MaxBlockBytes, DefaultMaxBlockBytes)
	}
}

func TestNormalizeOptionsRejectsTooSmallMaxBlockBytes(t *testing.T) {
	t.Parallel()

	_, err := normalizeOptions(Options{MaxBlockBytes: segformat.BlockPreambleSize - 1})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("normalizeOptions() error = %v, want %v", err, ErrInvalidOptions)
	}
}

func TestDefaultOptions(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	if opts.MaxBlockBytes != DefaultMaxBlockBytes {
		t.Fatalf("MaxBlockBytes = %d, want %d", opts.MaxBlockBytes, DefaultMaxBlockBytes)
	}
	if opts.ValidateSegmentHash {
		t.Fatal("ValidateSegmentHash = true, want false")
	}
}
