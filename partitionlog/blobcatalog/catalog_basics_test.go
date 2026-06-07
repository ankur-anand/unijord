package blobcatalog

import (
	"errors"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func TestPathsAreSelfDescribing(t *testing.T) {
	t.Parallel()

	if got := HeadPath("", 7); got != "catalog/p00000007/head.json" {
		t.Fatalf("HeadPath() = %q", got)
	}
	if got := HeadPath("/prod/catalog/", 7); got != "prod/catalog/p00000007/head.json" {
		t.Fatalf("HeadPath(custom) = %q", got)
	}
	if got := PagePrefix("", 7); got != "catalog/p00000007/pages/" {
		t.Fatalf("PagePrefix() = %q", got)
	}
	if got := LeafPagePath("", 7, 100, 199, 18, "abc"); got != "catalog/p00000007/pages/l00/leaf-00000000000000000100-00000000000000000199-00000000000000000018-abc.json" {
		t.Fatalf("LeafPagePath() = %q", got)
	}
	if got := IndexPagePath("", 7, 2, 100, 999, 22, "def"); got != "catalog/p00000007/pages/l02/index-l02-00000000000000000100-00000000000000000999-00000000000000000022-def.json" {
		t.Fatalf("IndexPagePath() = %q", got)
	}
}

func TestNormalizeOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := normalizeOptions(Options{Prefix: "/prod/catalog/"})
	if err != nil {
		t.Fatalf("normalizeOptions() error = %v", err)
	}
	if opts.Prefix != "prod/catalog" {
		t.Fatalf("Prefix = %q", opts.Prefix)
	}
	if opts.LeafSegmentLimit != pcatalog.DefaultSegmentPageLimit {
		t.Fatalf("LeafSegmentLimit = %d", opts.LeafSegmentLimit)
	}
	if opts.IndexRefLimit != DefaultIndexRefLimit {
		t.Fatalf("IndexRefLimit = %d", opts.IndexRefLimit)
	}
	if opts.WriterAcquireMaxAttempts != DefaultWriterAcquireMaxAttempts {
		t.Fatalf("WriterAcquireMaxAttempts = %d", opts.WriterAcquireMaxAttempts)
	}
	if opts.WriterAcquireInitialBackoff != DefaultWriterAcquireInitialBackoff {
		t.Fatalf("WriterAcquireInitialBackoff = %s", opts.WriterAcquireInitialBackoff)
	}
	if opts.WriterAcquireMaxBackoff != DefaultWriterAcquireMaxBackoff {
		t.Fatalf("WriterAcquireMaxBackoff = %s", opts.WriterAcquireMaxBackoff)
	}
}

func TestNormalizeOptionsRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	cases := []Options{
		{LeafSegmentLimit: pcatalog.MaxSegmentPageLimit + 1},
		{IndexRefLimit: pcatalog.MaxSegmentPageLimit + 1},
		{WriterAcquireInitialBackoff: -time.Nanosecond},
		{WriterAcquireMaxBackoff: -time.Nanosecond},
		{WriterAcquireInitialBackoff: 10 * time.Millisecond, WriterAcquireMaxBackoff: time.Millisecond},
	}
	for i, opts := range cases {
		if _, err := normalizeOptions(opts); !errors.Is(err, pcatalog.ErrInvalidRequest) {
			t.Fatalf("case %d normalizeOptions() error = %v, want %v", i, err, pcatalog.ErrInvalidRequest)
		}
	}
}

func TestNewRejectsNilBackend(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); !errors.Is(err, pcatalog.ErrInvalidRequest) {
		t.Fatalf("New(nil) error = %v, want %v", err, pcatalog.ErrInvalidRequest)
	}
}

func TestNewMemory(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{Prefix: "local"})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	if cat == nil || cat.backend == nil || cat.opts.Prefix != "local" {
		t.Fatalf("NewMemory() = %+v", cat)
	}
}
