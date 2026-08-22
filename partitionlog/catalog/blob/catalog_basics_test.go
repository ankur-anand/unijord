package blob

import (
	"errors"
	"strings"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func TestPathsAreSelfDescribing(t *testing.T) {
	t.Parallel()

	if got := HeadPath("", "", 7); got != "catalog/661/p00000007/head.json" {
		t.Fatalf("HeadPath() = %q", got)
	}
	if got := HeadPath("/prod/catalog/", "", 7); got != "prod/catalog/661/p00000007/head.json" {
		t.Fatalf("HeadPath(custom) = %q", got)
	}
	if got := HeadPath("/prod/catalog/", "hosts/host-a/events", 7); got != "prod/catalog/b78/streams/645c418edae21662304240f5181b1b63c713bfc0b062a2c3b1b84387aa786c91/p00000007/head.json" {
		t.Fatalf("HeadPath(stream) = %q", got)
	}
	if got := PagePrefix("", "", 7); got != "catalog/661/p00000007/pages/" {
		t.Fatalf("PagePrefix() = %q", got)
	}
	if got := LeafPagePath("", "", 7, 100, 199, 18, "abc"); got != "catalog/661/p00000007/pages/l00/leaf-00000000000000000199-00000000000000000100-00000000000000000018-abc.json" {
		t.Fatalf("LeafPagePath() = %q", got)
	}
	if got := IndexPagePath("", "", 7, 2, 100, 999, 22, "def"); got != "catalog/661/p00000007/pages/l02/index-l02-00000000000000000999-00000000000000000100-00000000000000000022-def.json" {
		t.Fatalf("IndexPagePath() = %q", got)
	}
}

func TestPagePathRoundTripAndLowerBound(t *testing.T) {
	t.Parallel()

	const pageID = "0123456789abcdef0123456789abcdef"
	leaf := LeafPagePath("catalog", "hosts/host-a/events", 7, 100, 199, 18, pageID)
	parsed, err := ParsePagePath("catalog", "hosts/host-a/events", 7, leaf)
	if err != nil {
		t.Fatalf("ParsePagePath(leaf) error = %v", err)
	}
	if parsed.Kind != PageObjectLeaf || parsed.Level != 0 || parsed.SeqLo != 100 || parsed.SeqHi != 199 || parsed.Generation != 18 || parsed.PageID != pageID {
		t.Fatalf("ParsePagePath(leaf) = %+v", parsed)
	}
	leafBefore := LeafPagePath("catalog", "hosts/host-a/events", 7, 100, 198, 18, pageID)
	if lower := PageEndLowerBound("catalog", "hosts/host-a/events", 7, 0, 199); leafBefore >= lower || lower >= leaf {
		t.Fatalf("leaf keys = before %q, lower %q, boundary %q; want before < lower < boundary", leafBefore, lower, leaf)
	}

	index := IndexPagePath("catalog", "hosts/host-a/events", 7, 2, 100, 999, 22, pageID)
	parsed, err = ParsePagePath("catalog", "hosts/host-a/events", 7, index)
	if err != nil {
		t.Fatalf("ParsePagePath(index) error = %v", err)
	}
	if parsed.Kind != PageObjectIndex || parsed.Level != 2 || parsed.SeqLo != 100 || parsed.SeqHi != 999 || parsed.Generation != 22 || parsed.PageID != pageID {
		t.Fatalf("ParsePagePath(index) = %+v", parsed)
	}
	indexBefore := IndexPagePath("catalog", "hosts/host-a/events", 7, 2, 100, 998, 22, pageID)
	if lower := PageEndLowerBound("catalog", "hosts/host-a/events", 7, 2, 999); indexBefore >= lower || lower >= index {
		t.Fatalf("index keys = before %q, lower %q, boundary %q; want before < lower < boundary", indexBefore, lower, index)
	}
}

func TestPagePathsOrderByInclusiveEndLSN(t *testing.T) {
	t.Parallel()

	const pageID = "0123456789abcdef0123456789abcdef"
	for _, test := range []struct {
		name   string
		wide   string
		narrow string
	}{
		{
			name:   "leaf",
			wide:   LeafPagePath("catalog", "hosts/host-a/events", 7, 0, 150, 1, pageID),
			narrow: LeafPagePath("catalog", "hosts/host-a/events", 7, 50, 80, 2, pageID),
		},
		{
			name:   "index",
			wide:   IndexPagePath("catalog", "hosts/host-a/events", 7, 1, 0, 150, 1, pageID),
			narrow: IndexPagePath("catalog", "hosts/host-a/events", 7, 1, 50, 80, 2, pageID),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.narrow >= test.wide {
				t.Fatalf("narrow page %q sorts at or after spanning page %q; page retention requires SeqHi order", test.narrow, test.wide)
			}
		})
	}
}

func TestParsePagePathRejectsMalformedPath(t *testing.T) {
	t.Parallel()

	bad := []string{
		"catalog/b78/streams/645c418edae21662304240f5181b1b63c713bfc0b062a2c3b1b84387aa786c91/p00000007/pages/l00/leaf-1-2-3-id.json",
		LeafPagePath("catalog", "hosts/host-a/events", 7, 200, 100, 18, "0123456789abcdef0123456789abcdef"),
		IndexPagePath("catalog", "hosts/host-a/events", 7, 1, 100, 999, 22, "0123456789abcdef0123456789abcdeG"),
	}
	for _, key := range bad {
		if _, err := ParsePagePath("catalog", "hosts/host-a/events", 7, key); err == nil {
			t.Fatalf("ParsePagePath(%q) error = nil", key)
		}
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
	if opts.WriterCommitMaxAttempts != DefaultWriterCommitMaxAttempts {
		t.Fatalf("WriterCommitMaxAttempts = %d", opts.WriterCommitMaxAttempts)
	}
	if opts.WriterCommitInitialBackoff != DefaultWriterCommitInitialBackoff {
		t.Fatalf("WriterCommitInitialBackoff = %s", opts.WriterCommitInitialBackoff)
	}
	if opts.WriterCommitMaxBackoff != DefaultWriterCommitMaxBackoff {
		t.Fatalf("WriterCommitMaxBackoff = %s", opts.WriterCommitMaxBackoff)
	}
}

func TestNormalizeOptionsRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	cases := []Options{
		{StreamID: string([]byte{0xff})},
		{StreamID: strings.Repeat("x", 513)},
		{LeafSegmentLimit: pcatalog.MaxSegmentPageLimit + 1},
		{IndexRefLimit: pcatalog.MaxSegmentPageLimit + 1},
		{IndexRefLimit: 1},
		{WriterAcquireInitialBackoff: -time.Nanosecond},
		{WriterAcquireMaxBackoff: -time.Nanosecond},
		{WriterAcquireInitialBackoff: 10 * time.Millisecond, WriterAcquireMaxBackoff: time.Millisecond},
		{WriterCommitInitialBackoff: -time.Nanosecond},
		{WriterCommitMaxBackoff: -time.Nanosecond},
		{WriterCommitInitialBackoff: 10 * time.Millisecond, WriterCommitMaxBackoff: time.Millisecond},
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
