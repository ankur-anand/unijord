package sink

import (
	"strings"
	"testing"

	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestSegmentLayoutRoundTripAndLowerBound(t *testing.T) {
	t.Parallel()

	layout := NewLayout("root")
	info := plwriter.SegmentInfo{
		StreamID:    "hosts/host-a/events",
		Partition:   7,
		BaseLSN:     700_000,
		WriterEpoch: 9,
		SegmentUUID: [16]byte{1, 2, 3, 4},
	}
	key := layout.SegmentKey(info)
	parsed, err := layout.ParseSegmentKey(info.StreamID, info.Partition, key)
	if err != nil {
		t.Fatalf("ParseSegmentKey() error = %v", err)
	}
	if parsed.Key != key || parsed.BaseLSN != info.BaseLSN || parsed.WriterEpoch != info.WriterEpoch || parsed.SegmentUUID != info.SegmentUUID {
		t.Fatalf("ParseSegmentKey() = %+v", parsed)
	}
	lower := layout.SegmentLowerBound(info.StreamID, info.Partition, info.BaseLSN)
	if lower >= key {
		t.Fatalf("SegmentLowerBound() = %q, want less than %q", lower, key)
	}
	if got := layout.SegmentPrefix(info.StreamID, info.Partition); !strings.HasPrefix(key, got) || !strings.HasSuffix(got, "/") {
		t.Fatalf("SegmentPrefix() = %q for key %q", got, key)
	}
}

func TestParseStagingKey(t *testing.T) {
	t.Parallel()

	layout := NewLayout("root")
	info := plwriter.SegmentInfo{
		StreamID:    "hosts/host-a/events",
		Partition:   7,
		BaseLSN:     700_000,
		WriterEpoch: 9,
		SegmentUUID: [16]byte{1, 2, 3, 4},
	}
	key := layout.StagingPrefix(info) + "/part-000001"
	parsed, err := layout.ParseStagingKey(info.StreamID, info.Partition, key)
	if err != nil {
		t.Fatalf("ParseStagingKey() error = %v", err)
	}
	if parsed.BaseLSN != info.BaseLSN || parsed.WriterEpoch != info.WriterEpoch || parsed.SegmentUUID != info.SegmentUUID || parsed.RelativeKey != "part-000001" {
		t.Fatalf("ParseStagingKey() = %+v", parsed)
	}
}

func TestParseSegmentKeyRejectsNonCanonicalNames(t *testing.T) {
	t.Parallel()

	layout := NewLayout("root")
	prefix := layout.SegmentPrefix("hosts/host-a/events", 7)
	tests := []string{
		prefix + "seg-1-e00000000000000000001-01020300000000000000000000000000.plseg",
		prefix + "seg-00000000000000000001-e00000000000000000000-01020300000000000000000000000000.plseg",
		prefix + "seg-00000000000000000001-e00000000000000000001-00000000000000000000000000000000.plseg",
		prefix + "seg-00000000000000000001-e00000000000000000001-0102030000000000000000000000000G.plseg",
		prefix + "nested/seg-00000000000000000001-e00000000000000000001-01020300000000000000000000000000.plseg",
	}
	for _, key := range tests {
		key := key
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			if _, err := layout.ParseSegmentKey("hosts/host-a/events", 7, key); err == nil {
				t.Fatalf("ParseSegmentKey(%q) error = nil", key)
			}
		})
	}
}
