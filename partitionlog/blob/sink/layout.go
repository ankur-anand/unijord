package sink

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

// Layout owns object key naming for partitionlog segment objects and staging
// prefixes. It is value-safe and can be copied.
type Layout struct {
	prefix string
}

func NewLayout(prefix string) Layout {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return Layout{prefix: prefix}
}

func (l Layout) Prefix() string {
	return l.root()
}

func (l Layout) SegmentKey(info plwriter.SegmentInfo) string {
	streamID := normalizeStreamID(info.StreamID)
	parts := []string{
		l.root(),
		"segments",
		keylayout.Bucket(streamID, info.Partition),
	}
	parts = appendStreamParts(parts, streamID)
	parts = append(parts,
		fmt.Sprintf("p%08d", info.Partition),
		fmt.Sprintf("seg-%020d-e%020d-%s%s", info.BaseLSN, info.WriterEpoch, hex.EncodeToString(info.SegmentUUID[:]), segmentFileSuffix))
	return strings.Join(parts, "/")
}

func (l Layout) StagingPrefix(info plwriter.SegmentInfo) string {
	streamID := normalizeStreamID(info.StreamID)
	parts := []string{
		l.root(),
		"staging",
		keylayout.Bucket(streamID, info.Partition),
	}
	parts = appendStreamParts(parts, streamID)
	parts = append(parts,
		fmt.Sprintf("p%08d", info.Partition),
		fmt.Sprintf("seg-%020d-e%020d-%s", info.BaseLSN, info.WriterEpoch, hex.EncodeToString(info.SegmentUUID[:])))
	return strings.Join(parts, "/")
}

func (l Layout) root() string {
	if l.prefix == "" {
		return DefaultPrefix
	}
	return l.prefix
}

func appendStreamParts(parts []string, streamID string) []string {
	if streamID == "" {
		return parts
	}
	return append(parts, "streams", streamID)
}

func normalizeStreamID(streamID string) string {
	return strings.Trim(streamID, "/")
}
