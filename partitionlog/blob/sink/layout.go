package sink

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"

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
	return path.Join(
		l.root(),
		"segments",
		fmt.Sprintf("p%08d", info.Partition),
		fmt.Sprintf("seg-%020d-e%020d-%s%s", info.BaseLSN, info.WriterEpoch, hex.EncodeToString(info.SegmentUUID[:]), segmentFileSuffix),
	)
}

func (l Layout) StagingPrefix(info plwriter.SegmentInfo) string {
	return path.Join(
		l.root(),
		"staging",
		fmt.Sprintf("p%08d", info.Partition),
		fmt.Sprintf("seg-%020d-e%020d-%s", info.BaseLSN, info.WriterEpoch, hex.EncodeToString(info.SegmentUUID[:])),
	)
}

func (l Layout) root() string {
	if l.prefix == "" {
		return DefaultPrefix
	}
	return l.prefix
}
