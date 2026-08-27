package blob

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/keylayout"
)

const DefaultPrefix = "catalog"

type PageObjectKind uint8

const (
	PageObjectLeaf PageObjectKind = iota + 1
	PageObjectIndex
)

// PageObjectKey is the immutable range identity encoded in a catalog page
// object key. SeqHi appears before SeqLo in the physical key so object-store
// listings are ordered by inclusive end LSN within each level.
type PageObjectKey struct {
	Key        string
	Kind       PageObjectKind
	Level      uint8
	SeqLo      uint64
	SeqHi      uint64
	Generation uint64
	PageID     string
}

func HeadPath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/head.plc", partitionPrefix(prefix, streamID, partition))
}

func PagePrefix(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/pages/", partitionPrefix(prefix, streamID, partition))
}

func PageLevelPrefix(prefix string, streamID string, partition uint32, level uint8) string {
	return fmt.Sprintf("%sl%02d/", PagePrefix(prefix, streamID, partition), level)
}

// PageEndLowerBound returns a synthetic key immediately before pages at level
// whose inclusive upper LSN bound equals seqHi.
func PageEndLowerBound(prefix string, streamID string, partition uint32, level uint8, seqHi uint64) string {
	levelPrefix := PageLevelPrefix(prefix, streamID, partition, level)
	if level == 0 {
		return fmt.Sprintf("%sleaf-%020d-", levelPrefix, seqHi)
	}
	return fmt.Sprintf("%sindex-l%02d-%020d-", levelPrefix, level, seqHi)
}

// Retention and GC state are mutable maintenance mailboxes, not catformat
// tree objects, and intentionally remain JSON.
func RetentionRequestPath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/maintenance/retention.json", partitionPrefix(prefix, streamID, partition))
}

func GCStatePath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/maintenance/gc/state.json", partitionPrefix(prefix, streamID, partition))
}

func LeafPagePath(prefix string, streamID string, partition uint32, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l00/leaf-%020d-%020d-%020d-%s.plc",
		partitionPrefix(prefix, streamID, partition), seqHi, seqLo, generation, pageID,
	)
}

func IndexPagePath(prefix string, streamID string, partition uint32, level uint8, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l%02d/index-l%02d-%020d-%020d-%020d-%s.plc",
		partitionPrefix(prefix, streamID, partition), level, level, seqHi, seqLo, generation, pageID,
	)
}

// ParsePagePath validates a catalog page key for one stream partition.
func ParsePagePath(prefix string, streamID string, partition uint32, key string) (PageObjectKey, error) {
	pagePrefix := PagePrefix(prefix, streamID, partition)
	relative, ok := strings.CutPrefix(key, pagePrefix)
	if !ok {
		return PageObjectKey{}, fmt.Errorf("%w: page key %q is outside prefix %q", ErrCorruptCatalog, key, pagePrefix)
	}
	levelDir, name, ok := strings.Cut(relative, "/")
	if !ok || strings.Contains(name, "/") || len(levelDir) != 3 || levelDir[0] != 'l' {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page path %q", ErrCorruptCatalog, key)
	}
	level64, err := strconv.ParseUint(levelDir[1:], 10, 8)
	if err != nil || level64 > MaxIndexLevel {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page level in %q", ErrCorruptCatalog, key)
	}
	level := uint8(level64)
	namePrefix := "leaf-"
	kind := PageObjectLeaf
	if level > 0 {
		namePrefix = fmt.Sprintf("index-l%02d-", level)
		kind = PageObjectIndex
	}
	if !strings.HasPrefix(name, namePrefix) || !strings.HasSuffix(name, ".plc") {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page name %q", ErrCorruptCatalog, key)
	}
	fields := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, namePrefix), ".plc"), "-")
	if len(fields) != 4 || len(fields[0]) != 20 || len(fields[1]) != 20 || len(fields[2]) != 20 || len(fields[3]) != 32 {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page fields in %q", ErrCorruptCatalog, key)
	}
	seqHi, err := parsePageUint(fields[0])
	if err != nil {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page seq_hi in %q", ErrCorruptCatalog, key)
	}
	seqLo, err := parsePageUint(fields[1])
	if err != nil || seqHi < seqLo {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page seq_lo in %q", ErrCorruptCatalog, key)
	}
	generation, err := parsePageUint(fields[2])
	if err != nil || generation == 0 {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page generation in %q", ErrCorruptCatalog, key)
	}
	for i := range fields[3] {
		if !isLowerHex(fields[3][i]) {
			return PageObjectKey{}, fmt.Errorf("%w: invalid page id in %q", ErrCorruptCatalog, key)
		}
	}
	return PageObjectKey{Key: key, Kind: kind, Level: level, SeqLo: seqLo, SeqHi: seqHi, Generation: generation, PageID: fields[3]}, nil
}

func parsePageUint(value string) (uint64, error) {
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("non-decimal digit")
		}
	}
	return strconv.ParseUint(value, 10, 64)
}

func isLowerHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f'
}

func partitionPrefix(prefix string, streamID string, partition uint32) string {
	streamID = keylayout.NormalizeStreamID(streamID)
	bucket := keylayout.Bucket(streamID, partition)
	if streamID == "" {
		return fmt.Sprintf("%s/%s/p%08d", normalizePrefix(prefix), bucket, partition)
	}
	return fmt.Sprintf("%s/%s/streams/%s/p%08d", normalizePrefix(prefix), bucket, keylayout.StreamKey(streamID), partition)
}

func normalizePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return DefaultPrefix
	}
	return prefix
}
