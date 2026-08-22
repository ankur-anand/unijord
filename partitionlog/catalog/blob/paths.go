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
// object key.
type PageObjectKey struct {
	Key        string
	Kind       PageObjectKind
	Level      uint8
	SeqLo      uint64
	SeqHi      uint64
	Generation uint64
	PageID     string
}

/**
Our Catalog History is like book:

catalog/
    <bucket>/
      streams/
        <sha256-stream-key>/
          p00000001/
        head.json

        pages/
          l00/
            leaf-00000000000000000299-00000000000000000000-g01-A.json
            leaf-00000000000000000599-00000000000000000300-g02-B.json
            leaf-00000000000000000899-00000000000000000600-g03-C.json

            leaf-00000000000000001199-00000000000000000900-g04-D.json
            leaf-00000000000000001499-00000000000000001200-g05-E.json
            leaf-00000000000000001799-00000000000000001500-g06-F.json

            leaf-00000000000000002099-00000000000000001800-g07-G.json
            leaf-00000000000000002399-00000000000000002100-g08-H.json
            leaf-00000000000000002699-00000000000000002400-g09-I.json

            leaf-00000000000000002999-00000000000000002700-g10-J.json

          l01/
            index-l01-00000000000000000899-00000000000000000000-g03-X.json
            index-l01-00000000000000001799-00000000000000000900-g06-Y.json
            index-l01-00000000000000002699-00000000000000001800-g09-Z.json

          l02/
            index-l02-00000000000000002699-00000000000000000000-g09-R.json

Leaf Page: is where the real segment history lives. It Contains actual SegmentRf entries:
leaf page 100:
SegmentRf: LSN 0-99 -> s3://bucket/p1/segment-a
SegmentRef: LSN 100-199 -> s3://bukcet/p1/segment-b

So if a reader wants LSN 150, eventually it must reach a leaf page because only leaf pages know
the actual segment URI.


Index Page: An Index Page doesn't store segments, It stores pointers to the other pages.
Index Page l01:
* Page Ref: LSN 0-299 -> leaf Page A
* Page Ref LSN 300-599 -> Leaf page B
* Page Ref: LSN 600-899 -> leaf Page C

So if a reader wants LSN 450, it checks the index page and finds that 450 is inside 350-599
so it goes and check leaf page B. Leaf Page B Gives the actual segment URI.

WHy?

Our Catalog is just like book: Without the index page our head would need to point to every leaf
page forever: else we will end up listing the pages.

head.JSON
-> leaf 0
-> leaf 1
-> leaf 2
-> leaf n

This will become the unbounded manifest problems.

Instead we do:
head.JSON
-> index_frontier
-> leaf_frontier
-> active_segments

An OLD history is summarized through Index Pages.
The newest sealed leaf stays in leaf_frontier. New SegmentRefs are buffered in
active_segments until that buffer is large enough to seal one l00 leaf page.

pages/
	l00/   all leaf pages
    l01/   index pages that point to l00 leaf pages
    l02/   index pages that point to l01 index pages
    l03/   index pages that point to l02 index pages

- l00 is one level, but it can contain many leaf page files.
- l01, l02, l03 are many index levels, and each level can contain many index page files

pages/
    l00/
      leaf-299-0.json
      leaf-599-300.json
      leaf-899-600.json
      leaf-1199-900.json
      leaf-1499-1200.json

    l01/
      index-l01-899-0.json
      index-l01-1799-900.json

    l02/
      index-l02-1799-0.json

LeafPagePath:
- A leaf page is level l00
- Leaf pages contain actual SegmentRef entries

catalog/<bucket>/streams/<sha256-stream-key>/p00000007/pages/l00/leaf-00000000000000000199-00000000000000000100-00000000000000000018-abc123.json
seqHi      = 199 (first so physical listings are ordered by page end)
seqLo      = 100
generation = 18
pageID     = abc123

IndexPagePath:
Builds the object key for an index page
Index pages start at l01
They do not store SegmentRef directly
They store pageRefs to lower-level pages
catalog/<bucket>/streams/<sha256-stream-key>/p00000007/pages/l01/index-l01-00000000000000000999-00000000000000000100-00000000000000000022-def456.json

- l01: index level 1
  - index-l01: index page at level 1
  - 999-100: inclusive high and low LSN bounds; high sorts first for retention
  - 22: catalog generation
  - def456: content-derived page ID

Difference from leaf:

l00 leaf  -> stores SegmentRefs
l01 index -> stores refs to l00 leaves
l02 index -> stores refs to l01 indexes
*/

func HeadPath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/head.json", partitionPrefix(prefix, streamID, partition))
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

func RetentionRequestPath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/maintenance/retention.json", partitionPrefix(prefix, streamID, partition))
}

func GCStatePath(prefix string, streamID string, partition uint32) string {
	return fmt.Sprintf("%s/maintenance/gc/state.json", partitionPrefix(prefix, streamID, partition))
}

func LeafPagePath(prefix string, streamID string, partition uint32, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l00/leaf-%020d-%020d-%020d-%s.json",
		partitionPrefix(prefix, streamID, partition), seqHi, seqLo, generation, pageID,
	)
}

func IndexPagePath(prefix string, streamID string, partition uint32, level uint8, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l%02d/index-l%02d-%020d-%020d-%020d-%s.json",
		partitionPrefix(prefix, streamID, partition), level, level, seqHi, seqLo, generation, pageID,
	)
}

// ParsePagePath validates a page key for one stream partition.
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
	if !strings.HasPrefix(name, namePrefix) || !strings.HasSuffix(name, ".json") {
		return PageObjectKey{}, fmt.Errorf("%w: invalid page name %q", ErrCorruptCatalog, key)
	}
	fields := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, namePrefix), ".json"), "-")
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
