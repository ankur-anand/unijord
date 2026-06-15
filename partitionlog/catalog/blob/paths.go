package blob

import (
	"fmt"
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/keylayout"
)

const DefaultPrefix = "catalog"

/**
Our Catalog History is like book:

catalog/
    <bucket>/
      streams/
        hosts/host-a/events/
          p00000001/
        head.json

        pages/
          l00/
            leaf-00000000000000000000-00000000000000000299-g01-A.json
            leaf-00000000000000000300-00000000000000000599-g02-B.json
            leaf-00000000000000000600-00000000000000000899-g03-C.json

            leaf-00000000000000000900-00000000000000001199-g04-D.json
            leaf-00000000000000001200-00000000000000001499-g05-E.json
            leaf-00000000000000001500-00000000000000001799-g06-F.json

            leaf-00000000000000001800-00000000000000002099-g07-G.json
            leaf-00000000000000002100-00000000000000002399-g08-H.json
            leaf-00000000000000002400-00000000000000002699-g09-I.json

            leaf-00000000000000002700-00000000000000002999-g10-J.json

          l01/
            index-l01-00000000000000000000-00000000000000000899-g03-X.json
            index-l01-00000000000000000900-00000000000000001799-g06-Y.json
            index-l01-00000000000000001800-00000000000000002699-g09-Z.json

          l02/
            index-l02-00000000000000000000-00000000000000002699-g09-R.json

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
      leaf-0-299.json
      leaf-300-599.json
      leaf-600-899.json
      leaf-900-1199.json
      leaf-1200-1499.json

    l01/
      index-l01-0-899.json
      index-l01-900-1799.json

    l02/
      index-l02-0-1799.json

LeafPagePath:
- A leaf page is level l00
- Leaf pages contain actual SegmentRef entries

catalog/<bucket>/streams/hosts/host-a/events/p00000007/pages/l00/leaf-00000000000000000100-00000000000000000199-00000000000000000018-abc123.json
seqLo      = 100
seqHi      = 199
generation = 18
pageID     = abc123

IndexPagePath:
Builds the object key for an index page
Index pages start at l01
They do not store SegmentRef directly
They store pageRefs to lower-level pages
catalog/<bucket>/streams/hosts/host-a/events/p00000007/pages/l01/index-l01-00000000000000000100-00000000000000000999-00000000000000000022-def456.json

- l01: index level 1
  - index-l01: index page at level 1
  - 100-999: LSN range covered by children
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

func LeafPagePath(prefix string, streamID string, partition uint32, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l00/leaf-%020d-%020d-%020d-%s.json",
		partitionPrefix(prefix, streamID, partition), seqLo, seqHi, generation, pageID,
	)
}

func IndexPagePath(prefix string, streamID string, partition uint32, level uint8, seqLo, seqHi, generation uint64, pageID string) string {
	return fmt.Sprintf(
		"%s/pages/l%02d/index-l%02d-%020d-%020d-%020d-%s.json",
		partitionPrefix(prefix, streamID, partition), level, level, seqLo, seqHi, generation, pageID,
	)
}

func partitionPrefix(prefix string, streamID string, partition uint32) string {
	streamID = keylayout.NormalizeStreamID(streamID)
	bucket := keylayout.Bucket(streamID, partition)
	if streamID == "" {
		return fmt.Sprintf("%s/%s/p%08d", normalizePrefix(prefix), bucket, partition)
	}
	return fmt.Sprintf("%s/%s/streams/%s/p%08d", normalizePrefix(prefix), bucket, streamID, partition)
}

func normalizePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return DefaultPrefix
	}
	return prefix
}
