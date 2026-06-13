# Blobcatalog Head-Buffered Page Tree

This note describes the intended catalog tree model for object-store backed
partition metadata.

The goal is to avoid one giant manifest file and also avoid rewriting an
immutable active leaf page on every segment commit.

## Core Idea

The catalog has one small mutable object per partition:

```text
catalog/p00000001/head.json
```

All durable history is reachable from that head.

The head should contain:

```text
index_frontier   older sealed history, one index page per level
leaf_frontier    the most recent sealed leaf page
active_segments  unsealed SegmentRefs stored directly in head.json
```

Only `head.json` is mutable. Leaf and index pages are immutable.

## Why Active Segments Live In Head

The earlier design wrote a new active leaf page every time a segment was
published:

```text
segment publish
  -> write new leaf page containing previous refs + new ref
  -> CAS head.json to point to that page
```

That is correct, but it creates many small orphan catalog pages:

```text
LeafSegmentLimit = 128
127 intermediate active leaf pages become unreachable
1 final full leaf remains reachable
```

The better default is:

```text
segment publish
  -> append SegmentRef into head.active_segments
  -> CAS head.json
```

Only when `active_segments` reaches `LeafSegmentLimit` do we write a real
immutable leaf page.

This changes steady-state catalog writes from:

```text
PUT leaf page + CAS head
```

to:

```text
CAS head only
```

Most segment commits produce no catalog-page orphan.

## Head Shape

Conceptual JSON:

```json
{
  "version": 1,
  "partition": 1,
  "next_lsn": 1500,
  "oldest_lsn": 0,
  "writer_epoch": 12,
  "writer_id": [1, 2, 3, 4],
  "segment_count": 15,
  "last_segment": { "base_lsn": 1400, "last_lsn": 1499 },
  "has_last_segment": true,

  "index_frontier": [
    { "level": 1, "seq_lo": 900, "seq_hi": 1199, "path": "..." },
    { "level": 2, "seq_lo": 0, "seq_hi": 899, "path": "..." }
  ],

  "leaf_frontier": {
    "level": 0,
    "seq_lo": 1200,
    "seq_hi": 1499,
    "path": "..."
  },

  "active_segments": [],
  "generation": 15
}
```

Meaning:

```text
index_frontier[0] -> l01 index page
index_frontier[1] -> l02 index page
index_frontier[2] -> l03 index page

leaf_frontier    -> one sealed l00 leaf page
active_segments  -> not yet sealed into a leaf page
```

## Page Types

Leaf page:

```text
l00 leaf
  contains actual SegmentRefs
```

Index page:

```text
l01 index
  contains refs to l00 leaves

l02 index
  contains refs to l01 indexes

l03 index
  contains refs to l02 indexes
```

Simple rule:

```text
leaf page answers:  where is the segment?
index page answers: which page should I open next?
```

## Object Store Layout

```text
catalog/
  p00000001/
    head.json

    pages/
      l00/
        leaf-00000000000000000000-00000000000000000299-00000000000000000003-A.json
        leaf-00000000000000000300-00000000000000000599-00000000000000000006-B.json

      l01/
        index-l01-00000000000000000000-00000000000000000899-000000000000000012-X.json

      l02/
        index-l02-00000000000000000000-00000000000000002699-000000000000000036-Y.json
```

There is one leaf level:

```text
pages/l00/
```

There can be many index levels:

```text
pages/l01/
pages/l02/
pages/l03/
...
```

## Step-By-Step Creation

Assume small limits for readability:

```text
LeafSegmentLimit = 3
IndexRefLimit = 3
```

Each segment covers 100 LSNs.

### Initial State

```json
{
  "next_lsn": 0,
  "index_frontier": [],
  "leaf_frontier": null,
  "active_segments": []
}
```

No leaf or index pages exist.

### Append Segment 0-99

Only head changes:

```text
active_segments:
  segment 0-99
```

No page write.

### Append Segment 100-199

Only head changes:

```text
active_segments:
  segment 0-99
  segment 100-199
```

No page write.

### Append Segment 200-299

`active_segments` reaches `LeafSegmentLimit`.

Write leaf:

```text
pages/l00/leaf-0-299.json
```

Leaf contains:

```text
segment 0-99
segment 100-199
segment 200-299
```

CAS head:

```text
leaf_frontier = leaf 0-299
active_segments = []
index_frontier = []
```

### Append Segments 300-599

After segment 500-599, active fills again.

Need to seal new leaf `300-599`.

But `leaf_frontier` already contains `leaf 0-299`.

So:

```text
old leaf_frontier 0-299 goes into index_frontier
new leaf 300-599 becomes leaf_frontier
```

Write:

```text
pages/l01/index-l01-0-299.json
pages/l00/leaf-300-599.json
```

CAS head:

```text
index_frontier[0] = l01 index 0-299
leaf_frontier = leaf 300-599
active_segments = []
```

### Append Segments 600-899

Seal `leaf 600-899`.

Old `leaf_frontier 300-599` goes into l01.

Current l01 has room.

Write:

```text
pages/l01/index-l01-0-599.json
pages/l00/leaf-600-899.json
```

CAS head:

```text
index_frontier[0] = l01 index 0-599
leaf_frontier = leaf 600-899
```

The old `index-l01-0-299.json` is now unreachable and can be removed by
orphan-page GC after a grace period.

### Append Segments 900-1199

Seal `leaf 900-1199`.

Old `leaf_frontier 600-899` goes into l01.

Current l01 still has room.

Write:

```text
pages/l01/index-l01-0-899.json
pages/l00/leaf-900-1199.json
```

CAS head:

```text
index_frontier[0] = l01 index 0-899
leaf_frontier = leaf 900-1199
```

Now l01 is full, but it does not rotate yet. It rotates only when the next
child needs to be inserted.

### Append Segments 1200-1499

Seal `leaf 1200-1499`.

Old `leaf_frontier 900-1199` must go into l01.

But l01 is already full.

So:

```text
old l01 index 0-899 goes into l02
new l01 starts with leaf 900-1199
new leaf 1200-1499 becomes leaf_frontier
```

Write:

```text
pages/l02/index-l02-0-899.json
pages/l01/index-l01-900-1199.json
pages/l00/leaf-1200-1499.json
```

CAS head:

```text
index_frontier[1] = l02 index 0-899
index_frontier[0] = l01 index 900-1199
leaf_frontier = leaf 1200-1499
active_segments = []
```

Tree:

```text
head.json
  index_frontier[1] -> l02 index 0-899
    -> l01 index 0-899
      -> leaf 0-299
      -> leaf 300-599
      -> leaf 600-899

  index_frontier[0] -> l01 index 900-1199
    -> leaf 900-1199

  leaf_frontier -> leaf 1200-1499

  active_segments -> []
```

## When l03 Is Created

`l03` is created when `l02` is full and another full `l01` needs to be carried
into it.

With:

```text
IndexRefLimit = 3
```

An l02 can hold:

```text
l01 0-899
l01 900-1799
l01 1800-2699
```

Now l02 is full.

When `l01 2700-3599` needs to be carried:

```text
old l02 0-2699 goes into l03
new l02 starts with l01 2700-3599
```

Object store:

```text
pages/
  l01/
    index-l01-0-899.json
    index-l01-900-1799.json
    index-l01-1800-2699.json
    index-l01-2700-3599.json

  l02/
    index-l02-0-2699.json
    index-l02-2700-3599.json

  l03/
    index-l03-0-2699.json
```

General rule:

```text
l(N+1) is created when:
  lN already has IndexRefLimit child refs
  and another l(N-1) page must be carried into lN
```

## How Readers Search

Reader loads `head.json`.

Reachable roots in read order:

```text
highest index frontier
lower index frontiers
leaf_frontier
active_segments
```

Example:

```text
index_frontier[1] -> l02 0-899
index_frontier[0] -> l01 900-1199
leaf_frontier -> leaf 1200-1499
active_segments -> segment 1500-1599
```

For LSN `450`:

```text
450 is in l02 0-899
open l02
open matching l01
open matching leaf
return matching SegmentRef
```

For LSN `950`:

```text
950 is in l01 900-1199
open l01
open matching leaf
return matching SegmentRef
```

For LSN `1550`:

```text
1550 is inside active_segments in head
return SegmentRef directly
```

## Practical Index Depth

With realistic defaults:

```text
LeafSegmentLimit = 128
IndexRefLimit = 1024
```

Coverage by segment count:

```text
1 leaf page
= 128 segments

1 l01 index
= 1024 leaves
= 131,072 segments

1 l02 index
= 1024 l01 indexes
= 134,217,728 segments

1 l03 index
= 1024 l02 indexes
= 137,438,953,472 segments
```

If one segment has 16,384 records:

```text
1 leaf
= 128 * 16,384
= 2,097,152 records

1 l01
= 131,072 * 16,384
= 2,147,483,648 records

1 l02
= 134,217,728 * 16,384
= 2,199,023,255,552 records

1 l03
= 137,438,953,472 * 16,384
= 2,251,799,813,685,248 records
```

In practice:

```text
most partitions: active_segments + leaf_frontier + maybe l01
large partitions: l02
extreme partitions: l03
```

The practical risk is not index depth. The practical risk is tiny segments.
Tiny segments create too many `SegmentRef`s and grow the catalog tree faster.

## Write Cost

Most segment commits:

```text
CAS head.json only
```

When active fills:

```text
PUT l00 leaf page
maybe PUT l01/l02/l03 index pages
CAS head.json
```

Orphan pages are only created when:

```text
candidate leaf/index pages are written but head CAS fails
or old frontier pages are replaced by newer immutable versions
```

This is much less orphan churn than rewriting active leaf pages on every
segment commit.

## Writer Session Flow

`OpenWriter(partition, writer_id)` fences the partition by CAS-updating
`head.json`:

```text
load head.json
increment writer_epoch
store writer_id
CAS head.json
```

If another writer wins the CAS, `OpenWriter` backs off and retries with the
new head. The returned writer session owns the observed `writer_epoch` and
head token.

`AppendSegment(segment)` is an ordered metadata commit:

```text
check writer_epoch and writer_id
check segment.base_lsn == head.next_lsn
check timestamp order against the previous segment
build candidate page set
CAS head.json from old token to new token
```

Most appends only modify `head.active_segments`. When that buffer reaches
`LeafSegmentLimit`, the append first writes immutable page candidates, then
publishes them by CASing `head.json`.

If CAS fails, the candidate pages are unreachable and become GC candidates.
Visibility is still correct because readers only follow pages reachable from
the committed head.

Idempotent retry is allowed only for the last committed segment. The session
re-reads the current head before returning retry success, so a stale cached
head cannot hide that a newer writer has already advanced the partition.

## Reader Flow

`LoadPartition` reads only `head.json` and returns the bounded partition head.

`FindSegment(partition, lsn)`:

```text
load head.json
reject if lsn is outside oldest_lsn...next_lsn-1
check each reachable root range
descend index pages until l00 leaf
search leaf SegmentRefs
if not sealed, search head.active_segments
```

`ListSegments(partition, from_lsn, limit)` uses the same traversal, but stops
as soon as it has filled the caller's bounded page. It does not list the object
store prefix and does not materialize full history.

## Invariants

The committed history is:

```text
index_frontier + leaf_frontier + active_segments
```

These must cover the committed LSN range contiguously:

```text
oldest_lsn ... next_lsn-1
```

Rules:

```text
index_frontier has at most one index page per level
leaf_frontier has at most one sealed leaf page
active_segments has fewer than LeafSegmentLimit refs after a commit
all page objects are immutable
only head.json is mutable
visibility flows only through head.json
```
