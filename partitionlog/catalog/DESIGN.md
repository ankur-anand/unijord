# partitionlog/catalog Design

`partitionlog/catalog` is the partition-level metadata layer above
`segwriter`. It records committed immutable segment objects and lets readers
find the segment covering an LSN without loading the full history of a
partition.

The catalog must not repeat the isledb flat-manifest problem: there must never
be one JSON manifest that grows with total retained history and is decoded,
cloned, or rewritten on every operation.

## Goals

- Keep partition head metadata bounded and cheap to read.
- Make segment history readable through bounded pages or point lookups.
- Keep storage backend decisions open: memory, object store, object store plus
  Redis, Cassandra, Scylla, or another strongly consistent metadata store.
- Provide atomic segment publication: a segment is visible only after the
  catalog append commits.
- Support writer fencing so stale writers cannot publish after a newer writer
  takes over.
- Preserve append-only invariants: contiguous LSNs and non-decreasing
  timestamps across committed segments.

## Non-goals

- No full durable object-store implementation in this slice.
- No retention or archive deletion policy yet.
- No reader service API in this package.
- No multi-partition transaction support.

## Public Shape

The catalog API is intentionally bounded:

```go
type Catalog interface {
    LoadPartition(ctx context.Context, partition uint32) (PartitionState, error)
    AppendSegment(ctx context.Context, req AppendSegmentRequest) (PartitionState, error)
    FindSegment(ctx context.Context, partition uint32, lsn uint64) (SegmentRef, bool, error)
    ListSegments(ctx context.Context, req ListSegmentsRequest) (SegmentPage, error)
}
```

`LoadPartition` returns only the partition head:

```go
type PartitionState struct {
    Partition      uint32
    NextLSN        uint64
    OldestLSN      uint64
    WriterEpoch    uint64
    SegmentCount   uint64
    LastSegment    SegmentRef
    HasLastSegment bool
}
```

It does not return every segment. History is accessed with:

- `FindSegment(partition, lsn)` for point lookup.
- `ListSegments(partition, fromLSN, limit)` for bounded scans.

Default page size is 128 segment refs. Maximum page size is 1024 segment refs.
Backends can choose smaller internal pages, but they must not return unbounded
history through the public API.

## SegmentRef

`SegmentRef` is the durable metadata for one committed segment object:

```go
type SegmentRef struct {
    URI              string
    Partition        uint32
    WriterEpoch      uint64
    SegmentUUID      [16]byte
    WriterTag        [16]byte
    BaseLSN          uint64
    LastLSN          uint64
    MinTimestampMS   int64
    MaxTimestampMS   int64
    RecordCount      uint32
    BlockCount       uint32
    SizeBytes        uint64
    BlockIndexOffset uint64
    BlockIndexLength uint32
    Codec            segformat.Codec
    HashAlgo         segformat.HashAlgo
    SegmentHash      uint64
    TrailerHash      uint64
}
```

This is enough for:

- reader open planning;
- catalog point lookup;
- segment integrity verification;
- retention planning;
- orphan diagnosis by `SegmentUUID`, `WriterTag`, and `WriterEpoch`.

## Core Invariants

For each partition:

- Segment ranges are contiguous in commit order.
- `PartitionState.NextLSN == LastSegment.LastLSN + 1` when a last segment
  exists.
- A new segment can commit only at `ExpectedNextLSN == PartitionState.NextLSN`.
- A segment's `BaseLSN` must equal `ExpectedNextLSN`.
- Segment timestamps are non-decreasing across segment boundaries.
- `WriterEpoch` on the append request must match the active writer fence.
- Segment identity is unique by `SegmentUUID` and by `URI`.

The memory implementation enforces these invariants directly. Durable backends
must enforce the same invariants with their native conditional write mechanism.

## Append Protocol

Segment publication is staged:

1. The partition writer uploads the segment object through `segwriter`.
2. The writer builds a `SegmentRef` from `segwriter.Result`.
3. The writer calls `AppendSegment` with:
   - `Partition`
   - `ExpectedNextLSN`
   - `WriterEpoch`
   - `Segment`
4. The catalog atomically validates and commits the segment ref.
5. If `AppendSegment` fails, the uploaded segment object is not visible and is
   treated as an orphan.

This ordering is deliberate. Object stores usually cannot participate in the
same transaction as metadata. The segment object is written first, then catalog
publication makes it visible.

Failure behavior:

| Failure point | Visible to readers | Recovery |
| --- | --- | --- |
| Upload fails | No | Caller retries upload |
| Upload succeeds, catalog append fails | No | Segment object is orphaned and GC removes it later |
| Catalog append succeeds | Yes | Normal |
| Writer crashes after upload before append | No | Orphan GC |
| Writer crashes after append | Yes | Normal |

## Fencing

The catalog uses a per-partition `WriterEpoch` fence. A writer must publish
only with the epoch it owns.

The durable design should expose a writer acquisition operation before the
partition writer is implemented:

```go
type WriterFence struct {
    Partition uint32
    WriterID  [16]byte
    Epoch     uint64
    State     PartitionState
    Token     HeadToken
}

AcquireWriter(partition, writerID) -> WriterFence
```

`HeadToken` is backend-specific. It may be:

- object-store generation or etag;
- Cassandra/Scylla head generation;
- Redis compare-and-set version;
- an opaque token carried only by the durable backend.

The public durable interface can keep this token opaque. The important rule is
that the partition writer does not need to re-read the head after every
successful append; the append response should carry the next token.

Acquire semantics:

1. Read current partition head.
2. CAS the head to set `WriterID = newWriterID` and
   `WriterEpoch = oldEpoch + 1`.
3. Return the new epoch, new `PartitionState`, and new head token.

Append semantics:

1. `AppendSegment.WriterEpoch` must equal the current head epoch.
2. `AppendSegment.ExpectedNextLSN` must equal the current head `NextLSN`.
3. If a newer writer has acquired the fence, older writers get
   `ErrStaleWriter`.

This is the same correctness role as isledb's writer fence in `CURRENT`, but
with one important difference: the fenced head is bounded and does not contain
all historical SST or segment refs.

### How `ErrStaleWriter` Happens

`ErrStaleWriter` is not inferred from object existence. It comes from reading
the current partition head during `AppendSegment`.

Durable append must follow this order:

```text
AppendSegment(req):
  // head may come from a caller-supplied hot snapshot or from a backend read.
  // token is the CAS token for that exact head version.
  head, token = currentHead(req.Partition)

  if req.WriterEpoch < head.WriterEpoch:
      return ErrStaleWriter

  if req.WriterEpoch > head.WriterEpoch:
      return ErrConflict or ErrStaleWriter
      // Writer must acquire the fence first. Append cannot invent epochs.

  if req.ExpectedNextLSN != head.NextLSN:
      return ErrConflict

  if req.Segment.BaseLSN != head.NextLSN:
      return ErrConflict

  write new bounded segment-ref page candidate

  casHead(
      token,
      condition = {
        writer_epoch == req.WriterEpoch,
        next_lsn == req.ExpectedNextLSN,
        generation == head.Generation,
      },
      update = {
        next_lsn = req.Segment.LastLSN + 1,
        last_segment = req.Segment,
        segment_count = head.SegmentCount + 1,
        active_leaf = newPageRef,
        generation = head.Generation + 1,
      },
  )

  if casHead fails:
      freshHead, freshToken = readHead(req.Partition)

      if req.WriterEpoch < freshHead.WriterEpoch:
          return ErrStaleWriter

      if req.ExpectedNextLSN != freshHead.NextLSN:
          return ErrConflict

      retry from freshHead/freshToken or return conflict
```

The epoch can change between `currentHead` and `casHead`. That is expected.
The CAS condition is what makes the append safe. If another writer acquires the
fence in that window, the CAS fails because `writer_epoch` or `generation`
changed. The backend then re-reads the head and maps the failure to
`ErrStaleWriter` if the fresh head has a greater `WriterEpoch`.

The initial `currentHead` does not always need to be an object-store GET. A
long-lived `partitionwriter` should keep the last successful `PartitionState`
and backend CAS token in memory. For the normal single-writer path:

1. `AcquireWriter` reads/CASes the head once and returns `(state, token)`.
2. Each successful `AppendSegment` returns the next `(state, token)`.
3. The next append starts from that hot state and token.
4. Only CAS failure requires a fresh backend read.

So the steady-state append path should be:

```text
write segment object
write bounded page candidate
CAS head with hot token
```

not:

```text
GET head
write segment object
write bounded page candidate
CAS head
```

Backends that cannot carry a CAS token across calls may need the extra head
read. That is a backend limitation, not the intended partitionwriter hot path.

So for the example:

1. Writer A owns epoch 7.
2. Writer B acquires the fence and commits head with `writer_epoch = 8`.
3. Writer A calls `AppendSegment(epoch=7, expected_next_lsn=100)`.
4. `AppendSegment` reads head and sees `head.writer_epoch = 8`.
5. Because `7 < 8`, it returns `ErrStaleWriter`.

If Writer A read head while it was still epoch 7, then Writer B acquired epoch
8 before A's CAS, A's CAS fails. A re-reads head, sees epoch 8, and then
returns `ErrStaleWriter`.

### Stale Writer Example

1. Writer A owns epoch 7 and uploads segment `100-199`.
2. Writer B acquires the partition and bumps epoch to 8.
3. Writer A calls `AppendSegment(epoch=7, expected_next_lsn=100)`.
4. Catalog rejects with `ErrStaleWriter`.
5. Writer A's uploaded segment is orphaned.
6. Writer B can append with epoch 8.

The object may exist, but it is not visible because only committed catalog refs
are readable.

## Hot and Cold Catalog

The catalog should be factored into three concepts:

```text
Head store
  Strongly consistent, tiny, mutable per-partition state.

Segment history store
  Immutable or append-only bounded pages/rows of SegmentRef.

Cache
  Optional, not authoritative. Usually Redis or in-process LRU.
```

### Hot State

Hot state is small and frequently read:

- `PartitionState`
- active writer fence
- active/open segment-history page pointer
- last segment summary

This state must support conditional updates.

### Cold State

Cold state is large and grows with retained history:

- sealed segment-history pages;
- old `SegmentRef` rows;
- optional page indexes.

Cold state must be immutable or append-only. It must be read in bounded units.

### Cache

Redis or an in-process cache can hold:

- partition head;
- recent segment refs;
- recent segment pages;
- LSN-to-page hints.

Cache entries are optimization only. The authoritative backend must still be
able to answer `LoadPartition`, `FindSegment`, and `ListSegments` correctly.

## Backend: Memory

`MemoryCatalog` is a correctness and API-shaping backend.

Internally it keeps all segment refs in a slice. That is acceptable only
because it is a test/development backend. Its public API still follows the
bounded contract:

- `LoadPartition` returns only `PartitionState`.
- `FindSegment` uses binary search.
- `ListSegments` returns a bounded page.

The memory backend should not influence durable storage shape beyond the API
contract.

## Backend: Object Store

Object-store storage should use a small mutable head plus immutable catalog
pages.

Example layout:

```text
catalog/
  p00000003/
    head.json
    pages/
      leaf/
        leaf-<seq_lo>-<seq_hi>-<generation>-<page_id>.json
      index/
        level-1/
          index-l01-<seq_lo>-<seq_hi>-<generation>-<page_id>.json
        level-2/
          index-l02-<seq_lo>-<seq_hi>-<generation>-<page_id>.json
    gc/
      orphan-pages/
      orphan-segments/
```

File names are deterministic and self-describing:

```text
head:
  catalog/p<partition>/head.json

leaf page:
  catalog/p<partition>/pages/leaf/
    leaf-<seq_lo>-<seq_hi>-<generation>-<page_id>.json

index page:
  catalog/p<partition>/pages/index/level-<level>/
    index-l<level>-<seq_lo>-<seq_hi>-<generation>-<page_id>.json
```

Formatting:

- `partition`: 8-digit zero-padded decimal, e.g. `p00000003`
- `level`: 2-digit zero-padded decimal, e.g. `l01`
- `seq_lo`, `seq_hi`: 20-digit zero-padded decimal LSNs
- `generation`: 20-digit zero-padded decimal head/catalog generation
- `page_id`: first 16 bytes of SHA-256 over the canonical page bytes,
  hex-encoded

Example:

```text
catalog/p00000003/head.json
catalog/p00000003/pages/leaf/
  leaf-00000000000000000100-00000000000000000199-00000000000000000042-a1b2c3d4e5f60718293a4b5c6d7e8f90.json
catalog/p00000003/pages/index/level-01/
  index-l01-00000000000000000000-00000000000000999999-00000000000000000042-b1c2d3e4f5061728394a5b6c7d8e9f00.json
```

`page_id` is required. Range plus generation is not enough: two writers can
race from the same head generation and produce different candidate pages for
the same `seq_lo/seq_hi/generation`. If the path did not include a unique page
identity, a losing writer could overwrite the winner's candidate object. With
`page_id`, different candidate contents produce different paths.

The filename identity must match file contents on load:

- leaf file `seq_lo` equals first segment `BaseLSN`;
- leaf file `seq_hi` equals last segment `LastLSN`;
- index file range covers all child refs;
- file `generation` matches page `generation`.
- file `page_id` matches the hash of the canonical page bytes.

This gives operators and GC enough information from object listing alone to
diagnose orphan pages, stale-fence outputs, and range overlaps.

### Head

`head.json` is bounded:

```json
{
  "version": 1,
  "partition": 3,
  "next_lsn": 200,
  "oldest_lsn": 0,
  "writer_id": "...",
  "writer_epoch": 8,
  "segment_count": 20,
  "last_segment": { "...": "SegmentRef" },
  "active_leaf": {
    "seq_lo": 100,
    "seq_hi": 199,
    "generation": 42,
    "page_id": "a1b2...",
    "path": "..."
  },
  "index_root": {
    "level": 1,
    "seq_lo": 0,
    "seq_hi": 199,
    "generation": 42,
    "page_id": "b1c2...",
    "path": "..."
  },
  "generation": 42
}
```

The head never embeds all segment refs. It embeds only small pointers and the
last segment summary.

### Leaf Pages

Leaf pages contain bounded `SegmentRef` arrays:

```json
{
  "version": 1,
  "partition": 3,
  "seq_lo": 0,
  "seq_hi": 99,
  "generation": 12,
  "segments": [ "... up to 1024 SegmentRef entries ..." ]
}
```

Leaf pages are immutable once visible. While appending, the backend can write a
new copy of the active leaf page with one additional segment ref. The head CAS
then points to the new active leaf. Old active-leaf candidates from failed CAS
attempts are orphans.

Leaf page path helper:

```go
func LeafPagePath(partition uint32, seqLo, seqHi, generation uint64, pageID string) string {
    return fmt.Sprintf(
        "catalog/p%08d/pages/leaf/leaf-%020d-%020d-%020d-%s.json",
        partition, seqLo, seqHi, generation, pageID,
    )
}
```

### Index Pages

If there are many leaf pages, object-store point lookup needs a bounded index.
Use a small tree of immutable index pages:

```json
{
  "version": 1,
  "level": 1,
  "refs": [
    { "seq_lo": 0, "seq_hi": 99999, "path": "pages/leaf/..." }
  ]
}
```

Index page path helper:

```go
func IndexPagePath(partition uint32, level uint8, seqLo, seqHi, generation uint64, pageID string) string {
    return fmt.Sprintf(
        "catalog/p%08d/pages/index/level-%02d/index-l%02d-%020d-%020d-%020d-%s.json",
        partition, level, level, seqLo, seqHi, generation, pageID,
    )
}
```

`FindSegment` reads:

1. `head.json`
2. `index_root`
3. one index page per level
4. one leaf page

This is `O(log_B pages)` object reads, where `B` is the number of refs per
index page. No read loads full partition history.

### Object Store CAS

The object-store backend needs a conditional update for `head.json`.

Options:

- If the object store supports conditional overwrite, use the head object's
  generation/etag as the CAS token.
- If it does not, keep the head CAS in a small strongly consistent store
  such as Redis, Cassandra, Scylla, DynamoDB, or etcd, and keep cold pages in
  object storage.

The object-store pages are not the source of visibility. The head pointer is
the source of visibility. A page exists but is not visible until the head points
to it.

### Rebuild and Candidate Page Filtering

Normal rebuild does not scan `pages/leaf/` and assume every page is live. That
would make failed CAS candidates visible.

Rebuild starts from the current head:

```text
head = read catalog/p00000003/head.json
live = set()

if head.index_root exists:
    walk index_root
    add every reachable index and leaf page path to live
else if head.active_leaf exists:
    add head.active_leaf.path to live

load only pages in live
verify path fields and page_id/checksum
```

All page files that are present in object storage but not reachable from the
current head are orphan candidates. They are not returned by `FindSegment` or
`ListSegments`; GC can delete them after a grace period.

Example:

```text
Current head:
  generation = 17
  active_leaf = leaf-100-199-17-aaaa.json

Writer A candidate:
  leaf-100-299-18-bbbb.json  // contains 100-199 and A's 200-299

Writer B candidate:
  leaf-100-299-18-cccc.json  // contains 100-199 and B's 200-299

Writer B wins CAS:
  head.generation = 18
  active_leaf = leaf-100-299-18-cccc.json
```

On rebuild, only `leaf-100-299-18-cccc.json` is loaded because only that path
is referenced by head. `leaf-100-299-18-bbbb.json` is stale even though its
range and generation look plausible.

If the head is lost, listing page files alone is not enough to prove which
candidate won a CAS race. Disaster recovery must restore or reconstruct a
valid head from a durable head store, head history, or external quorum log. Page
listing is useful for inventory and GC, not for deciding commit visibility.

## Backend: Cassandra or Scylla

Cassandra/Scylla can store the head row and segment rows directly.

Suggested tables:

```sql
partition_head (
  partition int primary key,
  next_lsn bigint,
  oldest_lsn bigint,
  writer_id blob,
  writer_epoch bigint,
  segment_count bigint,
  last_segment blob,
  generation bigint
)

partition_segments (
  partition int,
  bucket bigint,
  base_lsn bigint,
  last_lsn bigint,
  commit_generation bigint,
  segment blob,
  primary key ((partition, bucket), base_lsn)
)
```

`bucket` can be `base_lsn / bucket_lsn_span`. Choose a span large enough to
avoid too many partitions, but small enough to bound reads. Because a segment
can span a bucket boundary, `FindSegment` should check the current bucket and,
if needed, the previous bucket.

### Cassandra Append

Use generation-gated visibility:

1. Read `partition_head`.
2. Validate writer epoch and expected next LSN.
3. Insert the segment row with `commit_generation = head.generation + 1`.
4. CAS update `partition_head`:
   - `next_lsn = segment.LastLSN + 1`
   - `last_segment = segment`
   - `segment_count = segment_count + 1`
   - `generation = generation + 1`
   - condition: current `next_lsn`, `writer_epoch`, and `generation` match.
5. If the CAS fails, the inserted segment row is ignored because its
   `commit_generation` is not reachable from the successful head generation
   for that append path. Background GC can remove it.

Readers load head first and only return rows with
`commit_generation <= head.generation`.

This avoids requiring every reader to trust rows that may have been inserted by
a failed append attempt.

### Cassandra FindSegment

1. Load `partition_head`.
2. Compute candidate bucket from requested LSN.
3. Query candidate bucket for the greatest `base_lsn <= lsn`.
4. Check `last_lsn >= lsn`.
5. Filter by `commit_generation <= head.generation`.
6. If not found, check previous bucket for a spanning segment.

### Cassandra ListSegments

`ListSegments` pages over `(bucket, base_lsn)` with `LIMIT <=
MaxSegmentPageLimit`. The returned cursor can be represented as the next LSN
or as an internal `(bucket, base_lsn)` token. The public API currently uses
`NextLSN` because segment ranges are contiguous.

## Backend: Object Store plus Redis

Redis should usually be a hot cache or head-CAS accelerator, not the only
durable catalog.

Recommended split:

```text
Redis:
  partition head cache
  writer lease/fence if configured as the CAS store
  recent leaf-page cache
  lsn -> leaf-page hint cache

Object store:
  immutable segment objects
  immutable catalog pages
  optional head snapshot for recovery
```

If Redis is used for fencing, the system must define its durability mode
clearly. Without durable Redis configuration, Redis should not be the only
source of truth for committed segment refs.

## Retention

Retention should advance `OldestLSN` in the partition head and remove old
segment refs in bounded units.

Object-store backend:

- Remove or rewrite bounded leaf pages.
- Publish a new index root through head CAS.
- Queue old pages and old segment objects for GC after a grace period.

Cassandra/Scylla backend:

- Update `partition_head.oldest_lsn`.
- Delete old segment rows by bucket in bounded jobs.
- Use TTL only if it matches the retention semantics exactly; otherwise prefer
  explicit deletion.

Retention must not require decoding or rewriting all retained history.

## Recovery and Orphan GC

Orphans are expected:

- segment object uploaded but catalog append failed;
- object-store catalog page written but head CAS failed;
- stale writer uploaded data after losing its fence.

Every orphan should be diagnosable from:

- `SegmentUUID`
- `WriterTag`
- `WriterEpoch`
- object path or page generation

GC should list candidate objects/pages by prefix and remove those not reachable
from the current catalog head after a grace period.

## Comparison with isledb Manifest

isledb's generic KV manifest problem was:

- one manifest snapshot contained all live SST metadata;
- snapshot decode cost grew with total history;
- clone cost grew with total history;
- snapshot rewrite cost grew with total history.

This catalog design avoids that shape:

- `PartitionState` is bounded;
- segment history is paged;
- point lookup loads one path through an index, not the whole partition;
- append rewrites only bounded head/page metadata;
- cold history is immutable or append-only;
- caches are optional and do not define correctness.

The critical invariant is: no production backend may implement `LoadPartition`
by returning all segment refs.

## Implementation Order

1. Keep `MemoryCatalog` as the test backend for `partitionwriter`.
2. Add `AcquireWriter` / writer-fence API before the durable backend.
3. Implement `partitionwriter` against the bounded `Catalog` interface.
4. Add an object-store-page catalog prototype with a pluggable head CAS store.
5. Add Cassandra/Scylla backend if operational requirements point there.
6. Add retention and orphan GC as separate bounded jobs.
