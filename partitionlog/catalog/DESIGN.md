# partitionlog/catalog Design

`partitionlog/catalog` is the partition-level metadata system for committed
immutable segments.

The catalog does two jobs:

- keeps one bounded, strongly consistent partition head;
- stores retained segment history in immutable bounded pages.

The catalog does not own segment bytes. Segment objects are written elsewhere,
typically through `segwriter`. The catalog only makes those objects visible by
committing `pmeta.SegmentRef`.

Shared metadata types live in `partitionlog/pmeta`. `catalog` owns the durable
protocol around those types: fencing, conditional head updates, immutable page
references, and backend-specific storage.

## Scope

The catalog is responsible for:

- loading the current partition head;
- fencing writers so stale writers cannot publish;
- appending committed `SegmentRef`s in order;
- finding the segment covering an LSN;
- listing segments through bounded pages;
- storing monotonic retention intent;
- applying retention through the fenced writer session;
- making visibility depend only on the committed head.

The catalog is not responsible for:

- writing segment bytes;
- reader-service RPCs;
- retention policy decisions;
- event-level compaction or segment rewrite;
- cross-partition transactions.

Detailed retention and physical reclamation behavior is defined in
[`partitionlog/LIFECYCLE.md`](../LIFECYCLE.md).

## Package Layout

`partitionlog/catalog` is the common catalog API. It contains errors, request
types, reader/writer-session interfaces, validation helpers, and the memory
implementation used by tests and local tools.

Backend-specific implementations live below the catalog package:

- `partitionlog/catalog/blob`: object-store catalog using immutable JSON pages
  and a CAS-protected head object.
- `partitionlog/catalog/blob/s3`, `partitionlog/catalog/blob/gcs`,
  `partitionlog/catalog/blob/azure`: provider backends for the blob catalog.
- `partitionlog/catalog/cache/redis`: optional read-through Redis cache
  decorator for catalog readers.
- `partitionlog/catalog/notify`: optional commit hint emitters for writer
  sessions.
- `partitionlog/catalog/watch`: optional hint consumers/cache warmers.
- `partitionlog/catalog/writeradapter`: adapter from catalog writer sessions to
  the public partition writer session interface.

Future Cassandra/Scylla support should live under `partitionlog/catalog/cql`.
It should implement the same `catalog.Reader` and `catalog.WriterManager`
interfaces, but store history as partitioned rows/buckets instead of object
paths. That keeps Cassandra-specific bucket sizing, lightweight transactions,
and query-shape decisions out of the common catalog API.

## Construction

The normal user-facing constructors should live at the backend package that
knows how to build a complete catalog from an SDK client:

```go
cat := catalog.NewMemory()

s3Cat, err := s3catalog.New(s3Client, bucket, s3catalog.Options{
    Prefix: "unijord/catalog",
})

gcsCat, err := gcscatalog.New(gcsClient, bucket, gcscatalog.Options{
    Prefix: "unijord/catalog",
})

azureCat, err := azurecatalog.New(containerClient, azurecatalog.Options{
    Prefix: "unijord/catalog",
})
```

Advanced users can still construct the lower-level blob catalog explicitly:

```go
backend, err := s3catalog.NewBackend(s3Client, bucket)
cat, err := blob.New(backend, blob.Options{Prefix: "unijord/catalog"})
```

This keeps the root `catalog` package free of provider imports and avoids an
import cycle: `catalog/blob` depends on `catalog` for shared interfaces and
errors, so `catalog` itself cannot import `catalog/blob`.

## Core Model

Each partition has two metadata layers:

1. `head`
   - small, mutable, strongly consistent
   - read on every open, append, and fence acquisition

2. `pages`
   - immutable, bounded units of segment history
   - read only when the caller needs history beyond the head fast path

The system must never use one ever-growing manifest for the full retained
history of a partition.

### Public Head

The public partition head is `pmeta.PartitionHead`:

```go
type PartitionHead struct {
    Partition               uint32
    NextLSN                 uint64
    OldestLSN               uint64
    AppliedRetentionLSN     uint64
    AppliedRetentionVersion uint64
    WriterEpoch             uint64
    SegmentCount            uint64
    ReachableSegmentCount   uint64
    LastSegment             SegmentRef
    HasLastSegment          bool
}
```

`SegmentCount` is the lifetime publication count and never decreases.
`ReachableSegmentCount` is the number of segments still referenced by the
current head and decreases when retention removes history.

This stays bounded regardless of retained history.

### Internal Durable Head

Durable backends store more than the public head. The internal head record also
needs:

- `generation`
- `writer_id`
- `index_frontier`
- `leaf_frontier`
- `active_segments`
- backend CAS token or version

These fields are backend state, not public API.

## History Pages

History is stored in immutable pages. Two page classes are sufficient:

- `leaf page`
  - ordered `SegmentRef`s
  - optional `next_leaf_ref` for forward scans

- `branch page`
  - sparse index from LSN range to child page ref

`leaf page` is the unit of history storage. `branch page` is the lookup index
when retained history grows beyond one leaf.

### Leaf Page

A leaf page contains a bounded, ordered slice of `SegmentRef`s:

```go
type LeafPage struct {
    PageID           string
    Partition        uint32
    Generation       uint64
    BaseLSN          uint64
    LastLSN          uint64
    MinTimestampMS   int64
    MaxTimestampMS   int64
    Segments         []pmeta.SegmentRef
    NextLeafRef      string
}
```

Rules:

- segments are ordered by `BaseLSN`;
- segment ranges are contiguous;
- timestamps are non-decreasing across segment boundaries;
- the page timestamp range exactly matches its first and last segment;
- page size is bounded;
- pages are immutable after write.

### Branch Page

A branch page indexes child pages by LSN range:

```go
type BranchPage struct {
    PageID           string
    Partition        uint32
    Level            uint32
    Generation       uint64
    BaseLSN          uint64
    LastLSN          uint64
    MinTimestampMS   int64
    MaxTimestampMS   int64
    Children         []BranchChild
}

type BranchChild struct {
    BaseLSN        uint64
    LastLSN        uint64
    MinTimestampMS int64
    MaxTimestampMS int64
    ChildRef       string
}
```

Rules:

- children are ordered by `BaseLSN`;
- child ranges are non-overlapping and contiguous;
- child timestamp ranges are non-decreasing, and the branch range exactly
  matches its first and last child;
- branch fanout is bounded;
- pages are immutable after write.

## Public API Roles

The scalable design has three roles:

- `reader`
- `writer fence/session`
- `admin/compactor`

The current package API can expose them through one interface or through
smaller interfaces, but the protocol is the same.

### Read Role

The read surface is bounded:

```go
LoadPartition(ctx, partition) -> pmeta.PartitionHead
FindSegment(ctx, partition, lsn) -> (pmeta.SegmentRef, bool, error)
LookupTimestamp(ctx, partition, timestampMS) -> (head, pmeta.SegmentRef, bool, error)
ListSegments(ctx, partition, fromLSN, limit) -> pmeta.SegmentPage
```

No read method returns full history.

### Write Role

Writers open a fenced writer session before appending:

```go
OpenWriter(ctx, partition, writerID) -> WriterSession
```

The writer session owns the current fence plus any backend-specific hot state
needed for steady-state append:

```go
type WriterSession interface {
    Head() pmeta.PartitionHead
    Epoch() uint64
    WriterID() [16]byte
    AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error)
}
```

Backends may keep an opaque head token or generation inside the session. That
state is not part of the public API. The session keeps it hot across
successful appends.

## Core Invariants

For one partition:

- committed segment ranges are contiguous in append order;
- `NextLSN` is exactly `LastSegment.LastLSN + 1` when `HasLastSegment` is true;
- `OldestLSN <= NextLSN`;
- `SegmentRef.BaseLSN == ExpectedNextLSN` at publish time;
- segment timestamps are non-decreasing across committed segment boundaries;
- a segment is visible only if reachable from the committed head;
- `SegmentUUID` and `URI` are unique within the partition history;
- a writer may publish only with the current fenced `WriterEpoch`.

These invariants are the contract. Memory, object-store, Cassandra, and
Scylla backends must all enforce them.

## Steady-State Write Protocol

Segment publication is staged:

1. the partition writer uploads the segment object;
2. it builds `pmeta.SegmentRef` from the upload result;
3. it asks the writer session to append that segment;
4. the catalog validates and commits metadata;
5. only after the head CAS succeeds is the segment visible.

The object write and metadata commit are not one transaction. The catalog is
the visibility boundary.

`SegmentUUID` is the stable identity for one publication attempt. A retry must
reuse the complete `SegmentRef`, candidate pages, and candidate head bytes. It
must not upload a replacement segment or assign a new UUID.

### Writer Fence Rules

Fence acquisition:

1. read current head;
2. CAS head:
   - `writer_id = newWriterID`
   - `writer_epoch = oldWriterEpoch + 1`
   - `generation = oldGeneration + 1`
3. return `(epoch, state, token)`.

`writer_id` identifies one writer incarnation. Concurrent writers must not
share it, and a restarted or replacement writer uses a new ID. The ID remains
stable within one `OpenWriter` acquisition while the catalog retries an
ambiguous CAS result.

If a fence CAS response is lost, the catalog retries the exact candidate fence
instead of incrementing the epoch again. A current head that exactly matches
the candidate proves acquisition succeeded. If another writer superseded that
candidate, the same `OpenWriter` call builds a new candidate from the returned
head and attempts to acquire the next epoch. If neither the CAS result nor a
confirming head read is available, it returns `ErrFenceIndeterminate`.

Append with a writer fence:

1. `req.WriterEpoch` must equal current `head.WriterEpoch`;
2. `req.Segment.WriterTag` must equal current `head.WriterID`;
3. `req.ExpectedNextLSN` must equal current `head.NextLSN`;
4. `req.Segment.BaseLSN` must equal `head.NextLSN`;
5. if `req.WriterEpoch < head.WriterEpoch`, return `ErrStaleWriter`;
6. if `req.WriterEpoch > head.WriterEpoch`, return conflict or stale-writer;
7. commit candidate pages when needed and CAS the head.

### Steady-State Append

The common append case updates only the bounded segment buffer in the head:

1. use the session's cached `(state, token)`;
2. append the new `SegmentRef` to `active_segments`;
3. CAS the head from the cached token to:
   - `next_lsn = segment.LastLSN + 1`
   - `last_segment = segment`
   - `segment_count = old + 1`
   - `reachable_segment_count = old + 1`
   - `active_segments = old active_segments + segment`
   - `generation = old + 1`
4. on CAS success, the segment is visible.

The intended hot path is:

```text
write segment object
CAS head
```

not:

```text
GET full history
rewrite manifest
PUT manifest
```

### Split Append

When `active_segments` reaches `LeafSegmentLimit`:

1. seal the complete active buffer as a new immutable leaf page;
2. carry the previous leaf frontier through the bounded index frontier;
3. set the new leaf as `leaf_frontier` and clear `active_segments`;
4. CAS the head to the new leaf and index frontier.

This is copy-on-write on one bounded path, not a rewrite of full history.

The split cost is `O(log pages)` rather than `O(total history)`.

Every page reference is self-describing. Its canonical object key encodes the
page level, LSN range, catalog generation, and content-derived page ID. Readers
validate those fields against both the reference and decoded page before using
the page.

## Read Protocol

### LoadPartition

`LoadPartition` reads only the head.

This must be cheap and bounded regardless of retained history.

### FindSegment

`FindSegment(partition, lsn)` proceeds in this order:

1. read head;
2. if `lsn` is outside `[OldestLSN, NextLSN)`, return not found;
3. if `HasLastSegment` and `LastSegment` covers `lsn`, return immediately;
4. otherwise find the covering root in `index_frontier` and walk down by LSN;
5. load one leaf page;
6. binary search inside that leaf.

The head fast path is important for hot-tail reads.

### ListSegments

`ListSegments(partition, fromLSN, limit)` proceeds:

1. locate the first leaf covering or following `fromLSN`;
2. return up to `limit` segment refs;
3. if the first leaf does not satisfy `limit`, follow `next_leaf_ref`;
4. stop once `limit` is met.

The caller gets a bounded `SegmentPage`, not the full partition history.

### LookupTimestamp

`LookupTimestamp(partition, timestampMS)` finds the earliest retained segment
whose `MaxTimestampMS` is at least the requested timestamp:

1. read and validate the head;
2. binary search the reachable root timestamp ranges;
3. descend one index path by child `MaxTimestampMS`;
4. binary search the selected leaf's segment ranges;
5. return the selected segment with the exact head snapshot used for lookup.

The lookup costs `O(tree depth)` object reads and does not scan catalog pages
from `OldestLSN`. Returning the head and segment together prevents a concurrent
append or retention update from mixing two catalog snapshots.

## Visibility and Failure Semantics

Visibility is determined only by the committed head.

Failure cases:

| Failure point | Visible to readers | Result |
| --- | --- | --- |
| segment upload fails | No | caller retries upload |
| segment upload succeeds, metadata CAS is rejected | No | object and candidate pages are orphan candidates |
| metadata CAS response is lost | Unknown until reconciled | retry the same CAS and inspect committed history |
| metadata CAS succeeds | Yes | normal |
| writer crashes after upload, before CAS | No | orphan GC removes object/page later |
| writer crashes after CAS | Yes | normal |

This is the required rule:

- object existence does not imply visibility;
- only head reachability implies visibility.

### Ambiguous Head Commits

A provider error does not prove that a conditional head write failed. The
request may have committed while its response was lost. The writer session
reconciles an ambiguous result as follows:

1. retry the same head CAS with the same expected token and bytes;
2. read the current head after bounded retries;
3. if the exact `SegmentRef` is the last segment or is reachable in retained
   history at its `BaseLSN`, treat the append as committed;
4. if the old head is still current, treat the append as not committed;
5. if another segment owns the LSN range, return `ErrConflict` or
   `ErrStaleWriter` when the writer fence moved;
6. if committed state cannot be read or the target range was already removed,
   return `ErrCommitIndeterminate`.

Exact commit reconciliation runs before stale-writer rejection. A newer fence
prevents an old writer from publishing new data; it does not invalidate
read-only proof that the old writer's exact segment was already committed.

The writer adapter maps `ErrCommitIndeterminate` to
`writer.ErrPublishIndeterminate`. Higher layers must not manufacture a new
segment for that LSN range. They should stop the writer and reconcile catalog
state before resuming.

## Backend Mapping

The logical design is backend-neutral. Backends differ only in how they
implement:

- linearizable head read/CAS;
- immutable page storage;
- optional cache.

### Object Store

Use:

- `HeadStore`
  - one small linearizable head object or external KV record
- `PageStore`
  - immutable leaf and branch page blobs

Example layout:

```text
catalog/<bucket>/streams/<sha256-stream-key>/p00000003/head.plc
catalog/<bucket>/streams/<sha256-stream-key>/p00000003/pages/l00/leaf-<seq-hi>-<seq-lo>-<generation>-<page-id>.plc
catalog/<bucket>/streams/<sha256-stream-key>/p00000003/pages/l01/index-l01-<seq-hi>-<seq-lo>-<generation>-<page-id>.plc
catalog/<bucket>/streams/<sha256-stream-key>/p00000003/pages/l02/index-l02-<seq-hi>-<seq-lo>-<generation>-<page-id>.plc
```

The naming contract is defined in `partitionlog/OBJECT_LAYOUT.md`. Page refs
must be self-contained and immutable.

### Cassandra / Scylla

Use:

- one `partition_head` row per partition with LWT/CAS;
- `leaf_page` rows keyed by `(partition, page_id)`;
- `branch_page` rows keyed by `(partition, level, page_id)`.

This keeps the same protocol:

- head is the only mutable linearizable row;
- pages are immutable;
- appends rewrite only one bounded path.

### Cache

Redis or in-process cache may hold:

- partition head;
- recent leaf pages;
- recent branch pages;
- hot `lsn -> page_ref` hints.

The cache is never authoritative.

## Page Sizing

The catalog must return bounded results. A good default is:

- default page size: 128 segment refs
- maximum API page size: 1024 segment refs

Internal page sizes may differ by backend, but they should stay in the same
order of magnitude so one page remains a cheap unit to read, cache, and
rewrite.

## Encoding

Page bodies should be:

- versioned;
- checksummed;
- compact.

JSON is acceptable for tests and debugging. It should not be the canonical
durable encoding for production history pages.

## Summary

The scalable catalog design is:

- one bounded linearizable head per partition;
- immutable bounded leaf and branch pages for retained history;
- copy-on-write updates on one bounded path;
- writer fencing through `WriterEpoch`;
- visibility determined only by head reachability;
- bounded point lookup and bounded page scans.

This keeps the hot path small, keeps retained history scalable, and avoids the
flat-manifest failure mode entirely.
