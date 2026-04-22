# partitionlog/pwriter Design

`partitionlog/pwriter` is the partition-level writer above `segwriter`.
It owns one partition's append flow, rolls immutable segment objects, and
publishes committed segment metadata into `partitionlog/catalog`.

`segwriter` writes one segment. `pwriter` decides when a segment starts, when it
closes, what LSN each record gets, and when the segment becomes visible through
the catalog.

## Responsibilities

- Assign contiguous LSNs from the catalog partition head.
- Enforce non-decreasing timestamps across segment boundaries.
- Own one active `segwriter.Writer` at a time.
- Roll active segments by configured policy.
- Convert `segwriter.Result` into `catalog.SegmentRef`.
- Publish `SegmentRef` through `catalog.AppendSegment`.
- Treat uploaded-but-unpublished segments as orphan objects.

## Non-Responsibilities

- It does not define the segment byte format. That is `segformat`.
- It does not compress/hash blocks directly. That is `segblock`.
- It does not multipart-upload bytes directly. That is `segwriter`.
- It does not store the durable catalog. That is `catalog`.
- It does not delete orphan objects yet. That will be a GC layer.
- It does not acquire writer fences yet. Current code accepts `WriterEpoch`;
  durable backends will add explicit fence acquisition.

## Data Flow

```text
Append(record)
  -> assign LSN from nextLSN
  -> append to active segwriter
  -> if roll policy triggers:
       segwriter.Close()
       build catalog.SegmentRef
       catalog.AppendSegment(expected_next_lsn = segment.BaseLSN)
       update in-memory partition head
```

Flush and Close use the same publication path:

```text
Flush()
  if no active records:
      no-op
  else:
      close active segment
      publish SegmentRef

Close()
  Flush()
  mark writer closed
```

## Catalog Visibility

The segment object is uploaded before catalog publication:

```text
1. active segwriter closes and commits object bytes
2. pwriter builds SegmentRef
3. pwriter calls catalog.AppendSegment
4. catalog append success makes the segment visible
```

If step 1 succeeds and step 3 fails, the object exists but is not visible to
readers because readers consult the catalog. That object is an orphan and must
be cleaned by a later GC job.

This is intentional. Object storage and catalog metadata are not one
transaction.

## Roll Policy

The first implementation supports two roll triggers:

- `MaxSegmentRecords`
- `MaxSegmentRawBytes`

`MaxSegmentRawBytes` should be the primary production knob. It keeps object
sizes predictable for object storage and scan performance.

`MaxSegmentRecords` is a safety cap for tiny events where byte size grows
slowly. The current default is `16_384` records. This is a conservative
power-of-two guardrail, not a format limit.

Expected future roll triggers:

- max segment age;
- explicit fence/epoch change;
- shutdown;
- explicit flush from caller;
- target object-store part alignment.

## LSN Assignment

On startup:

```text
state = catalog.LoadPartition(partition)
nextLSN = state.NextLSN
```

Each successful append receives the current `nextLSN`, then increments it.

If the catalog already has committed segments, the writer continues from the
catalog head. It does not scan all history.

## Timestamp Ordering

`pwriter` enforces non-decreasing timestamps across the full partition writer
lifetime, including across segment rolls.

On startup, if the catalog head has a last segment:

```text
lastTimestamp = state.LastSegment.MaxTimestampMS
```

Each append must have `TimestampMS >= lastTimestamp`.

Timestamp regression is terminal. The writer aborts the active segment and must
be discarded.

## Segment Creation

When the first record for a new segment arrives:

1. Generate `SegmentUUID`.
2. Capture `CreatedUnixMS`.
3. Build `SegmentInfo`.
4. Ask `SinkFactory` for a new `segwriter.Sink`.
5. Start `segwriter.New` with per-segment metadata.

`SinkFactory` keeps storage policy outside `pwriter`. For tests it can return a
memory sink. In production it will derive object paths from:

- partition;
- base LSN;
- writer epoch;
- segment UUID.

## SegmentRef Publication

After `segwriter.Close`, `pwriter` builds:

```go
catalog.SegmentRef{
    URI:              result.Object.URI,
    Partition:        result.Metadata.Partition,
    WriterEpoch:      opts.WriterEpoch,
    SegmentUUID:      result.Metadata.SegmentUUID,
    WriterTag:        result.Trailer.WriterTag,
    BaseLSN:          result.Metadata.BaseLSN,
    LastLSN:          result.Metadata.LastLSN,
    MinTimestampMS:   result.Metadata.MinTimestampMS,
    MaxTimestampMS:   result.Metadata.MaxTimestampMS,
    RecordCount:      result.Metadata.RecordCount,
    BlockCount:       result.Metadata.BlockCount,
    SizeBytes:        result.Object.SizeBytes,
    BlockIndexOffset: result.Metadata.BlockIndexOffset,
    BlockIndexLength: result.Metadata.BlockIndexLength,
    Codec:            result.Metadata.Codec,
    HashAlgo:         result.Metadata.HashAlgo,
    SegmentHash:      result.Metadata.SegmentHash,
    TrailerHash:      result.Metadata.TrailerHash,
}
```

Then it calls:

```go
catalog.AppendSegment(ctx, catalog.AppendSegmentRequest{
    Partition:       opts.Partition,
    ExpectedNextLSN: active.baseLSN,
    WriterEpoch:     opts.WriterEpoch,
    Segment:         segment,
})
```

The catalog validates that the append is still at the partition head.

## Failure Semantics

| Failure | Behavior |
| --- | --- |
| `segwriter.Append` fails | `pwriter` aborts and becomes terminal |
| timestamp regression | `pwriter` aborts and becomes terminal |
| `segwriter.Close` fails during flush | `pwriter` aborts and becomes terminal |
| catalog append fails after object commit | object is orphaned, `pwriter` aborts and becomes terminal |
| caller calls `Abort` | active segment is aborted, no catalog publication |
| caller calls `Close` with no records | no-op close |

Terminal means future `Append` returns `ErrAborted` or `ErrClosed`.

## Fencing

The current implementation accepts `WriterEpoch` in options and passes it to
catalog publication. It rejects opening with a stale epoch if the loaded
catalog head already has a greater epoch.

This is not the final durable fencing API. The next catalog slice should add:

```go
AcquireWriter(partition, writerID) -> WriterFence
```

Then `pwriter.New` should receive a `WriterFence`, not a raw epoch. That fence
will carry:

- partition;
- writer ID;
- epoch;
- hot partition state;
- backend CAS token.

Until then, `MemoryCatalog` gives enough correctness coverage for rolling and
publication semantics.

## Concurrency

`Writer` is not safe for concurrent use. One goroutine should call `Append`,
`Flush`, `Close`, and `Abort`.

Internal `segwriter` may perform parallel sealing/uploading, but `pwriter` does
not expose concurrent append semantics.

## Current Limitations

- No max-age roll trigger yet.
- No durable writer-fence acquisition yet.
- No object-store sink factory yet.
- No orphan GC yet.
- No read path that opens catalog `SegmentRef` into a segment reader yet.

These are intentionally deferred so the first slice stays reviewable.
