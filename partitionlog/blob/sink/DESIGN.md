# partitionlog/blob/sink Design

`partitionlog/blob/sink` creates object-backed `segwriter.Sink`
implementations for `partitionlog/writer`.

It owns only segment-object mechanics:

- segment object key naming
- staging prefix naming
- adapting the ordered `segwriter.Txn` byte stream to `stream.MultipartUpload`
- bounded part buffering, concurrent upload, receipt ordering, and commit
  reconciliation

It does not own catalog metadata, retention, reader lookup, or cache
invalidation.

## Public Surface

`New` builds a `Factory` from:

- a `multipart.Store` implementation
- `Options`

`Factory` builds one `segwriter.Sink` per segment from:

- a `writer.SegmentInfo`

`Layout` exposes the deterministic object key naming used by the factory.

Provider packages live beside it:

- `blob/sink/s3`
- `blob/sink/gcs`
- `blob/sink/azure`

## Object Layout

For `Options.Prefix == "partitionlog"`, a final segment object is written as:

```text
partitionlog/segments/
  p00000007/
    seg-00000000000000000100-e00000000000000000003-<segment_uuid>.plseg
```

Staging keys are provider-specific but start under:

```text
partitionlog/staging/
  p00000007/
    seg-00000000000000000100-e00000000000000000003-<segment_uuid>/
```

The final key includes partition, base LSN, writer epoch, and segment UUID.
It does not include last LSN because the sink is created before the segment is
closed. Last LSN belongs in catalog metadata.

## Upload Flow

```text
segwriter ordered bytes
  -> blob sink transaction
  -> bounded multipart stream
  -> PutPart(part_number, bytes, sha256)
  -> Commit(receipts in order, expected size and checksum)
  -> final segment object becomes visible
```

The object store reassembles bytes by part order. Multipart parts are not
segment-format blocks; a part may contain multiple blocks or cut through a
block boundary.

The segment writer passes `PartSize`, upload parallelism, queue size, and an
optional process-wide upload limiter in `segwriter.Plan`. `Options.BufferPool`
can additionally impose one payload-memory bound across every segment upload
created by a factory.

## Failure Model

- part upload fails: the segment writer fails and aborts the multipart upload
- final object already exists: provider returns `ErrPreconditionFailed`
- final object succeeds but catalog publish later fails: the segment object is
  durable but invisible; catalog orphan GC can remove it later
- staging cleanup fails after final success: the final segment remains valid;
  staging leftovers are GC candidates

## Tests

Tests cover:

- in-memory multipart store through `sink.Factory`
- S3 with AWS SDK against `gofakes3`
- GCS with `fake-gcs-server`
- Azure with Azure SDK against a minimal fake HTTP blob service
