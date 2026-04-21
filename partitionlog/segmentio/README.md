# `segmentio` Writer Guide

`segmentio` writes one immutable partition segment in the v1 on-disk format:

- fixed header
- stored block bytes
- block index
- fixed footer

This package is intentionally split into two writer paths:

- `Encode(...)` for simple in-memory encoding
- `SegmentWriter` for the real transactional write path

If you are writing into blob/object storage, a multipart sink, or any staged
commit target, use `SegmentWriter`.

## What The Writer Guarantees

For a single segment, the writer enforces:

- one partition per segment
- contiguous LSNs
- non-decreasing timestamps
- bounded record size and block size
- full segment validation before sink commit
- transactional sink lifecycle: begin, upload parts, complete, abort

The segment only becomes committed after `Finish(...)` reaches sink
`Complete(...)` successfully.

## Main Types

```go
type Record struct {
    Partition   uint32
    LSN         uint64
    TimestampMS int64
    Value       []byte
}

type WriterOptions struct {
    Codec           Codec
    TargetBlockSize int
    MaxRecordSize   int
    UploadPartSize  int
    TempDir         string
}
```

Writer-side entry points:

```go
func Encode(records []Record, opts WriterOptions) ([]byte, Metadata, error)

func NewSegmentWriter(opts WriterOptions, sink SegmentSink) (*SegmentWriter, error)
func (w *SegmentWriter) Append(record Record) error
func (w *SegmentWriter) Finish(ctx context.Context) (CommitResult, error)
func (w *SegmentWriter) Abort(ctx context.Context) error
```

## Choose The Right API

Use `Encode(...)` when:

- you already have all records in memory
- you want the final segment bytes back as `[]byte`
- you are testing or generating fixture segments

Use `SegmentWriter` when:

- you want staged or multipart upload
- you want explicit commit / abort semantics
- you do not want to hold the final segment as one large heap buffer
- you are writing into a storage-specific sink

## Quick Start: `Encode(...)`

```go
records := []segmentio.Record{
    {Partition: 7, LSN: 101, TimestampMS: 1_777_000_000_000, Value: []byte("alpha")},
    {Partition: 7, LSN: 102, TimestampMS: 1_777_000_000_100, Value: []byte("beta")},
    {Partition: 7, LSN: 103, TimestampMS: 1_777_000_000_200, Value: []byte("gamma")},
}

segmentBytes, meta, err := segmentio.Encode(records, segmentio.WriterOptions{
    Codec: segmentio.CodecZstd,
})
if err != nil {
    panic(err)
}

fmt.Printf("size=%d base=%d last=%d blocks=%d\n",
    len(segmentBytes), meta.BaseLSN, meta.LastLSN, meta.BlockCount)
```

`Encode(...)` uses the same transactional writer path as `SegmentWriter`, but
with an internal in-memory sink.

## Transactional Writer Flow

Normal lifecycle:

1. Create the writer with `NewSegmentWriter(opts, sink)`.
2. Call `Append(...)` in partition order.
3. Call `Finish(ctx)` once.
4. If anything fails before `Finish(...)` returns success, call `Abort(ctx)`.

Practical notes:

- `Finish(...)` auto-aborts internal state if assembly or sink commit fails.
- `Abort(...)` is idempotent.
- After successful `Finish(...)`, the writer is closed and cannot accept more
  records.

## Minimal Sink Contract

`segmentio` does not currently ship an exported object-store sink. The expected
integration model is: your storage layer implements `SegmentSink`.

```go
type SegmentSink interface {
    Begin(ctx context.Context, plan AssemblyPlan) (SegmentTxn, error)
}

type SegmentTxn interface {
    UploadPart(ctx context.Context, part UploadPart) (PartReceipt, error)
    Complete(ctx context.Context, parts []PartReceipt) (CommittedObject, error)
    Abort(ctx context.Context) error
}
```

The important rule is:

- `UploadPart(...)` must fully consume `part.Body` before returning success

That matters because the writer may reuse the backing memory immediately after
`UploadPart(...)` returns.

## Example: Custom Sink

```go
type objectSink struct{}

func (s *objectSink) Begin(ctx context.Context, plan segmentio.AssemblyPlan) (segmentio.SegmentTxn, error) {
    _ = ctx
    fmt.Printf("begin upload size=%d base=%d last=%d\n",
        plan.TotalSize, plan.Metadata.BaseLSN, plan.Metadata.LastLSN)
    return &objectTxn{totalSize: plan.TotalSize}, nil
}

type objectTxn struct {
    totalSize uint64
}

func (t *objectTxn) UploadPart(ctx context.Context, part segmentio.UploadPart) (segmentio.PartReceipt, error) {
    _ = ctx

    n, err := io.Copy(io.Discard, part.Body)
    if err != nil {
        return segmentio.PartReceipt{}, err
    }
    if n != part.SizeBytes {
        return segmentio.PartReceipt{}, fmt.Errorf("short part read: got=%d want=%d", n, part.SizeBytes)
    }

    return segmentio.PartReceipt{
        PartNumber: part.PartNumber,
        ETag:       fmt.Sprintf("part-%d", part.PartNumber),
    }, nil
}

func (t *objectTxn) Complete(ctx context.Context, parts []segmentio.PartReceipt) (segmentio.CommittedObject, error) {
    _ = ctx
    _ = parts

    return segmentio.CommittedObject{
        ObjectID:  "segment-0001",
        SizeBytes: t.totalSize,
        ETag:      "final-etag",
    }, nil
}

func (t *objectTxn) Abort(ctx context.Context) error {
    _ = ctx
    return nil
}
```

## What `Finish(...)` Actually Does

At a high level, `Finish(...)` does this:

1. Flush the current raw block if records are pending.
2. Seal each block payload.
3. Assemble and validate the final segment layout.
4. Call `sink.Begin(...)` with the final `AssemblyPlan`.
5. Emit header, stored block bytes, block index, and footer as upload parts.
6. Call `txn.Complete(...)`.
7. Destroy temp spill files and mark the writer finished.

If any step fails:

- the sink transaction is aborted
- temporary payload state owned by the writer is destroyed
- `Finish(...)` returns an error

## `WriterOptions`

`Codec`

- `CodecNone`
- `CodecZstd`

`TargetBlockSize`

- preferred raw block cut size
- once the current block would exceed this size, the writer seals it and starts
  the next block

`MaxRecordSize`

- upper bound for one encoded record
- if zero, defaults to `DefaultMaxRecordSize`

`UploadPartSize`

- transport chunk size used by the emitter when talking to the sink
- this does not change the segment format

`TempDir`

- directory used for temp-file-backed sealed block payloads
- if empty, the process temp directory is used

## Size And Safety Limits

Version 1 currently enforces:

- `MaxBlockSize = 16 MiB`
- `MaxRecordValueLen = 4 MiB`
- `DefaultMaxRecordSize = MaxRecordValueLen + 12`

The `+12` is:

- 8 bytes `timestamp_ms`
- 4 bytes `value_len`

## Temp Spill Files

Sealed block payloads are stored in temp files before the final segment is
emitted into the sink.

Operationally:

- `Finish(...)` cleans them up on success
- `Abort(...)` cleans them up on failure or caller cancellation
- a process crash can still leave orphan spill files in `TempDir`

## Reader Compatibility

The writer emits the same v1 segment bytes that `segmentio.Open(...)` and
`segmentio.OpenBytes(...)` read.
