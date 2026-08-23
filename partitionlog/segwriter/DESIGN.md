# segwriter

`segwriter` writes one immutable partition segment using `segformat` for the
wire format and `segblock` for block sealing. It streams ordered segment bytes
into a sink and never holds the full segment object in memory.

`segwriter` does not own multipart scheduling, manifests, retention, partition
assignment, reader visibility, or segment roll policy. The caller decides when
a segment is closed and publishes the returned metadata to the catalog layer.

## Package Boundary

```text
segwriter -> segblock -> segformat
```

- `segformat`: fixed-width segment format, raw block layout, validation, hashes.
- `segblock`: compression and integrity for one block payload.
- `segwriter`: record validation, block buffering, ordered segment assembly,
  and commit metadata.

## Usage

```go
opts := segwriter.DefaultOptions(0)
opts.SegmentUUID = segmentUUID
opts.WriterTag = writerTag

w, err := segwriter.New(opts, sink)
if err != nil {
    return err
}
defer w.Abort(ctx) // no-op after successful Close

err = w.Append(ctx, segwriter.Record{
    LSN:         1000,
    TimestampMS: ts,
    Headers:     headers,
    Value:       payload,
})
if err != nil {
    return err
}

result, err := w.Close(ctx)
if err != nil {
    return err
}
```

`Writer` is single-owner: call `Append`, `Close`, and `Abort` from one
goroutine. Internal block sealing is concurrent. A sink may concurrently
upload multipart parts after it receives the ordered byte stream.

For in-memory encoding:

```go
bytes, meta, err := segwriter.Encode(ctx, records, opts)
```

## Options

```go
type Options struct {
    Partition uint32

    Codec    segformat.Codec
    HashAlgo segformat.HashAlgo

    TargetBlockSize   int
    PartSize          int
    SealParallelism   int
    BlockBufferCount  int
    UploadParallelism int
    UploadQueueSize   int
    UploadLimiter     UploadLimiter

    SegmentUUID   [16]byte
    WriterTag     [16]byte
    CreatedUnixMS int64
}
```

Defaults from `DefaultOptions(partition)`:

```text
Codec             = zstd
HashAlgo          = xxh64
TargetBlockSize   = 1 MiB
PartSize          = 8 MiB
SealParallelism   = min(runtime.NumCPU(), 4)
BlockBufferCount  = 2 * SealParallelism + 1
UploadParallelism = 2
UploadQueueSize   = UploadParallelism
```

If `SegmentUUID` is zero, `New` generates a random 16-byte segment identity.
If `CreatedUnixMS` is zero, `New` fills it with current UTC Unix milliseconds.
`WriterTag` is caller-owned and may remain zero if the deployment does not need
an operator-visible writer identity.

## Sink Contract

The sink API is an ordered byte transaction. Segment-format assembly does not
know where multipart boundaries fall. An in-memory sink can append the writes,
a file sink can stream them to a temporary file, and an object-store sink can
split them into provider parts behind this boundary.

```go
type Sink interface {
    Begin(ctx context.Context, plan Plan) (Txn, error)
}

type Txn interface {
    Write(ctx context.Context, bytes []byte) error
    Commit(ctx context.Context) (CommittedObject, error)
    Abort(ctx context.Context) error
}
```

`Txn.Write` calls are serialized and must consume or copy the complete slice
before returning. `Txn.Commit` must return a non-empty URI and the exact
committed object size. If it cannot establish whether the object became
durable, its error must match `ErrTxnCommitIndeterminate`. `Txn.Abort` must be
idempotent. All blocking sink calls
must return when their context is canceled. Contract violations make the
writer terminal and are reported through `ErrSinkContract`.

`PartSize`, upload parallelism, queue size, and the shared upload limiter are
transport hints carried in `Plan`. Object sinks use them to construct their
multipart stream; `segwriter` itself does not allocate part buffers.

`New` does not call `sink.Begin`. The sink transaction is opened lazily when the
first sealed block is emitted, which keeps segment rotation off the sink setup
latency path.

## Result

```go
type Result struct {
    Metadata Metadata
    Object   CommittedObject
    Trailer  segformat.Trailer
}
```

`Metadata` is the stable summary intended for manifest/catalog publication:
partition, LSN range, timestamp range, counts, object size, block index
location, segment identity, codec, hash algorithm, and hashes.

`Trailer` is the exact on-disk trailer, exposed for tests and low-level
debugging.

## Write Pipeline

```text
Append
  -> validate LSN and timestamp order
  -> append record into active raw block buffer
  -> when full, enqueue block buffer to seal workers

seal workers
  -> compress/hash raw block through segblock.SealOwned
  -> send sealed block to ordered emitter

ordered emitter
  -> emit blocks by sequence number
  -> lazily open packer/sink transaction
  -> write block preamble + stored bytes
  -> collect block index entries
  -> return raw block buffer to free pool

Close
  -> enqueue final block
  -> drain seal workers and emitter
  -> write index region
  -> compute segment hash
  -> write trailer
  -> complete sink transaction
```

The emitter is single-threaded, so segment byte order and block offsets remain
deterministic even when block sealing is parallel.

## Segment Byte Order

```text
file preamble
block preamble + stored block
block preamble + stored block
...
index preamble + block index entries
trailer
```

Sink parts are transport chunks, not format regions. A part may split a block
preamble, block payload, index region, or trailer. The sink commits parts in
part-number order.

## Memory And Backpressure

The writer uses a bounded ring of raw block buffers. A buffer belongs to exactly
one writer and one segment.

```text
peak memory ~= BlockBufferCount * TargetBlockSize
            + packer active/queued/uploading parts
            + small ordered-emitter pending map
```

Backpressure is intentional. If seal workers or uploads are slow, buffers stop
returning to the free pool and `Append` eventually blocks waiting for capacity.

`UploadLimiter` is optional and should be shared across writers when the caller
wants a process-level, backend-level, or bucket-level cap on concurrent part
uploads.

## Contexts

`Append(ctx)` uses `ctx` while enqueueing work or waiting for a free block
buffer. Work that has already been handed to seal workers, the emitter, or
upload workers runs on the writer lifetime context until `Close` or `Abort`.

`Close(ctx)` uses `ctx` for final enqueue, lazy `sink.Begin`, index/trailer
writes, final flush, and `Txn.Complete`. Already-enqueued upload workers use the
writer lifetime context. Canceling `ctx` makes the writer terminal, cancels that
lifetime context, drains the pipeline, and aborts the sink transaction.

`Abort(ctx)` cancels the writer lifetime context, drains internal workers, and
aborts the sink transaction if one exists.

## Failure Semantics

Any append, seal, upload, close, or validation error makes the writer terminal.
On terminal failure the writer drains internal workers and aborts the sink
transaction if it has been opened.

After terminal failure:

```text
Append -> ErrWriterAborted
Close  -> ErrWriterAborted or the first terminal error
Abort  -> nil
```

`Abort` is idempotent. Calling `Abort` after a successful `Close` is a no-op.

## Validation

The package test suite covers lazy sink creation, block and part splitting,
parallel sealing order, out-of-order upload completion, bounded queue
backpressure, shared upload limiting, cancellation at queue/begin/complete
boundaries, upload failure cleanup, terminal states, and goroutine drainage.

Sink results are treated as untrusted boundary data. The packer verifies that
every receipt identifies the part that produced it and that `Complete` returns
a non-empty URI with the exact accepted byte count.

Run the deterministic pipeline soak under the race detector with:

```text
make stress-write
```

`STRESSCOUNT` controls the number of complete writer lifecycles and defaults to
`100`.

## Roll Policy

`segwriter` closes only when the caller calls `Close`. Higher layers normally
roll segments on target segment size, segment age, shutdown, explicit flush, or
writer fence/epoch changes.
