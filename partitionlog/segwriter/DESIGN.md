# segwriter

`segwriter` writes one immutable partition segment using `segformat` and
`segblock`.

The writer streams final segment bytes into a multipart-shaped sink. It does
not keep the full segment object in memory.

## Package Boundaries

```text
segwriter -> segblock -> segformat
```

Responsibilities:

- `segformat`: byte layout, fixed-width structs, raw block format, hashes
- `segblock`: seal/open one block payload
- `segwriter`: collect records, build blocks, pack bytes, commit object

`segwriter` does not own manifests, retention, partition assignment, or reader
visibility.

## User API

```go
sink := s3sink.New(client, bucket, key)

w, err := segwriter.New(segwriter.Options{
    Partition:         0,
    Codec:             segformat.CodecZstd,
    HashAlgo:          segformat.HashXXH64,
    TargetBlockSize:   1 << 20,
    PartSize:          8 << 20,
    UploadParallelism: 2,
    SegmentUUID:       segmentUUID,
    WriterTag:         writerTag,
    CreatedUnixMS:     now,
}, sink)

err = w.Append(ctx, segwriter.Record{
    LSN:         1000,
    TimestampMS: ts,
    Value:       payload,
})

result, err := w.Close(ctx)
```

Convenience helper for tests and small local use:

```go
bytes, meta, err := segwriter.Encode(records, opts)
```

## Public Types

```go
type Options struct {
    Partition uint32

    Codec    segformat.Codec
    HashAlgo segformat.HashAlgo

    TargetBlockSize   int
    PartSize          int
    UploadParallelism int
    UploadQueueSize   int
    UploadLimiter     UploadLimiter

    SegmentUUID   [16]byte
    WriterTag     [16]byte
    CreatedUnixMS int64
}

type Record struct {
    LSN         uint64
    TimestampMS int64
    Value       []byte
}

type Result struct {
    Metadata Metadata
    Object   CommittedObject
}

type Metadata struct {
    Partition      uint32
    BaseLSN        uint64
    LastLSN        uint64
    MinTimestampMS int64
    MaxTimestampMS int64
    RecordCount    uint32
    BlockCount     uint32
    SizeBytes      uint64
    SegmentUUID    [16]byte
    Codec          segformat.Codec
    HashAlgo       segformat.HashAlgo
}
```

Defaults:

```text
TargetBlockSize   = 1 MiB
PartSize          = 8 MiB
UploadParallelism = 2
UploadQueueSize   = UploadParallelism
```

Use `DefaultOptions(partition)` when the desired defaults are zstd + xxh64.
The raw zero value of `Options` still maps to the format's zero-value codec and
hash algorithm (`none` + `crc32c`).

## Sink API

The sink is multipart-shaped because the production target is object storage.
Memory and file sinks can implement the same contract for tests.

```go
type Sink interface {
    Begin(ctx context.Context, plan Plan) (Txn, error)
}

type Txn interface {
    UploadPart(ctx context.Context, part Part) (PartReceipt, error)
    Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error)
    Abort(ctx context.Context) error
}

type Plan struct {
    Partition uint32
    Codec     segformat.Codec
    HashAlgo  segformat.HashAlgo
    PartSize  int
}

type Part struct {
    Number int
    Bytes  []byte
}

type PartReceipt struct {
    Number int
    Token  string
}

type CommittedObject struct {
    URI       string
    SizeBytes uint64
    Token     string
}
```

`Txn.UploadPart` must be safe for concurrent calls. It must consume or copy
`part.Bytes` before returning, because ownership returns to the caller after
the call completes.

## Internal Packer

The packer is internal to `segwriter`. The writer sees it as a byte-stream
builder for the final segment object.

```go
type packer struct { /* internal */ }

func newPacker(ctx context.Context, txn Txn, opts packerOptions) (*packer, error)

func (p *packer) Offset() uint64
func (p *packer) WriteBody(ctx context.Context, b []byte) error
func (p *packer) BodyHash() uint64
func (p *packer) WriteFinal(ctx context.Context, b []byte) error
func (p *packer) Complete(ctx context.Context) (CommittedObject, error)
func (p *packer) Abort(ctx context.Context) error
func (p *packer) Err() error
```

Rules:

- `WriteBody` appends bytes and updates `segment_hash`.
- `WriteFinal` appends bytes and does not update `segment_hash`.
- `BodyHash` is called after the index region is written and before the
  trailer is written.
- `Offset` is the current byte offset in the final object.
- Full non-final parts are uploaded asynchronously.
- `Complete` flushes the final part, waits for uploads, sorts receipts by part
  number, and calls `Txn.Complete`.
- `Abort` is idempotent and calls `Txn.Abort`.

## Packer Ownership Model

When a part is flushed:

```go
partBytes := p.buf
p.buf = make([]byte, 0, p.partSize)
```

Ownership of `partBytes` transfers to an upload worker. The packer must never
mutate those bytes again.

This keeps the implementation safe while allowing concurrent upload.

## Async Upload Model

```text
writer goroutine
  -> packer.WriteBody / WriteFinal
  -> packer fills part buffer
  -> full part sent to upload queue
  -> workers call Txn.UploadPart concurrently
  -> receipts collected by part number
  -> Complete sorts receipts and finalizes object
```

Part numbers are assigned in byte order. Uploads may finish out of order.
`Txn.Complete` always receives receipts sorted by part number.

Backpressure comes from a bounded upload queue:

```text
memory ~= active part buffer
        + queued parts
        + uploading parts
```

Default:

```text
PartSize = 8 MiB
UploadParallelism = 2
UploadQueueSize = 2
Approx max part memory = (1 + 2 + 2) * 8 MiB = 40 MiB
```

`UploadLimiter` is separate from `UploadParallelism`.

```text
UploadParallelism = per-segment worker count
UploadLimiter     = shared concurrency cap across packers
```

The partition writer should pass the same limiter to all active segment writers
when it wants a process-level or bucket-level bound on object-store upload
pressure.

## Lazy Sink Begin

`New` must not touch object storage.

Segment rotation happens on the hot append path. If `New` calls
`sink.Begin`, rotation inherits object-store latency from multipart-init or
other sink setup. In a high-throughput partition that can stall appends for
tens or hundreds of milliseconds.

The writer is created in memory first. The sink transaction and packer are
opened lazily when the first block is flushed or when `Close` needs to commit a
small segment.

```text
New
  -> validate options
  -> initialize in-memory writer state
  -> no sink.Begin
  -> no packer
  -> no object-store call

ensurePacker
  -> if packer already exists: return
  -> sink.Begin
  -> new packer
  -> write file preamble through WriteBody
```

This lets the partition layer rotate cheaply:

```text
old := active
active = new in-memory segment writer
go old.Close(ctx)
```

Appends continue into the new active writer while the old segment closes,
uploads final bytes, and publishes metadata through higher layers.

## Writer Flow

```text
New
  -> validate options
  -> initialize in-memory writer state
  -> no sink.Begin yet

Append(record)
  -> validate writer state
  -> validate LSN continuity
  -> validate timestamp monotonicity
  -> flush current block if record would exceed target block size
  -> append record to current block builder

flushBlock
  -> segformat.EncodeRawBlock(current records)
  -> segblock.Seal(codec, hash, raw, meta)
  -> ensurePacker
  -> blockOffset = packer.Offset()
  -> marshal block preamble
  -> packer.WriteBody(block preamble)
  -> packer.WriteBody(stored block bytes)
  -> append block index entry
  -> discard raw and stored bytes
  -> reset block builder

Close
  -> if no records were appended: return empty segment error
  -> flush current block
  -> indexOffset = packer.Offset()
  -> build trailer summary fields except hashes
  -> marshal index region from index entries
  -> packer.WriteBody(index region)
  -> segmentHash = packer.BodyHash()
  -> build trailer with segmentHash
  -> marshal trailer
  -> packer.WriteFinal(trailer)
  -> packer.Complete
  -> return Result
```

## Segment Byte Order

The writer emits bytes in exactly this order:

```text
file preamble
block preamble + stored block
block preamble + stored block
...
index preamble + block index entries
trailer
```

Multipart parts are not format regions. A part may cut across any byte
boundary. Object storage reassembles parts by part number.

## Block Builder

The writer has an internal block builder.

It tracks:

```text
base_lsn
record_count
min_timestamp_ms
max_timestamp_ms
raw_size
records
```

It is not public initially. If another package needs it later, it can be
extracted.

## Roll Conditions

The writer closes one segment when the caller calls `Close`.

The higher-level partition writer decides when to call `Close`, typically:

```text
target segment size reached
max segment age reached
max record count reached
shutdown
explicit flush
writer fence/epoch changed
```

`segwriter` itself only enforces block sizing and segment format limits.

## Failure Semantics

Any append/flush/close error makes the writer terminal.

```text
error during Append flush -> Abort transaction -> writer aborted
error during Close        -> Abort transaction -> writer aborted
upload part error         -> Abort transaction -> writer aborted
```

After abort:

```text
Append -> ErrWriterAborted
Close  -> ErrWriterAborted
Abort  -> nil
```

`Abort` is idempotent.

## First Implementation Slice

Implement before full writer:

```text
sink.go
memory_sink.go
packer.go
writer.go
packer_test.go
writer_test.go
benchmark_test.go
```

This slice proves:

- part splitting
- async upload
- receipt sorting
- offset tracking
- segment hash excludes trailer
- abort behavior
- upload error propagation
- upload limiter bounds concurrent uploads
- byte-level e2e validation through `segformat` + `segblock`

The first full writer implementation is intentionally serial for block sealing.
Parallel block sealing can be added later without changing the format or sink
contract.
