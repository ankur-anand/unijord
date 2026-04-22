# segwriter packer

The packer is an internal `segwriter` component that converts an ordered stream
of segment bytes into ordered sink parts.

It has no segment-format knowledge. The writer decides which bytes to emit; the
packer preserves byte order, computes the segment-body hash, splits bytes into
parts, sends full parts asynchronously, and completes or aborts the sink
transaction.

## Byte Model

The writer emits this ordered stream:

```text
file preamble
block preamble + stored block
block preamble + stored block
...
index preamble + index entries
trailer
```

The packer turns that stream into numbered parts:

```text
part 1
part 2
...
part N
```

Part boundaries are independent from segment-format boundaries. A part may cut
through any format region.

## API Shape

```go
func newPacker(ctx context.Context, txn Txn, opts packerOptions) (*packer, error)

func (p *packer) Offset() uint64
func (p *packer) WriteBody(ctx context.Context, b []byte) error
func (p *packer) BodyHash() uint64
func (p *packer) WriteFinal(ctx context.Context, b []byte) error
func (p *packer) Complete(ctx context.Context) (CommittedObject, error)
func (p *packer) Abort(ctx context.Context) error
```

`WriteBody` appends bytes to the object and includes them in the segment-body
hash. `WriteFinal` appends bytes without hashing them. `BodyHash` seals the
body hash and must be called before `WriteFinal` or `Complete`.

The segment trailer is written through `WriteFinal`, so it is outside
`segment_hash`.

## Sink Contract

```go
type Txn interface {
    UploadPart(ctx context.Context, part Part) (PartReceipt, error)
    Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error)
    Abort(ctx context.Context) error
}
```

`UploadPart` must be concurrency-safe and must fully consume or copy
`part.Bytes` before returning. `Complete` receives sorted contiguous receipts.
`Abort` must be idempotent.

## Part Splitting

The packer appends writes into an active part buffer. When the buffer reaches
`PartSize`, it transfers that buffer to an upload worker and starts a new
buffer.

```text
WriteBody("abc")
WriteBody("defgh")
PartSize = 4

part 1 = "abcd"
part 2 = "efgh"
```

Non-final parts are exactly `PartSize` bytes. The final part is flushed by
`Complete` and may be smaller.

## Ownership

When a part is flushed, ownership of the byte slice transfers to an upload
worker:

```go
part := Part{Number: n, Bytes: p.buf}
p.buf = nil
```

The packer never mutates the part bytes after enqueueing. The sink owns the
bytes for the duration of `UploadPart`; after `UploadPart` returns, ownership
ends.

## Async Upload

The packer starts `UploadParallelism` workers. Full parts are sent to the sink
while the writer continues producing later bytes.

```text
writer goroutine
  -> WriteBody / WriteFinal
  -> enqueue full part

upload workers
  -> Txn.UploadPart concurrently
  -> return receipts or first error

Complete
  -> flush final part
  -> wait for uploads
  -> sort receipts
  -> Txn.Complete
```

Part numbers are assigned in byte order. Uploads may finish out of order; the
packer sorts receipts before completion.

## Contexts

The context passed to `WriteBody`, `WriteFinal`, or `Complete` controls the
caller while it is enqueueing a part or waiting for completion.

Once a part has been accepted by the upload queue, the upload worker uses the
packer lifetime context created by `newPacker`. Canceling a single write call
does not cancel an already-enqueued upload. `Abort` cancels the packer lifetime
context and calls `Txn.Abort`.

## Backpressure

The upload queue is bounded by `UploadQueueSize`. If uploads are slower than
writes and the queue fills, `WriteBody` / `WriteFinal` block until capacity is
available, an upload fails, or the caller context is canceled.

Approximate part-buffer memory:

```text
(1 active buffer + UploadQueueSize queued + UploadParallelism uploading) * PartSize
```

`UploadLimiter` is optional and sits below `UploadParallelism`. Use it to share
a broader concurrency cap across many packers.

## Error Semantics

The first upload or sink error wins. After the first error:

- future writes return that error;
- `Complete` returns that error;
- the writer should call `Abort`.

`Complete` returns an error for an empty object. `Abort` is idempotent. After a
successful `Complete`, future writes and completes fail, while `Abort` is a
no-op.

## Invariants

- `Offset()` equals total accepted object bytes.
- Part numbers are contiguous starting at `1`.
- Non-final parts have `len(Bytes) == PartSize`.
- Receipts passed to `Txn.Complete` are sorted by part number.
- `BodyHash()` excludes bytes written through `WriteFinal`.
- Part byte slices are never mutated after enqueue.
