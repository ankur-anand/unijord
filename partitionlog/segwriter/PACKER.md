# segwriter packer

The packer turns an ordered stream of segment bytes into multipart upload
parts.

It is internal to `segwriter`.

## Purpose

The writer produces final segment bytes in order:

```text
file preamble
block preamble + stored block
block preamble + stored block
...
index preamble + index entries
trailer
```

The sink wants multipart upload parts:

```text
part 1
part 2
...
part N
```

The packer is the boundary between those two models.

Multipart parts are not format regions. A part may split anywhere in the final
byte stream.

## API

```go
type packerOptions struct {
    PartSize          int
    UploadParallelism int
    UploadQueueSize   int
    HashAlgo          segformat.HashAlgo
    UploadLimiter     UploadLimiter
}

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

## Sink Contract

```go
type Txn interface {
    UploadPart(ctx context.Context, part Part) (PartReceipt, error)
    Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error)
    Abort(ctx context.Context) error
}

type Part struct {
    Number int
    Bytes  []byte
}

type PartReceipt struct {
    Number int
    Token  string
}
```

`Txn.UploadPart` must be concurrency-safe.

`Txn.UploadPart` must fully consume or copy `part.Bytes` before returning.
The caller transfers ownership of the byte slice to the upload worker until
`UploadPart` returns.

## State

The packer owns:

```text
part buffer
current byte offset
next part number
upload queue
upload workers
part receipts
first error
segment hasher
terminal state
```

The writer owns:

```text
format bytes
block index entries
trailer construction
```

## Write Semantics

### `WriteBody`

```go
func (p *packer) WriteBody(ctx context.Context, b []byte) error
```

Effects:

- appends `b` to the final object stream
- advances `Offset()` by `len(b)`
- updates `segment_hash`
- flushes full parts to upload workers
- returns any prior upload error

### `WriteFinal`

```go
func (p *packer) WriteFinal(ctx context.Context, b []byte) error
```

Effects:

- appends `b` to the final object stream
- advances `Offset()` by `len(b)`
- does not update `segment_hash`
- flushes full parts to upload workers
- returns any prior upload error

`WriteFinal` may only be called after `BodyHash`.

### `BodyHash`

```go
func (p *packer) BodyHash() uint64
```

Returns the hash over bytes written through `WriteBody`.

The trailer is outside the segment hash.

## Part Splitting

The packer appends bytes into an active part buffer.

```text
if active buffer reaches PartSize:
  flush non-final part
```

Non-final parts are exactly `PartSize` bytes.

The final part is flushed by `Complete` and may be smaller.

Example:

```text
WriteBody("abc")
WriteBody("defgh")
PartSize = 4

part 1 = "abcd"
part 2 = "efgh"
```

The packer does not care whether `abcd` splits a block preamble or payload.
It is only preserving byte order.

## Ownership Transfer

When flushing a part:

```go
partBytes := p.buf
p.buf = make([]byte, 0, p.partSize)
```

The worker owns `partBytes` until `Txn.UploadPart` returns.

The packer must never mutate `partBytes` after enqueue.

This avoids copying full part buffers before upload.

## Async Upload

The packer starts `UploadParallelism` workers.

```text
packer write path
  -> enqueue part
  -> worker uploads part
  -> worker sends result
  -> packer collects receipt or first error
```

Part numbers are assigned in byte order.

Uploads may finish out of order.

`Complete` sorts receipts by part number before calling `Txn.Complete`.

## Upload Limiter

`UploadParallelism` is local to one packer.

`UploadLimiter` is shared across packers when the partition writer wants a
process-level or bucket-level cap.

```go
type UploadLimiter interface {
    Acquire(ctx context.Context) error
    Release()
}
```

Workers acquire the limiter immediately before `Txn.UploadPart` and release it
after `UploadPart` returns.

This gives two knobs:

```text
UploadParallelism = max upload goroutines for this segment
UploadLimiter     = max simultaneous object-store uploads across a wider scope
```

Example:

```text
16 active partitions
UploadParallelism = 4 per partition
shared limiter = 32
```

Without the limiter the process can issue up to 64 concurrent upload requests.
With the limiter it never exceeds 32, while each hot partition can still use up
to 4 slots when capacity is available.

## Backpressure

The part queue is bounded.

```text
UploadQueueSize default = UploadParallelism
```

Maximum part-buffer memory is approximately:

```text
(1 active buffer + UploadQueueSize queued + UploadParallelism uploading) * PartSize
```

Default:

```text
PartSize = 8 MiB
UploadParallelism = 2
UploadQueueSize = 2
Max part memory ~= 40 MiB
```

## Error Handling

The first upload error wins.

After first error:

- `Err()` returns that error
- future writes return that error
- `Complete` returns that error
- writer should call `Abort`

The packer must poll upload results during writes so background upload failure
does not remain hidden until `Complete`.

## Complete

```text
Complete
  -> flush active buffer, if non-empty
  -> close upload queue
  -> wait for all workers/results
  -> if any upload failed: return first error
  -> sort receipts by part number
  -> txn.Complete(receipts)
```

If no bytes were written, `Complete` returns an error. A valid segment is never
empty.

## Abort

```text
Abort
  -> cancel worker context
  -> stop accepting writes
  -> close upload queue if not already closed
  -> call txn.Abort
```

Abort is idempotent.

`Abort` may race with in-flight uploads. Sinks must tolerate abort while parts
are in flight, which matches object-store multipart semantics.

## Terminal States

After `Complete` succeeds:

- future writes fail
- future `Complete` fails
- `Abort` is a no-op

After `Abort`:

- future writes fail
- future `Complete` fails
- repeated `Abort` succeeds

## Invariants

- `Offset()` equals total bytes accepted by the packer.
- part numbers are contiguous starting at `1`.
- non-final parts have `len(Bytes) == PartSize`.
- receipts passed to `Txn.Complete` are sorted by part number.
- `BodyHash()` excludes bytes written by `WriteFinal`.
- a part byte slice is never mutated after enqueue.

## Tests

Required tests:

- one small write produces one final part
- large write produces multiple parts
- multiple small writes split correctly across part boundaries
- `Offset` advances correctly
- `WriteBody` changes body hash
- `WriteFinal` does not change body hash
- uploads may complete out of order and receipts are sorted
- upload error is returned by later writes or `Complete`
- `Abort` is idempotent
- writes after `Complete` fail
- writes after `Abort` fail
- non-final part size equals `PartSize`
- upload limiter bounds concurrent `UploadPart` calls

## Non-goals

- No segment-format knowledge.
- No block sealing.
- No index construction.
- No manifest publishing.
- No retry policy in the first implementation. Retries belong in sink
  implementations or a later wrapper.
