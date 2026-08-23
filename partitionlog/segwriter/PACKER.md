# segwriter packer

The packer is the small boundary between segment-format assembly and a sink's
ordered byte transaction. It has no multipart or provider knowledge.

## Byte model

The writer emits one ordered stream:

```text
file preamble
block preamble + stored block
block preamble + stored block
...
index preamble + index entries
trailer
```

The packer forwards those byte slices to `Txn.Write` in exactly that order. It
tracks the accepted offset and computes the segment-body hash. The trailer is
written through `WriteFinal`, so it is intentionally excluded from
`segment_hash`.

```go
func newPacker(txn Txn, hashAlgo segformat.HashAlgo) (*packer, error)

func (p *packer) Offset() uint64
func (p *packer) WriteBody(ctx context.Context, b []byte) error
func (p *packer) BodyHash() uint64
func (p *packer) WriteFinal(ctx context.Context, b []byte) error
func (p *packer) Complete(ctx context.Context) (CommittedObject, error)
func (p *packer) Abort(ctx context.Context) error
```

## Sink contract

```go
type Txn interface {
    Write(ctx context.Context, bytes []byte) error
    Commit(ctx context.Context) (CommittedObject, error)
    Abort(ctx context.Context) error
}
```

`Write` calls are serialized. A successful call must consume or copy the whole
slice before returning. `Commit` must return a non-empty URI and the exact
accepted byte size. `Abort` stops the transaction and cleans staging work.

The packer rejects an invalid committed URI or size with `ErrSinkContract` and
aborts the transaction.

## Multipart ownership

Part splitting, payload buffer pooling, upload queues, concurrent provider
requests, receipt ordering, checksums, commit reconciliation, and staging
cleanup are deliberately outside `segwriter`.

For object-backed segments, `partitionlog/blob/sink` adapts this ordered byte
transaction to `blob/sink/stream`, which owns those multipart concerns. Other
sinks may implement the ordered transaction directly; the in-memory sink simply
appends each write.

This separation keeps segment-format assembly independent from object-store
limits and prevents two layers from buffering and scheduling the same bytes.

## Invariants

- `Offset()` equals the bytes accepted by successful `Txn.Write` calls.
- `Txn.Write` observes exactly the segment format's byte order.
- `BodyHash()` excludes bytes written through `WriteFinal`.
- `Complete` is rejected before `BodyHash` or for an empty object.
- After the first write failure, later writes and completion return that error.
