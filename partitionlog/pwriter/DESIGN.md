# partitionlog/pwriter Design

`pwriter` is the per-partition append state machine above `segwriter`.

It owns:

- contiguous LSN assignment from the current partition head;
- timestamp monotonicity across segment rolls;
- one active `segwriter.Writer` at a time;
- publication of committed segment metadata through a small session boundary.

It does not own fence acquisition, catalog reads, retention, or read-side
segment lookup.

## Scope

`pwriter` is responsible for:

- accepting records in append order for one partition;
- deciding when to roll the active segment;
- creating new segment sinks and `segwriter.Writer` instances;
- converting a committed segment into `pmeta.SegmentRef`;
- publishing that segment and installing the returned partition snapshot;
- becoming terminal on hard failure.

`pwriter` is not responsible for:

- segment encoding, block layout, compression, or hashing;
- multipart implementation;
- durable catalog storage layout;
- writer-fence acquisition;
- orphan cleanup;
- retention;
- reader APIs.

## Shared Metadata

Shared partition metadata lives in `partitionlog/pmeta`:

- `pmeta.PartitionHead`
- `pmeta.SegmentRef`
- `pmeta.SegmentPage`

`pwriter` consumes `PartitionHead` and produces `SegmentRef`. It does not define
its own metadata model.

## Public API

```go
type WriterIdentity struct {
    Epoch uint64
    Tag   [16]byte
}

type Snapshot struct {
    Head     pmeta.PartitionHead
    Identity WriterIdentity
}

type PublishRequest struct {
    ExpectedNextLSN uint64
    Segment         pmeta.SegmentRef
}

type Session interface {
    Snapshot() Snapshot
    PublishSegment(ctx context.Context, req PublishRequest) (Snapshot, error)
}

type RollPolicy struct {
    MaxSegmentRecords  uint32
    MaxSegmentRawBytes uint64
}

type Options struct {
    Session        Session
    SinkFactory    SinkFactory
    SegmentOptions segwriter.Options
    Roll           RollPolicy
    Clock          func() time.Time
    UUIDGen        UUIDGen
}

type Record struct {
    TimestampMS int64
    Value       []byte
}

type AppendResult struct {
    LSN   uint64
    Flush *FlushResult
}

type FlushResult struct {
    Snapshot Snapshot
    Segment  pmeta.SegmentRef
}
```

Writer methods:

```go
func DefaultOptions(factory SinkFactory) Options
func New(opts Options) (*Writer, error)

func (w *Writer) Append(ctx context.Context, r Record) (AppendResult, error)
func (w *Writer) Flush(ctx context.Context) (*FlushResult, error)
func (w *Writer) Close(ctx context.Context) (*FlushResult, error)
func (w *Writer) Abort(ctx context.Context) error

func (w *Writer) State() Snapshot
func (w *Writer) Err() error
```

Notes:

- `Writer` is not safe for concurrent use.
- `Err()` returns the first terminal cause for diagnostics.
- `Flush()` and `Close()` return `nil, nil` when there is no active segment to
  publish.

## Session Contract

`Session` is the only metadata-facing dependency of `pwriter`.

`Session.Snapshot()` provides the current bounded hot head and the writer
identity to use for emitted segments.

`Session.PublishSegment(...)` is responsible for:

- validating writer ownership;
- validating expected head position;
- applying any backend CAS or compare-and-swap logic;
- returning the new authoritative `Snapshot`.

`pwriter` treats the session as authoritative for publication, but still
validates the returned snapshot before installing it.

## Sink Contract

`SinkFactory` creates the segment sink for one new segment:

```go
type SegmentInfo struct {
    Partition     uint32
    BaseLSN       uint64
    WriterEpoch   uint64
    WriterTag     [16]byte
    SegmentUUID   [16]byte
    CreatedUnixMS int64
}
```

`pwriter` supplies `SegmentInfo`; the sink factory owns object naming and
storage layout.

## State Model

`Writer` has four states:

- open, no active segment
- open, active segment
- closed
- aborted

Transitions:

- `New` starts in open, no active segment.
- first successful `Append` lazily creates the active segment.
- successful roll or `Flush` returns to open, no active segment.
- successful `Close` transitions to closed.
- `Abort` transitions to aborted.
- any hard failure transitions to aborted.

After `closed`:

- `Append`, `Flush`, and `Close` return `ErrClosed`.

After `aborted`:

- `Append`, `Flush`, and `Close` return `ErrAborted`.

## Initialization

`New(opts)` reads the starting `Snapshot` from `opts.Session`.

Initial runtime state is derived from the snapshot:

- `nextLSN = snapshot.Head.NextLSN`
- if `snapshot.Head.HasLastSegment`, then
  `lastTimestamp = snapshot.Head.LastSegment.MaxTimestampMS`

Validation at open time includes:

- non-nil session;
- non-nil sink factory;
- valid snapshot shape;
- identity epoch must equal `snapshot.Head.WriterEpoch`.

If the identity epoch is behind the head epoch, `New` returns
`ErrStaleWriter`.

## Append Flow

`Append(ctx, record)` performs:

1. reject `closed` or `aborted` state;
2. reject LSN exhaustion (`ErrLSNExhausted`);
3. reject timestamp regression (`ErrTimestampOrder`);
4. roll before append if the next record would breach the roll policy;
5. lazily start a new active segment if needed;
6. append the record to the active `segwriter.Writer`;
7. advance local LSN and timestamp state;
8. roll after append if the segment now meets the roll policy.

Returned LSN is the assigned partition LSN for that record.

If a roll happens inside `Append`, `AppendResult.Flush` is non-nil and contains
the published segment and new snapshot.

## Flush and Close

`Flush(ctx)`:

- closes and publishes the active segment if one exists and has records;
- returns the resulting `FlushResult`;
- returns `nil, nil` if there is no active segment.

`Close(ctx)`:

- performs the same work as `Flush`;
- marks the writer closed on success.

If the active segment exists but has zero records, `pwriter` aborts that
segment writer and treats the flush as a no-op.

## Abort

`Abort(ctx)` is idempotent.

It:

- marks the writer aborted;
- records `ErrAborted` as the terminal cause if no earlier terminal cause
  exists;
- aborts the active segment writer if present.

If sink cleanup fails, `Abort` returns that cleanup error, but the writer
remains aborted.

## Segment Lifecycle

When starting a new segment, `pwriter`:

1. generates a segment UUID;
2. captures `CreatedUnixMS`;
3. builds `SegmentInfo`;
4. asks `SinkFactory` for a `segwriter.Sink`;
5. creates `segwriter.New(...)` using `SegmentOptions` plus:
   - partition
   - segment UUID
   - writer tag
   - created timestamp

When closing a segment, `pwriter` converts `segwriter.Result` into
`pmeta.SegmentRef`, carrying:

- object URI;
- partition;
- writer epoch and writer tag from the current identity;
- LSN range;
- timestamp range;
- record and block counts;
- size, index offsets, codec, hashes.

The resulting `SegmentRef` is validated before publication.

## Publish Flow

After a segment is committed to object storage, `pwriter` calls:

```go
session.PublishSegment(ctx, PublishRequest{
    ExpectedNextLSN: active.baseLSN,
    Segment:         segment,
})
```

On success, `pwriter` validates the returned snapshot:

- same partition as the current writer;
- same `WriterIdentity` as before publish;
- `Head.WriterEpoch == Identity.Epoch`;
- `Head.HasLastSegment == true`;
- `Head.LastSegment == published segment`;
- `Head.NextLSN == publishedSegment.NextLSN()`.

If any of those checks fail, `pwriter` returns `ErrInvalidPublishResult` and
becomes terminal.

## Roll Policy

`pwriter` has one built-in roll policy:

- `MaxSegmentRecords`
- `MaxSegmentRawBytes`

Both are optional; zero means "use default" during option normalization.

Roll evaluation happens in two places:

- before append, if the next record would breach `MaxSegmentRawBytes`;
- after append, if the active segment now meets either threshold.

This keeps the active segment bounded without needing external readback.

## Invariants

`pwriter` maintains these invariants:

- one writer instance owns exactly one partition;
- emitted record LSNs are contiguous;
- timestamps are non-decreasing across the writer lifetime;
- at most one active segment writer exists at a time;
- published segments are appended in head order only;
- any hard failure makes the writer terminal.

## Error Contract

Open / configuration errors:

- `ErrInvalidOptions`
- `ErrInvalidSession`
- `ErrStaleWriter`

Runtime validation errors:

- `ErrTimestampOrder`
- `ErrLSNExhausted`

Segment write path:

- `ErrSegmentWriteFailed`

Publish path:

- `ErrStaleWriter`
- `ErrPublishFailed`
- `ErrPublishIndeterminate`
- `ErrInvalidPublishResult`

Terminal state:

- `ErrClosed`
- `ErrAborted`

Rules:

- the first terminal call returns the real cause;
- later calls return `ErrAborted` or `ErrClosed`;
- `Err()` exposes the first terminal cause.

## Defaults

`DefaultOptions(factory)` sets:

- `SinkFactory = factory`
- `Roll.MaxSegmentRecords = DefaultMaxSegmentRecords`
- `Roll.MaxSegmentRawBytes = DefaultMaxSegmentRawBytes`
- `Clock = time.Now().UTC`
- `UUIDGen = random UUID generator`

If `SegmentOptions` is zero-valued, `pwriter` fills it with
`segwriter.DefaultOptions(partition)` at construction time.

## Operational Notes

Object commit and metadata publish are not one transaction.

That means this failure mode is expected:

1. segment object commit succeeds;
2. `Session.PublishSegment(...)` fails;
3. writer becomes terminal;
4. committed object is now an orphan candidate until external cleanup decides
   whether it is reachable or safe to delete.

`pwriter` does not resolve or clean up orphans. That is a responsibility of the
metadata / storage layer above it.
