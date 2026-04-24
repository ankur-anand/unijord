# partitionlog/writer Design

`partitionlog/writer` is the public per-partition append package.

It accepts records for one partition, assigns contiguous LSNs, writes segment
objects through `segwriter`, and publishes segment metadata through a
`Session`.

## Scope

`writer` owns:

- one append stream for one partition;
- contiguous LSN assignment;
- timestamp monotonicity across the writer lifetime;
- one active segment accepting appends;
- time- or policy-driven segment cutting via active-writer swap;
- bounded in-flight segment state;
- ordered metadata publication;
- drain semantics for `Flush()` and `Close()`;
- terminal failure handling.

`writer` does not own:

- segment encoding, block compression, or multipart upload details;
- catalog storage layout or CAS token format;
- fence acquisition;
- retention or orphan cleanup;
- reader APIs;
- crash recovery.

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

type State struct {
    Snapshot          Snapshot
    OptimisticNextLSN uint64
    InflightSegments  int
    InflightBytes     uint64
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

type QueuePolicy struct {
    MaxInflightSegments int
    MaxInflightBytes    uint64
}

type Options struct {
    Session        Session
    SinkFactory    SinkFactory
    SegmentOptions segwriter.Options
    Roll           RollPolicy
    Queue          QueuePolicy
    Clock          func() time.Time
    UUIDGen        UUIDGen
}

type Record struct {
    TimestampMS int64
    Value       []byte
}

type AppendResult struct {
    LSN uint64
}

func DefaultOptions(factory SinkFactory) Options
func New(opts Options) (*Writer, error)

func (w *Writer) Append(ctx context.Context, r Record) (AppendResult, error)
func (w *Writer) Cut(ctx context.Context) error
func (w *Writer) Flush(ctx context.Context) (Snapshot, error)
func (w *Writer) Close(ctx context.Context) (Snapshot, error)
func (w *Writer) Abort(ctx context.Context) error

func (w *Writer) State() State
func (w *Writer) Err() error
```

`Writer` is not safe for concurrent use. The intended runtime model is one
serialized owner per partition, typically a partition actor or mailbox.

## Session Contract

`Session` is the metadata-facing dependency.

`Session.Snapshot()` returns the starting committed head and writer identity.

`Session.PublishSegment(...)` must:

- validate ownership and expected head position;
- publish exactly one segment for the provided `ExpectedNextLSN`;
- return the new committed snapshot on success.

`writer` validates the returned snapshot before installing it.

## Sink Contract

`SinkFactory` creates one `segwriter.Sink` per segment from:

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

The sink factory owns storage key layout and provider-specific behavior.

## State Model

`writer` tracks two positions:

- committed state: the latest snapshot returned by successful publish;
- optimistic state: the next LSN that will be assigned locally.

These may differ while cut segments are still being finalized or published.

`State()` returns:

- the current committed snapshot;
- the optimistic next LSN;
- the count and bytes of in-flight segments.

## Internal Pipeline

`writer` manages three internal classes of segment state:

### Active

The active segment receives new appends.

There is at most one active segment at a time.

### Detached

A detached segment has been removed from the append path by `Cut()`, but has
not yet been finalized into a committed segment object.

Detached segments still own a live `segwriter.Writer`.

### Ready

A ready segment has been finalized into a `pmeta.SegmentRef` and is waiting for
ordered metadata publication.

## Lifecycle

`Writer` has four states:

- open, no active segment;
- open, active segment;
- closed;
- aborted.

Rules:

- `New` starts open from the session snapshot.
- first append starts the active segment lazily.
- `Cut` swaps the active segment if it contains records.
- successful `Close` moves the writer to `closed`.
- any hard failure moves the writer to `aborted`.

After `closed`, foreground methods return `ErrClosed`.

After `aborted`, foreground methods return the terminal cause once if it was
recorded asynchronously, then return `ErrAborted`.

## Append

`Append(ctx, record)`:

1. rejects `closed` or `aborted` state;
2. rejects LSN exhaustion;
3. rejects timestamp regression;
4. performs a policy-driven `Cut()` if the next record cannot fit in the
   current active segment;
5. starts the active segment lazily if none exists;
6. appends the record to the active `segwriter.Writer`;
7. advances optimistic LSN and timestamp state;
8. may perform a post-append `Cut()` if the active segment has reached the
   configured policy limit.

`Append` returns after local acceptance and LSN assignment. It does not wait
for finalize or publish completion except when bounded in-flight backpressure
blocks a cut.

## Cut

`Cut(ctx)` is the periodic segment-boundary operation.

If the active segment has no records, `Cut` is a no-op.

If the active segment has records, `Cut`:

1. reserves in-flight capacity;
2. creates the next active segment;
3. swaps the current active segment out of the append path;
4. enqueues the detached old segment for background finalize;
5. returns without waiting for publish.

After a successful `Cut`, new appends go to the new active segment.

`Cut` is not a committed-state barrier.

## Background Finalize

Detached segments are finalized in background.

Finalize means:

1. close the detached `segwriter.Writer`;
2. convert the result to `pmeta.SegmentRef`;
3. validate the segment ref;
4. enqueue the segment into the ready-for-publish queue.

Finalize may complete out of order. Publish may not.

## Ordered Publish

Ready segments are published in cut order.

Exactly one publish worker owns metadata commit for one partition writer.

For each ready segment:

1. call `Session.PublishSegment(...)`;
2. validate the returned snapshot;
3. update committed state;
4. remove the segment from in-flight state.

There is never more than one concurrent publish for one partition writer.

## Backpressure

In-flight state is bounded by:

- `MaxInflightSegments`
- `MaxInflightBytes`

In-flight includes:

- detached segments not yet finalized;
- ready segments not yet published.

Rules:

- capacity must be reserved before `Cut` swaps the active segment;
- `Append` that requires a cut blocks when in-flight capacity is exhausted;
- explicit `Cut(ctx)` also blocks when in-flight capacity is exhausted;
- waits respect `ctx.Done()`.

This keeps memory and orphan risk bounded while preserving throughput under
normal operation.

## Flush

`Flush(ctx)` is the committed-state barrier.

It:

- cuts the active segment if it contains records;
- waits until all in-flight segments are finalized and published;
- returns the latest committed snapshot.

`Flush` is the API for shutdown, admin flush, tests, and any caller that
requires committed visibility before returning.

## Close

`Close(ctx)`:

- rejects future appends after success;
- cuts the active segment if needed;
- waits until all in-flight segments drain;
- marks the writer closed;
- returns the final committed snapshot.

## Abort

`Abort(ctx)` is idempotent.

It:

- marks the writer aborted;
- aborts the active segment if one exists;
- stops finalize and publish work;
- leaves already-written but unpublished segment objects as orphan candidates.

## Publish Result Validation

The snapshot returned by `Session.PublishSegment(...)` must satisfy:

- same partition as the writer;
- same writer identity as before publish;
- `Head.WriterEpoch == Identity.Epoch`;
- `Head.HasLastSegment == true`;
- `Head.LastSegment == published segment`;
- `Head.NextLSN == publishedSegment.NextLSN()`.

Any mismatch is `ErrInvalidPublishResult` and is terminal.

## Invariants

`writer` maintains:

- contiguous optimistic LSN assignment;
- non-decreasing timestamps across all accepted records;
- at most one active segment;
- append-path swap before background finalize;
- ordered publish in cut sequence;
- committed head advances only through successful publish;
- terminal-on-hard-failure behavior.

## Error Contract

Construction errors:

- `ErrInvalidOptions`
- `ErrInvalidSession`
- `ErrStaleWriter`

Foreground write errors:

- `ErrTimestampOrder`
- `ErrLSNExhausted`
- `ErrSegmentWriteFailed`

Publish errors:

- `ErrStaleWriter`
- `ErrPublishFailed`
- `ErrPublishIndeterminate`
- `ErrInvalidPublishResult`

Terminal state errors:

- `ErrClosed`
- `ErrAborted`

Context-sensitive waits may also return:

- `context.Canceled`
- `context.DeadlineExceeded`

Rules:

- a foreground failure returns its direct cause and makes the writer terminal;
- an asynchronous finalize or publish failure is recorded and returned by the
  next foreground call once;
- `Err()` returns the first terminal cause.

## Acknowledgement Semantics

`Append()` acknowledges:

- local acceptance;
- assigned LSN.

`Append()` does not guarantee:

- that the cut segment has been finalized;
- that metadata has been published;
- that the committed head has advanced;
- that readers can discover the new LSN yet.

`Cut()` creates a segment boundary without waiting for committed visibility.

`Flush()` and `Close()` wait for committed state to catch up to local accepted
state.
