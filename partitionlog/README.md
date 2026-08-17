# partitionlog

Durable partitioned event streams on object storage.

`partitionlog` writes immutable segment objects to S3-compatible storage,
Google Cloud Storage, or Azure Blob Storage, and keeps bounded catalog metadata
for readers.

The binary format specification, checked-in compatibility corpus, and
cross-language verification contract are documented in
[`segformat/COMPATIBILITY.md`](./segformat/COMPATIBILITY.md).

## Open A Log

Create a provider store first. The store wires the catalog, segment writer, and
segment reader for one object-storage location.

The examples assume `ctx`, provider clients, and writer IDs are created by the
application.

```go
import (
    "context"
    "time"

    "github.com/ankur-anand/unijord/partitionlog"
    pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
)

store, err := pls3.New(pls3.Options{
    Client:   s3Client,
    Bucket:   "events",
    Prefix:   "prod",
    StreamID: "hosts/host-a/events",
})
if err != nil {
    return err
}

log, err := partitionlog.Open(partitionlog.Options{
    Store: store,
    Reader: partitionlog.ReaderOptions{
        MaxRecordsPerBatch: 1024,
        RangeCacheBytes:    256 << 20,
        OpenSegmentReaders: 1024,
        Refresh: partitionlog.RefreshPolicy{
            PollInterval:           time.Second,
            MaxConcurrentRefreshes: 16,
            RefreshTimeout:         2 * time.Second,
        },
    },
})
if err != nil {
    return err
}
```

Provider packages:

```go
partitionlog/s3
partitionlog/gcs
partitionlog/azure
```

## Write

A writer owns one partition. `Append` assigns an LSN and accepts the record into
the active segment. `Flush` makes all accepted records visible to readers by
closing and publishing pending segments.

```go
writer, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
    Partition: 7,
    WriterID:  writerID,
    Batch: partitionlog.BatchPolicy{
        MaxDelay:   time.Second,
        MaxBytes:   64 << 20,
        MaxRecords: 16_384,
    },
})
if err != nil {
    return err
}
defer writer.Abort(context.Background())

appendResult, err := writer.Append(ctx, partitionlog.Record{
    TimestampMS: time.Now().UnixMilli(),
    Value:       []byte("hello"),
})
if err != nil {
    return err
}

snapshot, err := writer.Flush(ctx)
if err != nil {
    return err
}

_ = appendResult.LSN
_ = snapshot.Head.NextLSN
```

Services that acknowledge records asynchronously can wait for committed head
advancement without polling each request:

```go
for {
    changed := writer.Committed()
    state := writer.State()
    if state.Snapshot.Head.NextLSN > appendResult.LSN {
        break
    }
    select {
    case <-changed:
        if err := writer.Err(); err != nil {
            return err
        }
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

Obtain `Committed()` before reading `State()` so a commit cannot occur between
the state check and registering the waiter.

Use `Close` during graceful shutdown:

```go
snapshot, err := writer.Close(ctx)
```

## Retention

Retention is an explicit two-step operation. A scheduler records monotonic
intent without touching partition visibility:

```go
_, err := log.RequestRetention(ctx, partitionlog.RetentionRequest{
    Partition:     7,
    PolicyVersion: 42,
    BeforeLSN:     1_000_000,
})
```

The active partition writer applies the latest request through its existing
fence and ordered catalog session:

```go
result, err := writer.ApplyRetention(ctx)
```

Records below `result.Snapshot.Head.OldestLSN` are no longer visible. Retention
keeps a whole immutable segment when `BeforeLSN` falls inside it, so the
effective `OldestLSN` can be lower than `result.RequestedLSN`. Physical object
deletion is a separate grace-period GC operation.

Provider stores expose an explicit reclaimer from
`partitionlog/blob/lifecycle`. Run retention cleanup regularly and the more
expensive reachability scrub on a slower schedule:

```go
deleteLimiter, err := plifecycle.NewTokenBucketDeleteLimiter(10_000, 1_000)
if err != nil {
    return err
}

reclaimer, err := store.NewReclaimer(plifecycle.Options{
    DeleteDelay:       24 * time.Hour,
    MaxObjectsPerRun:  10_000,
    MaxDeletesPerRun:  1_000,
    DeleteBatchSize:   1_000,
    DeleteConcurrency: 16,
    DeleteRateLimiter: deleteLimiter,
})
if err != nil {
    return err
}

scheduler, err := plifecycle.NewScheduler(reclaimer, plifecycle.SchedulerOptions{
    MaxConcurrentPartitions: 8,
    PartitionRunTimeout:     30 * time.Second,
    MaxPassesPerTask:        64,
})
if err != nil {
    return err
}

summary, err := scheduler.Run(ctx, []plifecycle.Task{
    {Partition: 7, Operation: plifecycle.OperationReclaim},
    {Partition: 8, Operation: plifecycle.OperationReclaim},
})
```

Call the same scheduler separately with `OperationScrub` on a slower cadence.
One `Run` call is finite: it fairly requeues bounded continuations up to
`MaxPassesPerTask`, reports still-busy partitions as deferred, and returns. The
caller remains responsible for partition discovery and the recurring schedule.
Nothing runs implicitly inside writers or readers.

The physical object lifecycle is defined in
[`LIFECYCLE.md`](./LIFECYCLE.md).

## Read

`Read` is passive. It does not start background polling and does not wait for
future records.

```go
partition := log.Reader().Partition(7)

batch, err := partition.Read(ctx, partitionlog.ReadRequest{
    StartLSN:  0,
    Limit:     1000,
    Freshness: partitionlog.FreshnessOnTail,
})
if err != nil {
    return err
}

for _, record := range batch.Records {
    _ = record.LSN
    _ = record.Value
}
```

Freshness modes:

```go
partitionlog.FreshnessCached // use cached head if available
partitionlog.FreshnessOnTail // refresh only when StartLSN reaches cached tail
partitionlog.FreshnessLatest // refresh before reading
```

## Replay With A Cursor

A cursor is a lightweight local position over the shared reader runtime.

```go
cursor, err := partition.Cursor(partitionlog.CursorOptions{
    StartLSN: 0,
    Limit:    1000,
})
if err != nil {
    return err
}
defer cursor.Close()

batch, err := cursor.Next(ctx)
if err != nil {
    return err
}

_ = batch.NextLSN
_ = cursor.Position()
```

Persist a stream-bound checkpoint when processing is complete:

```go
checkpoint, err := cursor.Checkpoint(ctx)
if err != nil {
    return err
}

// Persist the complete checkpoint as JSON. Do not persist NextLSN alone.
resumed, err := partition.ResumeCursor(ctx, checkpoint, partitionlog.CursorResumeOptions{
    Limit: 1000,
})
if err != nil {
    return err
}
defer resumed.Close()
```

`Checkpoint` and `ResumeCursor` validate against the latest catalog head.
Resume fails if the checkpoint belongs to another stream or partition, is
below the retention floor, or is ahead of the committed tail.

## Tail

Tailing is explicit. A `Watch` starts background catalog refresh for selected
partitions. A `Tailer` waits on that watch.

```go
watch, err := log.Reader().Watch(ctx, partitionlog.WatchOptions{
    Partitions: []uint32{7},
})
if err != nil {
    return err
}
defer watch.Close()

tailer, err := watch.Tail(partitionlog.TailOptions{
    Partition: 7,
    StartLSN:  0,
    Limit:     1000,
})
if err != nil {
    return err
}
defer tailer.Close()

for {
    batch, err := tailer.Next(ctx)
    if err != nil {
        return err
    }
    for _, record := range batch.Records {
        _ = record
    }
}
```

## Reader Cache Options

```go
RangeCacheBytes
```

Memory budget for cached byte ranges read from segment objects. This reduces
repeated object-store range reads for trailers, indexes, and block payloads.

```go
OpenSegmentReaders
```

Number of parsed/open segment readers to keep in memory. This avoids repeatedly
opening hot segments and parsing their trailer/index metadata.

## Metrics

Attach a metrics sink at `Open`. The sink receives public API events and
background segment events. It must be safe for concurrent use.

```go
type metricsSink struct{}

func (metricsSink) Observe(m partitionlog.Metric) {
    switch m.Name {
    case partitionlog.MetricWriterAppend:
        // record append latency, bytes, errors
    case partitionlog.MetricWriterSegmentPublish:
        // record catalog publish latency
    case partitionlog.MetricReaderRead:
        // record read latency and batch size
    }
}

log, err := partitionlog.Open(partitionlog.Options{
    Store:   store,
    Metrics: metricsSink{},
})
```

## Visibility

Readers only see committed segments published through the catalog. An `Append`
acknowledges local acceptance by the writer. Records become visible after a
segment is cut and published, or after `Flush`/`Close` completes successfully.
