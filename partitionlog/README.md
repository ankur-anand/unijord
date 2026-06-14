# partitionlog

Durable partitioned event streams on object storage.

`partitionlog` writes immutable segment objects to S3-compatible storage,
Google Cloud Storage, or Azure Blob Storage, and keeps bounded catalog metadata
for readers.

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
    Client: s3Client,
    Bucket: "events",
    Prefix: "prod",
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

Use `Close` during graceful shutdown:

```go
snapshot, err := writer.Close(ctx)
```

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

## Visibility

Readers only see committed segments published through the catalog. An `Append`
acknowledges local acceptance by the writer. Records become visible after a
segment is cut and published, or after `Flush`/`Close` completes successfully.
