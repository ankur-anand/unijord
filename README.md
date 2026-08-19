<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/unijord-mark-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/unijord-mark.svg">
    <img src="docs/images/unijord-mark.svg" alt="Unijord timeline mark" width="112">
  </picture>
</p>

# Unijord

**Durable timelines for the agentic era.**

Unijord is an object-native event stream store for agents and workflows. It
keeps each execution as an independently ordered timeline in object storage
you control.

Agents come and go. Their execution history should not.

## One execution, one timeline

An agent run is not only its final output. It is the complete path the work
took:

```text
timeline: agent-run-123

0  run started
1  prompt received
2  tool called
3  tool failed
4  human corrected
5  output accepted
```

A timeline is the independently ordered event history of one agent, workflow,
host, or application. It has a stable identity, dense LSNs, timestamps, and an
independent lifecycle.

```text
Namespace -> Timeline -> Ordered records
```

One identity. One strict order. One independently replayable history.

## Agent systems produce many independent histories

Every agent run, workflow, sandbox, host, and edge process creates its own
history. These histories are bursty and independently owned. Some finish in
seconds. Some remain active for days. Many are opened only after something
fails, an evaluation runs, or a past decision needs to be understood.

A shared broker topic is good at moving events through live systems. Keying
records by agent preserves that agent's order, but its history remains a sparse
set of offsets mixed with unrelated work inside a shared partition. Retention,
consumption, and physical layout still belong to the topic.

Creating one topic per agent restores isolation, but turns every short-lived
timeline into broker metadata, partitions, replicas, and operational state.

Object storage already provides the durable, elastic foundation this workload
needs. Unijord adds the missing contract: ordered append, durable visibility,
replay by position or time, and independent retention.

## Object storage is the durable truth

Writers run close to where work happens. They seal records into immutable,
indexed segments and publish a small catalog update that makes the new range
visible. Readers use that catalog to range-read finalized history directly
from S3, GCS, Azure Blob, or MinIO.

```text
agent / workflow / host
          |
          v
     append records
          |
          v
immutable segments + catalog
          |
          v
S3 / GCS / Azure Blob / MinIO
          ^
          |
      replay readers
```

An idle timeline has no broker partition or resident writer to keep alive. A
worker can disappear and another can reconstruct the committed state from
object storage. Reader caches are disposable. The durable record remains in
open files rather than in the lifetime of a service deployment.

## One history, many uses

When the timeline identity is known, a reader can open only that history:

```text
replay agent-run-123 from LSN 900
inspect workflow-456 after a failure
audit host-789 between two timestamps
```

The reader walks bounded catalog metadata and range-reads only the indexed
segment blocks it needs. It does not call the writer and does not depend on
other readers.

Cross-timeline queries belong in derived structures built for their access
patterns:

```text
Unijord timelines
        |
        +--> Parquet / Iceberg --> DuckDB / Spark / Trino
        +--> search index      --> full-text discovery
        +--> vector index      --> semantic retrieval
        +--> materialized view --> current state
```

The timeline preserves exact order and provenance. A projector can reorganize
many timelines for analytics, search, or serving without becoming the only copy
of the original history.

Projectors are part of the product direction. The repository currently focuses
on the storage engine and direct replay path.

## Where Kafka still fits

Kafka is the better tool when messages must reach live processors with low
latency, consumer groups should divide shared work, or applications consume the
complete stream as events arrive.

Unijord is for histories that must be reopened and governed by identity later.
They can be used together:

```text
Kafka     = live distribution
Unijord   = durable owned history
Parquet   = cross-timeline analytics
```

## Status

Experimental. The Go storage engine supports immutable segment publication,
fenced writers, bounded catalog metadata, direct replay, retention, and garbage
collection on S3, GCS, Azure Blob, and MinIO. The service API, registry model,
and storage layout may still change before the first stable release.
