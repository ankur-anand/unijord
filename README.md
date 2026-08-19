# Unijord

**Durable timelines for the agentic era.**

## AI agents do not produce one stream

They produce millions of timelines.

Every agent run, workflow, sandbox, host, and edge process creates its own
execution history: prompts, tool calls, retries, corrections, decisions,
outputs, and evaluations.

These histories are bursty and independently owned. Some finish in seconds.
Some remain active for days. Most are read only after something fails, an
evaluation runs, or a past decision needs to be understood.

Shared broker topics are designed to move events through live systems. When
records are keyed by agent, each agent remains ordered, but its history becomes
a sparse set of offsets mixed with other agents inside a shared partition.
Retention, consumption, and physical layout still belong to the topic.

Creating one topic per agent restores isolation, but turns every short-lived
timeline into broker metadata, partitions, replicas, and operational state.

Object storage already has the durability and scale this workload needs. What
it lacks is an ordered append and replay contract.

## An owned timeline for every agent

Unijord gives each agent or workflow a contiguous ordered history in object
storage you control. Writers publish immutable, indexed segments. A fenced
catalog defines what is visible. Readers replay a timeline by LSN or timestamp
without scanning unrelated events.

```text
agent / workflow / host
        |
        v
owned ordered timeline
        |
        v
immutable segments + bounded catalog
        |
        v
S3 / GCS / Azure Blob / MinIO
```

An idle timeline has no broker partition or resident writer to keep alive. A
worker can disappear and another can reconstruct the committed state from the
catalog. The durable history remains in open files rather than in the lifetime
of a service deployment.

[Get started with the Go engine](partitionlog/README.md) | [Read the segment format](partitionlog/segformat/SPEC.md)

## One history, two read paths

### Replay one timeline directly

Use the direct reader when the timeline identity is known:

```text
replay agent-42 from LSN 900
inspect workflow-17 after a failure
audit host-a between two timestamps
```

The reader walks bounded catalog metadata and range-reads only the indexed
segment blocks needed for that timeline. It does not call the writer and does
not depend on other readers.

### Project many timelines for analysis

Cross-timeline joins and aggregations belong in derived query structures, not
in the append journal.

```text
Unijord timelines
        |
        +--> Parquet / Iceberg projector --> DuckDB / Spark / Trino
        +--> search projector            --> full-text index
        +--> vector projector            --> ANN index
        +--> state projector             --> materialized view
```

A projected row keeps its source identity:

```text
(namespace_id, timeline_id, lsn)
```

That tuple gives a projector a stable idempotency key. It can checkpoint its
position, resume after failure, or rebuild a derived dataset from the durable
timelines. Producers write once. Each downstream system chooses the physical
shape that suits its queries.

The raw timeline preserves exact order and provenance. Parquet or Iceberg
reorganizes many timelines for column scans, joins, and aggregates. Search and
vector indexes create their own immutable projections. None of them becomes the
only copy of the original history.

Projectors are part of the product direction. This repository currently
focuses on the storage engine and direct replay path.

## Where Kafka still fits

Kafka is the better tool when messages must reach live processors with low
latency, when consumer groups should divide shared work, or when applications
normally consume the complete stream.

Unijord is for history that must be opened and governed by identity later.
They can be used together:

```text
Kafka       = live distribution
Unijord     = durable owned history
Parquet     = cross-timeline analytics
```

## Status

Experimental. The segment format has a compatibility specification and golden
corpus, but the service API, registry model, and storage layout may still change
before the first stable release.
