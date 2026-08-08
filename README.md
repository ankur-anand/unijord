# Unijord

This repository currently contains `partitionlog`, a Go library for durable,
partitioned event streams on object storage.

`partitionlog` provides immutable segment encoding, object-store publication,
bounded catalog metadata, fenced writers, retention, and replay readers for S3,
GCS, Azure Blob Storage, and MinIO.

See [`partitionlog/README.md`](partitionlog/README.md) for the public API and
usage.

