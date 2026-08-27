# catformat compatibility

This document defines how `catformat` implementations prove compatibility
with the binary catalog contract in [`SPEC.md`](./SPEC.md).

## Version policy

The format version is the `u16` at offset 4 in every head and page object.
Version 1 readers accept only version 1. An unknown version is an explicit
`unsupported version` error; it is never interpreted using the closest known
layout.

The checked-in v1 corpus freezes the first byte representation. After any v1
object has been produced outside a disposable development bucket:

- existing field offsets, sizes, meanings, and validation rules do not change;
- reserved bytes remain zero and cannot acquire a meaning within v1;
- a change that alters encoded bytes or reader semantics allocates a new
  format version;
- readers may support several versions, but writers emit exactly one selected
  version;
- a catalog partition never mixes head or page versions.

`segment_layout_version` is independent of the catalog format version. It
selects the derivation grammar for `.plseg` object keys, not the layout of
`.plc` objects.

## Canonical corpus

Language-neutral fixtures live in
[`internal/catformat/testdata/v1`](./internal/catformat/testdata/v1). The
manifest records each object's exact size, SHA-256 digest, object kind, hash
algorithm, and page ID where applicable.

The corpus contains:

- an empty CRC32C head;
- a populated XXH64 head with an open leaf and an open index section;
- a fully retained XXH64 head that preserves only its historical append tip;
- a two-entry XXH64 leaf page;
- a two-entry CRC32C index page.

An implementation is byte-compatible when it can:

1. parse every fixture and enforce all structural and integrity checks;
2. re-encode the parsed value to bytes identical to the fixture;
3. recompute each page ID and match `manifest.json`;
4. reject corrupted body hashes, trailer hashes, reserved bytes, sizes,
   versions, ranges, counts, and non-contiguous entries.

Go runs those assertions in `TestCompatibilityCorpus`. The fixture generator
is intentionally checked in at
`internal/catformat/internal/cmd/gencorpus`; rerunning it is a format-change
review action, not a normal test step.

## Initialization

Production catalog code uses only `head.plc` and `.plc` pages; a missing head
means the partition is uninitialized.

This keeps the writer, reader, retention logic, and failure-recovery protocol
free of permanent dual-format branches and extra missing-head lookups.
