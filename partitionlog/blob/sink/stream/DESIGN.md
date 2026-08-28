# Ordered streaming upload

This package is the provider-neutral layer between a sequential segment writer
and an object store's multipart primitives. `partitionlog/blob/sink` wires it
into every object-backed `segwriter` transaction.

The caller writes one ordered byte stream. The implementation splits it into
fixed-size numbered parts, may upload those parts concurrently, sorts the
receipts by part number, and supplies that order to the final multipart commit.

## Payload-memory bound and backpressure

One upload can hold at most:

```
1 active buffer + UploadQueueSize queued buffers + UploadParallelism in-flight buffers
```

Every buffer is exactly `PartSize` bytes. With the private default pool, the
hard bound is therefore:

```
PartSize * (1 + UploadQueueSize + UploadParallelism)
```

When those buffers are occupied, `Write` waits for an upload worker to release
one. The producer cannot allocate around the limit. A shared `BufferPool`
applies the same bound across many simultaneous uploads, which is the preferred
configuration for a long-lived process.

The bound covers stream-owned payload buffers. It does not include memory
copied internally by a provider SDK. The stream also retains one small receipt
per uploaded part until commit, so metadata memory is `O(number of parts)`.
That metadata is bounded by the session's maximum part count.

The stream validates `PartSize` against the session's limits during
construction. Before each `Write`, it also checks the future object size and
part count. A rejected call accepts no bytes, so callers can recover by writing
a smaller payload.

## Lifecycle

`Commit` has two phases. Before backend `Commit` begins, `Abort` may cancel
the upload and discard staging work. After backend `Commit` begins, `Abort`
returns `ErrCommitInProgress`; it never reports success while the final object
may be landing.

A backend commit error whose outcome cannot be reconciled is reported with
`ErrCommitIndeterminate`. The caller may retry `Commit` with a fresh context;
the exact same receipts, expected size, checksum, and session identity are
reused so the provider can reconcile safely. The caller may instead ask
`Abort` to clean staging work, but that cleanup does not prove that the final
object is absent. Publication code must reconcile the final object or catalog
state before deciding whether the operation committed.

When a provider error contains both an indeterminate-outcome marker and an
otherwise definite retry error (for example, a cleaned staging session), the
indeterminate outcome takes precedence. The retry error describes the latest
attempt; it does not prove whether an earlier final-object commit landed.

If provider cleanup fails, `Abort` may be called again with a fresh context.
The byte stream remains stopped; only staging cleanup is retried.

Every constructed stream must end with a successful `Commit` or an `Abort`.
`Commit` on an empty stream returns `ErrEmptyUpload` and deliberately leaves the
stream open so the caller may still write data. A caller that abandons that
stream must call `Abort` to stop its workers.

The context passed to `Write` controls only that call's wait for buffers and
queue capacity. Upload workers use the stream lifetime context, so one request
deadline cannot cancel work already accepted from an earlier call. If a Write
accepts only part of its input before cancellation, the stream becomes terminal
because the API intentionally does not expose partial byte counts.

## Provider contract

The wrapped `multipart.Session` must:

- allow concurrent `PutPart` calls;
- copy or fully consume `Part.Bytes` before returning;
- return a receipt with the same part number, byte size and checksum;
- make retries of the same part identity safe;
- reconcile ambiguous final commits using the session identity;
- treat `Cleanup` as staging cleanup, not proof that the final object is absent.

Final-object write-once preconditions remain in the provider adapters, while
ordering, buffering, and lifecycle decisions remain in this package.

The SHA-256 values in parts, receipts, and commit metadata are logical content
identities used for retry conflict detection, contract validation, and commit
reconciliation. They are not, by themselves, provider-attested proof that the
remote service stored those bytes. Provider-native transport checksums are a
separate integrity layer.
