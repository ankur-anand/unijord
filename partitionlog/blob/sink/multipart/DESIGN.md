# Multipart provider sessions

`partitionlog/blob/sink/multipart` is the provider boundary beneath the common
ordered-stream layer. It knows about object keys, staging sessions, numbered
parts and final-object identity. It does not know about partitions, LSNs,
segments, catalogs or retention.

## Contract

```go
session, err := store.Begin(ctx, key, multipart.Options{...})
part := multipart.NewPart(1, bytes)
receipt, err := session.PutPart(ctx, part)
request := multipart.NewCommitRequest([]multipart.Receipt{receipt})
attrs, err := session.Commit(ctx, request)
err := session.Cleanup(ctx)
```

Every store and opened session publishes the same `Limits`. The common stream
selects a part size that satisfies those limits and rejects an object that
would exceed its byte or part-count limit before accepting that `Write`.

Every session receives a UUID. The UUID isolates staging objects and is copied
to final-object metadata so an ambiguous commit can be reconciled without
mistaking another writer's object for its own.

## Part identity and retries

Every `Part` contains a SHA-256 checksum. Providers validate the checksum before
performing I/O.

This checksum identifies the logical part across retries. Receipts currently
carry that identity back to the common stream; they do not attest that the
provider independently recomputed SHA-256 over its stored bytes. Native
provider checksums, when enabled by an adapter or SDK transport, are a separate
end-to-end integrity mechanism.

`PutPart` has these semantics:

- a new part number uploads normally;
- retrying the same number and checksum is safe and returns the same logical
  receipt;
- reusing a number with different content returns `ErrPartConflict`;
- different part numbers may upload concurrently.

S3 safely overwrites an upload's part number. Azure safely re-stages a
session-specific block ID. GCS creates a fresh attempt object for every remote
attempt, so an old object whose success response was lost cannot make all later
retries fail with a precondition error.

## Commit identity and reconciliation

`CommitRequest` carries ordered receipts, expected total size, and an optional
whole-object SHA-256. Final object creation remains conditional on the key not
existing.

If publication returns an error, the provider inspects the final key:

```text
matching session identity and size/checksum -> return success
different final object                     -> ErrPreconditionFailed
outcome cannot be established              -> ErrCommitIndeterminate
```

Azure and GCS attach the final size and SHA-256 as metadata during commit. S3
must attach metadata when its native multipart upload begins, before the final
checksum is known, so S3 reconciliation uses the unique session UUID plus the
native object size. A caller that supplies a SessionID is responsible for never
reusing it for different content.

Successful `Commit` is idempotent within the session. A retry returns the same
`ObjectAttrs`.

## Cleanup is not rollback

`Cleanup` means:

> stop using the session and make a best-effort attempt to remove staging work.

It never promises that the final object is absent. Once the provider's final
commit has started, `Cleanup` returns `ErrCommitInProgress` instead of claiming
success.

Provider behavior:

- S3 waits for every part request owned by the session to return, then calls
  `AbortMultipartUpload`, so a late part cannot land behind the abort.
- Azure marks the session terminal locally; Azure owns expiry of uncommitted
  blocks.
- GCS waits for part attempts to finish and deletes all known session-specific
  staging objects. Failed cleanup can be retried.

## GCS composition

GCS compose accepts at most 32 sources per request, so the adapter builds a
tree. Every commit retry receives a unique subtree:

```text
<staging-session>/compose/<commit-attempt>/level-00/group-000000
```

A partial tree from a failed attempt therefore cannot collide with the next
attempt. The final key alone remains stable and write-once. Source generations
are pinned in compose requests and staging deletion.
