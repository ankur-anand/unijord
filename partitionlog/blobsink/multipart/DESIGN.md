# blobsink/multipart Design

`partitionlog/blobsink/multipart` defines the small object-assembly contract
used by `partitionlog/blobsink`.

It does not know about partitions, LSNs, segment files, catalog pages, or
retention. Higher layers choose keys and adapt `Upload` into `segwriter.Txn`.

## Contract

```go
upload, err := store.BeginMultipart(ctx, key, multipart.Options{...})
receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: b})
attrs, err := upload.Complete(ctx, []multipart.Receipt{receipt})
err := upload.Abort(ctx)
```

Rules:

- part numbers are positive
- receipts passed to `Complete` are contiguous starting at 1
- `Complete` commits parts in receipt order
- final object creation must be conditional on non-existence
- `Abort` is idempotent
- provider precondition failures map to `ErrPreconditionFailed`

## Provider Mapping

S3 uses native multipart upload:

```text
CreateMultipartUpload
UploadPart(part_number, bytes)
CompleteMultipartUpload(receipts)
AbortMultipartUpload
```

GCS has no S3-style multipart commit API. It uploads each part as a temporary
object and commits by composing temporary objects into the final object. GCS
compose accepts at most 32 sources per call, so large commits are composed in
levels under the staging prefix.

```text
temporary object per part
compose temp objects into final object with DoesNotExist precondition
delete temporary objects best-effort
```

Azure Blob Storage uses block blobs:

```text
StageBlock(block_id, bytes)
CommitBlockList(block_ids, If-None-Match: *)
```

Azure has no explicit abort for staged blocks. `Abort` marks the upload
terminal locally; uncommitted blocks are provider-owned garbage and expire per
Azure Blob Storage rules.

## Memory Store

The in-memory store implements the same contract for tests. It is not part of
the production storage path.
