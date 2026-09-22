// Package gcs is the Google Cloud Storage leaf of blobstore. It implements
// blobstore.MetadataStore and blobstore.RunStore directly on
// cloud.google.com/go/storage: no gocloud.dev, no transfer manager, and no
// retry, admission, or key-layout policy. blobstore/DESIGN.md sections 3-5 and
// 6.2 are the contract.
//
// # Mapping
//
//   - Metadata tokens are the decimal object generation. CompareAndSwap sends
//     ifGenerationMatch=<generation>; an empty token, Put, and Create send
//     ifGenerationMatch=0 (Conditions.DoesNotExist).
//   - Run identity tokens are idtoken "gcs" tokens holding {generation}.
//   - OpenRange reads Object.Generation(g) with an exact Range. When the pinned
//     generation answers 404, exactly one Attrs call on the live object
//     separates "replaced" (ErrRunChanged) from "missing" (ErrRunChanged joined
//     with ErrNotFound). In a bucket with object versioning a noncurrent
//     generation stays readable; that is accepted, because the bytes served
//     are exactly the bytes of the pinned identity.
//   - DeleteIfIdentity is If(GenerationMatch: g).Delete on the LIVE object. It
//     never uses Object.Generation(g).Delete, which would destroy an archived
//     version instead of conditionally deleting the current one.
//   - Only HTTP 412 and gRPC FailedPrecondition are conditional conflicts.
//     gRPC AlreadyExists and Aborted are deliberately not: nothing in the
//     pinned SDK ties them to generation preconditions.
//
// # Facts verified in the pinned SDK sources
//
// Verified against cloud.google.com/go/storage v1.59.2 and
// google.golang.org/api v0.264.0 (the HTTP/JSON client built by
// storage.NewClient; a gRPC client is not qualified by this package's tests).
//
// Retry suppression. Attrs, Delete, the Objects iterator and the opening
// request of a reader all go through storage/invoke.go run(), which issues
// exactly one call when the policy is RetryNever; ObjectHandle.Retryer and
// BucketHandle.Retryer carry that configuration (http_client.go GetObject,
// DeleteObject, ListObjects, readerReopen). The Writer is different:
// http_client.go (*httpStorageClient).OpenWriter only decides whether to call
// ObjectsInsertCall.WithRetry. Under RetryNever it does not, so the first
// request (multipart insert or resumable-session initiation) is sent once by
// gensupport.SendRequest, but for a resumable upload
// gensupport/resumable.go (*ResumableUpload).uploadChunkWithRetries then runs
// with a nil RetryConfig, and gensupport/retry.go (*RetryConfig).errorFunc
// falls back to the default predicate: every chunk, including the finalizing
// one, is retried on 5xx/429/408/connection resets for up to
// ChunkRetryDeadline (32 s by default). RetryNever alone therefore does NOT
// suppress chunk retries; TestRetryNeverAloneStillRetriesChunks keeps the
// evidence. Create consequently derives its writer handle with
// WithPolicy(RetryAlways) + WithErrorFunc(never) + WithMaxAttempts(1): that is
// the only configuration under which OpenWriter forwards a predicate, and the
// predicate refuses every retry for both the first request
// (gensupport/send.go sendAndRetry) and every chunk. Metadata writes use
// ChunkSize 0 (one multipart request, no chunk loop) under plain RetryNever.
// Every other handle uses RetryNever. ObjectHandle.Retryer mutates a shared
// retryConfig when one is already present (storage.go), so handles are always
// derived from BucketHandle.Object, which clones it (bucket.go).
//
// Mid-stream read resumption cannot be disabled. http_client.go
// (*httpReader).Read calls reopen(seen) once for each body error that is not
// io.EOF, issuing a new ranged GET for the remaining bytes. Under RetryNever
// that request is itself sent once, and readerReopen always re-applies the
// pinned generation, so a resumed stream still carries only bytes of the
// pinned identity (TestOpenRangeMidStreamResumeStaysPinned). For unpinned
// metadata reads the SDK records X-Goog-Generation from the first response
// and pins any resumption to it, which is what makes Reader.Attrs.Generation
// the generation of exactly the bytes returned by BoundedGet.
//
// Cancellation before Close. writer.go (*Writer).monitorCancel reacts to a
// cancelled context by closing the upload pipe with the context error. That
// happens asynchronously, and io.Pipe keeps the FIRST close: a bare
// cancel();Close() can lose the race, deliver a clean EOF, and let
// gensupport/buffer.go (*MediaBuffer).Chunk report io.EOF, which is the SDK's
// only finalization signal (media.go PrepareUpload chooses a single multipart
// request on it; resumable.go Upload sends the final Content-Range on it).
// Create therefore aborts in a fixed order: cancel the context, then
// Writer.CloseWithError(cause) so the pipe is poisoned synchronously, then
// Close to join the upload goroutine. A non-EOF pipe error makes Upload
// return before sending anything further, and because blobstore.CountingBody
// withholds the final byte until producer EOF, nothing already sent can
// complete the object. The pinned SDK sends NO resumable-session cancellation
// request: gensupport contains no DELETE or cancel command for a session URI,
// and the session URI is not exposed to callers. An abandoned session is
// simply left behind. Unfinalized resumable sessions are retained by GCS for
// up to one week and are never visible as objects.
//
// Buffering. gensupport/buffer.go NewMediaBuffer allocates exactly one
// ChunkSize buffer per Writer and reuses it for every chunk (and for a chunk
// retry); Create sets ChunkSize to createChunkBytes (16 MiB), the only upload
// buffer, plus one 256 KiB copy buffer. A body that fits in one chunk is sent
// as a single multipart request. Because of that buffering a create-only 412
// can surface either from Write (first chunk flush, which initiates the
// session) or from Close; both map to AlreadyExists.
//
// Exact size. storage.Writer has SendCRC32C (with ObjectAttrs.CRC32C) but no
// field that declares or enforces the object size; ObjectAttrs.Size is
// read-only. Exact size is enforced by blobstore.CountingBody before the
// commit and by comparing Writer.Attrs().Size afterwards.
package gcs
