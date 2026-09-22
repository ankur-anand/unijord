// Package azure is the Azure Blob Storage leaf of blobstore. It implements
// blobstore.MetadataStore and blobstore.RunStore over one explicit
// *container.Client using the azblob SDK directly: no gocloud.dev and none of
// the SDK transfer helpers (UploadStream, UploadBuffer, UploadFile,
// DownloadBuffer, DownloadFile, RetryReader). blobstore/DESIGN.md sections 3,
// 4, 5 and 6.3 are the contract.
//
// # SDK facts verified against the pinned module source
//
// azblob v1.6.1 and azcore v1.18.1, read from the module cache:
//
//   - blockblob/client.go: (*Client).Upload and (*Client).StageBlock both take
//     io.ReadSeekCloser and call shared.ValidateSeekableStreamAt0AndGetCount,
//     which seeks to learn the length. A forward-only producer therefore
//     cannot be one request. Create is a native loop: fill one reused
//     stageBlockBytes buffer, StageBlock it, repeat, then exactly one
//     (*Client).CommitBlockList, which marshals every ID under <Latest>
//     (generated.BlockLookupList).
//   - azcore/policy/policy.go: WithRetryOptions(ctx, RetryOptions) stores a
//     per-call override that azcore/runtime/policy_retry.go (*retryPolicy).Do
//     reads from the request context; setDefaults maps MaxRetries < 0 to 0, so
//     the loop exits after try 1 ("try == options.MaxRetries+1"). Every
//     request in this package is issued under
//     WithRetryOptions(ctx, RetryOptions{MaxRetries: -1}).
//   - Access conditions: blockblob.UploadOptions.AccessConditions,
//     blockblob.CommitBlockListOptions.AccessConditions (+ HTTPHeaders),
//     blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset, Count},
//     AccessConditions}, blob.DeleteOptions.AccessConditions and
//     blob.GetPropertiesOptions.AccessConditions are all *blob.AccessConditions
//     whose ModifiedAccessConditions carries IfMatch / IfNoneMatch *azcore.ETag.
//     exported.FormatHTTPRange renders "bytes=<offset>-<offset+count-1>" in
//     the x-ms-range header. A version would be addressed with
//     blob/client.go (*Client).WithVersionID, which adds the versionid query
//     (not used; see Identity).
//   - blob/client.go (*Client).DownloadStream returns the generated
//     DownloadResponse whose Body is the raw *http.Response body
//     (zz_blob_client.go downloadCreateRequest calls runtime.SkipBodyDownload;
//     downloadHandleResponse assigns resp.Body). No re-request can happen
//     unless NewRetryReader is called, and this package never calls it.
//   - container/client.go (*Client).NewListBlobsFlatPager accepts only Prefix,
//     MaxResults, Marker and Include. The marker is opaque and there is no
//     start-after parameter.
//   - azcore/internal/exported/response_error.go NewResponseError takes the
//     error code from the x-ms-error-code header first (this is what makes
//     HEAD errors classifiable) and from the XML body otherwise.
//
// # Provider behaviour relied upon
//
// Azure has no abort operation for staged blocks. Uncommitted blocks are
// invisible to Get Blob, Get Blob Properties and List Blobs, are
// garbage-collected by the service seven days after they were staged, and are
// discarded earlier if another Put Blob or Put Block List commits the same
// blob first. A Create that fails before CommitBlockList was issued is
// therefore DefinitelyAbsent and leaves nothing to clean up. Block IDs are
// base64(attemptNonce[16] || big-endian uint32 index), so concurrent or
// repeated attempts on one key never adopt each other's blocks. Once the
// commit request has been issued, only an ETag-bearing success is Created and
// only a definite access-condition rejection is AlreadyExists; every other
// result, including a lost response, a 5xx, throttling or cancellation, is
// Indeterminate.
//
// Error mapping is by service error code: 404 BlobNotFound is the only
// object-missing signal (ContainerNotFound is a deployment fault and stays a
// plain error); 412 ConditionNotMet and 409 BlobAlreadyExists are the only
// conditional conflicts, with a bare 412 accepted only when the service gave
// no code. Lease, snapshot and container 409/412 codes, 400 InvalidBlockList,
// 429, 5xx, transport and context errors are never conflicts.
//
// Azure answers an If-Match read or delete of a missing blob with 404, while
// Azurite answers 412 ConditionNotMet. After a definite If-Match rejection
// OpenRange and DeleteIfIdentity therefore issue one read-only Get Blob
// Properties (once, never retried) to tell "missing" (ErrNotFound) from
// "replaced" (ErrRunChanged); if that lookup is inconclusive the result stays
// ErrRunChanged, which is always true of a rejected condition.
//
// # Identity
//
// Metadata tokens are the ETag exactly as returned. Run identity tokens are
// idtoken("azure", {etag, version_id?}). OpenRange reads the current blob
// under If-Match and, when a version is recorded, additionally requires the
// x-ms-version-id response header to equal it. It deliberately does not read
// through (*blob.Client).WithVersionID: on a versioned container a version URL
// keeps answering after the run was replaced or deleted, while DESIGN 5.2
// requires ErrRunChanged for a missing or replaced object. DeleteIfIdentity
// deletes the base blob under If-Match (snapshots included). On a container
// with versioning or soft delete the service retains the previous version
// after that delete; this is still a correct conditional delete of the
// current object, and retained versions are never reachable through this
// package.
//
// # List cost
//
// Because Azure offers no start-after, ListOptions.AfterKey is honoured by
// listing the caller's Prefix from its beginning and skipping names that are
// <= AfterKey. One List call may therefore issue several sequential List
// Blobs requests (each issued once, never retried), and paging through n
// objects costs O(n^2 / 1000) entries scanned in total. The scan requests at
// most 1,000 entries per request, stops as soon as limit+1 qualifying names
// are collected, and never narrows the server-side prefix beyond the one the
// caller supplied. Flat listings are lexicographic by name for the ASCII keys
// this system writes; the scan asserts strictly ascending order and the prefix
// on every entry and fails with ErrIndeterminate rather than return a wrong
// page. Names that are not blobstore.ValidKey (for example directory markers
// written by other tools) cannot be addressed through this API and are
// skipped.
package azure
