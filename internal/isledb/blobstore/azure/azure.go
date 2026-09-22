package azure

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
)

const (
	provider = "azure"

	// stageBlockBytes is the size of the one staging buffer a Create call
	// allocates and reuses for every Put Block request.
	stageBlockBytes = 8 << 20
	// maxBlocks is the service limit on committed blocks per block blob.
	maxBlocks = 50000
	// MaxCreateBytes is the largest run object Create accepts.
	MaxCreateBytes = int64(maxBlocks) * stageBlockBytes

	attemptNonceBytes = 16
	listRequestMax    = int32(blobstore.MaxListLimit)
	runContentType    = "application/octet-stream"
	maxTokenBytes     = 1024
)

// Store implements both blobstore contracts over one Azure Blob container.
type Store struct {
	container *container.Client
}

var (
	_ blobstore.MetadataStore = (*Store)(nil)
	_ blobstore.RunStore      = (*Store)(nil)
)

// New wraps an explicit container client. The container must already exist.
func New(client *container.Client) (*Store, error) {
	if client == nil {
		return nil, errors.Join(blobstore.ErrInvalidRequest, errors.New("azure: nil container client"))
	}
	return &Store{container: client}, nil
}

// once suppresses the azcore retry policy for one call: setDefaults in
// azcore/runtime/policy_retry.go turns a negative MaxRetries into zero.
func once(ctx context.Context) context.Context {
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
}

func begin(ctx context.Context, key string) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	return ctx.Err()
}

// --- error mapping (this leaf only) ---

func responseError(err error) *azcore.ResponseError {
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		return response
	}
	return nil
}

// isNotFound is true only for a missing blob. A missing container is a
// deployment fault, never an object-missing signal.
func isNotFound(err error) bool {
	response := responseError(err)
	return response != nil && response.StatusCode == http.StatusNotFound &&
		bloberror.Code(response.ErrorCode) == bloberror.BlobNotFound
}

// isConditionRejected is true only for a definite access-condition rejection.
// It matches on the service error code; an HTTP 412 is accepted alone only
// when the service supplied no code. Other 409/412 codes (lease, container,
// snapshot conflicts), 400 InvalidBlockList, throttling, 5xx, and transport
// or context errors are never conditional conflicts.
func isConditionRejected(err error) bool {
	response := responseError(err)
	if response == nil {
		return false
	}
	switch bloberror.Code(response.ErrorCode) {
	case bloberror.ConditionNotMet:
		return response.StatusCode == http.StatusPreconditionFailed
	case bloberror.BlobAlreadyExists:
		return response.StatusCode == http.StatusConflict
	case "":
		return response.StatusCode == http.StatusPreconditionFailed
	default:
		return false
	}
}

func validETag(etag string) bool {
	if etag == "" || etag == string(azcore.ETagAny) || len(etag) > maxTokenBytes {
		return false
	}
	for i := 0; i < len(etag); i++ {
		if etag[i] < 0x20 || etag[i] >= 0x7f {
			return false
		}
	}
	return true
}

func etagOf(etag *azcore.ETag) (string, bool) {
	if etag == nil || !validETag(string(*etag)) {
		return "", false
	}
	return string(*etag), true
}

func ifNoneMatchAny() *blob.AccessConditions {
	return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}
}

func ifMatch(etag string) *blob.AccessConditions {
	return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: to.Ptr(azcore.ETag(etag))}}
}

func closeBody(body io.Closer) error {
	if body == nil {
		return nil
	}
	if err := body.Close(); err != nil {
		return errors.Join(blobstore.ErrCleanup, err)
	}
	return nil
}

// --- metadata contract ---

func (s *Store) BoundedGet(ctx context.Context, key string, maxBytes int64) (blobstore.Object, error) {
	if maxBytes < 1 || maxBytes > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	response, err := s.container.NewBlobClient(key).DownloadStream(once(ctx), nil)
	if err != nil {
		if isNotFound(err) {
			return blobstore.Object{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.Object{}, err
	}
	// response.Body is the raw HTTP body (generated Download sets
	// runtime.SkipBodyDownload); it is closed on every path below.
	hint := int64(64 << 10)
	if response.ContentLength != nil {
		if *response.ContentLength > maxBytes {
			return blobstore.Object{}, errors.Join(blobstore.ErrTooLarge, closeBody(response.Body))
		}
		if *response.ContentLength < 0 {
			return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate,
				errors.New("azure: negative content length"), closeBody(response.Body))
		}
		hint = *response.ContentLength
	}
	// bytes.MinRead spare capacity lets ReadFrom observe EOF without growing.
	buffer := bytes.NewBuffer(make([]byte, 0, int(min(hint, maxBytes))+bytes.MinRead))
	_, readErr := buffer.ReadFrom(io.LimitReader(response.Body, maxBytes+1))
	if err := errors.Join(readErr, closeBody(response.Body)); err != nil {
		return blobstore.Object{}, err
	}
	read := int64(buffer.Len())
	if read > maxBytes {
		return blobstore.Object{}, blobstore.ErrTooLarge
	}
	if response.ContentLength != nil && *response.ContentLength != read {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate,
			fmt.Errorf("azure: read %d bytes, content length %d", read, *response.ContentLength))
	}
	token, ok := etagOf(response.ETag)
	if !ok {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, errors.New("azure: download response has no ETag"))
	}
	return blobstore.Object{Key: key, Body: buffer.Bytes(), Token: token}, nil
}

// upload issues exactly one Put Blob request with a small in-memory body.
func (s *Store) upload(ctx context.Context, key string, body []byte, conditions *blob.AccessConditions) (string, error) {
	response, err := s.container.NewBlockBlobClient(key).Upload(once(ctx),
		streaming.NopCloser(bytes.NewReader(body)), &blockblob.UploadOptions{AccessConditions: conditions})
	if err != nil {
		return "", err
	}
	token, ok := etagOf(response.ETag)
	if !ok {
		return "", errors.Join(blobstore.ErrIndeterminate, errors.New("azure: upload response has no ETag"))
	}
	return token, nil
}

func (s *Store) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	if int64(len(body)) > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	token, err := s.upload(ctx, key, body, ifNoneMatchAny())
	if err == nil {
		return blobstore.Object{Key: key, Body: bytes.Clone(body), Token: token}, nil
	}
	if !isConditionRejected(err) {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	existing, getErr := s.BoundedGet(ctx, key, int64(max(len(body), 1)))
	switch {
	case getErr == nil && bytes.Equal(existing.Body, body):
		return existing, nil
	case getErr == nil || errors.Is(getErr, blobstore.ErrTooLarge):
		return blobstore.Object{}, errors.Join(blobstore.ErrImmutableConflict, err)
	default:
		// The write was rejected, but the winner could not be compared.
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, getErr, err)
	}
}

func (s *Store) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.CASResult, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.CASResult{}, err
	}
	if int64(len(body)) > blobstore.MaxMetadataBytes || (expectedToken != "" && !validETag(expectedToken)) {
		return blobstore.CASResult{}, blobstore.ErrInvalidRequest
	}
	conditions := ifNoneMatchAny()
	if expectedToken != "" {
		conditions = ifMatch(expectedToken)
	}
	token, err := s.upload(ctx, key, body, conditions)
	if err == nil {
		return blobstore.CASResult{Outcome: blobstore.CASApplied, Object: blobstore.Object{Key: key, Token: token}}, nil
	}
	// If-Match against a missing blob is as definite a rejection as a 412.
	if !isConditionRejected(err) && !(expectedToken != "" && isNotFound(err)) {
		if errors.Is(err, blobstore.ErrIndeterminate) {
			return blobstore.CASResult{}, err
		}
		return blobstore.CASResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	result := blobstore.CASResult{Outcome: blobstore.CASConflict}
	if ctx.Err() != nil {
		return result, nil
	}
	properties, statErr := s.container.NewBlobClient(key).GetProperties(once(ctx), nil)
	if statErr != nil || properties.ContentLength == nil || *properties.ContentLength < 0 {
		return result, nil
	}
	current, ok := etagOf(properties.ETag)
	if !ok {
		return result, nil
	}
	result.Current = blobstore.ObjectInfo{Key: key, Size: *properties.ContentLength}
	result.CurrentToken, result.CurrentKnown = current, true
	return result, nil
}

func validListBound(value string) bool {
	if len(value) > blobstore.MaxKeyBytes || !utf8.ValidString(value) {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] == 0x7f {
			return false
		}
	}
	return true
}

func (s *Store) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if ctx == nil || !validListBound(opts.Prefix) || !validListBound(opts.AfterKey) {
		return blobstore.ObjectPage{}, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	limit := opts.NormalizedLimit()
	// Azure has no start-after parameter and its marker is opaque, so the
	// AfterKey cursor is honoured by scanning the prefix from its beginning
	// and skipping names <= AfterKey. See doc.go for the cost.
	request := listRequestMax
	if opts.AfterKey == "" && int32(limit)+1 < request {
		request = int32(limit) + 1
	}
	options := &container.ListBlobsFlatOptions{MaxResults: to.Ptr(request)}
	if opts.Prefix != "" {
		options.Prefix = to.Ptr(opts.Prefix)
	}
	pager := s.container.NewListBlobsFlatPager(options)
	objects := make([]blobstore.ObjectInfo, 0, min(limit+1, 256))
	previous, seen := "", false
scan:
	for pager.More() {
		if err := ctx.Err(); err != nil {
			return blobstore.ObjectPage{}, err
		}
		segment, err := pager.NextPage(once(ctx))
		if err != nil {
			return blobstore.ObjectPage{}, err
		}
		if segment.Segment == nil {
			continue
		}
		for _, item := range segment.Segment.BlobItems {
			if item == nil || item.Name == nil || item.Properties == nil ||
				item.Properties.ContentLength == nil || *item.Properties.ContentLength < 0 {
				return blobstore.ObjectPage{}, errors.Join(blobstore.ErrIndeterminate, errors.New("azure: incomplete list entry"))
			}
			name := *item.Name
			if (seen && name <= previous) || !strings.HasPrefix(name, opts.Prefix) {
				return blobstore.ObjectPage{}, errors.Join(blobstore.ErrIndeterminate,
					fmt.Errorf("azure: listing out of order or outside prefix at %q", name))
			}
			previous, seen = name, true
			if name <= opts.AfterKey || !blobstore.ValidKey(name) {
				continue
			}
			objects = append(objects, blobstore.ObjectInfo{Key: name, Size: *item.Properties.ContentLength})
			if len(objects) > limit {
				break scan
			}
		}
	}
	var page blobstore.ObjectPage
	if len(objects) > limit {
		objects, page.HasMore = objects[:limit], true
		page.NextAfterKey = objects[limit-1].Key
	}
	page.Objects = objects
	return page, nil
}

func deleteOptions(conditions *blob.AccessConditions) *blob.DeleteOptions {
	return &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude), AccessConditions: conditions}
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := begin(ctx, key); err != nil {
		return err
	}
	_, err := s.container.NewBlobClient(key).Delete(once(ctx), deleteOptions(nil))
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// --- run contract ---

type runToken struct {
	ETag      string `json:"etag"`
	VersionID string `json:"version_id,omitempty"`
}

func encodeIdentity(key string, size int64, etag *azcore.ETag, versionID *string) (blobstore.RunIdentity, error) {
	fields := runToken{}
	var ok bool
	if fields.ETag, ok = etagOf(etag); !ok {
		return blobstore.RunIdentity{}, errors.New("azure: response has no ETag")
	}
	if versionID != nil {
		fields.VersionID = *versionID
	}
	token, err := idtoken.Encode(provider, fields)
	if err != nil {
		return blobstore.RunIdentity{}, err
	}
	return blobstore.RunIdentity{Key: key, Size: size, Token: token}, nil
}

func decodeIdentity(identity blobstore.RunIdentity) (runToken, error) {
	var fields runToken
	if err := idtoken.Decode(provider, identity.Token, &fields); err != nil {
		return runToken{}, errors.Join(blobstore.ErrInvalidIdentity, err)
	}
	if !validETag(fields.ETag) || !validListBound(fields.VersionID) {
		return runToken{}, blobstore.ErrInvalidIdentity
	}
	return fields, nil
}

// blockID is base64(nonce[16] || big-endian uint32 index): fixed length,
// attempt-specific, and deterministic within the attempt.
func blockID(nonce [attemptNonceBytes]byte, index uint32) string {
	var raw [attemptNonceBytes + 4]byte
	copy(raw[:], nonce[:])
	binary.BigEndian.PutUint32(raw[attemptNonceBytes:], index)
	return base64.StdEncoding.EncodeToString(raw[:])
}

func (s *Store) Create(ctx context.Context, key string, body io.Reader, exactSize int64) (blobstore.CreateResult, error) {
	absent := blobstore.CreateResult{Outcome: blobstore.DefinitelyAbsent}
	if body == nil || exactSize < 1 || exactSize > MaxCreateBytes {
		return absent, blobstore.ErrInvalidRequest
	}
	if err := begin(ctx, key); err != nil {
		return absent, err
	}
	var nonce [attemptNonceBytes]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return absent, err
	}
	// exactSize <= MaxCreateBytes, so neither expression can overflow and the
	// block count is at most maxBlocks.
	blocks := (exactSize + stageBlockBytes - 1) / stageBlockBytes
	if blocks < 1 || blocks > maxBlocks {
		return absent, blobstore.ErrInvalidRequest
	}
	client := s.container.NewBlockBlobClient(key)
	counted := blobstore.NewCountingBody(body, exactSize)
	buffer := make([]byte, int(min(exactSize, stageBlockBytes)))
	ids := make([]string, 0, int(blocks))

	// Until CommitBlockList is issued nothing can be visible: staged blocks are
	// not part of any blob, so every failure in this loop is DefinitelyAbsent.
	for remaining := exactSize; remaining > 0; {
		if err := ctx.Err(); err != nil {
			return absent, errors.Join(err, counted.Err())
		}
		chunk := buffer[:int(min(remaining, int64(len(buffer))))]
		if _, err := io.ReadFull(counted, chunk); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return absent, errors.Join(err, counted.Err())
		}
		if err := ctx.Err(); err != nil {
			return absent, errors.Join(err, counted.Err())
		}
		id := blockID(nonce, uint32(len(ids)))
		if _, err := client.StageBlock(once(ctx), id, streaming.NopCloser(bytes.NewReader(chunk)), nil); err != nil {
			return absent, errors.Join(err, counted.Err())
		}
		ids = append(ids, id)
		remaining -= int64(len(chunk))
	}
	// Producer EOF at exactly exactSize is the only commit authorization.
	if !counted.Complete() || int64(len(ids)) != blocks {
		return absent, errors.Join(io.ErrUnexpectedEOF, counted.Err())
	}
	if err := ctx.Err(); err != nil {
		return absent, err
	}
	response, err := client.CommitBlockList(once(ctx), ids, &blockblob.CommitBlockListOptions{
		AccessConditions: ifNoneMatchAny(),
		HTTPHeaders:      &blob.HTTPHeaders{BlobContentType: to.Ptr(runContentType)},
	})
	if err != nil {
		if isConditionRejected(err) {
			return blobstore.CreateResult{Outcome: blobstore.AlreadyExists}, errors.Join(blobstore.ErrAlreadyExists, err)
		}
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	identity, err := encodeIdentity(key, exactSize, response.ETag, response.VersionID)
	if err != nil {
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return blobstore.CreateResult{Outcome: blobstore.Created, Identity: identity}, nil
}

func (s *Store) Stat(ctx context.Context, key string) (blobstore.RunIdentity, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.RunIdentity{}, err
	}
	properties, err := s.container.NewBlobClient(key).GetProperties(once(ctx), nil)
	if err != nil {
		if isNotFound(err) {
			return blobstore.RunIdentity{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.RunIdentity{}, err
	}
	if properties.ContentLength == nil || *properties.ContentLength < 0 {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, errors.New("azure: properties have no content length"))
	}
	identity, err := encodeIdentity(key, *properties.ContentLength, properties.ETag, properties.VersionID)
	if err != nil {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return identity, nil
}

func (s *Store) OpenRange(ctx context.Context, key string, identity blobstore.RunIdentity, offset, length int64) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, blobstore.ErrInvalidRequest
	}
	if err := blobstore.CheckRange(identity, key, offset, length); err != nil {
		return nil, err
	}
	fields, err := decodeIdentity(identity)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The read targets the current blob, never a versionid URL: on a versioned
	// container a replaced or deleted run would otherwise keep answering, and
	// the contract requires ErrRunChanged. The pinned version is verified
	// against the x-ms-version-id response header instead.
	response, err := s.container.NewBlobClient(key).DownloadStream(once(ctx), &blob.DownloadStreamOptions{
		Range:            blob.HTTPRange{Offset: offset, Count: length},
		AccessConditions: ifMatch(fields.ETag),
	})
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, errors.Join(blobstore.ErrRunChanged, blobstore.ErrNotFound, err)
	case isConditionRejected(err):
		return nil, errors.Join(blobstore.ErrRunChanged, s.missingAfterRejection(ctx, key), err)
	default:
		return nil, err
	}
	// response.Body is the raw HTTP body. NewRetryReader is deliberately not
	// used: it would issue hidden follow-up requests.
	etag, _ := etagOf(response.ETag)
	versionChanged := fields.VersionID != "" && response.VersionID != nil && *response.VersionID != fields.VersionID
	if response.ContentLength == nil || *response.ContentLength != length || etag != fields.ETag || versionChanged {
		return nil, errors.Join(blobstore.ErrRunChanged,
			errors.New("azure: range response does not match the pinned identity"), closeBody(response.Body))
	}
	return &rangeBody{ctx: ctx, body: response.Body, remaining: length}, nil
}

func (s *Store) DeleteIfIdentity(ctx context.Context, key string, identity blobstore.RunIdentity) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	if !identity.Valid() || identity.Key != key {
		return blobstore.ErrInvalidIdentity
	}
	fields, err := decodeIdentity(identity)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The base blob is deleted under If-Match. With versioning or soft delete
	// the service retains the prior version; that is still a correct delete.
	_, err = s.container.NewBlobClient(key).Delete(once(ctx), deleteOptions(ifMatch(fields.ETag)))
	switch {
	case err == nil:
		return nil
	case isNotFound(err):
		return errors.Join(blobstore.ErrNotFound, err)
	case isConditionRejected(err):
		if missing := s.missingAfterRejection(ctx, key); missing != nil {
			return errors.Join(missing, err)
		}
		return errors.Join(blobstore.ErrRunChanged, err)
	default:
		return errors.Join(blobstore.ErrIndeterminate, err)
	}
}

// missingAfterRejection separates "replaced" from "missing" after a definite
// If-Match rejection. Azure answers a conditional request on a missing blob
// with 404, but Azurite answers 412, so one read-only Get Blob Properties
// decides. It returns ErrNotFound only for a definite 404 BlobNotFound; any
// other result leaves the rejection classified as a changed identity.
func (s *Store) missingAfterRejection(ctx context.Context, key string) error {
	if ctx.Err() != nil {
		return nil
	}
	if _, err := s.container.NewBlobClient(key).GetProperties(once(ctx), nil); isNotFound(err) {
		return blobstore.ErrNotFound
	}
	return nil
}

// rangeBody delivers exactly the pinned range from the raw response body.
type rangeBody struct {
	ctx       context.Context
	body      io.ReadCloser
	remaining int64
	closeOnce sync.Once
	closed    bool
}

func (r *rangeBody) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("azure: read on closed range body")
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.body.Read(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrNoProgress
	}
	r.remaining -= int64(n)
	switch {
	case r.remaining == 0:
		return n, nil // the next Read reports io.EOF
	case err == io.EOF:
		return n, io.ErrUnexpectedEOF
	default:
		return n, err
	}
}

func (r *rangeBody) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.closed = true
		err = closeBody(r.body)
	})
	return err
}
