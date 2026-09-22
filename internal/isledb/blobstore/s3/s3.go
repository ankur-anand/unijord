// Package s3 is the AWS S3 leaf of blobstore. It talks to S3 through the AWS
// SDK for Go v2 only: no transfer manager, no multipart upload, and no hidden
// retry. Every request is issued exactly once with aws.NopRetryer; all retry
// and reconciliation policy belongs to callers.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
)

const (
	provider = "s3"
	// MaxCreateBytes is the S3 single-PutObject ceiling. Create never uses
	// multipart upload, so nothing larger can be stored.
	MaxCreateBytes = int64(5 << 30)

	// maxTokenBytes bounds a caller-supplied metadata token (an ETag) before
	// it is placed in a request header.
	maxTokenBytes = 1024
	// initialUnknownLength is the first allocation for a response that does
	// not announce its length. Growth stays bounded by the maxBytes+1 limiter.
	initialUnknownLength = 4 << 10
	maxEmptyReads        = 128
)

// Store implements both blobstore contracts over one bucket.
type Store struct {
	client *awss3.Client
	bucket string
}

var (
	_ blobstore.MetadataStore = (*Store)(nil)
	_ blobstore.RunStore      = (*Store)(nil)
)

// New binds an explicit client to one bucket. It performs no I/O.
func New(client *awss3.Client, bucket string) (*Store, error) {
	if client == nil {
		return nil, errors.Join(blobstore.ErrInvalidRequest, errors.New("blobstore/s3: nil client"))
	}
	if bucket == "" {
		return nil, errors.Join(blobstore.ErrInvalidRequest, errors.New("blobstore/s3: empty bucket"))
	}
	return &Store{client: client, bucket: bucket}, nil
}

// once is installed on every SDK call. The SDK must never retry, and it must
// not add a payload checksum that would force a forward-only body to be
// buffered or sought.
func once(o *awss3.Options) {
	o.Retryer = aws.NopRetryer{}
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
}

// streamingPut additionally swaps the payload-hash middleware for
// UNSIGNED-PAYLOAD. The default signer computes SHA-256 by reading and then
// seeking the body, which would reject a forward-only producer or force an
// object-sized buffer.
func streamingPut(o *awss3.Options) {
	once(o)
	o.APIOptions = append(o.APIOptions, awsv4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
}

// providerAnswer classifies only definite HTTP answers from S3. An error with
// no HTTP response (transport failure, timeout, cancellation, deserialization
// of a broken stream) is never definite, and neither is any 5xx, 503 SlowDown,
// or 429.
type providerAnswer uint8

const (
	answerUncertain providerAnswer = iota
	answerNotFound
	answerConflict
)

func classify(err error) providerAnswer {
	var response *smithyhttp.ResponseError
	if !errors.As(err, &response) {
		return answerUncertain
	}
	code := ""
	var api smithy.APIError
	if errors.As(err, &api) {
		code = api.ErrorCode()
	}
	switch status := response.HTTPStatusCode(); {
	case status == 412:
		return answerConflict
	case status == 409 && code == "ConditionalRequestConflict":
		return answerConflict
	case status == 404 && code != "NoSuchBucket":
		// NoSuchKey, NotFound (bodyless HEAD), or a bare 404.
		return answerNotFound
	}
	return answerUncertain
}

func begin(ctx context.Context, key string) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	return ctx.Err()
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

// ---------------------------------------------------------------- metadata

func (s *Store) BoundedGet(ctx context.Context, key string, maxBytes int64) (blobstore.Object, error) {
	if maxBytes < 1 || maxBytes > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	return s.get(ctx, key, maxBytes)
}

// get reads at most maxBytes (>= 1). The caller has validated ctx and key.
func (s *Store) get(ctx context.Context, key string, maxBytes int64) (blobstore.Object, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, once)
	if err != nil {
		if classify(err) == answerNotFound {
			return blobstore.Object{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.Object{}, err
	}
	announced := int64(-1)
	if out.ContentLength != nil && *out.ContentLength >= 0 {
		announced = *out.ContentLength
	}
	if announced > maxBytes {
		return blobstore.Object{}, errors.Join(blobstore.ErrTooLarge, closeBody(out.Body))
	}
	data, readErr := readBounded(out.Body, announced, maxBytes)
	if err := errors.Join(readErr, closeBody(out.Body)); err != nil {
		return blobstore.Object{}, err
	}
	token := aws.ToString(out.ETag)
	if token == "" {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: GetObject response has no ETag"))
	}
	return blobstore.Object{Key: key, Body: data, Token: token}, nil
}

// readBounded never reads more than maxBytes+1 bytes and never allocates from
// an unverified length beyond maxBytes. announced < 0 means unknown.
func readBounded(body io.Reader, announced, maxBytes int64) ([]byte, error) {
	if body == nil {
		return nil, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: response has no body"))
	}
	capacity := min(int64(initialUnknownLength), maxBytes)
	if announced >= 0 {
		capacity = min(announced, maxBytes)
	}
	limited := io.LimitReader(body, maxBytes+1) // maxBytes <= 64 MiB: no overflow
	data := make([]byte, 0, int(capacity))
	var probe [1]byte
	for empty := 0; ; {
		window := data[len(data):cap(data)]
		if len(window) == 0 {
			// Probe for EOF without growing a correctly sized buffer.
			window = probe[:]
		}
		n, err := limited.Read(window)
		if n > 0 {
			empty = 0
			if len(data) == cap(data) {
				data = append(data, probe[0])
			} else {
				data = data[:len(data)+n]
			}
		}
		if int64(len(data)) > maxBytes {
			return nil, blobstore.ErrTooLarge
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			if empty++; empty >= maxEmptyReads {
				return nil, io.ErrNoProgress
			}
		}
	}
	if announced >= 0 && int64(len(data)) != announced {
		return nil, errors.Join(blobstore.ErrIndeterminate,
			fmt.Errorf("blobstore/s3: read %d bytes, Content-Length announced %d", len(data), announced))
	}
	return data, nil
}

func (s *Store) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	out, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		IfNoneMatch:   aws.String("*"),
	}, once)
	if err == nil {
		token := aws.ToString(out.ETag)
		if token == "" {
			return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: PutObject response has no ETag"))
		}
		return blobstore.Object{Key: key, Body: bytes.Clone(body), Token: token}, nil
	}
	if classify(err) != answerConflict {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	// Definitely rejected: an object exists. A bound of len(body) is enough to
	// prove equality; anything longer is different by definition.
	existing, getErr := s.get(ctx, key, max(int64(len(body)), 1))
	switch {
	case getErr == nil && bytes.Equal(existing.Body, body):
		return existing, nil
	case getErr == nil || errors.Is(getErr, blobstore.ErrTooLarge):
		return blobstore.Object{}, errors.Join(blobstore.ErrImmutableConflict, err)
	default:
		// The winner could not be read (it may even have been deleted since),
		// so neither replay nor conflict is proven.
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, getErr, err)
	}
}

func validToken(token string) bool {
	if len(token) > maxTokenBytes {
		return false
	}
	for i := 0; i < len(token); i++ {
		if token[i] < 0x20 || token[i] == 0x7f {
			return false
		}
	}
	return true
}

func (s *Store) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.CASResult, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.CASResult{}, err
	}
	if !validToken(expectedToken) {
		return blobstore.CASResult{}, blobstore.ErrInvalidRequest
	}
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	if expectedToken == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(expectedToken)
	}
	out, err := s.client.PutObject(ctx, in, once)
	if err == nil {
		token := aws.ToString(out.ETag)
		if token == "" {
			return blobstore.CASResult{}, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: applied CAS returned no ETag"))
		}
		return blobstore.CASResult{Outcome: blobstore.CASApplied, Object: blobstore.Object{Key: key, Token: token}}, nil
	}
	answer := classify(err)
	// S3 answers If-Match on a missing key with 404 NoSuchKey. That is a
	// definite rejection. A 404 for If-None-Match is not about the condition.
	if answer != answerConflict && !(answer == answerNotFound && expectedToken != "") {
		return blobstore.CASResult{}, err
	}
	result := blobstore.CASResult{Outcome: blobstore.CASConflict}
	// One follow-up stat. Its failure never demotes the definite rejection.
	head, headErr := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, once)
	if headErr == nil && head.ContentLength != nil && *head.ContentLength >= 0 && aws.ToString(head.ETag) != "" {
		result.Current = blobstore.ObjectInfo{Key: key, Size: *head.ContentLength}
		result.CurrentToken, result.CurrentKnown = *head.ETag, true
	}
	return result, nil
}

func (s *Store) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if ctx == nil || len(opts.Prefix) > blobstore.MaxKeyBytes || len(opts.AfterKey) > blobstore.MaxKeyBytes {
		return blobstore.ObjectPage{}, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	limit := opts.NormalizedLimit()
	// One extra key proves HasMore without a second request, except at the
	// 1,000-key S3 ceiling where IsTruncated is the only evidence.
	request := min(limit+1, blobstore.MaxListLimit)
	in := &awss3.ListObjectsV2Input{Bucket: aws.String(s.bucket), MaxKeys: aws.Int32(int32(request))}
	if opts.Prefix != "" {
		in.Prefix = aws.String(opts.Prefix)
	}
	if opts.AfterKey != "" {
		in.StartAfter = aws.String(opts.AfterKey)
	}
	out, err := s.client.ListObjectsV2(ctx, in, once)
	if err != nil {
		return blobstore.ObjectPage{}, err
	}
	page := blobstore.ObjectPage{HasMore: aws.ToBool(out.IsTruncated)}
	contents := out.Contents
	if len(contents) > limit {
		contents, page.HasMore = contents[:limit], true
	}
	page.Objects = make([]blobstore.ObjectInfo, 0, len(contents))
	previous := opts.AfterKey
	for _, object := range contents {
		key := aws.ToString(object.Key)
		// A cursor-based listing is only sound over a strictly ascending
		// page inside the requested window.
		if key <= previous || !strings.HasPrefix(key, opts.Prefix) {
			return blobstore.ObjectPage{}, errors.Join(blobstore.ErrIndeterminate,
				fmt.Errorf("blobstore/s3: listing returned key %q out of order or outside the request", key))
		}
		previous = key
		page.Objects = append(page.Objects, blobstore.ObjectInfo{Key: key, Size: aws.ToInt64(object.Size)})
	}
	if page.HasMore {
		if len(page.Objects) == 0 {
			return blobstore.ObjectPage{}, errors.Join(blobstore.ErrIndeterminate,
				errors.New("blobstore/s3: truncated listing returned no keys"))
		}
		page.NextAfterKey = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := begin(ctx, key); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, once)
	if err != nil && classify(err) == answerNotFound {
		return nil
	}
	return err
}

// --------------------------------------------------------------------- runs

type runToken struct {
	ETag      string `json:"etag"`
	VersionID string `json:"version_id,omitempty"`
}

func encodeIdentity(key string, size int64, etag, versionID *string) (blobstore.RunIdentity, error) {
	fields := runToken{ETag: aws.ToString(etag), VersionID: aws.ToString(versionID)}
	if fields.ETag == "" || size < 1 {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/s3: response lacks the ETag or size a run identity needs"))
	}
	token, err := idtoken.Encode(provider, fields)
	if err != nil {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return blobstore.RunIdentity{Key: key, Size: size, Token: token}, nil
}

func decodeIdentity(id blobstore.RunIdentity) (runToken, error) {
	var fields runToken
	if err := idtoken.Decode(provider, id.Token, &fields); err != nil {
		return runToken{}, errors.Join(blobstore.ErrInvalidIdentity, err)
	}
	if fields.ETag == "" || !validToken(fields.ETag) || !validToken(fields.VersionID) {
		return runToken{}, blobstore.ErrInvalidIdentity
	}
	return fields, nil
}

// exclusiveBody serialises the transport's reads of the producer with the
// final accounting. net/http may keep reading a request body after RoundTrip
// has returned; stop makes every later Read fail without touching the
// producer, so the sampled counters are final and the caller's reader is never
// used after Create returns. It deliberately exposes Read only: the SDK must
// not discover Seek, Len, or WriteTo.
type exclusiveBody struct {
	mu      sync.Mutex
	counted *blobstore.CountingBody
	stopped bool
}

var errBodyStopped = errors.New("blobstore/s3: request body read after the request ended")

func (b *exclusiveBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return 0, errBodyStopped
	}
	return b.counted.Read(p)
}

func (b *exclusiveBody) stop() (consumed int64, complete bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
	return b.counted.Consumed(), b.counted.Complete(), b.counted.Err()
}

func (s *Store) Create(ctx context.Context, key string, body io.Reader, exactSize int64) (blobstore.CreateResult, error) {
	absent := blobstore.CreateResult{Outcome: blobstore.DefinitelyAbsent}
	if ctx == nil || !blobstore.ValidKey(key) || body == nil || exactSize < 1 || exactSize > MaxCreateBytes {
		return absent, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return absent, err
	}
	stream := &exclusiveBody{counted: blobstore.NewCountingBody(body, exactSize)}
	out, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          stream,
		ContentLength: aws.Int64(exactSize),
		ContentType:   aws.String("application/octet-stream"),
		IfNoneMatch:   aws.String("*"),
	}, streamingPut)
	consumed, complete, producerErr := stream.stop()
	if err == nil {
		if !complete {
			// S3 acknowledged a body this producer never finished.
			return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, producerErr,
				errors.New("blobstore/s3: PutObject succeeded before producer EOF"))
		}
		id, idErr := encodeIdentity(key, exactSize, out.ETag, out.VersionId)
		if idErr != nil {
			return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, idErr)
		}
		return blobstore.CreateResult{Outcome: blobstore.Created, Identity: id}, nil
	}
	switch {
	case classify(err) == answerConflict:
		return blobstore.CreateResult{Outcome: blobstore.AlreadyExists}, errors.Join(blobstore.ErrAlreadyExists, producerErr, err)
	case consumed < exactSize:
		// CountingBody withholds the final byte until producer EOF, so the
		// HTTP stack never held a complete body and S3 never commits a
		// partial PutObject.
		return absent, errors.Join(producerErr, err)
	default:
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, producerErr, err)
	}
}

func (s *Store) Stat(ctx context.Context, key string) (blobstore.RunIdentity, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.RunIdentity{}, err
	}
	return s.stat(ctx, key)
}

func (s *Store) stat(ctx context.Context, key string) (blobstore.RunIdentity, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, once)
	if err != nil {
		if classify(err) == answerNotFound {
			return blobstore.RunIdentity{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.RunIdentity{}, err
	}
	if out.ContentLength == nil {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: HeadObject response has no Content-Length"))
	}
	return encodeIdentity(key, *out.ContentLength, out.ETag, out.VersionId)
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
	// CheckRange proved offset+length <= Size, so this cannot overflow.
	last := offset + length - 1
	in := &awss3.GetObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(key),
		Range:   aws.String("bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(last, 10)),
		IfMatch: aws.String(fields.ETag),
	}
	if fields.VersionID != "" {
		in.VersionId = aws.String(fields.VersionID)
	}
	out, err := s.client.GetObject(ctx, in, once)
	if err != nil {
		switch classify(err) {
		case answerConflict:
			return nil, errors.Join(blobstore.ErrRunChanged, err)
		case answerNotFound:
			return nil, errors.Join(blobstore.ErrRunChanged, blobstore.ErrNotFound, err)
		}
		return nil, err
	}
	if mismatch := verifyRange(out, fields, identity.Size, offset, last); mismatch != nil {
		return nil, errors.Join(blobstore.ErrRunChanged, mismatch, closeBody(out.Body))
	}
	if out.Body == nil {
		return nil, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/s3: range response has no body"))
	}
	return &rangeBody{ctx: ctx, body: out.Body, remaining: length}, nil
}

func verifyRange(out *awss3.GetObjectOutput, fields runToken, size, first, last int64) error {
	if out.ContentLength == nil || *out.ContentLength != last-first+1 {
		return errors.New("blobstore/s3: range response length differs from the request")
	}
	if aws.ToString(out.ETag) != fields.ETag {
		return errors.New("blobstore/s3: range response ETag differs from the identity")
	}
	if fields.VersionID != "" && aws.ToString(out.VersionId) != fields.VersionID {
		return errors.New("blobstore/s3: range response version differs from the identity")
	}
	// Content-Range is "bytes first-last/total". When present it must describe
	// exactly the pinned window of an object of the pinned size.
	if got := aws.ToString(out.ContentRange); got != "" {
		want := "bytes " + strconv.FormatInt(first, 10) + "-" + strconv.FormatInt(last, 10) + "/" + strconv.FormatInt(size, 10)
		if got != want {
			return fmt.Errorf("blobstore/s3: Content-Range %q, want %q", got, want)
		}
	}
	return nil
}

// rangeBody yields exactly the pinned length or an error.
type rangeBody struct {
	ctx       context.Context
	body      io.ReadCloser
	remaining int64
	once      sync.Once
	closeErr  error
}

func (b *rangeBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if b.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.body.Read(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrNoProgress
	}
	b.remaining -= int64(n)
	if err == io.EOF && b.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	// When remaining reaches zero without EOF the next call reports EOF; the
	// provider is never read past the pinned length.
	return n, err
}

func (b *rangeBody) Close() error {
	b.once.Do(func() { b.closeErr = closeBody(b.body) })
	return b.closeErr
}

// DeleteIfIdentity deletes key only while it still has the supplied identity.
//
// A plain S3 DeleteObject answers 204 for a key that does not exist, so a
// successful conditional delete alone cannot distinguish "deleted" from
// "was already missing", and the contract requires ErrNotFound to be
// distinguishable. One HeadObject is therefore issued first: 404 is
// ErrNotFound and an ETag, version, or size mismatch is ErrRunChanged. The
// delete that follows still carries If-Match (and versionId), so a
// replacement that races between the head and the delete is never removed.
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
	current, err := s.stat(ctx, key)
	if err != nil {
		return err // ErrNotFound, or an uncertain read that deleted nothing
	}
	if !current.Equal(identity) {
		return blobstore.ErrRunChanged
	}
	in := &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), IfMatch: aws.String(fields.ETag)}
	if fields.VersionID != "" {
		in.VersionId = aws.String(fields.VersionID)
	}
	if _, err := s.client.DeleteObject(ctx, in, once); err != nil {
		switch classify(err) {
		case answerConflict:
			return errors.Join(blobstore.ErrRunChanged, err)
		case answerNotFound:
			return errors.Join(blobstore.ErrNotFound, err)
		}
		return errors.Join(blobstore.ErrIndeterminate, err)
	}
	return nil
}
