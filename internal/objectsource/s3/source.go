// Package s3 adapts one immutable S3 object to objectsource.RangeSource.
//
// New performs one HeadObject and binds the source to the returned object
// version. Range reads use both VersionId, when available, and If-Match with
// the observed ETag. A key replacement therefore cannot splice bytes from two
// object generations into one UJPK reader.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ankur-anand/unijord/internal/objectsource"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var (
	ErrInvalidOptions = errors.New("objectsource/s3: invalid options")
	ErrInvalidObject  = errors.New("objectsource/s3: invalid object")
	ErrInvalidRange   = errors.New("objectsource/s3: invalid range")
	ErrObjectChanged  = errors.New("objectsource/s3: object changed")
)

// Client is the subset of the AWS S3 client used by Source.
type Client interface {
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// Source is bound to one object generation at construction. It is safe for
// concurrent reads provided Client is safe for concurrent use.
type Source struct {
	client    Client
	bucket    string
	key       string
	size      uint64
	etag      string
	versionID string
}

var _ objectsource.RangeSource = (*Source)(nil)

// New inspects bucket/key once and binds all future reads to that object.
// S3-compatible implementations must return a non-empty ETag.
func New(ctx context.Context, client Client, bucket, key string) (*Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidOptions)
	}
	if bucket == "" {
		return nil, fmt.Errorf("%w: empty bucket", ErrInvalidOptions)
	}
	if key == "" {
		return nil, fmt.Errorf("%w: empty key", ErrInvalidOptions)
	}

	out, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("objectsource/s3: head object: %w", err)
	}
	if out == nil || out.ContentLength == nil || *out.ContentLength < 0 {
		return nil, fmt.Errorf("%w: missing or negative content length", ErrInvalidObject)
	}
	etag := aws.ToString(out.ETag)
	if etag == "" {
		return nil, fmt.Errorf("%w: empty ETag", ErrInvalidObject)
	}

	return &Source{
		client:    client,
		bucket:    bucket,
		key:       key,
		size:      uint64(*out.ContentLength),
		etag:      etag,
		versionID: aws.ToString(out.VersionId),
	}, nil
}

// Size returns the size captured by New without another provider request.
func (s *Source) Size(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.size, nil
}

// ReadRange returns exactly length bytes from the object generation captured
// by New. A replacement at the same key is reported as ErrObjectChanged.
func (s *Source) ReadRange(ctx context.Context, offset, length uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if length == 0 {
		if offset > s.size {
			return nil, fmt.Errorf("%w: offset=%d length=0 size=%d", ErrInvalidRange, offset, s.size)
		}
		return []byte{}, nil
	}
	if offset > s.size || length > s.size-offset {
		return nil, fmt.Errorf("%w: offset=%d length=%d size=%d", ErrInvalidRange, offset, length, s.size)
	}
	if offset > math.MaxInt64 || length >= math.MaxInt64 || offset > uint64(math.MaxInt64)-(length-1) {
		return nil, fmt.Errorf("%w: range overflows int64 offset=%d length=%d", ErrInvalidRange, offset, length)
	}

	end := offset + length - 1
	in := &awss3.GetObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(s.key),
		Range:   aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
		IfMatch: aws.String(s.etag),
	}
	if s.versionID != "" {
		in.VersionId = aws.String(s.versionID)
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		if isPreconditionFailure(err) {
			return nil, fmt.Errorf("%w: bucket=%q key=%q: %w", ErrObjectChanged, s.bucket, s.key, err)
		}
		return nil, fmt.Errorf("objectsource/s3: get range offset=%d length=%d: %w", offset, length, err)
	}
	if out == nil || out.Body == nil {
		return nil, fmt.Errorf("%w: range response has no body", ErrInvalidObject)
	}

	data, readErr := io.ReadAll(io.LimitReader(out.Body, int64(length)+1))
	closeErr := out.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("objectsource/s3: read range body: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("objectsource/s3: close range body: %w", closeErr)
	}
	if uint64(len(data)) != length {
		return nil, fmt.Errorf("%w: requested=%d received=%d", ErrInvalidObject, length, len(data))
	}
	if out.ContentLength != nil && (*out.ContentLength < 0 || uint64(*out.ContentLength) != length) {
		return nil, fmt.Errorf("%w: content length=%d requested=%d", ErrInvalidObject, aws.ToInt64(out.ContentLength), length)
	}
	if got := aws.ToString(out.ETag); got == "" || got != s.etag {
		return nil, fmt.Errorf("%w: response ETag=%q expected=%q", ErrObjectChanged, got, s.etag)
	}
	if s.versionID != "" && aws.ToString(out.VersionId) != s.versionID {
		return nil, fmt.Errorf("%w: response version=%q expected=%q", ErrObjectChanged, aws.ToString(out.VersionId), s.versionID)
	}
	return data, nil
}

func isPreconditionFailure(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict":
		return true
	default:
		return false
	}
}
