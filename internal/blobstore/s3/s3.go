package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ankur-anand/unijord/internal/blobstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type Backend struct {
	client *awss3.Client
	bucket string
}

const maxListKeys = 1000

var _ blobstore.Store = (*Backend)(nil)

func New(client *awss3.Client, bucket string) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("internal/blobstore/s3: nil client")
	}
	if bucket == "" {
		return nil, fmt.Errorf("internal/blobstore/s3: empty bucket")
	}
	return &Backend{client: client, bucket: bucket}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (blobstore.Object, error) {
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	out, err := b.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return blobstore.Object{}, mapError(err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return blobstore.Object{}, err
	}
	return blobstore.Object{
		Key:       key,
		Body:      body,
		Token:     aws.ToString(out.ETag),
		CreatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (b *Backend) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	obj, err := b.putObject(ctx, key, body, aws.String("*"), nil)
	if err == nil {
		return obj, nil
	}
	if !isPreconditionError(err) {
		return blobstore.Object{}, err
	}

	current, getErr := b.Get(ctx, key)
	if getErr != nil {
		return blobstore.Object{}, getErr
	}
	if !bytes.Equal(current.Body, body) {
		return blobstore.Object{}, fmt.Errorf("%w: %s", blobstore.ErrImmutableConflict, key)
	}
	return current, nil
}

func (b *Backend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	if key == "" {
		return blobstore.Object{}, false, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}

	var ifNoneMatch *string
	var ifMatch *string
	if expectedToken == "" {
		ifNoneMatch = aws.String("*")
	} else {
		ifMatch = aws.String(expectedToken)
	}

	obj, err := b.putObject(ctx, key, body, ifNoneMatch, ifMatch)
	if err == nil {
		return obj, true, nil
	}
	if !isPreconditionError(err) {
		return blobstore.Object{}, false, err
	}
	current, getErr := b.Get(ctx, key)
	if errors.Is(getErr, blobstore.ErrObjectNotFound) {
		return blobstore.Object{}, false, nil
	}
	if getErr != nil {
		return blobstore.Object{}, false, getErr
	}
	return current, false, nil
}

func (b *Backend) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	limit := opts.NormalizedLimit()
	if limit > maxListKeys {
		limit = maxListKeys
	}
	out, err := b.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:     aws.String(b.bucket),
		Prefix:     aws.String(opts.Prefix),
		StartAfter: stringPtr(opts.AfterKey),
		MaxKeys:    aws.Int32(int32(limit)),
	})
	if err != nil {
		return blobstore.ObjectPage{}, mapError(err)
	}

	objects := make([]blobstore.ObjectInfo, 0, len(out.Contents))
	for _, item := range out.Contents {
		key := aws.ToString(item.Key)
		if key == "" {
			continue
		}
		if item.Size != nil && *item.Size > int64(math.MaxInt) {
			return blobstore.ObjectPage{}, fmt.Errorf("%w: object %s size=%d exceeds int", blobstore.ErrInvalidRequest, key, *item.Size)
		}
		size := 0
		if item.Size != nil {
			size = int(*item.Size)
		}
		objects = append(objects, blobstore.ObjectInfo{
			Key:       key,
			Token:     aws.ToString(item.ETag),
			SizeBytes: size,
			CreatedAt: aws.ToTime(item.LastModified),
		})
	}
	page := blobstore.ObjectPage{Objects: objects, HasMore: aws.ToBool(out.IsTruncated)}
	if page.HasMore && len(page.Objects) > 0 {
		page.NextAfterKey = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	_, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	return mapDeleteError(err)
}

func (b *Backend) putObject(ctx context.Context, key string, body []byte, ifNoneMatch *string, ifMatch *string) (blobstore.Object, error) {
	out, err := b.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String(blobstore.JSONContentType),
		IfNoneMatch:   ifNoneMatch,
		IfMatch:       ifMatch,
	})
	if err != nil {
		return blobstore.Object{}, mapError(err)
	}
	obj := blobstore.Object{
		Key:   key,
		Body:  bytes.Clone(body),
		Token: aws.ToString(out.ETag),
	}
	if head, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}); err == nil {
		obj.Token = aws.ToString(head.ETag)
		obj.CreatedAt = aws.ToTime(head.LastModified)
	}
	return obj, nil
}

func mapDeleteError(err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundError(err) {
		return nil
	}
	return mapError(err)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundError(err) {
		return fmt.Errorf("%w: %w", blobstore.ErrObjectNotFound, err)
	}
	if isPreconditionAPIError(err) {
		return fmt.Errorf("%w: %w", blobstore.ErrImmutableConflict, err)
	}
	return err
}

func isPreconditionError(err error) bool {
	return errors.Is(err, blobstore.ErrImmutableConflict) || isPreconditionAPIError(err)
}

func isPreconditionAPIError(err error) bool {
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

func isNotFoundError(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NotFound", "NoSuchBucket":
		return true
	default:
		return false
	}
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}
