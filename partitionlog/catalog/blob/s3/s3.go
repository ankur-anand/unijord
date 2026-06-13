package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type Backend struct {
	client *awss3.Client
	bucket string
}

const maxListKeys = 1000

var _ blob.Backend = (*Backend)(nil)

func NewBackend(client *awss3.Client, bucket string) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("catalog/blob/s3: nil client")
	}
	if bucket == "" {
		return nil, fmt.Errorf("catalog/blob/s3: empty bucket")
	}
	return &Backend{client: client, bucket: bucket}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (blob.Object, error) {
	if key == "" {
		return blob.Object{}, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	out, err := b.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return blob.Object{}, mapError(err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return blob.Object{}, err
	}
	return blob.Object{
		Key:       key,
		Body:      body,
		Token:     aws.ToString(out.ETag),
		CreatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (b *Backend) Put(ctx context.Context, key string, body []byte) (blob.Object, error) {
	if key == "" {
		return blob.Object{}, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	obj, err := b.putObject(ctx, key, body, aws.String("*"), nil)
	if err == nil {
		return obj, nil
	}
	if !isPreconditionError(err) {
		return blob.Object{}, err
	}

	current, getErr := b.Get(ctx, key)
	if getErr != nil {
		return blob.Object{}, getErr
	}
	if !bytes.Equal(current.Body, body) {
		return blob.Object{}, fmt.Errorf("%w: %s", blob.ErrImmutableConflict, key)
	}
	return current, nil
}

func (b *Backend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (blob.Object, bool, error) {
	if key == "" {
		return blob.Object{}, false, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
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
		return blob.Object{}, false, err
	}
	current, getErr := b.Get(ctx, key)
	if errors.Is(getErr, blob.ErrObjectNotFound) {
		return blob.Object{}, false, nil
	}
	if getErr != nil {
		return blob.Object{}, false, getErr
	}
	return current, false, nil
}

func (b *Backend) List(ctx context.Context, opts blob.ListOptions) (blob.ObjectPage, error) {
	limit := opts.NormalizedLimit()
	if limit > maxListKeys {
		limit = maxListKeys
	}
	out, err := b.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:            aws.String(b.bucket),
		Prefix:            aws.String(opts.Prefix),
		ContinuationToken: stringPtr(opts.Cursor),
		MaxKeys:           aws.Int32(int32(limit)),
	})
	if err != nil {
		return blob.ObjectPage{}, mapError(err)
	}

	objects := make([]blob.ObjectInfo, 0, len(out.Contents))
	for _, item := range out.Contents {
		key := aws.ToString(item.Key)
		if key == "" {
			continue
		}
		if item.Size != nil && *item.Size > int64(math.MaxInt) {
			return blob.ObjectPage{}, fmt.Errorf("%w: object %s size=%d exceeds int", blob.ErrCorruptCatalog, key, *item.Size)
		}
		size := 0
		if item.Size != nil {
			size = int(*item.Size)
		}
		objects = append(objects, blob.ObjectInfo{
			Key:       key,
			Token:     aws.ToString(item.ETag),
			SizeBytes: size,
			CreatedAt: aws.ToTime(item.LastModified),
		})
	}
	return blob.ObjectPage{
		Objects:    objects,
		NextCursor: aws.ToString(out.NextContinuationToken),
		HasMore:    aws.ToBool(out.IsTruncated),
	}, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	_, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	return mapDeleteError(err)
}

func (b *Backend) putObject(ctx context.Context, key string, body []byte, ifNoneMatch *string, ifMatch *string) (blob.Object, error) {
	out, err := b.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String(blob.ObjectContentType),
		IfNoneMatch:   ifNoneMatch,
		IfMatch:       ifMatch,
	})
	if err != nil {
		return blob.Object{}, mapError(err)
	}
	obj := blob.Object{
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
		return fmt.Errorf("%w: %w", blob.ErrObjectNotFound, err)
	}
	if isPreconditionAPIError(err) {
		return fmt.Errorf("%w: %w", blob.ErrImmutableConflict, err)
	}
	return err
}

func isPreconditionError(err error) bool {
	return errors.Is(err, blob.ErrImmutableConflict) || isPreconditionAPIError(err)
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
