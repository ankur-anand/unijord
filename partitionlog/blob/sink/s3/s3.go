package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const maxPartNumber = 10_000

type Store struct {
	client *awss3.Client
	bucket string
}

var _ multipart.Store = (*Store)(nil)

func NewStore(client *awss3.Client, bucket string) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil s3 client", multipart.ErrInvalidStore)
	}
	if bucket == "" {
		return nil, fmt.Errorf("%w: empty s3 bucket", multipart.ErrInvalidStore)
	}
	return &Store{client: client, bucket: bucket}, nil
}

func (s *Store) BeginMultipart(ctx context.Context, key string, opts multipart.Options) (multipart.Upload, error) {
	opts, err := multipart.NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	out, err := s.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(opts.ContentType),
	})
	if err != nil {
		return nil, mapError(err)
	}
	if out.UploadId == nil || *out.UploadId == "" {
		return nil, fmt.Errorf("%w: create multipart returned empty upload id", multipart.ErrInvalidStore)
	}
	return &upload{
		client:   s.client,
		bucket:   s.bucket,
		key:      key,
		uploadID: *out.UploadId,
	}, nil
}

type upload struct {
	client   *awss3.Client
	bucket   string
	key      string
	uploadID string
}

func (u *upload) UploadPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	if err := multipart.ValidatePart(part); err != nil {
		return multipart.Receipt{}, err
	}
	if part.Number > maxPartNumber {
		return multipart.Receipt{}, fmt.Errorf("%w: s3 part number=%d max=%d", multipart.ErrInvalidStore, part.Number, maxPartNumber)
	}

	partNumber := int32(part.Number)
	out, err := u.client.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(u.key),
		UploadId:      aws.String(u.uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          bytes.NewReader(part.Bytes),
		ContentLength: aws.Int64(int64(len(part.Bytes))),
	})
	if err != nil {
		return multipart.Receipt{}, mapError(err)
	}
	return multipart.Receipt{
		Number:    part.Number,
		Token:     aws.ToString(out.ETag),
		SizeBytes: uint64(len(part.Bytes)),
	}, nil
}

func (u *upload) Complete(ctx context.Context, receipts []multipart.Receipt) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateReceipts(receipts); err != nil {
		return multipart.ObjectAttrs{}, err
	}

	parts := make([]types.CompletedPart, 0, len(receipts))
	var size uint64
	for _, receipt := range receipts {
		if receipt.Number > maxPartNumber {
			return multipart.ObjectAttrs{}, fmt.Errorf("%w: s3 part number=%d max=%d", multipart.ErrInvalidStore, receipt.Number, maxPartNumber)
		}
		partNumber := int32(receipt.Number)
		parts = append(parts, types.CompletedPart{
			ETag:       aws.String(receipt.Token),
			PartNumber: aws.Int32(partNumber),
		})
		size += receipt.SizeBytes
	}

	out, err := u.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return multipart.ObjectAttrs{}, mapError(err)
	}

	key := aws.ToString(out.Key)
	if key == "" {
		key = u.key
	}
	token := aws.ToString(out.ETag)
	if head, err := u.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(u.key),
	}); err == nil {
		if head.ContentLength != nil && *head.ContentLength >= 0 {
			size = uint64(*head.ContentLength)
		}
		if head.ETag != nil {
			token = *head.ETag
		}
	}

	return multipart.ObjectAttrs{
		Key:       key,
		SizeBytes: size,
		Token:     token,
	}, nil
}

func (u *upload) Abort(ctx context.Context) error {
	_, err := u.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
	})
	return mapAbortError(err)
}

func mapAbortError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return nil
	}
	return mapError(err)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
		case "NoSuchUpload":
			return fmt.Errorf("%w: %w", multipart.ErrAborted, err)
		}
	}
	return err
}
