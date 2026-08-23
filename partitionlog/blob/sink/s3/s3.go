package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var limits = multipart.Limits{
	MinPartSize:   5 << 20,
	MaxPartSize:   5 << 30,
	MaxPartCount:  10_000,
	MaxObjectSize: (5 << 30) * 10_000,
}

type s3API interface {
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
}

type Store struct {
	client s3API
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

func (s *Store) Limits() multipart.Limits { return limits }

func (s *Store) Begin(ctx context.Context, key string, opts multipart.Options) (multipart.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := multipart.NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	out, err := s.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(opts.ContentType),
		Metadata:    multipart.SessionMetadata(opts),
	})
	if err != nil {
		return nil, mapError(err)
	}
	if out.UploadId == nil || *out.UploadId == "" {
		return nil, fmt.Errorf("%w: create multipart returned empty upload id", multipart.ErrInvalidStore)
	}
	return &session{
		client:   s.client,
		bucket:   s.bucket,
		key:      key,
		opts:     opts,
		uploadID: *out.UploadId,
		parts:    make(map[int]*s3Part),
	}, nil
}

type session struct {
	mu sync.Mutex

	client   s3API
	bucket   string
	key      string
	opts     multipart.Options
	uploadID string
	parts    map[int]*s3Part

	committing  bool
	committed   *multipart.ObjectAttrs
	cleaning    bool
	cleaned     bool
	cleanupDone chan struct{}
	cleanupErr  error

	partsInFlight int
	partsDone     chan struct{}
}

type s3Part struct {
	checksum [32]byte
	done     chan struct{}
	doneOnce sync.Once
	receipt  multipart.Receipt
	complete bool
}

func (u *session) Limits() multipart.Limits { return limits }

func (u *session) PutPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	if err := multipart.ValidatePartLimits(part, limits); err != nil {
		return multipart.Receipt{}, err
	}
	for {
		entry, owner, cached, err := u.reservePart(part)
		if err != nil {
			return multipart.Receipt{}, err
		}
		if cached != nil {
			return *cached, nil
		}
		if !owner {
			select {
			case <-entry.done:
				continue
			case <-ctx.Done():
				return multipart.Receipt{}, ctx.Err()
			}
		}

		partNumber := int32(part.Number)
		out, uploadErr := u.client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:        aws.String(u.bucket),
			Key:           aws.String(u.key),
			UploadId:      aws.String(u.uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          bytes.NewReader(part.Bytes),
			ContentLength: aws.Int64(int64(len(part.Bytes))),
		})
		if uploadErr != nil {
			u.finishPart(part.Number, entry, multipart.Receipt{}, false)
			return multipart.Receipt{}, mapError(uploadErr)
		}
		receipt := multipart.Receipt{
			Number:         part.Number,
			Token:          aws.ToString(out.ETag),
			SizeBytes:      uint64(len(part.Bytes)),
			ChecksumSHA256: part.ChecksumSHA256,
		}
		if err := u.finishPart(part.Number, entry, receipt, true); err != nil {
			return multipart.Receipt{}, err
		}
		return receipt, nil
	}
}

func (u *session) Commit(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateCommitRequest(request, limits); err != nil {
		return multipart.ObjectAttrs{}, err
	}
	if attrs, done, err := u.beginCommit(request); done || err != nil {
		return attrs, err
	}

	parts := make([]types.CompletedPart, 0, len(request.Receipts))
	for _, receipt := range request.Receipts {
		partNumber := int32(receipt.Number)
		parts = append(parts, types.CompletedPart{ETag: aws.String(receipt.Token), PartNumber: aws.Int32(partNumber)})
	}
	out, commitErr := u.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
		IfNoneMatch: aws.String("*"),
	})
	if commitErr != nil {
		return u.finishCommitError(ctx, request, mapError(commitErr))
	}

	attrs := multipart.ObjectAttrs{
		Key:          u.key,
		SizeBytes:    request.SizeBytes,
		Token:        aws.ToString(out.ETag),
		SessionID:    u.opts.SessionID,
		ObjectSHA256: request.ObjectSHA256,
	}
	if key := aws.ToString(out.Key); key != "" {
		attrs.Key = key
	}
	if head, err := u.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(u.bucket), Key: aws.String(u.key)}); err == nil {
		if head.ContentLength != nil && *head.ContentLength >= 0 {
			attrs.SizeBytes = uint64(*head.ContentLength)
		}
		if head.ETag != nil {
			attrs.Token = *head.ETag
		}
	}
	if attrs.SizeBytes != request.SizeBytes {
		u.endCommit(nil)
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: s3 committed size=%d want=%d", multipart.ErrCommitIndeterminate, attrs.SizeBytes, request.SizeBytes)
	}
	u.endCommit(&attrs)
	return attrs, nil
}

func (u *session) Cleanup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	if u.committed != nil {
		u.mu.Unlock()
		return nil
	}
	if u.committing {
		u.mu.Unlock()
		return multipart.ErrCommitInProgress
	}
	if u.cleaning {
		done := u.cleanupDone
		u.mu.Unlock()
		select {
		case <-done:
			u.mu.Lock()
			err := u.cleanupErr
			u.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if u.cleaned && u.cleanupErr == nil {
		u.mu.Unlock()
		return nil
	}
	u.cleaning = true
	u.cleaned = true
	u.cleanupDone = make(chan struct{})
	done := u.cleanupDone
	partsDone := u.partsDone
	if u.partsInFlight == 0 {
		partsDone = nil
	}
	for _, part := range u.parts {
		part.signal()
	}
	u.mu.Unlock()

	if partsDone != nil {
		select {
		case <-partsDone:
		case <-ctx.Done():
			u.finishCleanup(ctx.Err(), done)
			return ctx.Err()
		}
	}
	_, err := u.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket: aws.String(u.bucket), Key: aws.String(u.key), UploadId: aws.String(u.uploadID),
	})
	err = mapCleanupError(err)
	u.finishCleanup(err, done)
	return err
}

func (u *session) reservePart(part multipart.Part) (*s3Part, bool, *multipart.Receipt, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned || u.cleaning {
		return nil, false, nil, multipart.ErrCleaned
	}
	if u.committed != nil {
		return nil, false, nil, multipart.ErrCommitted
	}
	if u.committing {
		return nil, false, nil, multipart.ErrCommitInProgress
	}
	if existing := u.parts[part.Number]; existing != nil {
		if existing.checksum != part.ChecksumSHA256 {
			return nil, false, nil, fmt.Errorf("%w: part %d", multipart.ErrPartConflict, part.Number)
		}
		if existing.complete {
			receipt := existing.receipt
			return existing, false, &receipt, nil
		}
		return existing, false, nil, nil
	}
	entry := &s3Part{checksum: part.ChecksumSHA256, done: make(chan struct{})}
	u.parts[part.Number] = entry
	if u.partsInFlight == 0 {
		u.partsDone = make(chan struct{})
	}
	u.partsInFlight++
	return entry, true, nil, nil
}

func (u *session) finishPart(number int, entry *s3Part, receipt multipart.Receipt, success bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	defer u.finishPartAttemptLocked()
	current := u.parts[number]
	if current != entry {
		entry.signal()
		return multipart.ErrCleaned
	}
	if !success {
		delete(u.parts, number)
		entry.signal()
		return nil
	}
	if u.cleaned || u.cleaning {
		delete(u.parts, number)
		entry.signal()
		return multipart.ErrCleaned
	}
	entry.receipt = receipt
	entry.complete = true
	entry.signal()
	return nil
}

func (u *session) finishPartAttemptLocked() {
	u.partsInFlight--
	if u.partsInFlight < 0 {
		panic("blob/sink/s3: negative in-flight part count")
	}
	if u.partsInFlight == 0 {
		close(u.partsDone)
	}
}

func (u *session) finishCleanup(err error, done chan struct{}) {
	u.mu.Lock()
	u.cleaning = false
	u.cleanupErr = err
	u.parts = nil
	u.mu.Unlock()
	close(done)
}

func (p *s3Part) signal() {
	p.doneOnce.Do(func() { close(p.done) })
}

func (u *session) beginCommit(request multipart.CommitRequest) (multipart.ObjectAttrs, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned || u.cleaning {
		return multipart.ObjectAttrs{}, true, multipart.ErrCleaned
	}
	if u.committed != nil {
		attrs := *u.committed
		if attrs.SizeBytes != request.SizeBytes || request.ObjectSHA256 != ([32]byte{}) && attrs.ObjectSHA256 != request.ObjectSHA256 {
			return multipart.ObjectAttrs{}, true, multipart.ErrPreconditionFailed
		}
		return attrs, true, nil
	}
	if u.committing {
		return multipart.ObjectAttrs{}, true, multipart.ErrCommitInProgress
	}
	if len(u.parts) != len(request.Receipts) {
		return multipart.ObjectAttrs{}, true, fmt.Errorf("%w: session has %d parts but commit has %d receipts", multipart.ErrPartConflict, len(u.parts), len(request.Receipts))
	}
	for _, receipt := range request.Receipts {
		part := u.parts[receipt.Number]
		if part == nil || !part.complete || part.receipt != receipt {
			return multipart.ObjectAttrs{}, true, fmt.Errorf("%w: receipt mismatch for s3 part %d", multipart.ErrPartConflict, receipt.Number)
		}
	}
	u.committing = true
	return multipart.ObjectAttrs{}, false, nil
}

func (u *session) finishCommitError(ctx context.Context, request multipart.CommitRequest, commitErr error) (multipart.ObjectAttrs, error) {
	attrs, found, reconcileErr := u.reconcile(ctx, request)
	if reconcileErr == nil && found {
		u.endCommit(&attrs)
		return attrs, nil
	}
	u.endCommit(nil)
	if errors.Is(reconcileErr, multipart.ErrPreconditionFailed) {
		return multipart.ObjectAttrs{}, reconcileErr
	}
	if errors.Is(commitErr, multipart.ErrPreconditionFailed) && reconcileErr == nil {
		return multipart.ObjectAttrs{}, commitErr
	}
	return multipart.ObjectAttrs{}, errors.Join(multipart.ErrCommitIndeterminate, commitErr, reconcileErr)
}

func (u *session) reconcile(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, bool, error) {
	head, err := u.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(u.bucket), Key: aws.String(u.key)})
	if err != nil {
		if isNotFound(err) {
			return multipart.ObjectAttrs{}, false, nil
		}
		return multipart.ObjectAttrs{}, false, mapError(err)
	}
	size := uint64(0)
	if head.ContentLength != nil && *head.ContentLength >= 0 {
		size = uint64(*head.ContentLength)
	}
	if !multipart.MatchesCommittedObject(size, head.Metadata, u.opts, request, false) {
		return multipart.ObjectAttrs{}, false, multipart.ErrPreconditionFailed
	}
	return multipart.ObjectAttrs{
		Key: u.key, SizeBytes: size, Token: aws.ToString(head.ETag), SessionID: u.opts.SessionID, ObjectSHA256: request.ObjectSHA256,
	}, true, nil
}

func (u *session) endCommit(attrs *multipart.ObjectAttrs) {
	u.mu.Lock()
	u.committing = false
	if attrs != nil {
		copy := *attrs
		u.committed = &copy
		u.parts = nil
	}
	u.mu.Unlock()
}

func mapCleanupError(err error) error {
	if err == nil || isAPIError(err, "NoSuchUpload") {
		return nil
	}
	return mapError(err)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if isAPIError(err, "PreconditionFailed") || isAPIError(err, "ConditionalRequestConflict") {
		return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
	}
	if isAPIError(err, "NoSuchUpload") {
		return fmt.Errorf("%w: %w", multipart.ErrCleaned, err)
	}
	return err
}

func isNotFound(err error) bool {
	return isAPIError(err, "NotFound") || isAPIError(err, "NoSuchKey")
}

func isAPIError(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}
