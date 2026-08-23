package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/internal/sinktest"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestStoreMultipartEndToEndWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "segments"
	client := newFakeS3Client(t, bucket)

	store, err := NewStore(client, bucket)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	upload, err := store.Begin(ctx, "partitionlog/segments/p00000001/test.plseg", multipart.Options{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("hello s3 multipart")))
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	attrs, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt}))
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if attrs.Key != "partitionlog/segments/p00000001/test.plseg" {
		t.Fatalf("attrs.Key = %q", attrs.Key)
	}
	if attrs.SizeBytes != uint64(len("hello s3 multipart")) {
		t.Fatalf("attrs.SizeBytes = %d", attrs.SizeBytes)
	}
	if attrs.Token == "" {
		t.Fatal("attrs.Token is empty")
	}

	got, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(attrs.Key),
	})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "hello s3 multipart" {
		t.Fatalf("body = %q", body)
	}
}

func TestStoreSessionRetryContractWithFakeS3(t *testing.T) {
	client := newFakeS3Client(t, "segments")
	store, err := NewStore(client, "segments")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	sinktest.RunSessionRetryContract(t, store, "partitionlog/retry-contract")
}

func TestS3CommitReconcilesMatchingFinalObject(t *testing.T) {
	ctx := context.Background()
	const (
		bucket    = "segments"
		key       = "partitionlog/reconciled.seg"
		sessionID = "00000000-0000-4000-8000-000000000003"
	)
	client := newFakeS3Client(t, bucket)
	store, err := NewStore(client, bucket)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	started, err := store.Begin(ctx, key, multipart.Options{SessionID: sessionID})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	body := []byte("already committed")
	receipt, err := started.PutPart(ctx, multipart.NewPart(1, body))
	if err != nil {
		t.Fatalf("PutPart() error = %v", err)
	}
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		Metadata: map[string]string{multipart.MetadataSessionID: sessionID},
	}); err != nil {
		t.Fatalf("PutObject(existing final) error = %v", err)
	}
	attrs, err := started.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt}))
	if err != nil {
		t.Fatalf("Commit() error = %v, want reconciled success", err)
	}
	if attrs.SessionID != sessionID || attrs.SizeBytes != uint64(len(body)) {
		t.Fatalf("Commit() attrs = %+v", attrs)
	}
}

func TestStoreCleanupWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "segments"
	client := newFakeS3Client(t, bucket)

	store, err := NewStore(client, bucket)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	upload, err := store.Begin(ctx, "partitionlog/segments/p00000001/aborted.plseg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("abc")))
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if err := upload.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt})); !errors.Is(err, multipart.ErrCleaned) {
		t.Fatalf("Commit(after cleanup) error = %v, want %v", err, multipart.ErrCleaned)
	}
}

func TestCleanupWaitsForOwnedPutPartBeforeAbort(t *testing.T) {
	t.Parallel()

	client := newCleanupOrderS3()
	u := &session{
		client:   client,
		bucket:   "segments",
		key:      "partitionlog/segments/cleanup-order.seg",
		uploadID: "upload-1",
		parts:    make(map[int]*s3Part),
	}

	partResult := make(chan error, 1)
	go func() {
		_, err := u.PutPart(context.Background(), multipart.NewPart(1, []byte("part")))
		partResult <- err
	}()
	waitForS3Signal(t, client.partStarted, "PutPart to reach S3")

	// Cleanup evaluates Done when it starts waiting for the owned PutPart. That
	// event releases the fake part request. An implementation that calls S3
	// AbortMultipartUpload first receives errAbortBeforePartFinished instead.
	cleanupCtx := &doneCallbackContext{
		Context: context.Background(),
		callback: func() {
			client.releasePart()
		},
	}
	if err := u.Cleanup(cleanupCtx); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := receiveS3Error(t, partResult, "PutPart to return"); !errors.Is(err, multipart.ErrCleaned) {
		t.Fatalf("PutPart() error = %v, want ErrCleaned", err)
	}
}

func TestCleanupCanceledWhileWaitingForPartCanBeRetried(t *testing.T) {
	t.Parallel()

	client := newCleanupOrderS3()
	u := &session{
		client:   client,
		bucket:   "segments",
		key:      "partitionlog/segments/cleanup-retry.seg",
		uploadID: "upload-1",
		parts:    make(map[int]*s3Part),
	}
	partResult := make(chan error, 1)
	go func() {
		_, err := u.PutPart(context.Background(), multipart.NewPart(1, []byte("part")))
		partResult <- err
	}()
	waitForS3Signal(t, client.partStarted, "PutPart to reach S3")

	baseCtx, cancel := context.WithCancel(context.Background())
	waitStarted := make(chan struct{})
	cleanupCtx := &doneCallbackContext{
		Context:  baseCtx,
		callback: func() { close(waitStarted) },
	}
	cleanupResult := make(chan error, 1)
	go func() { cleanupResult <- u.Cleanup(cleanupCtx) }()
	waitForS3Signal(t, waitStarted, "Cleanup to wait for the part")
	cancel()
	if err := receiveS3Error(t, cleanupResult, "canceled Cleanup to return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Cleanup(first) error = %v, want context.Canceled", err)
	}

	client.releasePart()
	if err := receiveS3Error(t, partResult, "PutPart to return"); !errors.Is(err, multipart.ErrCleaned) {
		t.Fatalf("PutPart() error = %v, want ErrCleaned", err)
	}
	if err := u.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup(retry) error = %v", err)
	}
}

func TestStoreRejectsBadInputs(t *testing.T) {
	t.Parallel()

	client := newFakeS3Client(t, "segments")
	if _, err := NewStore(nil, "segments"); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("NewStore(nil) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
	if _, err := NewStore(client, ""); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("NewStore(empty bucket) error = %v, want %v", err, multipart.ErrInvalidStore)
	}

	store, err := NewStore(client, "segments")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.Begin(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("BeginMultipart(empty key) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
}

func TestMapErrorPreconditionFailure(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"PreconditionFailed", "ConditionalRequestConflict"} {
		t.Run(code, func(t *testing.T) {
			err := mapError(&smithy.GenericAPIError{Code: code, Message: "conditional write lost"})
			if !errors.Is(err, multipart.ErrPreconditionFailed) {
				t.Fatalf("mapError() = %v, want %v", err, multipart.ErrPreconditionFailed)
			}
		})
	}
}

func newFakeS3Client(t *testing.T, bucket string) *awss3.Client {
	t.Helper()

	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	faker := gofakes3.New(backend)
	server := httptest.NewServer(faker.Server())
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access-key", "secret-key", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
}

func waitForS3Signal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func receiveS3Error(t *testing.T, result <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

var errAbortBeforePartFinished = errors.New("abort reached S3 before the owned part returned")

type cleanupOrderS3 struct {
	partStarted  chan struct{}
	partFinished chan struct{}
	release      chan struct{}
	releaseOnce  sync.Once
}

func newCleanupOrderS3() *cleanupOrderS3 {
	return &cleanupOrderS3{
		partStarted:  make(chan struct{}),
		partFinished: make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (c *cleanupOrderS3) releasePart() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *cleanupOrderS3) CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	panic("unexpected CreateMultipartUpload")
}

func (c *cleanupOrderS3) UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	close(c.partStarted)
	<-c.release
	close(c.partFinished)
	return &awss3.UploadPartOutput{ETag: aws.String("part-etag")}, nil
}

func (c *cleanupOrderS3) CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	panic("unexpected CompleteMultipartUpload")
}

func (c *cleanupOrderS3) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	panic("unexpected HeadObject")
}

func (c *cleanupOrderS3) AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	select {
	case <-c.partFinished:
		return &awss3.AbortMultipartUploadOutput{}, nil
	default:
		c.releasePart()
		return nil, errAbortBeforePartFinished
	}
}

type doneCallbackContext struct {
	context.Context
	once     sync.Once
	callback func()
}

func (c *doneCallbackContext) Done() <-chan struct{} {
	c.once.Do(c.callback)
	return c.Context.Done()
}
