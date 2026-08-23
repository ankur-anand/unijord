package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

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
