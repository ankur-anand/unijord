package s3

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blobsink/multipart"
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
	upload, err := store.BeginMultipart(ctx, "partitionlog/segments/p00000001/test.plseg", multipart.Options{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("hello s3 multipart")})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	attrs, err := upload.Complete(ctx, []multipart.Receipt{receipt})
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

func TestStoreAbortWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "segments"
	client := newFakeS3Client(t, bucket)

	store, err := NewStore(client, bucket)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	upload, err := store.BeginMultipart(ctx, "partitionlog/segments/p00000001/aborted.plseg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart() error = %v", err)
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("abc")})
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if err := upload.Abort(ctx); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if _, err := upload.Complete(ctx, []multipart.Receipt{receipt}); !errors.Is(err, multipart.ErrAborted) {
		t.Fatalf("Complete(after abort) error = %v, want %v", err, multipart.ErrAborted)
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
	if _, err := store.BeginMultipart(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("BeginMultipart(empty key) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
}

func TestMapErrorPreconditionFailure(t *testing.T) {
	t.Parallel()

	err := mapError(&smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"})
	if !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("mapError() = %v, want %v", err, multipart.ErrPreconditionFailed)
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
