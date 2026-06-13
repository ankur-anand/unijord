package s3

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blobcatalog"
	"github.com/ankur-anand/unijord/partitionlog/blobcatalog/internal/backendtest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestBackendConformanceWithFakeS3(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, backendtest.Config{
		NewBackend: func(t testing.TB) blobcatalog.Backend {
			t.Helper()
			backend, _ := newFakeBackend(t, "catalog")
			return backend
		},
	})
}

func TestBackendContentTypeWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "catalog"
	backend, client := newFakeBackend(t, bucket)
	obj, err := backend.Put(ctx, "catalog/p00000001/pages/l00/leaf.json", []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj.Key),
	})
	if err != nil {
		t.Fatalf("HeadObject() error = %v", err)
	}
	if aws.ToString(head.ContentType) != blobcatalog.ObjectContentType {
		t.Fatalf("ContentType = %q, want %q", aws.ToString(head.ContentType), blobcatalog.ObjectContentType)
	}
}

func TestBackendRejectsBadInputs(t *testing.T) {
	t.Parallel()

	client := newFakeS3Client(t, "catalog")
	if _, err := NewBackend(nil, "catalog"); err == nil {
		t.Fatal("NewBackend(nil) error = nil, want error")
	}
	if _, err := NewBackend(client, ""); err == nil {
		t.Fatal("NewBackend(empty bucket) error = nil, want error")
	}
}

func newFakeBackend(t testing.TB, bucket string) (*Backend, *awss3.Client) {
	t.Helper()
	client := newFakeS3Client(t, bucket)
	backend, err := NewBackend(client, bucket)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	return backend, client
}

func newFakeS3Client(t testing.TB, bucket string) *awss3.Client {
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
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
}
