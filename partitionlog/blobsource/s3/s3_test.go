package s3

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blobsource/internal/sourcetest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestStoreConformanceWithFakeS3(t *testing.T) {
	t.Parallel()

	sourcetest.Run(t, sourcetest.Config{
		NewFixture: func(t testing.TB) sourcetest.Fixture {
			t.Helper()
			const bucket = "segments"
			const key = "partitionlog/segments/p00000001/test.plseg"
			body := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
			client := newFakeS3Client(t, bucket)
			putS3Object(t, client, bucket, key, body)
			store, err := NewStore(client, bucket)
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			return sourcetest.Fixture{Store: store, Key: key, Body: body}
		},
	})
}

func TestStoreRejectsBadS3ConstructorInputs(t *testing.T) {
	t.Parallel()

	client := newFakeS3Client(t, "segments")
	if _, err := NewStore(nil, "segments"); err == nil {
		t.Fatal("NewStore(nil) error = nil, want error")
	}
	if _, err := NewStore(client, ""); err == nil {
		t.Fatal("NewStore(empty bucket) error = nil, want error")
	}
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

func putS3Object(t testing.TB, client *awss3.Client, bucket string, key string, body []byte) {
	t.Helper()
	_, err := client.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	out, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject() after put error = %v", err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("ReadAll() after put error = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("put object = %q, want %q", got, body)
	}
}

func TestRangeEnd(t *testing.T) {
	t.Parallel()

	end, err := rangeEnd(2, 3)
	if err != nil {
		t.Fatalf("rangeEnd() error = %v", err)
	}
	if end != 4 {
		t.Fatalf("rangeEnd() = %d, want 4", end)
	}
	if _, err := rangeEnd(^uint64(0), 2); err == nil {
		t.Fatal("rangeEnd(overflow) error = nil, want error")
	}
}
