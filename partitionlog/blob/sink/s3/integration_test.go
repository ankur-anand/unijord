package s3

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/internal/sinktest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var runIntegration = flag.Bool("integration", false, "run live integration tests against local object stores")

func TestLiveMinIOStoreAndSegmentWriter(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local MinIO")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	endpoint := getenv("BLOB_SINK_MINIO_ENDPOINT", "http://127.0.0.1:9000")
	bucket := getenv("BLOB_SINK_MINIO_BUCKET", "blob-sink-it")
	accessKey := getenv("BLOB_SINK_MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minioadmin"))
	secretKey := getenv("BLOB_SINK_MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if err := ensureBucket(ctx, client, bucket); err != nil {
		t.Fatalf("ensure bucket %q at %s: %v", bucket, endpoint, err)
	}
	store, err := NewStore(client, bucket)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	prefix := livePrefix(t, "minio")
	read := func(ctx context.Context, key string) (sinktest.Object, error) {
		out, err := client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return sinktest.Object{}, err
		}
		body, err := sinktest.ReadAll(out.Body)
		if err != nil {
			return sinktest.Object{}, err
		}
		size := uint64(len(body))
		if out.ContentLength != nil && *out.ContentLength >= 0 {
			size = uint64(*out.ContentLength)
		}
		return sinktest.Object{
			Body:        body,
			ContentType: aws.ToString(out.ContentType),
			SizeBytes:   size,
		}, nil
	}

	sinktest.RunMultipartStore(t, store, prefix, read)
	sinktest.RunSegmentWriter(t, store, prefix, read)
}

func ensureBucket(ctx context.Context, client *awss3.Client, bucket string) error {
	_, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil || isBucketExists(err) {
		return nil
	}
	return err
}

func isBucketExists(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
		return true
	default:
		return false
	}
}

func livePrefix(t *testing.T, provider string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("integration/%s/%d/%s", provider, time.Now().UnixNano(), name)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
