package s3_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/internal/lifecycletest"
	pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var runIntegration = flag.Bool("integration", false, "run integration tests against local object stores")

func TestMinIOLifecycleConformance(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local MinIO")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	lifecycletest.Run(t, ctx, newMinIOStore(t, ctx), lifecycletest.Config{Partition: 701})
}

func TestMinIOLifecycleSoak(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local MinIO")
	}
	if os.Getenv(lifecycletest.SoakEnvironment) == "" {
		t.Skipf("set %s to run the lifecycle soak", lifecycletest.SoakEnvironment)
	}
	lifecycletest.RunSoak(t, context.Background(), newMinIOStore(t, context.Background()), 10_000)
}

func newMinIOStore(t testing.TB, ctx context.Context) *pls3.Store {
	t.Helper()
	endpoint := getenv("PARTITIONLOG_MINIO_ENDPOINT", getenv("CATALOG_BLOB_MINIO_ENDPOINT", "http://127.0.0.1:9000"))
	bucket := getenv("PARTITIONLOG_MINIO_BUCKET", "partitionlog-lifecycle-it")
	accessKey := getenv("PARTITIONLOG_MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minioadmin"))
	secretKey := getenv("PARTITIONLOG_MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

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
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil && !bucketExists(err) {
		t.Fatalf("CreateBucket(%q) error = %v", bucket, err)
	}
	store, err := pls3.New(pls3.Options{
		Client: client, Bucket: bucket,
		Prefix: integrationPrefix(t, "minio"), StreamID: "integration/lifecycle",
	})
	if err != nil {
		t.Fatalf("s3.New() error = %v", err)
	}
	return store
}

func bucketExists(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists"
}

func integrationPrefix(t testing.TB, provider string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("integration/lifecycle/%s/%d/%s", provider, time.Now().UnixNano(), name)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
