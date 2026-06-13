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

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	blobcatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var runIntegration = flag.Bool("integration", false, "run live integration tests against local object stores")

func TestLiveMinIOBackendAndCatalog(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local MinIO")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	endpoint := getenv("BLOBCATALOG_MINIO_ENDPOINT", getenv("BLOBSINK_MINIO_ENDPOINT", "http://127.0.0.1:9000"))
	bucket := getenv("BLOBCATALOG_MINIO_BUCKET", "blobcatalog-it")
	accessKey := getenv("BLOBCATALOG_MINIO_ACCESS_KEY", getenv("MINIO_ROOT_USER", "minioadmin"))
	secretKey := getenv("BLOBCATALOG_MINIO_SECRET_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

	client := newLiveMinIOClient(t, ctx, endpoint, accessKey, secretKey)
	if err := ensureBucket(ctx, client, bucket); err != nil {
		t.Fatalf("ensure bucket %q at %s: %v", bucket, endpoint, err)
	}

	backend, err := NewBackend(client, bucket)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	prefix := livePrefix(t, "minio")
	runLiveBackendCAS(t, ctx, backend, prefix)
	runLiveCatalogFlow(t, ctx, backend, prefix)
}

func runLiveBackendCAS(t *testing.T, ctx context.Context, backend *Backend, prefix string) {
	t.Helper()

	immutableKey := prefix + "/pages/l00/leaf.json"
	first, err := backend.Put(ctx, immutableKey, []byte(`{"leaf":1}`))
	if err != nil {
		t.Fatalf("Put(immutable first) error = %v", err)
	}
	if first.Token == "" {
		t.Fatal("Put(immutable first) token is empty")
	}
	replay, err := backend.Put(ctx, immutableKey, []byte(`{"leaf":1}`))
	if err != nil {
		t.Fatalf("Put(immutable replay) error = %v", err)
	}
	if replay.Token != first.Token {
		t.Fatalf("replay token = %q, want %q", replay.Token, first.Token)
	}
	if _, err := backend.Put(ctx, immutableKey, []byte(`{"leaf":2}`)); !errors.Is(err, blobcatalog.ErrImmutableConflict) {
		t.Fatalf("Put(immutable conflict) error = %v, want %v", err, blobcatalog.ErrImmutableConflict)
	}

	headKey := prefix + "/head.json"
	created, swapped, err := backend.CompareAndSwap(ctx, headKey, "", []byte(`{"generation":1}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(create) error = %v", err)
	}
	if !swapped || created.Token == "" {
		t.Fatalf("CompareAndSwap(create) swapped=%v object=%+v", swapped, created)
	}
	current, swapped, err := backend.CompareAndSwap(ctx, headKey, "wrong-token", []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(wrong token) error = %v", err)
	}
	if swapped || current.Token != created.Token {
		t.Fatalf("CompareAndSwap(wrong token) swapped=%v current=%+v created=%+v", swapped, current, created)
	}
	updated, swapped, err := backend.CompareAndSwap(ctx, headKey, created.Token, []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(update) error = %v", err)
	}
	if !swapped || updated.Token == "" || updated.Token == created.Token {
		t.Fatalf("CompareAndSwap(update) swapped=%v updated=%+v created=%+v", swapped, updated, created)
	}
}

func runLiveCatalogFlow(t *testing.T, ctx context.Context, backend *Backend, prefix string) {
	t.Helper()

	cat, err := blobcatalog.New(backend, blobcatalog.Options{
		Prefix:           prefix + "/catalog",
		LeafSegmentLimit: 2,
		IndexRefLimit:    2,
	})
	if err != nil {
		t.Fatalf("blobcatalog.New() error = %v", err)
	}
	ws, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for _, segment := range []pmeta.SegmentRef{
		liveSegmentRef(1, 0, 9, ws.Epoch()),
		liveSegmentRef(1, 10, 19, ws.Epoch()),
		liveSegmentRef(1, 20, 29, ws.Epoch()),
		liveSegmentRef(1, 30, 39, ws.Epoch()),
		liveSegmentRef(1, 40, 49, ws.Epoch()),
	} {
		if _, err := ws.AppendSegment(ctx, segment); err != nil {
			t.Fatalf("AppendSegment(%d-%d) error = %v", segment.BaseLSN, segment.LastLSN, err)
		}
	}

	loaded, err := cat.LoadPartition(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.NextLSN != 50 || loaded.SegmentCount != 5 || loaded.LastSegment.BaseLSN != 40 {
		t.Fatalf("loaded head = %+v", loaded)
	}
	found, ok, err := cat.FindSegment(ctx, 1, 35)
	if err != nil {
		t.Fatalf("FindSegment() error = %v", err)
	}
	if !ok || found.BaseLSN != 30 {
		t.Fatalf("FindSegment(35) = %+v ok=%v, want base 30", found, ok)
	}
	page, err := cat.ListSegments(ctx, pcatalog.ListSegmentsRequest{Partition: 1, FromLSN: 5, Limit: 3})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 3 || !page.HasMore || page.NextLSN != 30 {
		t.Fatalf("ListSegments() = %+v, want 3 segments has_more next_lsn=30", page)
	}

	reopened, err := blobcatalog.New(backend, blobcatalog.Options{
		Prefix:           prefix + "/catalog",
		LeafSegmentLimit: 2,
		IndexRefLimit:    2,
	})
	if err != nil {
		t.Fatalf("blobcatalog.New(reopen) error = %v", err)
	}
	reopenedHead, err := reopened.LoadPartition(ctx, 1)
	if err != nil {
		t.Fatalf("reopened LoadPartition() error = %v", err)
	}
	if reopenedHead != loaded {
		t.Fatalf("reopened head = %+v, want %+v", reopenedHead, loaded)
	}
}

func newLiveMinIOClient(t *testing.T, ctx context.Context, endpoint string, accessKey string, secretKey string) *awss3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
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

func liveSegmentRef(partition uint32, base uint64, last uint64, epoch uint64) pmeta.SegmentRef {
	return pmeta.SegmentRef{
		URI:              fmt.Sprintf("s3://blobcatalog-it/p%08d/%020d-%020d", partition, base, last),
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      [16]byte{byte(partition), byte(base + 1), byte(last + 1), byte(epoch + 1)},
		WriterTag:        [16]byte{7, 7, 7},
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   int64(base),
		MaxTimestampMS:   int64(last),
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        128,
		BlockIndexOffset: 64,
		BlockIndexLength: 64,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      base + 100,
		TrailerHash:      last + 100,
	}
}
