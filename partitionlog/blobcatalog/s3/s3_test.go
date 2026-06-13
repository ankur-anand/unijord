package s3

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blobcatalog"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestBackendPutGetAndImmutableReplayWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "catalog"
	backend, client := newFakeBackend(t, bucket)

	first, err := backend.Put(ctx, "catalog/p00000001/pages/l00/leaf.json", []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put(first) error = %v", err)
	}
	if first.Token == "" || string(first.Body) != `{"one":1}` {
		t.Fatalf("first object = %+v", first)
	}

	replay, err := backend.Put(ctx, first.Key, []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put(replay) error = %v", err)
	}
	if replay.Token != first.Token || string(replay.Body) != string(first.Body) {
		t.Fatalf("replay = %+v, want %+v", replay, first)
	}

	if _, err := backend.Put(ctx, first.Key, []byte(`{"two":2}`)); !errors.Is(err, blobcatalog.ErrImmutableConflict) {
		t.Fatalf("Put(conflict) error = %v, want %v", err, blobcatalog.ErrImmutableConflict)
	}

	got, err := backend.Get(ctx, first.Key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Token != first.Token || string(got.Body) != string(first.Body) {
		t.Fatalf("Get() = %+v, want %+v", got, first)
	}
	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(first.Key),
	})
	if err != nil {
		t.Fatalf("HeadObject() error = %v", err)
	}
	if aws.ToString(head.ContentType) != blobcatalog.ObjectContentType {
		t.Fatalf("ContentType = %q, want %q", aws.ToString(head.ContentType), blobcatalog.ObjectContentType)
	}
}

func TestBackendCompareAndSwapWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, _ := newFakeBackend(t, "catalog")
	const key = "catalog/p00000001/head.json"

	created, swapped, err := backend.CompareAndSwap(ctx, key, "", []byte(`{"generation":1}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(create) error = %v", err)
	}
	if !swapped || created.Token == "" {
		t.Fatalf("create swapped=%v object=%+v", swapped, created)
	}

	current, swapped, err := backend.CompareAndSwap(ctx, key, "wrong", []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(wrong token) error = %v", err)
	}
	if swapped {
		t.Fatal("CompareAndSwap(wrong token) swapped=true, want false")
	}
	if current.Token != created.Token || string(current.Body) != string(created.Body) {
		t.Fatalf("current = %+v, want created %+v", current, created)
	}

	updated, swapped, err := backend.CompareAndSwap(ctx, key, created.Token, []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(update) error = %v", err)
	}
	if !swapped || updated.Token == "" || updated.Token == created.Token || string(updated.Body) != `{"generation":2}` {
		t.Fatalf("update swapped=%v object=%+v previous=%+v", swapped, updated, created)
	}
}

func TestBackendListAndDeleteWithFakeS3(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, _ := newFakeBackend(t, "catalog")
	keys := []string{
		"catalog/p00000001/pages/l00/a.json",
		"catalog/p00000001/pages/l00/b.json",
		"catalog/p00000001/pages/l00/c.json",
		"catalog/p00000002/pages/l00/d.json",
	}
	for _, key := range keys {
		if _, err := backend.Put(ctx, key, []byte(key)); err != nil {
			t.Fatalf("Put(%s) error = %v", key, err)
		}
	}

	page, err := backend.List(ctx, blobcatalog.ListOptions{Prefix: "catalog/p00000001/", Limit: 2})
	if err != nil {
		t.Fatalf("List(first) error = %v", err)
	}
	if len(page.Objects) != 2 || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("first page = %+v, want 2 objects with cursor", page)
	}

	next, err := backend.List(ctx, blobcatalog.ListOptions{Prefix: "catalog/p00000001/", Cursor: page.NextCursor, Limit: 2})
	if err != nil {
		t.Fatalf("List(next) error = %v", err)
	}
	if len(next.Objects) != 1 || next.HasMore {
		t.Fatalf("next page = %+v, want final single object", next)
	}

	if err := backend.Delete(ctx, page.Objects[0].Key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := backend.Get(ctx, page.Objects[0].Key); !errors.Is(err, blobcatalog.ErrObjectNotFound) {
		t.Fatalf("Get(deleted) error = %v, want %v", err, blobcatalog.ErrObjectNotFound)
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
	backend, err := NewBackend(client, "catalog")
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	if _, err := backend.Get(context.Background(), ""); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("Get(empty key) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
}

func newFakeBackend(t *testing.T, bucket string) (*Backend, *awss3.Client) {
	t.Helper()
	client := newFakeS3Client(t, bucket)
	backend, err := NewBackend(client, bucket)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	return backend, client
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
