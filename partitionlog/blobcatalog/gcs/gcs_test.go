package gcs

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blobcatalog"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func TestBackendPutGetAndImmutableReplayWithFakeGCS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, client := newFakeBackend(t, "catalog")

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

	attrs, err := client.Bucket("catalog").Object(first.Key).Attrs(ctx)
	if err != nil {
		t.Fatalf("Attrs() error = %v", err)
	}
	if attrs.ContentType != blobcatalog.ObjectContentType {
		t.Fatalf("ContentType = %q, want %q", attrs.ContentType, blobcatalog.ObjectContentType)
	}
}

func TestBackendCompareAndSwapWithFakeGCS(t *testing.T) {
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

	current, swapped, err := backend.CompareAndSwap(ctx, key, "1", []byte(`{"generation":2}`))
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

func TestBackendListAndDeleteWithFakeGCS(t *testing.T) {
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
	if err := backend.Delete(ctx, page.Objects[0].Key); err != nil {
		t.Fatalf("Delete(missing) error = %v", err)
	}
	if _, err := backend.Get(ctx, page.Objects[0].Key); !errors.Is(err, blobcatalog.ErrObjectNotFound) {
		t.Fatalf("Get(deleted) error = %v, want %v", err, blobcatalog.ErrObjectNotFound)
	}
}

func TestBackendRejectsBadInputs(t *testing.T) {
	t.Parallel()

	client := newFakeGCSClient(t, "catalog")
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
	if _, _, err := backend.CompareAndSwap(context.Background(), "x", "not-a-generation", []byte("x")); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("CompareAndSwap(bad token) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
}

func newFakeBackend(t *testing.T, bucket string) (*Backend, *storage.Client) {
	t.Helper()
	client := newFakeGCSClient(t, bucket)
	backend, err := NewBackend(client, bucket)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	return backend, client
}

func newFakeGCSClient(t *testing.T, bucket string) *storage.Client {
	t.Helper()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		t.Fatalf("NewServerWithOptions() error = %v", err)
	}
	t.Cleanup(server.Stop)
	server.CreateBucket(bucket)
	client := server.Client()
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func readGCSObject(t *testing.T, client *storage.Client, bucket string, key string) []byte {
	t.Helper()
	r, err := client.Bucket(bucket).Object(key).NewReader(context.Background())
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return body
}
