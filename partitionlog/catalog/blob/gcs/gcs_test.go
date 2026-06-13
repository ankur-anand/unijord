package gcs

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/storage"
	blobcatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/backendtest"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func TestBackendConformanceWithFakeGCS(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, backendtest.Config{
		NewBackend: func(t testing.TB) blobcatalog.Backend {
			t.Helper()
			backend, _ := newFakeBackend(t, "catalog")
			return backend
		},
		WrongToken: func(valid string) string {
			if valid == "1" {
				return "2"
			}
			return "1"
		},
	})
}

func TestBackendContentTypeWithFakeGCS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, client := newFakeBackend(t, "catalog")
	obj, err := backend.Put(ctx, "catalog/p00000001/pages/l00/leaf.json", []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	attrs, err := client.Bucket("catalog").Object(obj.Key).Attrs(ctx)
	if err != nil {
		t.Fatalf("Attrs() error = %v", err)
	}
	if attrs.ContentType != blobcatalog.ObjectContentType {
		t.Fatalf("ContentType = %q, want %q", attrs.ContentType, blobcatalog.ObjectContentType)
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
	if _, _, err := backend.CompareAndSwap(context.Background(), "x", "not-a-generation", []byte("x")); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("CompareAndSwap(bad token) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
}

func newFakeBackend(t testing.TB, bucket string) (*Backend, *storage.Client) {
	t.Helper()
	client := newFakeGCSClient(t, bucket)
	backend, err := NewBackend(client, bucket)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	return backend, client
}

func newFakeGCSClient(t testing.TB, bucket string) *storage.Client {
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
