package gcs

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blob/source/internal/sourcetest"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func TestStoreConformanceWithFakeGCS(t *testing.T) {
	t.Parallel()

	sourcetest.Run(t, sourcetest.Config{
		NewFixture: func(t testing.TB) sourcetest.Fixture {
			t.Helper()
			const bucket = "test-bucket"
			const key = "segments/p-1.seg"
			body := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
			client := newFakeGCSClient(t, bucket)
			putGCSObject(t, client, bucket, key, body)
			store, err := NewStore(client, bucket)
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			return sourcetest.Fixture{Store: store, Key: key, Body: body}
		},
	})
}

func TestStoreRejectsBadGCSConstructorInputs(t *testing.T) {
	t.Parallel()

	client := newFakeGCSClient(t, "test-bucket")
	if _, err := NewStore(nil, "test-bucket"); err == nil {
		t.Fatal("NewStore(nil) error = nil, want error")
	}
	if _, err := NewStore(client, ""); err == nil {
		t.Fatal("NewStore(empty bucket) error = nil, want error")
	}
}

func newFakeGCSClient(t testing.TB, bucket string) *storage.Client {
	t.Helper()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		t.Fatalf("NewServerWithOptions error = %v", err)
	}
	t.Cleanup(server.Stop)
	server.CreateBucket(bucket)
	client := server.Client()
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func putGCSObject(t testing.TB, client *storage.Client, bucket string, key string, body []byte) {
	t.Helper()
	w := client.Bucket(bucket).Object(key).NewWriter(context.Background())
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		t.Fatalf("Write object error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close object error = %v", err)
	}
	r, err := client.Bucket(bucket).Object(key).NewReader(context.Background())
	if err != nil {
		t.Fatalf("NewReader after put error = %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll after put error = %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("put object = %q, want %q", got, body)
	}
}

func TestGCSMissingObjectMapsToProviderError(t *testing.T) {
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	_, err = store.ReadAt(context.Background(), "missing", 0, 1)
	if !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("ReadAt(missing) error = %v, want %v", err, storage.ErrObjectNotExist)
	}
}
