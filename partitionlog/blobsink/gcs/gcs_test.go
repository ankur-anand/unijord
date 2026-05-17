package gcs

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blobsink/multipart"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/googleapi"
)

func TestStoreMultipartEndToEndWithFakeGCS(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}

	upload, err := store.BeginMultipart(ctx, "segments/p-1.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	r1, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("hello ")})
	if err != nil {
		t.Fatalf("UploadPart(1) error = %v", err)
	}
	r2, err := upload.UploadPart(ctx, multipart.Part{Number: 2, Bytes: []byte("world")})
	if err != nil {
		t.Fatalf("UploadPart(2) error = %v", err)
	}
	attrs, err := upload.Complete(ctx, []multipart.Receipt{r1, r2})
	if err != nil {
		t.Fatalf("Complete error = %v", err)
	}
	if attrs.Key != "segments/p-1.seg" {
		t.Fatalf("attrs.Key = %q, want %q", attrs.Key, "segments/p-1.seg")
	}
	if attrs.SizeBytes != uint64(len("hello world")) {
		t.Fatalf("attrs.SizeBytes = %d, want %d", attrs.SizeBytes, len("hello world"))
	}
	if attrs.Token == "" {
		t.Fatal("attrs.Token is empty")
	}

	got := readGCSObject(t, client, "test-bucket", "segments/p-1.seg")
	if string(got) != "hello world" {
		t.Fatalf("object bytes = %q, want %q", got, "hello world")
	}
}

func TestStoreCompletesMoreThanOneGCSComposeBatch(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	upload, err := store.BeginMultipart(ctx, "segments/many.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}

	receipts := make([]multipart.Receipt, 0, composeSourceLimit+3)
	want := make([]byte, 0, composeSourceLimit+3)
	for i := 1; i <= composeSourceLimit+3; i++ {
		b := []byte{byte('a' + i%26)}
		receipt, err := upload.UploadPart(ctx, multipart.Part{Number: i, Bytes: b})
		if err != nil {
			t.Fatalf("UploadPart(%d) error = %v", i, err)
		}
		receipts = append(receipts, receipt)
		want = append(want, b...)
	}
	if _, err := upload.Complete(ctx, receipts); err != nil {
		t.Fatalf("Complete error = %v", err)
	}
	got := readGCSObject(t, client, "test-bucket", "segments/many.seg")
	if string(got) != string(want) {
		t.Fatalf("object bytes = %q, want %q", got, want)
	}
}

func TestStoreAbortWithFakeGCS(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	upload, err := store.BeginMultipart(ctx, "segments/abort.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("x")})
	if err != nil {
		t.Fatalf("UploadPart error = %v", err)
	}
	if err := upload.Abort(ctx); err != nil {
		t.Fatalf("Abort error = %v", err)
	}
	if _, err := upload.Complete(ctx, []multipart.Receipt{receipt}); !errors.Is(err, multipart.ErrAborted) {
		t.Fatalf("Complete after abort error = %v, want %v", err, multipart.ErrAborted)
	}
	if _, err := client.Bucket("test-bucket").Object("segments/abort.seg").Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("final object error = %v, want object not exist", err)
	}
}

func TestStoreRejectsBadGCSInputs(t *testing.T) {
	client := newFakeGCSClient(t, "test-bucket")
	if _, err := NewStore(nil, "test-bucket"); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("NewStore(nil) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
	if _, err := NewStore(client, ""); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("NewStore(empty bucket) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	if _, err := store.BeginMultipart(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("BeginMultipart(empty key) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
}

func TestMapErrorGCSPreconditionFailure(t *testing.T) {
	err := mapError(&googleapi.Error{Code: 412})
	if !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("mapError(412) = %v, want %v", err, multipart.ErrPreconditionFailed)
	}
}

func newFakeGCSClient(t *testing.T, bucket string) *storage.Client {
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

func readGCSObject(t *testing.T, client *storage.Client, bucket, key string) []byte {
	t.Helper()
	reader, err := client.Bucket(bucket).Object(key).NewReader(context.Background())
	if err != nil {
		t.Fatalf("NewReader error = %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll error = %v", err)
	}
	return got
}
