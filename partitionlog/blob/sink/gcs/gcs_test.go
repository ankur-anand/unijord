package gcs

import (
	"context"
	"errors"
	"io"
	"path"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/internal/sinktest"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
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

	upload, err := store.Begin(ctx, "segments/p-1.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	r1, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("hello ")))
	if err != nil {
		t.Fatalf("UploadPart(1) error = %v", err)
	}
	r2, err := upload.PutPart(ctx, multipart.NewPart(2, []byte("world")))
	if err != nil {
		t.Fatalf("UploadPart(2) error = %v", err)
	}
	attrs, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{r1, r2}))
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

func TestStoreSessionRetryContractWithFakeGCS(t *testing.T) {
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	sinktest.RunSessionRetryContract(t, store, "segments/retry-contract")
}

func TestPutPartRetryUsesANewGCSAttemptKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	started, err := store.Begin(ctx, "segments/retry.seg", multipart.Options{
		SessionID: "00000000-0000-4000-8000-000000000001",
	})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	session := started.(*session)
	attempts := []string{"attempt-that-already-exists", "fresh-attempt"}
	session.newAttemptID = func() (string, error) {
		attempt := attempts[0]
		attempts = attempts[1:]
		return attempt, nil
	}

	conflictingKey := multipart.StagingPartKey(session.opts.StagingPrefix, 1, "attempt-that-already-exists")
	writeGCSObject(t, client, "test-bucket", conflictingKey, []byte("ambiguous prior attempt"))
	part := multipart.NewPart(1, []byte("payload"))
	if _, err := session.PutPart(ctx, part); !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("PutPart(first attempt) error = %v, want ErrPreconditionFailed", err)
	}
	receipt, err := session.PutPart(ctx, part)
	if err != nil {
		t.Fatalf("PutPart(retry) error = %v", err)
	}
	if receipt.Number != 1 || receipt.ChecksumSHA256 != part.ChecksumSHA256 {
		t.Fatalf("retry receipt = %+v", receipt)
	}
	if got := session.parts[1].object.key; got != multipart.StagingPartKey(session.opts.StagingPrefix, 1, "fresh-attempt") {
		t.Fatalf("retry staging key = %q", got)
	}
	if err := session.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

func TestStoreCompletesMoreThanOneGCSComposeBatch(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	upload, err := store.Begin(ctx, "segments/many.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}

	receipts := make([]multipart.Receipt, 0, composeSourceLimit+3)
	want := make([]byte, 0, composeSourceLimit+3)
	for i := 1; i <= composeSourceLimit+3; i++ {
		b := []byte{byte('a' + i%26)}
		receipt, err := upload.PutPart(ctx, multipart.NewPart(i, b))
		if err != nil {
			t.Fatalf("UploadPart(%d) error = %v", i, err)
		}
		receipts = append(receipts, receipt)
		want = append(want, b...)
	}
	if _, err := upload.Commit(ctx, multipart.NewCommitRequest(receipts)); err != nil {
		t.Fatalf("Complete error = %v", err)
	}
	got := readGCSObject(t, client, "test-bucket", "segments/many.seg")
	if string(got) != string(want) {
		t.Fatalf("object bytes = %q, want %q", got, want)
	}
}

func TestCommitRetryUsesNewGCSComposeTree(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	started, err := store.Begin(ctx, "segments/compose-retry.seg", multipart.Options{
		SessionID: "00000000-0000-4000-8000-000000000002",
	})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	session := started.(*session)
	receipts := make([]multipart.Receipt, 0, composeSourceLimit+1)
	for number := 1; number <= composeSourceLimit+1; number++ {
		receipt, err := session.PutPart(ctx, multipart.NewPart(number, []byte{byte(number)}))
		if err != nil {
			t.Fatalf("PutPart(%d) error = %v", number, err)
		}
		receipts = append(receipts, receipt)
	}
	attempts := []string{"commit-attempt-1", "commit-attempt-2"}
	session.newAttemptID = func() (string, error) {
		attempt := attempts[0]
		attempts = attempts[1:]
		return attempt, nil
	}
	realCompose := session.composeObjects
	var composeAttempts []string
	session.composeObjects = func(ctx context.Context, sources []gcsObject, attemptID string, request multipart.CommitRequest) (gcsObject, error) {
		composeAttempts = append(composeAttempts, attemptID)
		if len(composeAttempts) == 1 {
			return gcsObject{}, errors.New("lost compose response")
		}
		return realCompose(ctx, sources, attemptID, request)
	}
	request := multipart.NewCommitRequest(receipts)
	if _, err := session.Commit(ctx, request); !errors.Is(err, multipart.ErrCommitIndeterminate) {
		t.Fatalf("Commit(first attempt) error = %v, want ErrCommitIndeterminate", err)
	}
	if _, err := session.Commit(ctx, request); err != nil {
		t.Fatalf("Commit(retry) error = %v", err)
	}
	if len(composeAttempts) != 2 || composeAttempts[0] == composeAttempts[1] {
		t.Fatalf("compose attempts = %v, want two unique attempt IDs", composeAttempts)
	}
	if got := readGCSObject(t, client, "test-bucket", "segments/compose-retry.seg"); len(got) != composeSourceLimit+1 {
		t.Fatalf("final object size = %d, want %d", len(got), composeSourceLimit+1)
	}
}

func TestStoreCleanupWithFakeGCS(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient(t, "test-bucket")
	store, err := NewStore(client, "test-bucket")
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	upload, err := store.Begin(ctx, "segments/abort.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("x")))
	if err != nil {
		t.Fatalf("UploadPart error = %v", err)
	}
	if err := upload.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup error = %v", err)
	}
	if _, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt})); !errors.Is(err, multipart.ErrCleaned) {
		t.Fatalf("Commit after cleanup error = %v, want %v", err, multipart.ErrCleaned)
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
	if _, err := store.Begin(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
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

func writeGCSObject(t *testing.T, client *storage.Client, bucket, key string, body []byte) {
	t.Helper()
	w := client.Bucket(bucket).Object(path.Clean(key)).NewWriter(context.Background())
	if _, err := w.Write(body); err != nil {
		t.Fatalf("Write(%q) error = %v", key, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(%q) error = %v", key, err)
	}
}
