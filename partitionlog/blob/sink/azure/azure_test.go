package azure

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/internal/sinktest"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
)

func TestStoreMultipartEndToEndWithFakeAzure(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
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
	if got := server.object("segments/p-1.seg"); string(got) != "hello world" {
		t.Fatalf("object bytes = %q, want %q", got, "hello world")
	}
}

func TestStoreSessionRetryContractWithFakeAzure(t *testing.T) {
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	sinktest.RunSessionRetryContract(t, store, "segments/retry-contract")
}

func TestStoreCleanupWithFakeAzure(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
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
	if got := server.object("segments/abort.seg"); got != nil {
		t.Fatalf("object exists after cleanup: %q", got)
	}
}

func TestCleanupRefusesWhileAzureCommitIsLanding(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	server.commitStarted = make(chan struct{})
	server.commitGate = make(chan struct{})
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	session, err := store.Begin(ctx, "segments/commit-race.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	receipt, err := session.PutPart(ctx, multipart.NewPart(1, []byte("payload")))
	if err != nil {
		t.Fatalf("PutPart() error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := session.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt}))
		result <- err
	}()
	select {
	case <-server.commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Azure CommitBlockList did not start")
	}
	if err := session.Cleanup(ctx); !errors.Is(err, multipart.ErrCommitInProgress) {
		t.Fatalf("Cleanup() error = %v, want ErrCommitInProgress", err)
	}
	close(server.commitGate)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Commit() did not return")
	}
	if got := server.object("segments/commit-race.seg"); string(got) != "payload" {
		t.Fatalf("committed object = %q", got)
	}
}

func TestAzureCommitReconcilesLostSuccessResponse(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	server.loseNextCommitResponse = true
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	session, err := store.Begin(ctx, "segments/reconcile.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	receipt, err := session.PutPart(ctx, multipart.NewPart(1, []byte("committed")))
	if err != nil {
		t.Fatalf("PutPart() error = %v", err)
	}
	request := multipart.NewCommitRequest([]multipart.Receipt{receipt})
	attrs, err := session.Commit(ctx, request)
	if err != nil {
		t.Fatalf("Commit() error = %v, want reconciled success", err)
	}
	if attrs.SessionID == "" || attrs.SizeBytes != uint64(len("committed")) {
		t.Fatalf("Commit() attrs = %+v", attrs)
	}
}

func TestStoreAzurePreconditionFailure(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	server.putObject("segments/existing.seg", []byte("old"))
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	upload, err := store.Begin(ctx, "segments/existing.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	receipt, err := upload.PutPart(ctx, multipart.NewPart(1, []byte("new")))
	if err != nil {
		t.Fatalf("UploadPart error = %v", err)
	}
	if _, err := upload.Commit(ctx, multipart.NewCommitRequest([]multipart.Receipt{receipt})); !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("Complete overwrite error = %v, want %v", err, multipart.ErrPreconditionFailed)
	}
	if got := server.object("segments/existing.seg"); string(got) != "old" {
		t.Fatalf("object bytes = %q, want %q", got, "old")
	}
}

func TestStoreRejectsBadAzureInputs(t *testing.T) {
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	if _, err := NewStore(nil); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("NewStore(nil) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	if _, err := store.Begin(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
		t.Fatalf("BeginMultipart(empty key) error = %v, want %v", err, multipart.ErrInvalidStore)
	}
}

func newFakeAzureContainerClient(t *testing.T, serverURL, containerName string) *container.Client {
	t.Helper()
	client, err := container.NewClientWithNoCredential(serverURL+"/"+containerName, nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential error = %v", err)
	}
	return client
}

type fakeAzureBlobServer struct {
	*httptest.Server
	mu                     sync.Mutex
	objects                map[string][]byte
	blocks                 map[string]map[string][]byte
	etags                  map[string]string
	metadata               map[string]map[string]string
	seq                    int
	commitStarted          chan struct{}
	commitGate             chan struct{}
	commitStart            sync.Once
	loseNextCommitResponse bool
}

func newFakeAzureBlobServer(t *testing.T) *fakeAzureBlobServer {
	t.Helper()
	f := &fakeAzureBlobServer{
		objects:  make(map[string][]byte),
		blocks:   make(map[string]map[string][]byte),
		etags:    make(map[string]string),
		metadata: make(map[string]map[string]string),
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAzureBlobServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	key, ok := azureBlobKey(r.URL.Path)
	if !ok {
		writeAzureError(w, http.StatusNotFound, "ContainerNotFound")
		return
	}
	switch {
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "block":
		f.stageBlock(w, r, key)
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "blocklist":
		f.commitBlockList(w, r, key)
	case r.Method == http.MethodHead:
		f.getProperties(w, key)
	default:
		writeAzureError(w, http.StatusBadRequest, "UnsupportedOperation")
	}
}

func (f *fakeAzureBlobServer) stageBlock(w http.ResponseWriter, r *http.Request, key string) {
	blockID := r.URL.Query().Get("blockid")
	if blockID == "" {
		writeAzureError(w, http.StatusBadRequest, "MissingBlockID")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAzureError(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	f.mu.Lock()
	if f.blocks[key] == nil {
		f.blocks[key] = make(map[string][]byte)
	}
	f.blocks[key][blockID] = append([]byte(nil), body...)
	f.seq++
	etag := fmt.Sprintf("\"block-%d\"", f.seq)
	f.mu.Unlock()
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeAzureBlobServer) commitBlockList(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	started := f.commitStarted
	gate := f.commitGate
	f.mu.Unlock()
	if started != nil {
		f.commitStart.Do(func() { close(started) })
	}
	if gate != nil {
		<-gate
	}
	if r.Header.Get("If-None-Match") == "*" && f.object(key) != nil {
		writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
		return
	}
	metadata := azureRequestMetadata(r.Header)
	for name := range metadata {
		if !validAzureMetadataName(name) {
			writeAzureError(w, http.StatusBadRequest, "InvalidMetadata")
			return
		}
	}
	var list blockListXML
	if err := xml.NewDecoder(r.Body).Decode(&list); err != nil {
		writeAzureError(w, http.StatusBadRequest, "InvalidBlockList")
		return
	}
	ids := list.ids()
	f.mu.Lock()
	defer f.mu.Unlock()
	blocks := f.blocks[key]
	if blocks == nil {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	var body []byte
	for _, id := range ids {
		block, ok := blocks[id]
		if !ok {
			writeAzureError(w, http.StatusBadRequest, "InvalidBlockList")
			return
		}
		body = append(body, block...)
	}
	f.seq++
	etag := fmt.Sprintf("\"etag-%d\"", f.seq)
	f.objects[key] = body
	f.etags[key] = etag
	f.metadata[key] = metadata
	delete(f.blocks, key)
	if f.loseNextCommitResponse {
		f.loseNextCommitResponse = false
		writeAzureError(w, http.StatusInternalServerError, "InternalError")
		return
	}
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeAzureBlobServer) getProperties(w http.ResponseWriter, key string) {
	f.mu.Lock()
	body, ok := f.objects[key]
	etag := f.etags[key]
	metadata := f.metadata[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.Header().Set("ETag", etag)
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	for name, value := range metadata {
		w.Header().Set("x-ms-meta-"+name, value)
	}
	w.WriteHeader(http.StatusOK)
}

func (f *fakeAzureBlobServer) object(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	if !ok {
		return nil
	}
	return append([]byte(nil), body...)
}

func (f *fakeAzureBlobServer) putObject(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	f.objects[key] = append([]byte(nil), body...)
	f.etags[key] = fmt.Sprintf("\"etag-%d\"", f.seq)
}

type blockListXML struct {
	Latest      []string `xml:"Latest"`
	Committed   []string `xml:"Committed"`
	Uncommitted []string `xml:"Uncommitted"`
}

func (b blockListXML) ids() []string {
	ids := append([]string(nil), b.Latest...)
	ids = append(ids, b.Committed...)
	ids = append(ids, b.Uncommitted...)
	return ids
}

func azureBlobKey(requestPath string) (string, bool) {
	parts := strings.SplitN(strings.TrimPrefix(requestPath, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	key, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", false
	}
	return key, true
}

func writeAzureError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func TestBlockIDIsBase64AndStableLength(t *testing.T) {
	const sessionID = "00000000-0000-4000-8000-000000000001"
	first, err := base64.StdEncoding.DecodeString(blockID(sessionID, 1))
	if err != nil {
		t.Fatalf("blockID(1) is not base64: %v", err)
	}
	last, err := base64.StdEncoding.DecodeString(blockID(sessionID, limits.MaxPartCount))
	if err != nil {
		t.Fatalf("blockID(maxBlockNumber) is not base64: %v", err)
	}
	if len(first) != len(last) {
		t.Fatalf("block id raw lengths differ: %d vs %d", len(first), len(last))
	}
}

func azureRequestMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for name, values := range header {
		name = strings.ToLower(name)
		if !strings.HasPrefix(name, "x-ms-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(name, "x-ms-meta-")] = values[0]
	}
	return metadata
}

func validAzureMetadataName(name string) bool {
	for i := range len(name) {
		char := name[i]
		if char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || i > 0 && char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return name != ""
}
