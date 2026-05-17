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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blobsink/multipart"
)

func TestStoreMultipartEndToEndWithFakeAzure(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
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
	if got := server.object("segments/p-1.seg"); string(got) != "hello world" {
		t.Fatalf("object bytes = %q, want %q", got, "hello world")
	}
}

func TestStoreAbortWithFakeAzure(t *testing.T) {
	ctx := context.Background()
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	store, err := NewStore(client)
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
	if got := server.object("segments/abort.seg"); got != nil {
		t.Fatalf("object exists after abort: %q", got)
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
	upload, err := store.BeginMultipart(ctx, "segments/existing.seg", multipart.Options{})
	if err != nil {
		t.Fatalf("BeginMultipart error = %v", err)
	}
	receipt, err := upload.UploadPart(ctx, multipart.Part{Number: 1, Bytes: []byte("new")})
	if err != nil {
		t.Fatalf("UploadPart error = %v", err)
	}
	if _, err := upload.Complete(ctx, []multipart.Receipt{receipt}); !errors.Is(err, multipart.ErrPreconditionFailed) {
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
	if _, err := store.BeginMultipart(context.Background(), "", multipart.Options{}); !errors.Is(err, multipart.ErrInvalidStore) {
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
	mu      sync.Mutex
	objects map[string][]byte
	blocks  map[string]map[string][]byte
	etags   map[string]string
	seq     int
}

func newFakeAzureBlobServer(t *testing.T) *fakeAzureBlobServer {
	t.Helper()
	f := &fakeAzureBlobServer{
		objects: make(map[string][]byte),
		blocks:  make(map[string]map[string][]byte),
		etags:   make(map[string]string),
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
	if r.Header.Get("If-None-Match") == "*" && f.object(key) != nil {
		writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
		return
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
	delete(f.blocks, key)
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeAzureBlobServer) getProperties(w http.ResponseWriter, key string) {
	f.mu.Lock()
	body, ok := f.objects[key]
	etag := f.etags[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.Header().Set("ETag", etag)
	w.Header().Set("x-ms-blob-type", "BlockBlob")
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
	first, err := base64.StdEncoding.DecodeString(blockID(1))
	if err != nil {
		t.Fatalf("blockID(1) is not base64: %v", err)
	}
	last, err := base64.StdEncoding.DecodeString(blockID(maxBlockNumber))
	if err != nil {
		t.Fatalf("blockID(maxBlockNumber) is not base64: %v", err)
	}
	if len(first) != len(last) {
		t.Fatalf("block id raw lengths differ: %d vs %d", len(first), len(last))
	}
}
