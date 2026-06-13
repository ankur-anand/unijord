package azure

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blobcatalog"
	"github.com/ankur-anand/unijord/partitionlog/blobcatalog/internal/backendtest"
)

func TestBackendConformanceWithFakeAzure(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, backendtest.Config{
		NewBackend: func(t testing.TB) blobcatalog.Backend {
			t.Helper()
			backend, _, _ := newFakeBackend(t)
			return backend
		},
	})
}

func TestBackendContentTypeWithFakeAzure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, client, server := newFakeBackend(t)
	obj, err := backend.Put(ctx, "catalog/p00000001/pages/l00/leaf.json", []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	props, err := client.NewBlobClient(obj.Key).GetProperties(ctx, nil)
	if err != nil {
		t.Fatalf("GetProperties() error = %v", err)
	}
	if props.ContentType == nil || *props.ContentType != blobcatalog.ObjectContentType {
		t.Fatalf("ContentType = %v, want %q", props.ContentType, blobcatalog.ObjectContentType)
	}
	if server.object(obj.Key).etag != obj.Token {
		t.Fatalf("server token = %q, want %q", server.object(obj.Key).etag, obj.Token)
	}
}

func TestBackendRejectsBadInputs(t *testing.T) {
	t.Parallel()

	if _, err := NewBackend(nil); err == nil {
		t.Fatal("NewBackend(nil) error = nil, want error")
	}
}

func newFakeBackend(t testing.TB) (*Backend, *container.Client, *fakeAzureBlobServer) {
	t.Helper()
	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	backend, err := NewBackend(client)
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	return backend, client, server
}

func newFakeAzureContainerClient(t testing.TB, serverURL string, containerName string) *container.Client {
	t.Helper()
	client, err := container.NewClientWithNoCredential(serverURL+"/"+containerName, nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential() error = %v", err)
	}
	return client
}

type fakeAzureObject struct {
	body        []byte
	etag        string
	contentType string
	modified    time.Time
}

type fakeAzureBlobServer struct {
	*httptest.Server
	mu      sync.Mutex
	objects map[string]fakeAzureObject
	seq     int
}

func newFakeAzureBlobServer(t testing.TB) *fakeAzureBlobServer {
	t.Helper()
	f := &fakeAzureBlobServer{
		objects: make(map[string]fakeAzureObject),
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAzureBlobServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("restype") == "container" && r.URL.Query().Get("comp") == "list":
		f.listBlobs(w, r)
		return
	}

	key, ok := azureBlobKey(r.URL.Path)
	if !ok {
		writeAzureError(w, http.StatusNotFound, "ContainerNotFound")
		return
	}
	switch r.Method {
	case http.MethodPut:
		f.putBlob(w, r, key)
	case http.MethodGet:
		f.getBlob(w, key)
	case http.MethodHead:
		f.getProperties(w, key)
	case http.MethodDelete:
		f.deleteBlob(w, key)
	default:
		writeAzureError(w, http.StatusBadRequest, "UnsupportedOperation")
	}
}

func (f *fakeAzureBlobServer) putBlob(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAzureError(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	if r.Header.Get("x-ms-blob-type") != "BlockBlob" {
		writeAzureError(w, http.StatusBadRequest, "InvalidBlobType")
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	current, exists := f.objects[key]
	if r.Header.Get("If-None-Match") == "*" && exists {
		writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
		return
	}
	if match := r.Header.Get("If-Match"); match != "" {
		if !exists || match != current.etag {
			writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
			return
		}
	}

	f.seq++
	now := time.Now().UTC().Truncate(time.Second)
	obj := fakeAzureObject{
		body:        append([]byte(nil), body...),
		etag:        fmt.Sprintf("\"etag-%d\"", f.seq),
		contentType: r.Header.Get("x-ms-blob-content-type"),
		modified:    now,
	}
	if obj.contentType == "" {
		obj.contentType = r.Header.Get("Content-Type")
	}
	f.objects[key] = obj
	writeAzureBlobHeaders(w, obj)
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeAzureBlobServer) getBlob(w http.ResponseWriter, key string) {
	obj := f.object(key)
	if obj.etag == "" {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	writeAzureBlobHeaders(w, obj)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(obj.body)
}

func (f *fakeAzureBlobServer) getProperties(w http.ResponseWriter, key string) {
	obj := f.object(key)
	if obj.etag == "" {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	writeAzureBlobHeaders(w, obj)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.body)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeAzureBlobServer) deleteBlob(w http.ResponseWriter, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[key]; !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	delete(f.objects, key)
	w.WriteHeader(http.StatusAccepted)
}

func (f *fakeAzureBlobServer) listBlobs(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	limit := 5000
	if raw := r.URL.Query().Get("maxresults"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeAzureError(w, http.StatusBadRequest, "InvalidQueryParameterValue")
			return
		}
		if parsed > 0 {
			limit = parsed
		}
	}
	start := 0
	marker := r.URL.Query().Get("marker")
	if marker != "" {
		parsed, err := strconv.Atoi(marker)
		if err != nil || parsed < 0 {
			writeAzureError(w, http.StatusBadRequest, "InvalidMarker")
			return
		}
		start = parsed
	}

	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	objects := make([]fakeAzureListBlob, 0, limit)
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}
	for _, key := range keys[start:end] {
		obj := f.objects[key]
		objects = append(objects, fakeAzureListBlob{
			Name: key,
			Properties: fakeAzureListBlobProperties{
				LastModified:  obj.modified.Format(http.TimeFormat),
				Etag:          obj.etag,
				ContentLength: len(obj.body),
				ContentType:   obj.contentType,
				BlobType:      "BlockBlob",
			},
		})
	}
	f.mu.Unlock()

	nextMarker := ""
	if end < len(keys) {
		nextMarker = strconv.Itoa(end)
	}
	resp := fakeAzureListResponse{
		ServiceEndpoint: f.URL + "/",
		ContainerName:   "container",
		Prefix:          prefix,
		Marker:          marker,
		MaxResults:      limit,
		Blobs:           fakeAzureListBlobs{Blob: objects},
		NextMarker:      nextMarker,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(resp)
}

func (f *fakeAzureBlobServer) object(key string) fakeAzureObject {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return fakeAzureObject{}
	}
	obj.body = append([]byte(nil), obj.body...)
	return obj
}

func writeAzureBlobHeaders(w http.ResponseWriter, obj fakeAzureObject) {
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.Header().Set("Content-Type", obj.contentType)
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

type fakeAzureListResponse struct {
	XMLName         xml.Name           `xml:"EnumerationResults"`
	ServiceEndpoint string             `xml:"ServiceEndpoint,attr"`
	ContainerName   string             `xml:"ContainerName,attr"`
	Prefix          string             `xml:"Prefix,omitempty"`
	Marker          string             `xml:"Marker,omitempty"`
	MaxResults      int                `xml:"MaxResults"`
	Blobs           fakeAzureListBlobs `xml:"Blobs"`
	NextMarker      string             `xml:"NextMarker"`
}

type fakeAzureListBlobs struct {
	Blob []fakeAzureListBlob `xml:"Blob"`
}

type fakeAzureListBlob struct {
	Name       string                      `xml:"Name"`
	Properties fakeAzureListBlobProperties `xml:"Properties"`
}

type fakeAzureListBlobProperties struct {
	LastModified  string `xml:"Last-Modified"`
	Etag          string `xml:"Etag"`
	ContentLength int    `xml:"Content-Length"`
	ContentType   string `xml:"Content-Type"`
	BlobType      string `xml:"BlobType"`
}
