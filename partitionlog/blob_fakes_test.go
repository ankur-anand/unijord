package partitionlog_test

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

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

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
	objects map[string]fakeAzureObject
	blocks  map[string]map[string][]byte
	seq     int
}

type fakeAzureObject struct {
	body         []byte
	etag         string
	contentType  string
	lastModified time.Time
}

func newFakeAzureBlobServer(t *testing.T) *fakeAzureBlobServer {
	t.Helper()
	f := &fakeAzureBlobServer{
		objects: make(map[string]fakeAzureObject),
		blocks:  make(map[string]map[string][]byte),
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAzureBlobServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	key, ok := azureBlobKey(r.URL.Path)
	if r.Method == http.MethodGet && r.URL.Query().Get("comp") == "list" {
		f.listObjects(w, r)
		return
	}
	if !ok {
		writeAzureError(w, http.StatusNotFound, "ContainerNotFound")
		return
	}
	switch {
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "block":
		f.stageBlock(w, r, key)
	case r.Method == http.MethodPut && r.URL.Query().Get("comp") == "blocklist":
		f.commitBlockList(w, r, key)
	case r.Method == http.MethodPut:
		f.putObject(w, r, key)
	case r.Method == http.MethodHead:
		f.getProperties(w, key)
	case r.Method == http.MethodGet:
		f.getObject(w, r, key)
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

func (f *fakeAzureBlobServer) putObject(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeAzureError(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("If-None-Match") == "*" {
		if _, exists := f.objects[key]; exists {
			writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
			return
		}
	}
	if match := r.Header.Get("If-Match"); match != "" {
		current, exists := f.objects[key]
		if !exists || current.etag != match {
			writeAzureError(w, http.StatusPreconditionFailed, "ConditionNotMet")
			return
		}
	}
	f.seq++
	obj := fakeAzureObject{
		body:         append([]byte(nil), body...),
		etag:         fmt.Sprintf("\"etag-%d\"", f.seq),
		contentType:  r.Header.Get("x-ms-blob-content-type"),
		lastModified: time.Unix(int64(f.seq), 0).UTC(),
	}
	f.objects[key] = obj
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.lastModified.Format(http.TimeFormat))
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
	lastModified := time.Unix(int64(f.seq), 0).UTC()
	f.objects[key] = fakeAzureObject{
		body:         body,
		etag:         etag,
		contentType:  r.Header.Get("x-ms-blob-content-type"),
		lastModified: lastModified,
	}
	delete(f.blocks, key)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeAzureBlobServer) getProperties(w http.ResponseWriter, key string) {
	f.mu.Lock()
	obj, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(obj.body)))
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.lastModified.Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.WriteHeader(http.StatusOK)
}

func (f *fakeAzureBlobServer) getObject(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	obj, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	out, partial, err := applyRange(obj.body, r.Header.Get("x-ms-range"), r.Header.Get("Range"))
	if err != nil {
		writeAzureError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(out)))
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.lastModified.Format(http.TimeFormat))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	if partial {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(out)
}

func (f *fakeAzureBlobServer) listObjects(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	marker := query.Get("marker")
	limit, _ := strconv.Atoi(query.Get("maxresults"))
	if limit <= 0 {
		limit = 5000
	}

	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) && (marker == "" || key > marker) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit+1]
	}
	objects := make([]fakeAzureObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, f.objects[key])
	}
	f.mu.Unlock()

	nextMarker := ""
	if len(keys) > limit {
		nextMarker = keys[limit-1]
		keys = keys[:limit]
		objects = objects[:limit]
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="%s/" ContainerName="container">`, f.URL)
	_, _ = fmt.Fprintf(w, `<Prefix>%s</Prefix><Marker>%s</Marker><MaxResults>%d</MaxResults><Blobs>`, prefix, marker, limit)
	for i, key := range keys {
		obj := objects[i]
		_, _ = fmt.Fprintf(w, `<Blob><Name>%s</Name><Properties><Last-Modified>%s</Last-Modified><Etag>%s</Etag><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>`,
			key, obj.lastModified.Format(http.TimeFormat), obj.etag, len(obj.body))
	}
	_, _ = fmt.Fprintf(w, `</Blobs><NextMarker>%s</NextMarker></EnumerationResults>`, nextMarker)
}

func (f *fakeAzureBlobServer) object(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil
	}
	return append([]byte(nil), obj.body...)
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

func applyRange(body []byte, headers ...string) ([]byte, bool, error) {
	for _, header := range headers {
		if header == "" {
			continue
		}
		if !strings.HasPrefix(header, "bytes=") {
			return nil, false, fmt.Errorf("invalid range %q", header)
		}
		parts := strings.SplitN(strings.TrimPrefix(header, "bytes="), "-", 2)
		if len(parts) != 2 {
			return nil, false, fmt.Errorf("invalid range %q", header)
		}
		start, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, false, err
		}
		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, false, err
		}
		if start < 0 || end < start || start >= int64(len(body)) || end >= int64(len(body)) {
			return nil, false, fmt.Errorf("range outside body")
		}
		return append([]byte(nil), body[start:end+1]...), true, nil
	}
	return append([]byte(nil), body...), false, nil
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
