package azure

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blobsource/internal/sourcetest"
)

func TestStoreConformanceWithFakeAzure(t *testing.T) {
	t.Parallel()

	sourcetest.Run(t, sourcetest.Config{
		NewFixture: func(t testing.TB) sourcetest.Fixture {
			t.Helper()
			const key = "segments/p-1.seg"
			body := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
			server := newFakeAzureBlobServer(t)
			server.putObject(key, body)
			client := newFakeAzureContainerClient(t, server.URL, "container")
			store, err := NewStore(client)
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			return sourcetest.Fixture{Store: store, Key: key, Body: body}
		},
	})
}

func TestStoreRejectsBadAzureConstructorInputs(t *testing.T) {
	t.Parallel()

	server := newFakeAzureBlobServer(t)
	client := newFakeAzureContainerClient(t, server.URL, "container")
	if _, err := NewStore(nil); err == nil {
		t.Fatal("NewStore(nil) error = nil, want error")
	}
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore error = %v", err)
	}
	_ = store
}

func newFakeAzureContainerClient(t testing.TB, serverURL, containerName string) *container.Client {
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
}

func newFakeAzureBlobServer(t testing.TB) *fakeAzureBlobServer {
	t.Helper()
	f := &fakeAzureBlobServer{objects: make(map[string][]byte)}
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
	if r.Method != http.MethodGet {
		writeAzureError(w, http.StatusBadRequest, "UnsupportedOperation")
		return
	}
	f.mu.Lock()
	body, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		writeAzureError(w, http.StatusNotFound, "BlobNotFound")
		return
	}
	out, partial, err := applyRange(body, r.Header.Get("x-ms-range"), r.Header.Get("Range"))
	if err != nil {
		writeAzureError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(out)))
	w.Header().Set("ETag", `"etag"`)
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	if partial {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(out)
}

func (f *fakeAzureBlobServer) putObject(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
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

func TestApplyRange(t *testing.T) {
	t.Parallel()

	got, partial, err := applyRange([]byte("abcdef"), "bytes=2-4")
	if err != nil {
		t.Fatalf("applyRange() error = %v", err)
	}
	if !partial || string(got) != "cde" {
		t.Fatalf("applyRange() = %q partial=%v, want cde true", got, partial)
	}
	got, partial, err = applyRange([]byte("abcdef"), "")
	if err != nil {
		t.Fatalf("applyRange(empty) error = %v", err)
	}
	if partial || string(got) != "abcdef" {
		t.Fatalf("applyRange(empty) = %q partial=%v", got, partial)
	}
}

func TestFakeAzureMissingObject(t *testing.T) {
	server := newFakeAzureBlobServer(t)
	resp, err := http.Get(server.URL + "/container/missing")
	if err != nil {
		t.Fatalf("http.Get error = %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
