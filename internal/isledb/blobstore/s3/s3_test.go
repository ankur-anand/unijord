package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/ankur-anand/isledb/blobstore"
	s3store "github.com/ankur-anand/isledb/blobstore/s3"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

const fakeBucket = "conformance"

// conditionalFake fronts gofakes3, which honours If-None-Match / If-Match on
// PutObject and start-after on ListObjectsV2 but ignores If-Match on GetObject
// and DeleteObject and answers 412 (not S3's 404 NoSuchKey) for If-Match on a
// missing key. The shim serialises requests and evaluates every object
// precondition itself so the conformance suites exercise real conditional
// semantics.
//
// It also owns the ETag namespace: every successful write is given a fresh
// opaque ETag. gofakes3 ETags are the MD5 of the content, so two keys with
// equal bytes would carry byte-identical identity tokens and
// storetest.RunSuite "DeleteIfIdentity" (another object's token must never
// authorise this delete, exercised with equal payloads) could not pass. Real
// S3 also issues non-MD5 ETags (SSE-KMS, SSE-C), so the leaf must treat the
// ETag as opaque either way.
type conditionalFake struct {
	mu    sync.Mutex
	next  http.Handler
	etags map[string]string // request path -> current ETag
	seq   uint64
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

type etagWriter struct {
	http.ResponseWriter
	etag   string
	status int
}

func (w *etagWriter) WriteHeader(status int) {
	w.status = status
	// gofakes3 returns a version id from PutObject even for an unversioned
	// bucket, but none from HeadObject. Model a plain unversioned bucket.
	w.Header().Del("x-amz-version-id")
	if w.etag != "" && status < 300 {
		w.Header().Set("ETag", w.etag)
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *etagWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (f *conditionalFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	isObject := strings.Count(strings.Trim(path, "/"), "/") >= 1
	if !isObject {
		f.next.ServeHTTP(w, r)
		return
	}
	current, exists := f.etags[path]
	ifMatch, ifNoneMatch := r.Header.Get("If-Match"), r.Header.Get("If-None-Match")
	r.Header.Del("If-Match")
	r.Header.Del("If-None-Match")
	if ifNoneMatch == "*" && exists && r.Method == http.MethodPut {
		s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	if ifMatch != "" {
		switch {
		case !exists && r.Method == http.MethodPut:
			s3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		case exists && ifMatch != current:
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusPreconditionFailed)
			} else {
				s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			}
			return
		}
	}
	out := &etagWriter{ResponseWriter: w, etag: current}
	if r.Method == http.MethodPut {
		f.seq++
		out.etag = fmt.Sprintf(`"%032x"`, f.seq)
	}
	f.next.ServeHTTP(out, r)
	if out.status == 0 {
		out.WriteHeader(http.StatusOK)
	}
	switch {
	case r.Method == http.MethodPut && out.status < 300:
		f.etags[path] = out.etag
	case r.Method == http.MethodDelete && out.status < 300:
		delete(f.etags, path)
	}
}

// faultTransport injects one fault into the first request of the armed shape
// and counts every request of that shape since arming.
type faultTransport struct {
	next http.RoundTripper

	mu    sync.Mutex
	match func(*http.Request) bool
	kind  storetest.FaultKind
	armed bool
	seen  int
}

func matcher(op storetest.Op) func(*http.Request) bool {
	switch op {
	case storetest.OpPut, storetest.OpCompareAndSwap, storetest.OpCreate:
		return func(r *http.Request) bool { return r.Method == http.MethodPut }
	case storetest.OpOpenRange:
		return func(r *http.Request) bool { return r.Method == http.MethodGet && r.Header.Get("Range") != "" }
	case storetest.OpDeleteIfIdentity:
		return func(r *http.Request) bool { return r.Method == http.MethodDelete }
	}
	return func(*http.Request) bool { return false }
}

func (f *faultTransport) arm(op storetest.Op, kind storetest.FaultKind) func() int {
	f.mu.Lock()
	f.match, f.kind, f.armed, f.seen = matcher(op), kind, true, 0
	f.mu.Unlock()
	return func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.seen
	}
}

func (f *faultTransport) disarm() {
	f.mu.Lock()
	f.match, f.armed = nil, false
	f.mu.Unlock()
}

type failingCloseBody struct {
	io.ReadCloser
	err error
}

func (b *failingCloseBody) Close() error { return errors.Join(b.ReadCloser.Close(), b.err) }

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	fire := false
	if f.match != nil && f.match(r) {
		f.seen++
		fire, f.armed = f.armed, false
	}
	kind := f.kind
	f.mu.Unlock()
	if !fire {
		return f.next.RoundTrip(r)
	}
	injected := errors.Join(storetest.ErrInjected, errors.New("s3 transport fault"))
	switch kind {
	case storetest.FaultBeforeSend:
		// The request body is closed unread: nothing reached the provider.
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, injected
	case storetest.FaultThrottle:
		if r.Body != nil {
			_ = r.Body.Close()
		}
		body := `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`
		return &http.Response{
			Status: "503 Slow Down", StatusCode: http.StatusServiceUnavailable, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:        http.Header{"Content-Type": {"application/xml"}},
			ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body)), Request: r,
		}, nil
	case storetest.FaultLostResponse:
		response, err := f.next.RoundTrip(r)
		if err != nil {
			return nil, errors.Join(injected, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, injected
	case storetest.FaultCloseBody:
		response, err := f.next.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		response.Body = &failingCloseBody{ReadCloser: response.Body, err: injected}
		return response, nil
	}
	return f.next.RoundTrip(r)
}

// newClient deliberately leaves the client-level retryer at the SDK default so
// the single-request assertions prove the per-call aws.NopRetryer.
func newClient(endpoint, accessKey, secretKey string, transport http.RoundTripper) *awss3.Client {
	return awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		HTTPClient:   &http.Client{Transport: transport},
	})
}

func fakeHarness(t *testing.T) storetest.Harness {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket(fakeBucket); err != nil {
		t.Fatal(err)
	}
	shim := &conditionalFake{next: gofakes3.New(backend).Server(), etags: make(map[string]string)}
	server := httptest.NewServer(shim)
	t.Cleanup(server.Close)
	base := &http.Transport{MaxIdleConnsPerHost: 8}
	t.Cleanup(base.CloseIdleConnections)
	faults := &faultTransport{next: base}

	open := func(t *testing.T) *s3store.Store {
		t.Helper()
		store, err := s3store.New(newClient(server.URL, "fake", "fake", faults), fakeBucket)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open(t)
	raw := newClient(server.URL, "fake", "fake", base)
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: "conformance/",
		Reopen: func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened := open(t)
			return reopened, reopened
		},
		Replace: func(t *testing.T, key string, body []byte) {
			t.Helper()
			if _, err := raw.PutObject(context.Background(), &awss3.PutObjectInput{
				Bucket: aws.String(fakeBucket), Key: aws.String(key), Body: bytes.NewReader(body),
			}); err != nil {
				t.Fatal(err)
			}
		},
		Inject: func(t *testing.T, op storetest.Op, kind storetest.FaultKind) func() int {
			t.Cleanup(faults.disarm)
			return faults.arm(op, kind)
		},
	}
}

func TestMetadataConformance(t *testing.T) { storetest.MetadataSuite(t, fakeHarness(t)) }
func TestRunConformance(t *testing.T)      { storetest.RunSuite(t, fakeHarness(t)) }

func TestNewRejectsMissingArguments(t *testing.T) {
	if _, err := s3store.New(nil, "bucket"); !errors.Is(err, blobstore.ErrInvalidRequest) {
		t.Fatalf("nil client: %v", err)
	}
	client := newClient("http://127.0.0.1:1", "a", "b", http.DefaultTransport)
	if _, err := s3store.New(client, ""); !errors.Is(err, blobstore.ErrInvalidRequest) {
		t.Fatalf("empty bucket: %v", err)
	}
}
