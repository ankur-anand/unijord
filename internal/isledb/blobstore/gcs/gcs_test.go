package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/option"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

const testBucket = "conformance-bucket"

// ---- request classification shared by the shim and the fault injector ----

func isUpload(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/upload/") }

func isResumableInit(r *http.Request) bool {
	q := r.URL.Query()
	return isUpload(r) && q.Get("uploadType") == "resumable" && q.Get("upload_id") == ""
}

// isCommit reports the request that can make an object visible: the single
// multipart insert, or the resumable chunk whose Content-Range has a total.
func isCommit(r *http.Request) bool {
	if !isUpload(r) {
		return false
	}
	q := r.URL.Query()
	if q.Get("upload_id") == "" {
		return q.Get("uploadType") == "multipart"
	}
	cr := r.Header.Get("Content-Range")
	return cr != "" && !strings.HasSuffix(cr, "/*")
}

func isMediaRead(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	return r.Header.Get("Range") != "" || r.URL.Query().Get("alt") == "media" ||
		!strings.HasPrefix(r.URL.Path, "/storage/v1/")
}

func synthesized(r *http.Request, code int, reason string) *http.Response {
	body := fmt.Sprintf(`{"error":{"code":%d,"message":%q,"errors":[{"reason":%q,"message":%q}]}}`,
		code, http.StatusText(code), reason, http.StatusText(code))
	return &http.Response{
		StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:        http.Header{"Content-Type": {"application/json; charset=UTF-8"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)), Request: r,
	}
}

// conditionShim enforces the generation preconditions that fake-gcs-server
// v1.53.1 accepts but ignores. The fake enforces ifGenerationMatch only on
// multipart inserts; it ignores it on resumable uploads and on DELETE. The
// shim looks up the live generation through the fake itself and answers 412
// exactly as GCS does. It is test-only and not atomic, which is sufficient for
// the sequential conformance suites.
type conditionShim struct {
	base http.RoundTripper

	mu       sync.Mutex
	sessions map[string]sessionCond // upload_id -> condition recorded at initiation
}

type sessionCond struct {
	object string
	want   int64
}

func (c *conditionShim) liveGeneration(r *http.Request, object string) (int64, error) {
	u := *r.URL
	u.Path = "/storage/v1/b/" + testBucket + "/o/" + object
	u.RawPath = "/storage/v1/b/" + testBucket + "/o/" + url.PathEscape(object)
	u.RawQuery = ""
	probe, err := http.NewRequestWithContext(context.WithoutCancel(r.Context()), http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.base.RoundTrip(probe)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	var attrs struct {
		Generation string `json:"generation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&attrs); err != nil {
		return 0, err
	}
	return strconv.ParseInt(attrs.Generation, 10, 64)
}

func (c *conditionShim) RoundTrip(r *http.Request) (*http.Response, error) {
	q := r.URL.Query()
	reject := func() (*http.Response, error) {
		if r.Body != nil {
			r.Body.Close()
		}
		return synthesized(r, http.StatusPreconditionFailed, "conditionNotMet"), nil
	}
	switch {
	case r.Method == http.MethodDelete && q.Get("ifGenerationMatch") != "":
		want, _ := strconv.ParseInt(q.Get("ifGenerationMatch"), 10, 64)
		object := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/"+testBucket+"/o/")
		live, err := c.liveGeneration(r, object)
		if err != nil {
			return nil, err
		}
		if live != 0 && live != want { // live == 0: let the fake answer 404
			return reject()
		}
	case isResumableInit(r) && q.Get("ifGenerationMatch") != "":
		want, _ := strconv.ParseInt(q.Get("ifGenerationMatch"), 10, 64)
		live, err := c.liveGeneration(r, q.Get("name"))
		if err != nil {
			return nil, err
		}
		if live != want {
			return reject()
		}
		resp, err := c.base.RoundTrip(r)
		if err == nil {
			if loc, perr := url.Parse(resp.Header.Get("Location")); perr == nil && loc.Query().Get("upload_id") != "" {
				c.mu.Lock()
				c.sessions[loc.Query().Get("upload_id")] = sessionCond{object: q.Get("name"), want: want}
				c.mu.Unlock()
			}
		}
		return resp, err
	case isCommit(r) && q.Get("upload_id") != "":
		c.mu.Lock()
		cond, ok := c.sessions[q.Get("upload_id")]
		delete(c.sessions, q.Get("upload_id"))
		c.mu.Unlock()
		if ok {
			live, err := c.liveGeneration(r, cond.object)
			if err != nil {
				return nil, err
			}
			if live != cond.want {
				return reject()
			}
		}
	}
	return c.base.RoundTrip(r)
}

// faultTransport arms one fault for the next request of the armed shape.
type faultTransport struct {
	base http.RoundTripper

	mu    sync.Mutex
	match func(*http.Request) bool
	kind  storetest.FaultKind
	armed bool
	seen  int
}

func (f *faultTransport) arm(match func(*http.Request) bool, kind storetest.FaultKind) func() int {
	f.mu.Lock()
	f.match, f.kind, f.armed, f.seen = match, kind, true, 0
	f.mu.Unlock()
	return func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.seen
	}
}

type failingCloseBody struct {
	io.ReadCloser
	err error
}

func (b failingCloseBody) Close() error { return errors.Join(b.ReadCloser.Close(), b.err) }

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	hit := f.match != nil && f.match(r)
	fire := hit && f.armed
	if hit {
		f.seen++
	}
	if fire {
		f.armed = false
	}
	kind := f.kind
	f.mu.Unlock()
	if !fire {
		return f.base.RoundTrip(r)
	}
	injected := errors.Join(storetest.ErrInjected, errors.New("gcs transport fault"))
	switch kind {
	case storetest.FaultBeforeSend:
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, injected
	case storetest.FaultThrottle:
		if r.Body != nil {
			r.Body.Close()
		}
		return synthesized(r, http.StatusTooManyRequests, "rateLimitExceeded"), nil
	case storetest.FaultLostResponse:
		resp, err := f.base.RoundTrip(r)
		if err != nil {
			return nil, errors.Join(injected, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, errors.Join(injected, fmt.Errorf("fake did not apply the request: %s", resp.Status))
		}
		return nil, injected
	case storetest.FaultCloseBody:
		resp, err := f.base.RoundTrip(r)
		if err == nil {
			resp.Body = failingCloseBody{resp.Body, injected}
		}
		return resp, err
	}
	return f.base.RoundTrip(r)
}

type fakeEnv struct {
	server *fakestorage.Server
	faults *faultTransport
	chunk  int
}

func newFakeEnv(t *testing.T, chunk int) *fakeEnv {
	t.Helper()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{Host: "127.0.0.1", Port: 0, Scheme: "http", PublicHost: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: testBucket})
	shim := &conditionShim{base: server.HTTPClient().Transport, sessions: make(map[string]sessionCond)}
	return &fakeEnv{server: server, faults: &faultTransport{base: shim}, chunk: chunk}
}

func (e *fakeEnv) client(t *testing.T) *storage.Client {
	t.Helper()
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint(e.server.URL()+"/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: e.faults}),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func (e *fakeEnv) store(t *testing.T) *Store {
	t.Helper()
	store, err := New(e.client(t), testBucket)
	if err != nil {
		t.Fatal(err)
	}
	store.chunkBytes = e.chunk
	return store
}

func (e *fakeEnv) harness(t *testing.T, listObjects int) storetest.Harness {
	store := e.store(t)
	raw := e.client(t)
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: "conformance/", ListObjects: listObjects,
		Reopen: func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened := e.store(t)
			return reopened, reopened
		},
		Replace: func(t *testing.T, key string, body []byte) {
			w := raw.Bucket(testBucket).Object(key).NewWriter(context.Background())
			if _, err := w.Write(body); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		},
		Inject: func(t *testing.T, op storetest.Op, kind storetest.FaultKind) func() int {
			var match func(*http.Request) bool
			switch op {
			case storetest.OpPut, storetest.OpCompareAndSwap, storetest.OpCreate:
				// Only the commit request is counted and faulted, so a
				// multi-request resumable session still reports one request.
				match = isCommit
			case storetest.OpOpenRange:
				match = isMediaRead
			case storetest.OpDeleteIfIdentity:
				match = func(r *http.Request) bool { return r.Method == http.MethodDelete }
			default:
				t.Skipf("gcs harness cannot express a fault on %s", op)
			}
			requests := e.faults.arm(match, kind)
			t.Cleanup(func() { e.faults.arm(nil, 0) })
			return requests
		},
	}
}

func TestMetadataConformance(t *testing.T) {
	storetest.MetadataSuite(t, newFakeEnv(t, createChunkBytes).harness(t, 0))
}

func TestRunConformance(t *testing.T) {
	storetest.RunSuite(t, newFakeEnv(t, createChunkBytes).harness(t, 0))
}

// TestRunConformanceResumable repeats the run suite with a 256 KiB ChunkSize so
// every multi-hundred-KiB Create takes the multi-request resumable path
// (initiation, intermediate chunks, finalizing chunk) against the fake.
func TestRunConformanceResumable(t *testing.T) {
	storetest.RunSuite(t, newFakeEnv(t, 256<<10).harness(t, 0))
}

// TestFakeEnforcement proves the suites are not vacuous: every precondition the
// leaf relies on is really enforced by fake plus shim, observed with the raw
// SDK and no leaf code.
func TestFakeEnforcement(t *testing.T) {
	env := newFakeEnv(t, createChunkBytes)
	ctx := context.Background()
	bucket := env.client(t).Bucket(testBucket)
	write := func(obj *storage.ObjectHandle, chunk int, body []byte) (*storage.ObjectAttrs, error) {
		w := obj.NewWriter(ctx)
		w.ChunkSize = chunk
		if _, err := w.Write(body); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return w.Attrs(), nil
	}
	is412 := func(err error) bool { return isPrecondition(err) }
	body := bytes.Repeat([]byte("x"), 600<<10)

	obj := bucket.Object("probe/a")
	first, err := write(obj.If(storage.Conditions{DoesNotExist: true}), 0, body)
	if err != nil || first.Generation == 0 {
		t.Fatalf("create: %v", err)
	}
	if _, err := write(obj.If(storage.Conditions{DoesNotExist: true}), 0, body); !is412(err) {
		t.Fatalf("multipart DoesNotExist not enforced: %v", err)
	}
	if _, err := write(obj.If(storage.Conditions{DoesNotExist: true}), 256<<10, body); !is412(err) {
		t.Fatalf("resumable DoesNotExist not enforced: %v", err)
	}
	if _, err := write(obj.If(storage.Conditions{GenerationMatch: first.Generation + 1}), 0, body); !is412(err) {
		t.Fatalf("stale GenerationMatch not enforced: %v", err)
	}
	second, err := write(obj.If(storage.Conditions{GenerationMatch: first.Generation}), 0, []byte("second"))
	if err != nil || second.Generation == first.Generation {
		t.Fatalf("matching GenerationMatch: %v", err)
	}
	if _, err := obj.Generation(first.Generation).NewRangeReader(ctx, 0, 1); !isNotFound(err) {
		t.Fatalf("pinned read of a replaced generation: %v", err)
	}
	r, err := obj.Generation(second.Generation).NewRangeReader(ctx, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "eco" || r.Attrs.Generation != second.Generation || r.Attrs.Size != 6 {
		t.Fatalf("pinned range: %q %+v", got, r.Attrs)
	}
	if err := obj.If(storage.Conditions{GenerationMatch: first.Generation}).Delete(ctx); !is412(err) {
		t.Fatalf("conditional delete not enforced: %v", err)
	}
	if _, err := obj.Attrs(ctx); err != nil {
		t.Fatalf("rejected delete removed the object: %v", err)
	}
	if err := obj.If(storage.Conditions{GenerationMatch: second.Generation}).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := obj.If(storage.Conditions{GenerationMatch: second.Generation}).Delete(ctx); !isNotFound(err) {
		t.Fatalf("conditional delete of a missing object: %v", err)
	}
}
