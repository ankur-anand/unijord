package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/ankur-anand/isledb/blobstore"
)

// ---- scripted transport: no server, every request recorded ----

type recorded struct {
	method    string
	path      string
	query     url.Values
	header    http.Header
	bodyBytes int64
	kind      string // init | chunk | final | multipart | media | meta | delete | list
}

func classify(r *http.Request) string {
	switch {
	case isResumableInit(r):
		return "init"
	case isUpload(r) && r.URL.Query().Get("upload_id") != "":
		if isCommit(r) {
			return "final"
		}
		return "chunk"
	case isUpload(r):
		return "multipart"
	case r.Method == http.MethodDelete:
		return "delete"
	case isMediaRead(r):
		return "media"
	case strings.HasSuffix(r.URL.Path, "/o"):
		return "list"
	default:
		return "meta"
	}
}

// trackedBody counts Read and Close calls on one response body.
type trackedBody struct {
	r      io.Reader
	reads  atomic.Int64
	closes atomic.Int64
	err    error // returned instead of EOF when set
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	n, err := b.r.Read(p)
	if err == io.EOF && b.err != nil {
		err = b.err
	}
	return n, err
}

func (b *trackedBody) Close() error { b.closes.Add(1); return nil }

type scripted struct {
	t *testing.T
	// respond answers request number i (0-based, in arrival order).
	respond func(i int, r *http.Request, rec recorded) (*http.Response, error)
	// onBody observes each request after its body was drained.
	onBody func(rec recorded)

	mu        sync.Mutex
	requests  []recorded
	cancelled []recorded // presented with an already-cancelled context: never sent
	bodies    []*trackedBody
}

func (s *scripted) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := recorded{method: r.Method, path: r.URL.Path, query: r.URL.Query(), header: r.Header.Clone(), kind: classify(r)}
	if err := r.Context().Err(); err != nil {
		// net/http's Transport refuses such a request before writing a byte.
		if r.Body != nil {
			r.Body.Close()
		}
		s.mu.Lock()
		s.cancelled = append(s.cancelled, rec)
		s.mu.Unlock()
		return nil, err
	}
	var bodyErr error
	if r.Body != nil {
		rec.bodyBytes, bodyErr = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	s.mu.Lock()
	i := len(s.requests)
	s.requests = append(s.requests, rec)
	s.mu.Unlock()
	if s.onBody != nil {
		s.onBody(rec)
	}
	if bodyErr != nil {
		return nil, bodyErr
	}
	resp, err := s.respond(i, r, rec)
	if resp != nil {
		if tb, ok := resp.Body.(*trackedBody); ok {
			s.mu.Lock()
			s.bodies = append(s.bodies, tb)
			s.mu.Unlock()
		}
	}
	return resp, err
}

func (s *scripted) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.requests))
	for i, r := range s.requests {
		out[i] = r.kind
	}
	return out
}

func (s *scripted) count(kind string) int {
	n := 0
	for _, k := range s.kinds() {
		if k == kind {
			n++
		}
	}
	return n
}

func (s *scripted) first(kind string) recorded {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.requests {
		if r.kind == kind {
			return r
		}
	}
	s.t.Fatalf("no %s request recorded", kind)
	return recorded{}
}

// assertBodiesClosedOnce checks every response body handed to the SDK.
func (s *scripted) assertBodiesClosedOnce() {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, b := range s.bodies {
		if n := b.closes.Load(); n != 1 {
			s.t.Fatalf("response body %d closed %d times", i, n)
		}
	}
}

func respond(r *http.Request, code int, header http.Header, body string) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "application/json; charset=UTF-8")
	}
	return &http.Response{
		StatusCode: code, Status: strconv.Itoa(code) + " " + http.StatusText(code),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: header,
		Body: &trackedBody{r: strings.NewReader(body)}, ContentLength: int64(len(body)), Request: r,
	}
}

func apiError(r *http.Request, code int) *http.Response {
	return respond(r, code, nil, fmt.Sprintf(`{"error":{"code":%d,"message":%q}}`, code, http.StatusText(code)))
}

func objectJSON(name string, generation, size int64) string {
	return fmt.Sprintf(`{"kind":"storage#object","bucket":"b","name":%q,"generation":"%d","metageneration":"1","size":"%d"}`,
		name, generation, size)
}

const sessionLocation = "http://scripted.invalid/upload/storage/v1/b/b/o?uploadType=resumable&name=k&upload_id=session-1"

// happyUpload answers a whole create: multipart, or init + chunks + final.
func happyUpload(key string, generation int64) func(int, *http.Request, recorded) (*http.Response, error) {
	var total atomic.Int64
	return func(_ int, r *http.Request, rec recorded) (*http.Response, error) {
		switch rec.kind {
		case "init":
			return respond(r, 200, http.Header{"Location": {sessionLocation}}, ""), nil
		case "chunk":
			total.Add(rec.bodyBytes)
			return respond(r, 200, http.Header{"X-Http-Status-Code-Override": {"308"}}, ""), nil
		case "final":
			return respond(r, 200, nil, objectJSON(key, generation, total.Add(rec.bodyBytes))), nil
		}
		return apiError(r, 500), nil // multipart inserts are scripted by multipartOK
	}
}

func scriptedStore(t *testing.T, chunk int, respondFn func(int, *http.Request, recorded) (*http.Response, error)) (*Store, *scripted) {
	t.Helper()
	script := &scripted{t: t, respond: respondFn}
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint("http://scripted.invalid/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: script}),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	store, err := New(client, "b")
	if err != nil {
		t.Fatal(err)
	}
	store.chunkBytes = chunk
	return store, script
}

// generator yields a deterministic stream without ever materializing it.
type generator struct {
	remaining int64
	hook      func(produced int64) error
	produced  int64
}

func (g *generator) Read(p []byte) (int, error) {
	if g.hook != nil {
		if err := g.hook(g.produced); err != nil {
			return 0, err
		}
	}
	if g.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), g.remaining, 64<<10))
	for i := range p[:n] {
		p[i] = byte(g.produced + int64(i))
	}
	g.remaining -= int64(n)
	g.produced += int64(n)
	return n, nil
}

func identityFor(t *testing.T, key string, size, generation int64) blobstore.RunIdentity {
	t.Helper()
	id, err := runIdentity(key, size, generation)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// multipartOK answers multipart inserts with the given object size.
func multipartOK(key string, generation, size int64) func(int, *http.Request, recorded) (*http.Response, error) {
	return func(_ int, r *http.Request, rec recorded) (*http.Response, error) {
		if rec.kind != "multipart" {
			return apiError(r, 500), nil
		}
		return respond(r, 200, nil, objectJSON(key, generation, size)), nil
	}
}

// ---- request shape ----

func TestCreateRequestShapeAndTokenRoundTrip(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, multipartOK("k", 77, 1000))
	res, err := store.Create(context.Background(), "k", &generator{remaining: 1000}, 1000)
	if err != nil || res.Outcome != blobstore.Created {
		t.Fatalf("%+v %v", res, err)
	}
	if got := script.kinds(); len(got) != 1 || got[0] != "multipart" {
		t.Fatalf("requests: %v", got)
	}
	req := script.first("multipart")
	if req.query.Get("ifGenerationMatch") != "0" || req.query.Get("name") != "k" {
		t.Fatalf("create-only condition missing: %v", req.query)
	}
	generation, err := decodeGeneration(res.Identity)
	if err != nil || generation != 77 || res.Identity.Size != 1000 || res.Identity.Key != "k" {
		t.Fatalf("token round trip: %d %v %+v", generation, err, res.Identity)
	}
	if again := identityFor(t, "k", 1000, 77); !again.Equal(res.Identity) {
		t.Fatalf("token not deterministic: %q vs %q", again.Token, res.Identity.Token)
	}
	script.assertBodiesClosedOnce()
}

func TestResumableCreateRequestShape(t *testing.T) {
	const size = 1<<20 + 5
	store, script := scriptedStore(t, 256<<10, happyUpload("k", 9))
	res, err := store.Create(context.Background(), "k", &generator{remaining: size}, size)
	if err != nil || res.Outcome != blobstore.Created || res.Identity.Size != size {
		t.Fatalf("%+v %v", res, err)
	}
	if got := strings.Join(script.kinds(), ","); got != "init,chunk,chunk,chunk,chunk,final" {
		t.Fatalf("requests: %s", got)
	}
	if init := script.first("init"); init.query.Get("ifGenerationMatch") != "0" {
		t.Fatalf("create-only condition missing on session initiation: %v", init.query)
	}
	if final := script.first("final"); final.header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", 4*(256<<10), size-1, size) {
		t.Fatalf("final Content-Range: %q", final.header.Get("Content-Range"))
	}
	script.assertBodiesClosedOnce()
}

func TestCompareAndSwapSendsGenerationMatch(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, multipartOK("m", 43, 2))
	res, err := store.CompareAndSwap(context.Background(), "m", "42", []byte("v2"))
	if err != nil || res.Outcome != blobstore.CASApplied || res.Object.Token != "43" {
		t.Fatalf("%+v %v", res, err)
	}
	if q := script.first("multipart").query; q.Get("ifGenerationMatch") != "42" {
		t.Fatalf("query: %v", q)
	}
	res, err = store.CompareAndSwap(context.Background(), "m", "", []byte("v2"))
	if err != nil || res.Outcome != blobstore.CASApplied {
		t.Fatalf("%+v %v", res, err)
	}
	script.mu.Lock()
	second := script.requests[1].query.Get("ifGenerationMatch")
	script.mu.Unlock()
	if second != "0" {
		t.Fatalf("create-if-absent sent ifGenerationMatch=%q", second)
	}
	for _, bad := range []string{"g42", "042", "-1", "0", "4 2", "99999999999999999999"} {
		res, err := store.CompareAndSwap(context.Background(), "m", bad, []byte("x"))
		if res.Outcome != blobstore.CASUnknown || !errors.Is(err, blobstore.ErrInvalidRequest) {
			t.Fatalf("token %q: %+v %v", bad, res, err)
		}
	}
	if n := len(script.kinds()); n != 2 {
		t.Fatalf("malformed tokens reached the provider: %d requests", n)
	}
	script.assertBodiesClosedOnce()
}

func rangeResponse(r *http.Request, generation, offset, length, size int64, body io.Reader, bodyErr error) *http.Response {
	header := http.Header{
		"Content-Type":      {"application/octet-stream"},
		"Content-Range":     {fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, size)},
		"X-Goog-Generation": {strconv.FormatInt(generation, 10)},
	}
	return &http.Response{StatusCode: 206, Status: "206 Partial Content", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: header, Body: &trackedBody{r: body, err: bodyErr}, ContentLength: length, Request: r}
}

func TestOpenRangeRequestIsPinned(t *testing.T) {
	payload := bytes.Repeat([]byte("abcdefgh"), 100)
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return rangeResponse(r, 1234, 16, 64, 800, bytes.NewReader(payload[16:80]), nil), nil
	})
	id := identityFor(t, "run/k", 800, 1234)
	body, err := store.OpenRange(context.Background(), "run/k", id, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	if err != nil || !bytes.Equal(got, payload[16:80]) {
		t.Fatalf("%q %v", got, err)
	}
	for i := 0; i < 2; i++ {
		if err := body.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
	req := script.first("media")
	if req.query.Get("generation") != "1234" || req.header.Get("Range") != "bytes=16-79" {
		t.Fatalf("pinned read: query=%v range=%q", req.query, req.header.Get("Range"))
	}
	if n := len(script.kinds()); n != 1 {
		t.Fatalf("%d requests", n)
	}
	script.assertBodiesClosedOnce()
}

func TestOpenRangeRejectsUnexpectedShape(t *testing.T) {
	for name, tc := range map[string]struct{ size, length int64 }{
		"size differs":   {size: 801, length: 64},
		"length differs": {size: 800, length: 63},
	} {
		store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
			return rangeResponse(r, 5, 0, tc.length, tc.size, bytes.NewReader(make([]byte, tc.length)), nil), nil
		})
		if _, err := store.OpenRange(context.Background(), "k", identityFor(t, "k", 800, 5), 0, 64); !errors.Is(err, blobstore.ErrRunChanged) {
			t.Fatalf("%s: %v", name, err)
		}
		script.assertBodiesClosedOnce()
	}
}

func TestOpenRangeTruncatedStream(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		// Declares 64 bytes, delivers 10 and a clean EOF.
		return rangeResponse(r, 5, 0, 64, 800, bytes.NewReader(make([]byte, 10)), nil), nil
	})
	body, err := store.OpenRange(context.Background(), "k", identityFor(t, "k", 800, 5), 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	if len(got) != 10 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("%d bytes, %v", len(got), err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	script.assertBodiesClosedOnce()
}

// TestOpenRangeMidStreamResumeStaysPinned pins down SDK behaviour this leaf
// cannot switch off (doc.go): on a mid-stream body error httpReader.Read
// re-requests the remaining bytes once. The test proves the re-request is
// still pinned to the identity's generation, so the stream can only ever
// contain bytes of the pinned object, and that a failed re-request surfaces.
func TestOpenRangeMidStreamResumeStaysPinned(t *testing.T) {
	broken := errors.New("connection reset mid body")
	store, script := scriptedStore(t, createChunkBytes, func(i int, r *http.Request, _ recorded) (*http.Response, error) {
		if i == 0 {
			return rangeResponse(r, 5, 0, 64, 800, bytes.NewReader(make([]byte, 10)), broken), nil
		}
		return apiError(r, 503), nil
	})
	body, err := store.OpenRange(context.Background(), "k", identityFor(t, "k", 800, 5), 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	if len(got) != 10 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("%d bytes, %v", len(got), err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	script.mu.Lock()
	defer script.mu.Unlock()
	if len(script.requests) != 2 {
		t.Fatalf("pinned SDK sent %d requests; doc.go documents exactly one resume", len(script.requests))
	}
	resume := script.requests[1]
	if resume.query.Get("generation") != "5" || resume.header.Get("Range") != "bytes=10-63" {
		t.Fatalf("resume not pinned: %v %q", resume.query, resume.header.Get("Range"))
	}
}

func TestDeleteIfIdentityRequestShape(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return respond(r, 204, nil, ""), nil
	})
	if err := store.DeleteIfIdentity(context.Background(), "k", identityFor(t, "k", 10, 321)); err != nil {
		t.Fatal(err)
	}
	req := script.first("delete")
	if req.query.Get("ifGenerationMatch") != "321" || req.query.Get("generation") != "" {
		t.Fatalf("conditional delete must target the live object: %v", req.query)
	}
	script.assertBodiesClosedOnce()
}

// ---- 412: the only definite conflict ----

func TestPreconditionFailedIsDefiniteConflict(t *testing.T) {
	ctx := context.Background()
	always412 := func(_ int, r *http.Request, _ recorded) (*http.Response, error) { return apiError(r, 412), nil }

	t.Run("Create multipart", func(t *testing.T) {
		store, script := scriptedStore(t, createChunkBytes, always412)
		res, err := store.Create(ctx, "k", &generator{remaining: 100}, 100)
		if res.Outcome != blobstore.AlreadyExists || !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("%+v %v", res, err)
		}
		if n := len(script.kinds()); n != 1 {
			t.Fatalf("%d requests", n)
		}
		script.assertBodiesClosedOnce()
	})
	t.Run("Create resumable at session initiation", func(t *testing.T) {
		// The 412 arrives at the first chunk flush, while the producer still
		// has bytes: it must surface through the failed Write, not as Absent.
		store, script := scriptedStore(t, 256<<10, always412)
		res, err := store.Create(ctx, "k", &generator{remaining: 1 << 20}, 1<<20)
		if res.Outcome != blobstore.AlreadyExists || !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("%+v %v", res, err)
		}
		if got := strings.Join(script.kinds(), ","); got != "init" {
			t.Fatalf("requests: %s", got)
		}
		script.assertBodiesClosedOnce()
	})
	t.Run("Create resumable at finalization", func(t *testing.T) {
		happy := happyUpload("k", 3)
		store, script := scriptedStore(t, 256<<10, func(i int, r *http.Request, rec recorded) (*http.Response, error) {
			if rec.kind == "final" {
				return apiError(r, 412), nil
			}
			return happy(i, r, rec)
		})
		res, err := store.Create(ctx, "k", &generator{remaining: 600 << 10}, 600<<10)
		if res.Outcome != blobstore.AlreadyExists || !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("%+v %v", res, err)
		}
		if script.count("final") != 1 {
			t.Fatalf("requests: %v", script.kinds())
		}
		script.assertBodiesClosedOnce()
	})
	t.Run("CompareAndSwap", func(t *testing.T) {
		store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, rec recorded) (*http.Response, error) {
			if rec.kind == "multipart" {
				return apiError(r, 412), nil
			}
			return respond(r, 200, nil, objectJSON("m", 50, 7)), nil
		})
		res, err := store.CompareAndSwap(ctx, "m", "42", []byte("x"))
		if err != nil || res.Outcome != blobstore.CASConflict || !res.CurrentKnown || res.CurrentToken != "50" || res.Current.Size != 7 {
			t.Fatalf("%+v %v", res, err)
		}
		if got := strings.Join(script.kinds(), ","); got != "multipart,meta" {
			t.Fatalf("exactly one follow-up stat expected: %s", got)
		}
		script.assertBodiesClosedOnce()
	})
	t.Run("CompareAndSwap follow-up stat fails", func(t *testing.T) {
		store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, rec recorded) (*http.Response, error) {
			if rec.kind == "multipart" {
				return apiError(r, 412), nil
			}
			return apiError(r, 503), nil
		})
		res, err := store.CompareAndSwap(ctx, "m", "42", []byte("x"))
		if err != nil || res.Outcome != blobstore.CASConflict || res.CurrentKnown {
			t.Fatalf("a definite rejection must win over the stat failure: %+v %v", res, err)
		}
		if n := len(script.kinds()); n != 2 {
			t.Fatalf("%d requests", n)
		}
	})
	t.Run("DeleteIfIdentity", func(t *testing.T) {
		store, _ := scriptedStore(t, createChunkBytes, always412)
		if err := store.DeleteIfIdentity(ctx, "k", identityFor(t, "k", 10, 5)); !errors.Is(err, blobstore.ErrRunChanged) {
			t.Fatal(err)
		}
	})
	t.Run("Put differing winner", func(t *testing.T) {
		store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, rec recorded) (*http.Response, error) {
			if rec.kind == "multipart" {
				return apiError(r, 412), nil
			}
			return &http.Response{StatusCode: 200, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
				Header: http.Header{"X-Goog-Generation": {"8"}}, Body: &trackedBody{r: strings.NewReader("zzzz")},
				ContentLength: 4, Request: r}, nil
		})
		if _, err := store.Put(ctx, "m", []byte("abcd")); !errors.Is(err, blobstore.ErrImmutableConflict) {
			t.Fatal(err)
		}
		got, err := store.Put(ctx, "m", []byte("zzzz"))
		if err != nil || got.Token != "8" {
			t.Fatalf("identical replay: %+v %v", got, err)
		}
		script.assertBodiesClosedOnce()
	})
}

// ---- 429 / 503: never a conflict, never retried ----

func TestThrottleAndUnavailableAreNeverConflictsAndNeverRetried(t *testing.T) {
	ctx := context.Background()
	for _, code := range []int{429, 503} {
		fail := func(_ int, r *http.Request, _ recorded) (*http.Response, error) { return apiError(r, code), nil }
		failKind := func(kind string) func(int, *http.Request, recorded) (*http.Response, error) {
			happy := happyUpload("k", 3)
			return func(i int, r *http.Request, rec recorded) (*http.Response, error) {
				if rec.kind == kind {
					return apiError(r, code), nil
				}
				return happy(i, r, rec)
			}
		}
		notConflict := func(t *testing.T, err error) {
			t.Helper()
			for _, sentinel := range []error{blobstore.ErrAlreadyExists, blobstore.ErrRunChanged, blobstore.ErrImmutableConflict, blobstore.ErrNotFound} {
				if err == nil || errors.Is(err, sentinel) {
					t.Fatalf("HTTP %d mapped to %v", code, err)
				}
			}
		}
		t.Run(fmt.Sprintf("%d", code), func(t *testing.T) {
			store, script := scriptedStore(t, createChunkBytes, fail)
			res, err := store.Create(ctx, "k", &generator{remaining: 100}, 100)
			notConflict(t, err)
			if res.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) || len(script.kinds()) != 1 {
				t.Fatalf("Create multipart: %+v %v %v", res, err, script.kinds())
			}

			for kind, want := range map[string]blobstore.CreateOutcome{
				// Initiation and intermediate chunks fail while the producer
				// still holds bytes: nothing was finalized.
				"init":  blobstore.DefinitelyAbsent,
				"chunk": blobstore.DefinitelyAbsent,
				"final": blobstore.CreateIndeterminate,
			} {
				store, script = scriptedStore(t, 256<<10, failKind(kind))
				res, err = store.Create(ctx, "k", &generator{remaining: 1 << 20}, 1<<20)
				notConflict(t, err)
				if res.Outcome != want || script.count(kind) != 1 {
					t.Fatalf("Create resumable, %s fails: %+v %v requests=%v", kind, res, err, script.kinds())
				}
				if kind != "final" && script.count("final") != 0 {
					t.Fatalf("finalized after a failed %s: %v", kind, script.kinds())
				}
			}

			store, script = scriptedStore(t, createChunkBytes, fail)
			cas, err := store.CompareAndSwap(ctx, "m", "42", []byte("x"))
			notConflict(t, err)
			if cas.Outcome != blobstore.CASUnknown || len(script.kinds()) != 1 {
				t.Fatalf("CAS: %+v %v %v", cas, err, script.kinds())
			}

			store, script = scriptedStore(t, createChunkBytes, fail)
			_, err = store.Put(ctx, "m", []byte("x"))
			notConflict(t, err)
			if !errors.Is(err, blobstore.ErrIndeterminate) || len(script.kinds()) != 1 {
				t.Fatalf("Put: %v %v", err, script.kinds())
			}

			store, script = scriptedStore(t, createChunkBytes, fail)
			err = store.DeleteIfIdentity(ctx, "k", identityFor(t, "k", 10, 5))
			notConflict(t, err)
			if !errors.Is(err, blobstore.ErrIndeterminate) || len(script.kinds()) != 1 {
				t.Fatalf("DeleteIfIdentity: %v %v", err, script.kinds())
			}

			store, script = scriptedStore(t, createChunkBytes, fail)
			_, err = store.OpenRange(ctx, "k", identityFor(t, "k", 10, 5), 0, 10)
			notConflict(t, err)
			if len(script.kinds()) != 1 {
				t.Fatalf("OpenRange: %v", script.kinds())
			}

			store, script = scriptedStore(t, createChunkBytes, fail)
			_, err = store.Stat(ctx, "k")
			notConflict(t, err)
			_, err = store.BoundedGet(ctx, "k", 10)
			notConflict(t, err)
			_, err = store.List(ctx, blobstore.ListOptions{Prefix: "p/"})
			notConflict(t, err)
			err = store.Delete(ctx, "k")
			notConflict(t, err)
			if got := strings.Join(script.kinds(), ","); got != "meta,media,list,delete" {
				t.Fatalf("read-only operations were retried: %s", got)
			}
			script.assertBodiesClosedOnce()
		})
	}
}

// TestRetryNeverAloneStillRetriesChunks is the evidence for createObject's
// retry options. With only WithPolicy(RetryNever) the pinned SDK still retries
// a failed resumable chunk, because httpStorageClient.OpenWriter then never
// calls ObjectsInsertCall.WithRetry and gensupport's
// ResumableUpload.uploadChunkWithRetries falls back to its default predicate.
func TestRetryNeverAloneStillRetriesChunks(t *testing.T) {
	happy := happyUpload("k", 3)
	var failed atomic.Bool
	_, script := scriptedStore(t, 256<<10, nil)
	script.respond = func(i int, r *http.Request, rec recorded) (*http.Response, error) {
		if rec.kind == "chunk" && failed.CompareAndSwap(false, true) {
			return apiError(r, 503), nil
		}
		if rec.kind == "chunk" {
			// The retried chunk must not be double counted by happyUpload.
			return respond(r, 200, http.Header{"X-Http-Status-Code-Override": {"308"}}, ""), nil
		}
		return happy(i, r, rec)
	}
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint("http://scripted.invalid/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: script}), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	w := client.Bucket("b").Object("k").If(storage.Conditions{DoesNotExist: true}).
		Retryer(storage.WithPolicy(storage.RetryNever)).NewWriter(context.Background())
	w.ChunkSize = 256 << 10
	if _, err := io.Copy(w, &generator{remaining: 300 << 10}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if n := script.count("chunk"); n != 2 {
		t.Fatalf("pinned SDK sent the failed chunk %d time(s) under RetryNever; "+
			"if this is now 1, createObject can return to plain RetryNever", n)
	}
}

// ---- lost responses ----

func TestLostFinalResponseIsIndeterminate(t *testing.T) {
	lost := errors.New("response lost")
	for name, chunk := range map[string]int{"multipart": createChunkBytes, "resumable": 256 << 10} {
		happy := happyUpload("k", 3)
		store, script := scriptedStore(t, chunk, func(i int, r *http.Request, rec recorded) (*http.Response, error) {
			if rec.kind == "final" || rec.kind == "multipart" {
				return nil, lost // the body was fully consumed first
			}
			return happy(i, r, rec)
		})
		res, err := store.Create(context.Background(), "k", &generator{remaining: 600 << 10}, 600<<10)
		if res.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, lost) {
			t.Fatalf("%s: %+v %v", name, res, err)
		}
		if n := script.count("final") + script.count("multipart"); n != 1 {
			t.Fatalf("%s: %d commit requests", name, n)
		}
		script.assertBodiesClosedOnce()
	}
}

func TestCreateAcknowledgedWithWrongSizeIsIndeterminate(t *testing.T) {
	store, _ := scriptedStore(t, createChunkBytes, multipartOK("k", 3, 99))
	res, err := store.Create(context.Background(), "k", &generator{remaining: 100}, 100)
	if res.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) {
		t.Fatalf("%+v %v", res, err)
	}
	store, _ = scriptedStore(t, createChunkBytes, multipartOK("k", 0, 100))
	res, err = store.Create(context.Background(), "k", &generator{remaining: 100}, 100)
	if res.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) {
		t.Fatalf("zero generation: %+v %v", res, err)
	}
}

// ---- GCS resumable-session failure behaviour ----

func assertNeverFinalized(t *testing.T, script *scripted) {
	t.Helper()
	script.mu.Lock()
	defer script.mu.Unlock()
	for _, group := range [][]recorded{script.requests, script.cancelled} {
		for _, r := range group {
			if r.kind == "final" || r.kind == "multipart" {
				t.Fatalf("a finalizing request was issued: %s %s %q", r.method, r.path, r.header.Get("Content-Range"))
			}
			// Pinned SDK fact (doc.go): gensupport has no resumable-session
			// cancellation request at all, so none is ever observed.
			if r.method == http.MethodDelete {
				t.Fatalf("unexpected session cancellation request: %s", r.path)
			}
		}
	}
}

func TestProducerFailureNeverFinalizesResumableSession(t *testing.T) {
	boom := errors.New("producer failed")
	const size = 1 << 20
	for name, failAt := range map[string]int64{
		"before first chunk":   100 << 10,
		"mid stream":           700 << 10,
		"at last byte":         size - 1,
		"after every byte":     size, // late error instead of EOF
		"on chunk boundary":    512 << 10,
		"first read":           0,
		"one byte into chunk2": 256<<10 + 1,
	} {
		t.Run(name, func(t *testing.T) {
			store, script := scriptedStore(t, 256<<10, happyUpload("k", 3))
			body := &generator{remaining: size, hook: func(produced int64) error {
				if produced >= failAt {
					return boom
				}
				return nil
			}}
			res, err := store.Create(context.Background(), "k", body, size)
			if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, boom) || errors.Is(err, context.Canceled) {
				t.Fatalf("%+v %v", res, err)
			}
			assertNeverFinalized(t, script)
			// Bytes that did leave are whole, non-final chunks only.
			if want := int(failAt / (256 << 10)); script.count("chunk") > want {
				t.Fatalf("chunks sent %d > %d", script.count("chunk"), want)
			}
			script.assertBodiesClosedOnce()
		})
	}
}

func TestShortAndOversizedBodiesNeverFinalize(t *testing.T) {
	for _, chunk := range []int{createChunkBytes, 256 << 10} {
		for name, tc := range map[string]struct {
			produced, declared int64
			want               error
		}{
			"short":         {600<<10 - 1, 600 << 10, io.ErrUnexpectedEOF},
			"long":          {600<<10 + 1, 600 << 10, blobstore.ErrInvalidRequest},
			"short tiny":    {9, 10, io.ErrUnexpectedEOF},
			"long tiny":     {11, 10, blobstore.ErrInvalidRequest},
			"empty":         {0, 10, io.ErrUnexpectedEOF},
			"chunk aligned": {512<<10 + 1, 512 << 10, blobstore.ErrInvalidRequest},
		} {
			store, script := scriptedStore(t, chunk, happyUpload("k", 3))
			res, err := store.Create(context.Background(), "k", &generator{remaining: tc.produced}, tc.declared)
			if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, tc.want) {
				t.Fatalf("%s/%d: %+v %v", name, chunk, res, err)
			}
			assertNeverFinalized(t, script)
		}
	}
	store, script := scriptedStore(t, createChunkBytes, happyUpload("k", 3))
	for _, size := range []int64{0, -1, MaxCreateBytes + 1} {
		res, err := store.Create(context.Background(), "k", &generator{remaining: 1}, size)
		if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, blobstore.ErrInvalidRequest) {
			t.Fatalf("size %d: %+v %v", size, res, err)
		}
	}
	if res, err := store.Create(context.Background(), "k", nil, 1); res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, blobstore.ErrInvalidRequest) {
		t.Fatalf("nil body: %+v %v", res, err)
	}
	if n := len(script.kinds()); n != 0 {
		t.Fatalf("invalid requests reached the provider: %d", n)
	}
}

func TestCreateCancellation(t *testing.T) {
	t.Run("before send", func(t *testing.T) {
		store, script := scriptedStore(t, 256<<10, happyUpload("k", 3))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		body := &generator{remaining: 1 << 20}
		res, err := store.Create(ctx, "k", body, 1<<20)
		if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) {
			t.Fatalf("%+v %v", res, err)
		}
		if body.produced != 0 || len(script.kinds()) != 0 || len(script.cancelled) != 0 {
			t.Fatalf("I/O after cancellation: produced=%d requests=%v", body.produced, script.kinds())
		}
	})
	t.Run("mid body", func(t *testing.T) {
		store, script := scriptedStore(t, 256<<10, happyUpload("k", 3))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := &generator{remaining: 1 << 20, hook: func(produced int64) error {
			if produced >= 600<<10 {
				cancel() // the producer keeps producing; the store must notice
			}
			return nil
		}}
		res, err := store.Create(ctx, "k", body, 1<<20)
		if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) {
			t.Fatalf("%+v %v", res, err)
		}
		if body.remaining == 0 {
			t.Fatal("producer drained after cancellation")
		}
		assertNeverFinalized(t, script)
		script.assertBodiesClosedOnce()
	})
}

// ---- bounded upload buffering ----

func TestCreateUploadBufferingIsBounded(t *testing.T) {
	const size = 40 << 20
	const slack = 64 << 10
	store, script := scriptedStore(t, createChunkBytes, happyUpload("k", 3))

	var baseline, peakHeap uint64
	var sampleMu sync.Mutex // the producer and the SDK's upload goroutine both sample
	sample := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		sampleMu.Lock()
		if m.HeapAlloc > peakHeap {
			peakHeap = m.HeapAlloc
		}
		sampleMu.Unlock()
	}
	script.onBody = func(recorded) { sample() }
	body := &generator{remaining: size, hook: func(produced int64) error {
		if produced%(4<<20) == 0 {
			sample()
		}
		return nil
	}}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	baseline = before.HeapAlloc

	res, err := store.Create(context.Background(), "k", body, size)

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if err != nil || res.Outcome != blobstore.Created || res.Identity.Size != size {
		t.Fatalf("%+v %v", res, err)
	}
	if got := strings.Join(script.kinds(), ","); got != "init,chunk,chunk,final" {
		t.Fatalf("requests: %s", got)
	}
	script.mu.Lock()
	var sent int64
	for _, r := range script.requests {
		if r.kind == "init" {
			continue
		}
		sent += r.bodyBytes
		if r.bodyBytes > createChunkBytes+slack {
			t.Fatalf("%s request body of %d bytes exceeds the %d byte chunk", r.kind, r.bodyBytes, createChunkBytes)
		}
	}
	script.mu.Unlock()
	if sent != size {
		t.Fatalf("sent %d of %d bytes", sent, size)
	}
	// Everything allocated during the whole upload, garbage included, stays
	// near one chunk: the 40 MiB object is never materialized, not even
	// piecewise.
	allocated := after.TotalAlloc - before.TotalAlloc
	sampleMu.Lock()
	growth := int64(peakHeap) - int64(baseline)
	sampleMu.Unlock()
	t.Logf("object=%d MiB total-allocated=%.1f MiB peak-heap-growth=%.1f MiB",
		size>>20, float64(allocated)/(1<<20), float64(growth)/(1<<20))
	if allocated > createChunkBytes+8<<20 {
		t.Fatalf("allocated %d bytes for a %d byte object; want about one %d byte chunk", allocated, size, createChunkBytes)
	}
	if growth > createChunkBytes+8<<20 {
		t.Fatalf("heap grew %d bytes", growth)
	}
	script.assertBodiesClosedOnce()
}

// ---- identity tokens ----

func TestForeignAndGarbageTokensNeverReachTheProvider(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return apiError(r, 500), nil
	})
	good := identityFor(t, "k", 10, 5)
	for name, token := range map[string]string{
		"garbage":          "garbage",
		"empty json":       "gcs.v1.e30",                   // {} -> generation 0
		"other provider":   "s3.v1.eyJnZW5lcmF0aW9uIjo1fQ", // {"generation":5}
		"other version":    "gcs.v2.eyJnZW5lcmF0aW9uIjo1fQ",
		"negative":         "gcs.v1.eyJnZW5lcmF0aW9uIjotNX0",        // {"generation":-5}
		"unknown field":    "gcs.v1.eyJnZW5lcmF0aW9uIjo1LCJ4IjoxfQ", // {"generation":5,"x":1}
		"non canonical":    "gcs.v1.eyJnZW5lcmF0aW9uIjogNX0",        // {"generation": 5}
		"string":           "gcs.v1.eyJnZW5lcmF0aW9uIjoiNSJ9",       // {"generation":"5"}
		"trailing":         good.Token + ".x",
		"metadata token":   "5",
		"padded base64":    good.Token + "=",
		"whitespace":       " " + good.Token,
		"memory provider":  "memory.v1.eyJnZW5lcmF0aW9uIjo1fQ",
		"uppercase prefix": "GCS.v1.eyJnZW5lcmF0aW9uIjo1fQ",
	} {
		id := blobstore.RunIdentity{Key: "k", Size: 10, Token: token}
		if _, err := store.OpenRange(context.Background(), "k", id, 0, 1); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("OpenRange %s: %v", name, err)
		}
		if err := store.DeleteIfIdentity(context.Background(), "k", id); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("DeleteIfIdentity %s: %v", name, err)
		}
	}
	other := good
	other.Key = "other"
	if err := store.DeleteIfIdentity(context.Background(), "k", other); !errors.Is(err, blobstore.ErrInvalidIdentity) {
		t.Fatalf("identity of another key: %v", err)
	}
	if n := len(script.kinds()); n != 0 {
		t.Fatalf("invalid identities reached the provider: %d requests", n)
	}
	if generation, err := decodeGeneration(good); err != nil || generation != 5 {
		t.Fatalf("round trip: %d %v", generation, err)
	}
}

func TestStatWithoutGenerationIsIndeterminate(t *testing.T) {
	store, _ := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return respond(r, 200, nil, `{"kind":"storage#object","name":"k","size":"10"}`), nil
	})
	if _, err := store.Stat(context.Background(), "k"); !errors.Is(err, blobstore.ErrIndeterminate) {
		t.Fatal(err)
	}
}

// ---- BoundedGet ----

func fullRead(r *http.Request, generation string, declared int64, body string) *http.Response {
	header := http.Header{"Content-Type": {"application/octet-stream"}}
	if generation != "" {
		header.Set("X-Goog-Generation", generation)
	}
	return &http.Response{StatusCode: 200, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: header, Body: &trackedBody{r: strings.NewReader(body)}, ContentLength: declared, Request: r}
}

func TestBoundedGetRejectsOversizedBeforeReadingTheBody(t *testing.T) {
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return fullRead(r, "9", 1000, strings.Repeat("x", 1000)), nil
	})
	if _, err := store.BoundedGet(context.Background(), "m", 999); !errors.Is(err, blobstore.ErrTooLarge) {
		t.Fatal(err)
	}
	script.mu.Lock()
	body := script.bodies[0]
	script.mu.Unlock()
	if body.reads.Load() != 0 || body.closes.Load() != 1 {
		t.Fatalf("oversized body: %d reads, %d closes", body.reads.Load(), body.closes.Load())
	}
}

func TestBoundedGetLengthDisagreementAndMissingGeneration(t *testing.T) {
	for name, tc := range map[string]struct {
		generation string
		declared   int64
		body       string
	}{
		"body shorter than declared": {"9", 10, "short"},
		"body longer than declared":  {"9", 4, "longer"},
		"no generation":              {"", 4, "four"},
		"no length":                  {"9", -1, "four"},
	} {
		store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
			return fullRead(r, tc.generation, tc.declared, tc.body), nil
		})
		if _, err := store.BoundedGet(context.Background(), "m", 64); !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("%s: %v", name, err)
		}
		script.assertBodiesClosedOnce()
	}
	store, script := scriptedStore(t, createChunkBytes, func(_ int, r *http.Request, _ recorded) (*http.Response, error) {
		return fullRead(r, "9", 4, "four"), nil
	})
	got, err := store.BoundedGet(context.Background(), "m", 4)
	if err != nil || string(got.Body) != "four" || got.Token != "9" {
		t.Fatalf("%+v %v", got, err)
	}
	if q := script.first("media").query; q.Get("generation") != "" {
		t.Fatalf("metadata read must follow the live object: %v", q)
	}
	script.assertBodiesClosedOnce()
}

func TestNewRejectsMissingArguments(t *testing.T) {
	if _, err := New(nil, "b"); !errors.Is(err, blobstore.ErrInvalidRequest) {
		t.Fatal(err)
	}
	client, err := storage.NewClient(context.Background(), option.WithHTTPClient(&http.Client{}), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := New(client, ""); !errors.Is(err, blobstore.ErrInvalidRequest) {
		t.Fatal(err)
	}
}
