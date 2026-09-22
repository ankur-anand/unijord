package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
	s3store "github.com/ankur-anand/isledb/blobstore/s3"
)

// scripted is a pure in-memory http.RoundTripper: no server, no sockets. Each
// test supplies the provider's answer and inspects the exact requests.
type scripted struct {
	mu       sync.Mutex
	handle   func(n int, r *http.Request) (*http.Response, error)
	requests []seenRequest
	opened   int // response bodies handed to the SDK
	closes   int // Close calls on those bodies
}

type seenRequest struct {
	method        string
	path          string
	query         url.Values
	header        http.Header
	contentLength int64
}

type countedBody struct {
	io.Reader
	owner *scripted
}

func (b *countedBody) Close() error {
	b.owner.mu.Lock()
	b.owner.closes++
	b.owner.mu.Unlock()
	return nil
}

func (s *scripted) RoundTrip(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.requests = append(s.requests, seenRequest{r.Method, r.URL.Path, r.URL.Query(), r.Header.Clone(), r.ContentLength})
	n := len(s.requests)
	s.mu.Unlock()
	if r.Body == nil {
		r.Body = http.NoBody // bodyless requests: let every handler drain safely
	}
	response, err := s.handle(n, r)
	_ = r.Body.Close()
	if response != nil {
		response.Request = r
	}
	return response, err
}

func (s *scripted) respond(status int, header map[string]string, body io.Reader) *http.Response {
	h := make(http.Header)
	for k, v := range header {
		h.Set(k, v)
	}
	length := int64(-1)
	if v := h.Get("Content-Length"); v != "" {
		length, _ = strconv.ParseInt(v, 10, 64)
	}
	if body == nil {
		body = strings.NewReader("")
	}
	s.mu.Lock()
	s.opened++
	s.mu.Unlock()
	return &http.Response{Status: http.StatusText(status), StatusCode: status, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: h, ContentLength: length, Body: &countedBody{Reader: body, owner: s}}
}

func (s *scripted) apiError(status int, code string) *http.Response {
	return s.respond(status, map[string]string{"Content-Type": "application/xml"},
		strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+code+`</Code><Message>`+code+`</Message></Error>`))
}

func (s *scripted) snapshot() (requests []seenRequest, opened, closes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]seenRequest(nil), s.requests...), s.opened, s.closes
}

func (s *scripted) assertClosedOnce(t *testing.T) {
	t.Helper()
	_, opened, closes := s.snapshot()
	if opened == 0 || opened != closes {
		t.Fatalf("response bodies opened=%d closed=%d", opened, closes)
	}
}

func scriptedStore(t *testing.T, handle func(n int, r *http.Request) (*http.Response, error)) (*s3store.Store, *scripted) {
	t.Helper()
	transport := &scripted{handle: handle}
	store, err := s3store.New(newClient("https://s3.example.invalid", "test", "test", transport), "bucket")
	if err != nil {
		t.Fatal(err)
	}
	return store, transport
}

// seekSpy is a producer that could be sought or sized. The leaf must never
// let the SDK discover that.
type seekSpy struct {
	*bytes.Reader
	seeks int
}

func (s *seekSpy) Seek(offset int64, whence int) (int64, error) {
	s.seeks++
	return s.Reader.Seek(offset, whence)
}

type s3Fields struct {
	ETag      string `json:"etag"`
	VersionID string `json:"version_id,omitempty"`
}

func identityFor(t *testing.T, key string, size int64, etag, version string) blobstore.RunIdentity {
	t.Helper()
	token, err := idtoken.Encode("s3", s3Fields{ETag: etag, VersionID: version})
	if err != nil {
		t.Fatal(err)
	}
	return blobstore.RunIdentity{Key: key, Size: size, Token: token}
}

const runKey = "runs/0ab/0123.ujrn"

func TestCreateRequestShape(t *testing.T) {
	payload := bytes.Repeat([]byte("run-bytes-"), 1000)
	var received []byte
	store, transport := scriptedStore(t, nil)
	transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		received = data
		return transport.respond(200, map[string]string{"ETag": `"abc"`, "x-amz-version-id": "v-7"}, nil), nil
	}

	producer := &seekSpy{Reader: bytes.NewReader(payload)}
	result, err := store.Create(context.Background(), runKey, producer, int64(len(payload)))
	if err != nil || result.Outcome != blobstore.Created {
		t.Fatalf("Create: %+v %v", result, err)
	}
	if want := identityFor(t, runKey, int64(len(payload)), `"abc"`, "v-7"); !result.Identity.Equal(want) {
		t.Fatalf("identity %+v want %+v", result.Identity, want)
	}
	if producer.seeks != 0 {
		t.Fatalf("producer sought %d times", producer.seeks)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("transport received different bytes")
	}
	requests, _, _ := transport.snapshot()
	if len(requests) != 1 {
		t.Fatalf("%d requests", len(requests))
	}
	r := requests[0]
	if r.method != http.MethodPut || r.path != "/bucket/"+runKey {
		t.Fatalf("%s %s", r.method, r.path)
	}
	for _, multipart := range []string{"uploads", "partNumber", "uploadId"} {
		if r.query.Has(multipart) {
			t.Fatalf("multipart query %q", multipart)
		}
	}
	if got := r.header.Get("If-None-Match"); got != "*" {
		t.Fatalf("If-None-Match %q", got)
	}
	if r.header.Get("If-Match") != "" {
		t.Fatal("unexpected If-Match")
	}
	if r.contentLength != int64(len(payload)) {
		t.Fatalf("Content-Length %d", r.contentLength)
	}
	if got := r.header.Get("X-Amz-Content-Sha256"); got != "UNSIGNED-PAYLOAD" {
		t.Fatalf("x-amz-content-sha256 %q", got)
	}
	if got := r.header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type %q", got)
	}
	for name := range r.header {
		if lower := strings.ToLower(name); strings.HasPrefix(lower, "x-amz-checksum") || lower == "x-amz-sdk-checksum-algorithm" || lower == "x-amz-trailer" {
			t.Fatalf("unexpected checksum header %s", name)
		}
	}
	transport.assertClosedOnce(t)
}

func TestCreateValidationIssuesNoRequest(t *testing.T) {
	store, transport := scriptedStore(t, func(int, *http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected request")
	})
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	var nilCtx context.Context
	for name, tc := range map[string]struct {
		ctx  context.Context
		key  string
		body io.Reader
		size int64
		want error
	}{
		"nil context": {nilCtx, runKey, strings.NewReader("x"), 1, blobstore.ErrInvalidRequest},
		"bad key":     {ctx, "a//b", strings.NewReader("x"), 1, blobstore.ErrInvalidRequest},
		"nil body":    {ctx, runKey, nil, 1, blobstore.ErrInvalidRequest},
		"zero size":   {ctx, runKey, strings.NewReader(""), 0, blobstore.ErrInvalidRequest},
		"over 5 GiB":  {ctx, runKey, strings.NewReader("x"), s3store.MaxCreateBytes + 1, blobstore.ErrInvalidRequest},
		"cancelled":   {cancelled, runKey, strings.NewReader("x"), 1, context.Canceled},
	} {
		result, err := store.Create(tc.ctx, tc.key, tc.body, tc.size)
		if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, tc.want) {
			t.Fatalf("%s: %+v %v", name, result, err)
		}
	}
	if requests, _, _ := transport.snapshot(); len(requests) != 0 {
		t.Fatalf("%d requests for rejected input", len(requests))
	}
}

func TestCreateClassification(t *testing.T) {
	payload := bytes.Repeat([]byte{7}, 4096)
	reset := &net0pError{}
	drain := func(r *http.Request) error { _, err := io.Copy(io.Discard, r.Body); return err }
	for name, tc := range map[string]struct {
		answer  func(s *scripted, r *http.Request) (*http.Response, error)
		outcome blobstore.CreateOutcome
		want    error
	}{
		"412 unread": {func(s *scripted, _ *http.Request) (*http.Response, error) {
			return s.apiError(412, "PreconditionFailed"), nil
		}, blobstore.AlreadyExists, blobstore.ErrAlreadyExists},
		"412 after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(412, "PreconditionFailed"), nil
		}, blobstore.AlreadyExists, blobstore.ErrAlreadyExists},
		"412 without xml": {func(s *scripted, r *http.Request) (*http.Response, error) {
			return s.respond(412, nil, nil), nil
		}, blobstore.AlreadyExists, blobstore.ErrAlreadyExists},
		"409 conditional conflict": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(409, "ConditionalRequestConflict"), nil
		}, blobstore.AlreadyExists, blobstore.ErrAlreadyExists},
		"409 other code after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(409, "OperationAborted"), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"500 after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(500, "InternalError"), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"503 SlowDown after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(503, "SlowDown"), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"429 after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.apiError(429, "TooManyRequests"), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"reset after body": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return nil, reset
		}, blobstore.CreateIndeterminate, reset},
		"200 without ETag": {func(s *scripted, r *http.Request) (*http.Response, error) {
			_ = drain(r)
			return s.respond(200, nil, nil), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"200 before the body was consumed": {func(s *scripted, r *http.Request) (*http.Response, error) {
			return s.respond(200, map[string]string{"ETag": `"early"`}, nil), nil
		}, blobstore.CreateIndeterminate, blobstore.ErrIndeterminate},
		"500 unread": {func(s *scripted, _ *http.Request) (*http.Response, error) {
			return s.apiError(500, "InternalError"), nil
		}, blobstore.DefinitelyAbsent, nil},
		"503 SlowDown unread": {func(s *scripted, _ *http.Request) (*http.Response, error) {
			return s.apiError(503, "SlowDown"), nil
		}, blobstore.DefinitelyAbsent, nil},
		"reset before body": {func(*scripted, *http.Request) (*http.Response, error) {
			return nil, reset
		}, blobstore.DefinitelyAbsent, reset},
		"reset one byte short": {func(_ *scripted, r *http.Request) (*http.Response, error) {
			_, _ = io.CopyN(io.Discard, r.Body, int64(len(payload))-1)
			return nil, reset
		}, blobstore.DefinitelyAbsent, reset},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) { return tc.answer(transport, r) }
			result, err := store.Create(context.Background(), runKey, bytes.NewReader(payload), int64(len(payload)))
			if result.Outcome != tc.outcome || err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("%+v %v", result, err)
			}
			if result.Outcome != blobstore.CreateIndeterminate && errors.Is(err, blobstore.ErrIndeterminate) {
				t.Fatalf("definite outcome carries ErrIndeterminate: %v", err)
			}
			if result.Identity != (blobstore.RunIdentity{}) {
				t.Fatalf("identity on failure: %+v", result.Identity)
			}
			requests, opened, closes := transport.snapshot()
			if len(requests) != 1 {
				t.Fatalf("hidden retry: %d requests", len(requests))
			}
			if opened != closes {
				t.Fatalf("response bodies opened=%d closed=%d", opened, closes)
			}
		})
	}
}

// net0pError stands in for a connection reset without opening a socket.
type net0pError struct{}

func (*net0pError) Error() string   { return "read tcp: connection reset by peer" }
func (*net0pError) Unwrap() error   { return syscall.ECONNRESET }
func (*net0pError) Timeout() bool   { return false }
func (*net0pError) Temporary() bool { return true }

type erroringProducer struct {
	data []byte
	err  error
}

func (p *erroringProducer) Read(b []byte) (int, error) {
	if len(p.data) == 0 {
		return 0, p.err
	}
	n := copy(b, p.data)
	p.data = p.data[n:]
	return n, nil
}

func TestCreateProducerNeverYieldsACompleteBody(t *testing.T) {
	const exact = 1001
	data := bytes.Repeat([]byte{3}, exact+50)
	failed := errors.New("producer failed")
	for name, tc := range map[string]struct {
		body io.Reader
		want error
	}{
		"short":       {bytes.NewReader(data[:exact-1]), io.ErrUnexpectedEOF},
		"empty":       {bytes.NewReader(nil), io.ErrUnexpectedEOF},
		"long":        {bytes.NewReader(data[:exact+1]), blobstore.ErrInvalidRequest},
		"much longer": {bytes.NewReader(data), blobstore.ErrInvalidRequest},
		"error":       {&erroringProducer{data: bytes.Clone(data[:500]), err: failed}, failed},
		"late error":  {&erroringProducer{data: bytes.Clone(data[:exact]), err: failed}, failed},
	} {
		t.Run(name, func(t *testing.T) {
			var carried int64
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
				// Behave like net/http: stream the body, fail the round trip
				// when the body fails.
				n, err := io.Copy(io.Discard, r.Body)
				carried = n
				if err != nil {
					return nil, err
				}
				return transport.respond(200, map[string]string{"ETag": `"must-not-happen"`}, nil), nil
			}
			result, err := store.Create(context.Background(), runKey, tc.body, exact)
			if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, tc.want) {
				t.Fatalf("%+v %v", result, err)
			}
			if carried >= exact {
				t.Fatalf("request carried %d bytes of a rejected producer", carried)
			}
			if requests, _, _ := transport.snapshot(); len(requests) != 1 || requests[0].contentLength != exact {
				t.Fatalf("requests %+v", requests)
			}
		})
	}
}

func TestCreateCancelledMidBody(t *testing.T) {
	payload := bytes.Repeat([]byte{9}, 8192)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, transport := scriptedStore(t, func(_ int, r *http.Request) (*http.Response, error) {
		if _, err := io.CopyN(io.Discard, r.Body, 4096); err != nil {
			return nil, err
		}
		cancel()
		return nil, r.Context().Err()
	})
	result, err := store.Create(ctx, runKey, bytes.NewReader(payload), int64(len(payload)))
	if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) {
		t.Fatalf("%+v %v", result, err)
	}
	if requests, _, _ := transport.snapshot(); len(requests) != 1 {
		t.Fatalf("%d requests", len(requests))
	}
}

func TestCreateCancelledAfterFullBodyIsIndeterminate(t *testing.T) {
	payload := bytes.Repeat([]byte{9}, 8192)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _ := scriptedStore(t, func(_ int, r *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		cancel()
		return nil, r.Context().Err()
	})
	result, err := store.Create(ctx, runKey, bytes.NewReader(payload), int64(len(payload)))
	if result.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, context.Canceled) {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestCreateResponseClosedOnce(t *testing.T) {
	payload := []byte("payload")
	for name, answer := range map[string]func(s *scripted) *http.Response{
		"success": func(s *scripted) *http.Response { return s.respond(200, map[string]string{"ETag": `"e"`}, nil) },
		"412":     func(s *scripted) *http.Response { return s.apiError(412, "PreconditionFailed") },
		"500":     func(s *scripted) *http.Response { return s.apiError(500, "InternalError") },
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
				_, _ = io.Copy(io.Discard, r.Body)
				return answer(transport), nil
			}
			_, _ = store.Create(context.Background(), runKey, bytes.NewReader(payload), int64(len(payload)))
			transport.assertClosedOnce(t)
			if _, opened, _ := transport.snapshot(); opened != 1 {
				t.Fatalf("%d responses", opened)
			}
		})
	}
}

func rangeHeaders(etag, version string, first, last, size int64) map[string]string {
	h := map[string]string{
		"ETag":           etag,
		"Content-Length": strconv.FormatInt(last-first+1, 10),
		"Content-Range":  fmt.Sprintf("bytes %d-%d/%d", first, last, size),
	}
	if version != "" {
		h["x-amz-version-id"] = version
	}
	return h
}

func TestStatAndOpenRangeRequestShape(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	store, transport := scriptedStore(t, nil)
	transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodHead {
			return transport.respond(200, map[string]string{"ETag": `"pinned"`, "x-amz-version-id": "ver.1", "Content-Length": "1000"}, nil), nil
		}
		return transport.respond(206, rangeHeaders(`"pinned"`, "ver.1", 10, 109, 1000), bytes.NewReader(content[10:110])), nil
	}
	ctx := context.Background()
	id, err := store.Stat(ctx, runKey)
	if err != nil || !id.Equal(identityFor(t, runKey, 1000, `"pinned"`, "ver.1")) {
		t.Fatalf("Stat: %+v %v", id, err)
	}
	var fields s3Fields
	if err := idtoken.Decode("s3", id.Token, &fields); err != nil || fields != (s3Fields{`"pinned"`, "ver.1"}) {
		t.Fatalf("token round trip: %+v %v", fields, err)
	}
	body, err := store.OpenRange(ctx, runKey, id, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(body)
	if err := errors.Join(readErr, body.Close(), body.Close()); err != nil || !bytes.Equal(got, content[10:110]) {
		t.Fatalf("range: %v", err)
	}
	requests, opened, closes := transport.snapshot()
	if len(requests) != 2 || opened != 2 || closes != 2 {
		t.Fatalf("requests=%d opened=%d closes=%d", len(requests), opened, closes)
	}
	r := requests[1]
	if r.method != http.MethodGet || r.path != "/bucket/"+runKey || r.header.Get("Range") != "bytes=10-109" ||
		r.header.Get("If-Match") != `"pinned"` || r.query.Get("versionId") != "ver.1" {
		t.Fatalf("range request: %+v", r)
	}

	// An unversioned identity sends no versionId at all.
	plain := identityFor(t, runKey, 1000, `"pinned"`, "")
	transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
		return transport.respond(206, rangeHeaders(`"pinned"`, "", 0, 0, 1000), bytes.NewReader(content[:1])), nil
	}
	body, err = store.OpenRange(ctx, runKey, plain, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	requests, _, _ = transport.snapshot()
	if last := requests[len(requests)-1]; last.query.Has("versionId") || last.header.Get("Range") != "bytes=0-0" {
		t.Fatalf("unversioned range request: %+v", last)
	}
}

func TestStatRequiresIdentityFields(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		header map[string]string
		want   error
	}{
		"no etag":   {200, map[string]string{"Content-Length": "10"}, blobstore.ErrIndeterminate},
		"no length": {200, map[string]string{"ETag": `"e"`}, blobstore.ErrIndeterminate},
		"missing":   {404, nil, blobstore.ErrNotFound},
	} {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(tc.status, tc.header, nil), nil
		}
		if _, err := store.Stat(context.Background(), runKey); !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v", name, err)
		}
		transport.assertClosedOnce(t)
	}
	store, transport := scriptedStore(t, nil)
	transport.handle = func(int, *http.Request) (*http.Response, error) { return transport.respond(503, nil, nil), nil }
	_, err := store.Stat(context.Background(), runKey)
	if err == nil || errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("503 head: %v", err)
	}
	if requests, _, _ := transport.snapshot(); len(requests) != 1 {
		t.Fatalf("hidden Stat retry: %d", len(requests))
	}
}

func TestOpenRangeVerification(t *testing.T) {
	id := identityFor(t, runKey, 1000, `"pinned"`, "v1")
	data := bytes.Repeat([]byte{1}, 100)
	for name, tc := range map[string]struct {
		status   int
		header   map[string]string
		body     []byte
		want     []error
		readWant error
	}{
		"wrong etag":     {206, rangeHeaders(`"other"`, "v1", 0, 99, 1000), data, []error{blobstore.ErrRunChanged}, nil},
		"missing etag":   {206, map[string]string{"Content-Length": "100", "x-amz-version-id": "v1"}, data, []error{blobstore.ErrRunChanged}, nil},
		"wrong version":  {206, rangeHeaders(`"pinned"`, "v2", 0, 99, 1000), data, []error{blobstore.ErrRunChanged}, nil},
		"short length":   {206, rangeHeaders(`"pinned"`, "v1", 0, 98, 1000), data[:99], []error{blobstore.ErrRunChanged}, nil},
		"whole object":   {200, map[string]string{"ETag": `"pinned"`, "x-amz-version-id": "v1", "Content-Length": "1000"}, bytes.Repeat(data, 10), []error{blobstore.ErrRunChanged}, nil},
		"resized object": {206, rangeHeaders(`"pinned"`, "v1", 0, 99, 2000), data, []error{blobstore.ErrRunChanged}, nil},
		"412":            {412, nil, nil, []error{blobstore.ErrRunChanged}, nil},
		"404":            {404, nil, nil, []error{blobstore.ErrRunChanged, blobstore.ErrNotFound}, nil},
		"truncated":      {206, rangeHeaders(`"pinned"`, "v1", 0, 99, 1000), data[:50], nil, io.ErrUnexpectedEOF},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(int, *http.Request) (*http.Response, error) {
				return transport.respond(tc.status, tc.header, bytes.NewReader(tc.body)), nil
			}
			body, err := store.OpenRange(context.Background(), runKey, id, 0, 100)
			if tc.readWant != nil {
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := io.ReadAll(body)
				if !errors.Is(readErr, tc.readWant) || len(got) != len(tc.body) {
					t.Fatalf("read %d bytes: %v", len(got), readErr)
				}
				if err := body.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if body != nil {
					t.Fatal("body returned with an error")
				}
				for _, want := range tc.want {
					if !errors.Is(err, want) {
						t.Fatalf("%v lacks %v", err, want)
					}
				}
			}
			transport.assertClosedOnce(t)
			if requests, _, _ := transport.snapshot(); len(requests) != 1 {
				t.Fatalf("%d requests", len(requests))
			}
		})
	}

	t.Run("503 is not a change", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) { return transport.apiError(503, "SlowDown"), nil }
		_, err := store.OpenRange(context.Background(), runKey, id, 0, 100)
		if err == nil || errors.Is(err, blobstore.ErrRunChanged) || errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("%v", err)
		}
		if requests, _, _ := transport.snapshot(); len(requests) != 1 {
			t.Fatalf("hidden retry: %d", len(requests))
		}
	})

	t.Run("never reads past the pinned length", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(206, rangeHeaders(`"pinned"`, "v1", 0, 99, 1000), bytes.NewReader(bytes.Repeat(data, 3))), nil
		}
		body, err := store.OpenRange(context.Background(), runKey, id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		if err != nil || len(got) != 100 {
			t.Fatalf("%d %v", len(got), err)
		}
		_ = body.Close()
	})

	t.Run("cancelled reader", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(206, rangeHeaders(`"pinned"`, "v1", 0, 99, 1000), bytes.NewReader(data)), nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		body, err := store.OpenRange(ctx, runKey, id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err := body.Read(make([]byte, 8)); !errors.Is(err, context.Canceled) {
			t.Fatalf("read after cancel: %v", err)
		}
		_ = body.Close()
		transport.assertClosedOnce(t)
	})
}

func TestInvalidIdentityIssuesNoRequest(t *testing.T) {
	store, transport := scriptedStore(t, func(int, *http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected request")
	})
	encode := func(providerName string, fields any) string {
		token, err := idtoken.Encode(providerName, fields)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	for name, token := range map[string]string{
		"garbage": "garbage",
		"other provider": encode("memory", struct {
			Generation uint64 `json:"generation"`
		}{7}),
		"azure shape":    encode("azure", s3Fields{ETag: `"e"`}),
		"empty etag":     encode("s3", s3Fields{}),
		"unknown field":  encode("s3", struct{ ETag, Extra string }{`"e"`, "x"}),
		"other version":  strings.Replace(encode("s3", s3Fields{ETag: `"e"`}), ".v1.", ".v2.", 1),
		"header smuggle": encode("s3", s3Fields{ETag: "\"e\"\r\nx-evil: 1"}),
		"trailing data":  encode("s3", s3Fields{ETag: `"e"`}) + "A",
	} {
		id := blobstore.RunIdentity{Key: runKey, Size: 10, Token: token}
		if _, err := store.OpenRange(context.Background(), runKey, id, 0, 1); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("OpenRange %s: %v", name, err)
		}
		if err := store.DeleteIfIdentity(context.Background(), runKey, id); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("DeleteIfIdentity %s: %v", name, err)
		}
	}
	if requests, _, _ := transport.snapshot(); len(requests) != 0 {
		t.Fatalf("%d requests for invalid identities", len(requests))
	}
}

func TestDeleteIfIdentity(t *testing.T) {
	id := identityFor(t, runKey, 1000, `"pinned"`, "v1")
	head := map[string]string{"ETag": `"pinned"`, "x-amz-version-id": "v1", "Content-Length": "1000"}
	for name, tc := range map[string]struct {
		head         func(s *scripted) (*http.Response, error)
		deleteStatus int
		deleteErr    error
		want         error // nil means success
		deletes      int
	}{
		"deleted":  {nil, 204, nil, nil, 1},
		"head 404": {func(s *scripted) (*http.Response, error) { return s.respond(404, nil, nil), nil }, 0, nil, blobstore.ErrNotFound, 0},
		"head other etag": {func(s *scripted) (*http.Response, error) {
			return s.respond(200, map[string]string{"ETag": `"new"`, "x-amz-version-id": "v1", "Content-Length": "1000"}, nil), nil
		}, 0, nil, blobstore.ErrRunChanged, 0},
		"head other size": {func(s *scripted) (*http.Response, error) {
			return s.respond(200, map[string]string{"ETag": `"pinned"`, "x-amz-version-id": "v1", "Content-Length": "999"}, nil), nil
		}, 0, nil, blobstore.ErrRunChanged, 0},
		"head new version": {func(s *scripted) (*http.Response, error) {
			return s.respond(200, map[string]string{"ETag": `"pinned"`, "x-amz-version-id": "v2", "Content-Length": "1000"}, nil), nil
		}, 0, nil, blobstore.ErrRunChanged, 0},
		"raced 412":    {nil, 412, nil, blobstore.ErrRunChanged, 1},
		"raced 404":    {nil, 404, nil, blobstore.ErrNotFound, 1},
		"delete 500":   {nil, 500, nil, blobstore.ErrIndeterminate, 1},
		"delete 503":   {nil, 503, nil, blobstore.ErrIndeterminate, 1},
		"delete reset": {nil, 0, &net0pError{}, blobstore.ErrIndeterminate, 1},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodHead {
					if tc.head != nil {
						return tc.head(transport)
					}
					return transport.respond(200, head, nil), nil
				}
				if tc.deleteErr != nil {
					return nil, tc.deleteErr
				}
				return transport.respond(tc.deleteStatus, nil, nil), nil
			}
			err := store.DeleteIfIdentity(context.Background(), runKey, id)
			if (tc.want == nil) != (err == nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("%v", err)
			}
			requests, opened, closes := transport.snapshot()
			deletes := 0
			for _, r := range requests {
				if r.method != http.MethodDelete {
					continue
				}
				deletes++
				if r.header.Get("If-Match") != `"pinned"` || r.query.Get("versionId") != "v1" {
					t.Fatalf("unconditional delete: %+v", r)
				}
			}
			if deletes != tc.deletes || len(requests) != 1+tc.deletes || opened != closes {
				t.Fatalf("requests=%d deletes=%d opened=%d closes=%d", len(requests), deletes, opened, closes)
			}
		})
	}
}

// endless never reports EOF; it fails the test if read beyond its budget.
type endless struct {
	t      *testing.T
	budget int64
	read   int64
}

func (e *endless) Read(p []byte) (int, error) {
	e.read += int64(len(p))
	if e.read > e.budget {
		e.t.Errorf("read %d bytes, budget %d", e.read, e.budget)
		return 0, io.EOF
	}
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestBoundedGetBounds(t *testing.T) {
	ctx := context.Background()
	t.Run("huge Content-Length rejected before reading", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		body := &endless{t: t, budget: 0}
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(200, map[string]string{"ETag": `"e"`, "Content-Length": strconv.FormatInt(1<<40, 10)}, body), nil
		}
		if _, err := store.BoundedGet(ctx, "meta/big", 64); !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("%v", err)
		}
		transport.assertClosedOnce(t)
	})
	t.Run("absent Content-Length still bounded", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		body := &endless{t: t, budget: 65}
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(200, map[string]string{"ETag": `"e"`}, body), nil
		}
		if _, err := store.BoundedGet(ctx, "meta/liar", 64); !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("%v", err)
		}
		transport.assertClosedOnce(t)
	})
	t.Run("absent Content-Length within bound", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		want := bytes.Repeat([]byte("abc"), 3000)
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(200, map[string]string{"ETag": `"e"`}, bytes.NewReader(want)), nil
		}
		got, err := store.BoundedGet(ctx, "meta/unknown", 9000)
		if err != nil || !bytes.Equal(got.Body, want) || got.Token != `"e"` {
			t.Fatalf("%v", err)
		}
	})
	for name, tc := range map[string]struct {
		announced string
		body      []byte
		want      error
	}{
		"understated length":         {"10", bytes.Repeat([]byte{1}, 20), blobstore.ErrIndeterminate},
		"overstated length":          {"30", bytes.Repeat([]byte{1}, 20), blobstore.ErrIndeterminate},
		"understated beyond bound":   {"10", bytes.Repeat([]byte{1}, 200), blobstore.ErrTooLarge},
		"exact and equal to maximum": {"64", bytes.Repeat([]byte{1}, 64), nil},
		"empty object":               {"0", nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(int, *http.Request) (*http.Response, error) {
				return transport.respond(200, map[string]string{"ETag": `"e"`, "Content-Length": tc.announced}, bytes.NewReader(tc.body)), nil
			}
			got, err := store.BoundedGet(ctx, "meta/length", 64)
			if tc.want == nil {
				if err != nil || !bytes.Equal(got.Body, tc.body) {
					t.Fatalf("%v", err)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("%v", err)
			}
			transport.assertClosedOnce(t)
		})
	}
	t.Run("missing ETag", func(t *testing.T) {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) {
			return transport.respond(200, map[string]string{"Content-Length": "1"}, strings.NewReader("x")), nil
		}
		if _, err := store.BoundedGet(ctx, "meta/no-etag", 64); !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("%v", err)
		}
		transport.assertClosedOnce(t)
	})
}

func TestCompareAndSwapOutcomes(t *testing.T) {
	ctx := context.Background()
	const key = "run-manifest/CURRENT"
	type step = func(s *scripted, r *http.Request) (*http.Response, error)
	status := func(code int, header map[string]string) step {
		return func(s *scripted, r *http.Request) (*http.Response, error) {
			if r.Body != nil {
				_, _ = io.Copy(io.Discard, r.Body)
			}
			return s.respond(code, header, nil), nil
		}
	}
	api := func(code int, name string) step {
		return func(s *scripted, r *http.Request) (*http.Response, error) { return s.apiError(code, name), nil }
	}
	for name, tc := range map[string]struct {
		token    string
		put      step
		head     step
		outcome  blobstore.CASOutcome
		wantErr  error
		known    bool
		requests int
	}{
		"applied":                 {`"old"`, status(200, map[string]string{"ETag": `"new"`}), nil, blobstore.CASApplied, nil, false, 1},
		"applied without etag":    {`"old"`, status(200, nil), nil, blobstore.CASUnknown, blobstore.ErrIndeterminate, false, 1},
		"412 then head":           {`"old"`, api(412, "PreconditionFailed"), status(200, map[string]string{"ETag": `"winner"`, "Content-Length": "42"}), blobstore.CASConflict, nil, true, 2},
		"412 then head fails":     {`"old"`, api(412, "PreconditionFailed"), status(503, nil), blobstore.CASConflict, nil, false, 2},
		"412 then head 404":       {"", api(412, "PreconditionFailed"), status(404, nil), blobstore.CASConflict, nil, false, 2},
		"409 conditional":         {"", api(409, "ConditionalRequestConflict"), status(200, map[string]string{"ETag": `"winner"`, "Content-Length": "42"}), blobstore.CASConflict, nil, true, 2},
		"if-match on missing key": {`"old"`, api(404, "NoSuchKey"), status(404, nil), blobstore.CASConflict, nil, false, 2},
		"404 on create-if-absent": {"", api(404, "NoSuchKey"), nil, blobstore.CASUnknown, nil, false, 1},
		"missing bucket":          {`"old"`, api(404, "NoSuchBucket"), nil, blobstore.CASUnknown, nil, false, 1},
		"503 SlowDown":            {`"old"`, api(503, "SlowDown"), nil, blobstore.CASUnknown, nil, false, 1},
		"500":                     {`"old"`, api(500, "InternalError"), nil, blobstore.CASUnknown, nil, false, 1},
		"429":                     {`"old"`, api(429, "TooManyRequests"), nil, blobstore.CASUnknown, nil, false, 1},
		"409 other":               {`"old"`, api(409, "OperationAborted"), nil, blobstore.CASUnknown, nil, false, 1},
		"undecodable 200":         {`"old"`, func(s *scripted, r *http.Request) (*http.Response, error) { return nil, io.ErrUnexpectedEOF }, nil, blobstore.CASUnknown, io.ErrUnexpectedEOF, false, 1},
		"reset":                   {`"old"`, func(*scripted, *http.Request) (*http.Response, error) { return nil, &net0pError{} }, nil, blobstore.CASUnknown, syscall.ECONNRESET, false, 1},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodHead {
					return tc.head(transport, r)
				}
				return tc.put(transport, r)
			}
			result, err := store.CompareAndSwap(ctx, key, tc.token, []byte("manifest"))
			if result.Outcome != tc.outcome || (err != nil) != (tc.outcome == blobstore.CASUnknown) ||
				(tc.wantErr != nil && !errors.Is(err, tc.wantErr)) || result.CurrentKnown != tc.known {
				t.Fatalf("%+v %v", result, err)
			}
			if tc.known && (result.CurrentToken != `"winner"` || result.Current != (blobstore.ObjectInfo{Key: key, Size: 42})) {
				t.Fatalf("current: %+v", result)
			}
			if tc.outcome == blobstore.CASApplied && (result.Object.Key != key || result.Object.Token != `"new"` || result.Object.Body != nil) {
				t.Fatalf("applied object: %+v", result.Object)
			}
			requests, opened, closes := transport.snapshot()
			if len(requests) != tc.requests || opened != closes {
				t.Fatalf("requests=%d opened=%d closes=%d", len(requests), opened, closes)
			}
			put := requests[0]
			if put.method != http.MethodPut || put.header.Get("If-Match") != tc.token ||
				(tc.token == "") != (put.header.Get("If-None-Match") == "*") {
				t.Fatalf("conditional headers: %+v", put.header)
			}
		})
	}

	store, transport := scriptedStore(t, func(int, *http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected request")
	})
	for _, token := range []string{"\"e\"\r\nx-evil: 1", strings.Repeat("e", 2000)} {
		result, err := store.CompareAndSwap(ctx, key, token, nil)
		if result.Outcome != blobstore.CASUnknown || !errors.Is(err, blobstore.ErrInvalidRequest) {
			t.Fatalf("malformed token: %+v %v", result, err)
		}
	}
	if requests, _, _ := transport.snapshot(); len(requests) != 0 {
		t.Fatalf("%d requests for malformed tokens", len(requests))
	}
}

func TestPutOutcomes(t *testing.T) {
	ctx := context.Background()
	body := []byte("receipt-page")
	get := func(content []byte) func(s *scripted) *http.Response {
		return func(s *scripted) *http.Response {
			return s.respond(200, map[string]string{"ETag": `"existing"`, "Content-Length": strconv.Itoa(len(content))}, bytes.NewReader(content))
		}
	}
	for name, tc := range map[string]struct {
		body     []byte
		put      func(s *scripted) (*http.Response, error)
		get      func(s *scripted) *http.Response
		want     error
		requests int
	}{
		"created": {body, func(s *scripted) (*http.Response, error) {
			return s.respond(200, map[string]string{"ETag": `"new"`}, nil), nil
		}, nil, nil, 1},
		"replay":           {body, func(s *scripted) (*http.Response, error) { return s.apiError(412, "PreconditionFailed"), nil }, get(body), nil, 2},
		"empty replay":     {nil, func(s *scripted) (*http.Response, error) { return s.apiError(412, "PreconditionFailed"), nil }, get(nil), nil, 2},
		"different":        {body, func(s *scripted) (*http.Response, error) { return s.apiError(412, "PreconditionFailed"), nil }, get([]byte("receipt-pagE")), blobstore.ErrImmutableConflict, 2},
		"longer":           {body, func(s *scripted) (*http.Response, error) { return s.apiError(412, "PreconditionFailed"), nil }, get(append(bytes.Clone(body), 'x')), blobstore.ErrImmutableConflict, 2},
		"non-empty vs nil": {nil, func(s *scripted) (*http.Response, error) { return s.apiError(412, "PreconditionFailed"), nil }, get([]byte("xy")), blobstore.ErrImmutableConflict, 2},
		"winner unreadable": {body, func(s *scripted) (*http.Response, error) { return s.apiError(409, "ConditionalRequestConflict"), nil },
			func(s *scripted) *http.Response { return s.apiError(503, "SlowDown") }, blobstore.ErrIndeterminate, 2},
		"503":         {body, func(s *scripted) (*http.Response, error) { return s.apiError(503, "SlowDown"), nil }, nil, blobstore.ErrIndeterminate, 1},
		"no etag":     {body, func(s *scripted) (*http.Response, error) { return s.respond(200, nil, nil), nil }, nil, blobstore.ErrIndeterminate, 1},
		"lost answer": {body, func(*scripted) (*http.Response, error) { return nil, &net0pError{} }, nil, blobstore.ErrIndeterminate, 1},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet {
					return tc.get(transport), nil
				}
				_, _ = io.Copy(io.Discard, r.Body)
				return tc.put(transport)
			}
			object, err := store.Put(ctx, "receipts/page", tc.body)
			if tc.want == nil {
				if err != nil || !bytes.Equal(object.Body, tc.body) || object.Token == "" {
					t.Fatalf("%+v %v", object, err)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("%v", err)
			}
			if tc.want == blobstore.ErrImmutableConflict && errors.Is(err, blobstore.ErrIndeterminate) {
				t.Fatalf("definite conflict carries ErrIndeterminate: %v", err)
			}
			requests, opened, closes := transport.snapshot()
			if len(requests) != tc.requests || opened != closes {
				t.Fatalf("requests=%d opened=%d closes=%d", len(requests), opened, closes)
			}
			if requests[0].header.Get("If-None-Match") != "*" {
				t.Fatal("Put is not create-only")
			}
		})
	}
}

func listXML(truncated bool, keys ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><IsTruncated>%t</IsTruncated>`, truncated)
	for _, key := range keys {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>3</Size></Contents>`, key)
	}
	b.WriteString(`</ListBucketResult>`)
	return b.String()
}

func TestListRequestShapeAndPaging(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		opts     blobstore.ListOptions
		maxKeys  string
		response string
		wantKeys int
		hasMore  bool
		wantErr  bool
	}{
		"extra key proves more": {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 2}, "3", listXML(true, "p/b", "p/c", "p/d"), 2, true, false},
		"exact fit":             {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 2}, "3", listXML(false, "p/b", "p/c"), 2, false, false},
		"short truncated page":  {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5}, "6", listXML(true, "p/b"), 1, true, false},
		"ceiling uses flag":     {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5000}, "1000", listXML(true, "p/b", "p/c"), 2, true, false},
		"default limit":         {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a"}, "1000", listXML(false), 0, false, false},
		"unordered page":        {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5}, "6", listXML(false, "p/c", "p/b"), 0, false, true},
		"cursor not honoured":   {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5}, "6", listXML(false, "p/a", "p/b"), 0, false, true},
		"foreign prefix":        {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5}, "6", listXML(false, "q/b"), 0, false, true},
		"truncated but empty":   {blobstore.ListOptions{Prefix: "p/", AfterKey: "p/a", Limit: 5}, "6", listXML(true), 0, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			store, transport := scriptedStore(t, nil)
			transport.handle = func(int, *http.Request) (*http.Response, error) {
				return transport.respond(200, map[string]string{"Content-Type": "application/xml"}, strings.NewReader(tc.response)), nil
			}
			page, err := store.List(ctx, tc.opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%v", err)
			}
			if err == nil {
				if len(page.Objects) != tc.wantKeys || page.HasMore != tc.hasMore {
					t.Fatalf("%+v", page)
				}
				if tc.hasMore != (page.NextAfterKey != "") || (tc.hasMore && page.NextAfterKey != page.Objects[len(page.Objects)-1].Key) {
					t.Fatalf("cursor: %+v", page)
				}
			}
			requests, _, _ := transport.snapshot()
			if len(requests) != 1 {
				t.Fatalf("%d requests", len(requests))
			}
			q := requests[0].query
			if q.Get("list-type") != "2" || q.Get("prefix") != "p/" || q.Get("start-after") != "p/a" ||
				q.Get("max-keys") != tc.maxKeys || q.Has("continuation-token") || q.Has("delimiter") {
				t.Fatalf("query: %v", q)
			}
			transport.assertClosedOnce(t)
		})
	}
}

// Every operation is issued exactly once even though the client itself is
// configured with the SDK's default (retrying) retryer.
func TestNoOperationRetries(t *testing.T) {
	ctx := context.Background()
	id := identityFor(t, runKey, 1000, `"pinned"`, "")
	for name, call := range map[string]func(*s3store.Store) error{
		"BoundedGet": func(s *s3store.Store) error { _, err := s.BoundedGet(ctx, "meta/k", 10); return err },
		"Put":        func(s *s3store.Store) error { _, err := s.Put(ctx, "meta/k", []byte("x")); return err },
		"CompareAndSwap": func(s *s3store.Store) error {
			_, err := s.CompareAndSwap(ctx, "meta/k", `"t"`, []byte("x"))
			return err
		},
		"List": func(s *s3store.Store) error {
			_, err := s.List(ctx, blobstore.ListOptions{Prefix: "meta/"})
			return err
		},
		"Delete": func(s *s3store.Store) error { return s.Delete(ctx, "meta/k") },
		"Create": func(s *s3store.Store) error {
			_, err := s.Create(ctx, runKey, strings.NewReader("abc"), 3)
			return err
		},
		"Stat":             func(s *s3store.Store) error { _, err := s.Stat(ctx, runKey); return err },
		"OpenRange":        func(s *s3store.Store) error { _, err := s.OpenRange(ctx, runKey, id, 0, 1); return err },
		"DeleteIfIdentity": func(s *s3store.Store) error { return s.DeleteIfIdentity(ctx, runKey, id) },
	} {
		for _, failure := range []string{"500", "503 SlowDown", "reset"} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				store, transport := scriptedStore(t, nil)
				transport.handle = func(_ int, r *http.Request) (*http.Response, error) {
					if r.Body != nil {
						_, _ = io.Copy(io.Discard, r.Body)
					}
					switch failure {
					case "500":
						return transport.apiError(500, "InternalError"), nil
					case "503 SlowDown":
						return transport.apiError(503, "SlowDown"), nil
					}
					return nil, &net0pError{}
				}
				err := call(store)
				if err == nil {
					t.Fatal("failure swallowed")
				}
				for _, definite := range []error{blobstore.ErrNotFound, blobstore.ErrAlreadyExists, blobstore.ErrRunChanged, blobstore.ErrImmutableConflict} {
					if errors.Is(err, definite) {
						t.Fatalf("uncertain failure mapped to %v: %v", definite, err)
					}
				}
				requests, opened, closes := transport.snapshot()
				if len(requests) != 1 || opened != closes {
					t.Fatalf("requests=%d opened=%d closes=%d", len(requests), opened, closes)
				}
			})
		}
	}
}

func TestDeleteMetadataTreats404AsSuccess(t *testing.T) {
	for status, wantErr := range map[int]bool{204: false, 404: false, 500: true} {
		store, transport := scriptedStore(t, nil)
		transport.handle = func(int, *http.Request) (*http.Response, error) { return transport.respond(status, nil, nil), nil }
		if err := store.Delete(context.Background(), "meta/k"); (err != nil) != wantErr {
			t.Fatalf("%d: %v", status, err)
		}
		requests, _, _ := transport.snapshot()
		if len(requests) != 1 || requests[0].method != http.MethodDelete || requests[0].header.Get("If-Match") != "" {
			t.Fatalf("%+v", requests)
		}
	}
}
