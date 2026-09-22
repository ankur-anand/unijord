package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

type fakeAct = func(f *fakeService, r *fakeRequest, apply func() *http.Response) (*http.Response, error)

func isPutBlob(r *fakeRequest) bool {
	return r.method == http.MethodPut && r.blob != "" && r.comp == ""
}
func isBlock(r *fakeRequest) bool  { return r.method == http.MethodPut && r.comp == "block" }
func isCommit(r *fakeRequest) bool { return r.method == http.MethodPut && r.comp == "blocklist" }
func isDelete(r *fakeRequest) bool { return r.method == http.MethodDelete }
func isRange(r *fakeRequest) bool  { return r.isRange() }
func isHead(r *fakeRequest) bool   { return r.method == http.MethodHead }
func isGet(r *fakeRequest) bool    { return r.method == http.MethodGet && r.blob != "" }
func isList(r *fakeRequest) bool   { return r.method == http.MethodGet && r.blob == "" }

func injectedError(what string) error {
	return errors.Join(storetest.ErrInjected, errors.New("fake azure: "+what))
}

func actBeforeSend(*fakeService, *fakeRequest, func() *http.Response) (*http.Response, error) {
	return nil, injectedError("connection refused before send")
}

func actLostResponse(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
	if err := apply().Body.Close(); err != nil {
		return nil, err
	}
	return nil, injectedError("response lost")
}

func actStatus(status int, code string) fakeAct {
	return func(f *fakeService, r *fakeRequest, _ func() *http.Response) (*http.Response, error) {
		return f.fail(r, status, code), nil
	}
}

func actCloseBody(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
	response := apply()
	response.Body.(*trackedBody).closeErr = injectedError("body close failed")
	return response, nil
}

func fakeHarness(t *testing.T, service *fakeService) storetest.Harness {
	store := service.store(t)
	t.Cleanup(func() { service.assertBodiesClosed(t) })
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: "conformance/",
		Reopen: func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened := service.store(t)
			return reopened, reopened
		},
		Replace: func(_ *testing.T, key string, body []byte) { service.replace(key, body) },
		Inject: func(t *testing.T, op storetest.Op, kind storetest.FaultKind) func() int {
			var match func(*fakeRequest) bool
			switch op {
			case storetest.OpPut, storetest.OpCompareAndSwap:
				match = isPutBlob
			case storetest.OpCreate:
				// A Create that must not be sent fails on its first Put Block;
				// every other Create fault targets the single commit request.
				if match = isCommit; kind == storetest.FaultBeforeSend {
					match = isBlock
				}
			case storetest.OpOpenRange:
				match = isRange
			case storetest.OpDeleteIfIdentity:
				match = isDelete
			default:
				t.Fatalf("unsupported fault op %q", op)
			}
			var act fakeAct
			switch kind {
			case storetest.FaultBeforeSend:
				act = actBeforeSend
			case storetest.FaultLostResponse:
				act = actLostResponse
			case storetest.FaultThrottle:
				act = actStatus(http.StatusServiceUnavailable, "ServerBusy")
			case storetest.FaultCloseBody:
				act = actCloseBody
			default:
				t.Fatalf("unsupported fault kind %d", kind)
			}
			fault := service.arm(match, act)
			t.Cleanup(func() { service.disarm(fault) })
			return func() int { return service.seen(fault) }
		},
	}
}

func TestFakeMetadataConformance(t *testing.T) {
	storetest.MetadataSuite(t, fakeHarness(t, newFakeService()))
}

func TestFakeRunConformance(t *testing.T) { storetest.RunSuite(t, fakeHarness(t, newFakeService())) }

// Azurite answers If-Match requests on a missing blob with 412, not 404.
func TestFakeRunConformanceAzuriteDialect(t *testing.T) {
	service := newFakeService()
	service.missingIs412 = true
	storetest.RunSuite(t, fakeHarness(t, service))
}

func TestFakeRunConformanceVersioned(t *testing.T) {
	service := newFakeService()
	service.versioning = true
	storetest.RunSuite(t, fakeHarness(t, service))
}

// patternReader is a forward-only generator; the payload is never materialized.
type patternReader struct {
	position, size int64
	failAt         int64 // when > 0, fail once position reaches it
	fail           error
	onRead         func(position int64)
}

func patternByte(position int64) byte { return byte(position*7 + position>>9) }

func (p *patternReader) Read(b []byte) (int, error) {
	if p.onRead != nil {
		p.onRead(p.position)
	}
	if p.fail != nil && p.position >= p.failAt {
		return 0, p.fail
	}
	if p.position >= p.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(b)), p.size-p.position, 64<<10))
	if p.fail != nil {
		n = int(min(int64(n), p.failAt-p.position))
	}
	for i := 0; i < n; i++ {
		b[i] = patternByte(p.position + int64(i))
	}
	p.position += int64(n)
	return n, nil
}

func patternBytes(size int64) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = patternByte(int64(i))
	}
	return out
}

func decodeBlockID(t *testing.T, id string) (nonce []byte, index uint32) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil || len(raw) != attemptNonceBytes+4 {
		t.Fatalf("block id %q: %v len=%d", id, err, len(raw))
	}
	return raw[:attemptNonceBytes], uint32(raw[16])<<24 | uint32(raw[17])<<16 | uint32(raw[18])<<8 | uint32(raw[19])
}

func TestCreateRequestShape(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	ctx := context.Background()
	size := int64(2*stageBlockBytes + 5)
	var nonces [][]byte
	for _, key := range []string{"shape/a", "shape/b"} {
		start := service.mark()
		result, err := store.Create(ctx, key, &patternReader{size: size}, size)
		if err != nil || result.Outcome != blobstore.Created {
			t.Fatalf("Create: %+v %v", result, err)
		}
		blocks := service.requests(start, isBlock)
		if len(blocks) != 3 || blocks[0].bodyLen != stageBlockBytes || blocks[1].bodyLen != stageBlockBytes || blocks[2].bodyLen != 5 {
			t.Fatalf("unexpected Put Block requests: %d", len(blocks))
		}
		var ids []string
		var nonce []byte
		for i, block := range blocks {
			if block.blob != key || len(block.blockID) != len(blocks[0].blockID) {
				t.Fatalf("block %d: blob=%q id=%q", i, block.blob, block.blockID)
			}
			if got := block.header.Get("Content-Length"); got != "" && got != strconv.Itoa(block.bodyLen) {
				t.Fatalf("block %d content length %q for %d bytes", i, got, block.bodyLen)
			}
			gotNonce, index := decodeBlockID(t, block.blockID)
			if nonce == nil {
				nonce = gotNonce
			}
			if !bytes.Equal(nonce, gotNonce) || index != uint32(i) {
				t.Fatalf("block %d: nonce/index mismatch (index %d)", i, index)
			}
			ids = append(ids, block.blockID)
		}
		nonces = append(nonces, nonce)
		commits := service.requests(start, isCommit)
		if len(commits) != 1 {
			t.Fatalf("%d commit requests", len(commits))
		}
		commit := commits[0]
		if commit.header.Get("If-None-Match") != "*" || commit.header.Get("If-Match") != "" ||
			commit.header.Get("x-ms-blob-content-type") != runContentType {
			t.Fatalf("commit headers: %v", commit.header)
		}
		var list struct {
			XMLName     xml.Name `xml:"BlockList"`
			Latest      []string `xml:"Latest"`
			Committed   []string `xml:"Committed"`
			Uncommitted []string `xml:"Uncommitted"`
		}
		if err := xml.Unmarshal(commit.body, &list); err != nil || len(list.Committed)+len(list.Uncommitted) != 0 ||
			fmt.Sprint(list.Latest) != fmt.Sprint(ids) {
			t.Fatalf("commit body %s: %v", commit.body, err)
		}
		if other := service.requests(start, func(r *fakeRequest) bool { return !isBlock(r) && !isCommit(r) }); len(other) != 0 {
			t.Fatalf("Create issued %d unexpected requests", len(other))
		}
		body, err := store.OpenRange(ctx, key, result.Identity, 0, size)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		if err = errors.Join(err, body.Close()); err != nil || !bytes.Equal(got, patternBytes(size)) {
			t.Fatalf("read back: %v", err)
		}
	}
	if bytes.Equal(nonces[0], nonces[1]) {
		t.Fatal("two Create attempts shared a nonce")
	}
	service.assertBodiesClosed(t)
}

func TestUncommittedBlocksStayInvisible(t *testing.T) {
	producerFailed := errors.New("producer failed")
	for name, tc := range map[string]struct {
		body io.Reader
		size int64
		want error
	}{
		"producer-error": {&patternReader{size: stageBlockBytes + 100, failAt: stageBlockBytes + 10, fail: producerFailed}, stageBlockBytes + 100, producerFailed},
		"short":          {&patternReader{size: stageBlockBytes + 99}, stageBlockBytes + 100, io.ErrUnexpectedEOF},
		"oversized":      {&patternReader{size: stageBlockBytes + 101}, stageBlockBytes + 100, blobstore.ErrInvalidRequest},
	} {
		t.Run(name, func(t *testing.T) {
			service := newFakeService()
			store := service.store(t)
			ctx := context.Background()
			key := "staged/" + name
			result, err := store.Create(ctx, key, tc.body, tc.size)
			if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, tc.want) {
				t.Fatalf("Create: %+v %v", result, err)
			}
			if n := len(service.requests(0, isBlock)); n != 1 {
				t.Fatalf("%d Put Block requests, want the one full block", n)
			}
			if n := len(service.requests(0, isCommit)); n != 0 {
				t.Fatalf("%d commit requests after a failed producer", n)
			}
			if _, err := store.Stat(ctx, key); !errors.Is(err, blobstore.ErrNotFound) {
				t.Fatalf("Stat: %v", err)
			}
			if _, err := store.BoundedGet(ctx, key, 16); !errors.Is(err, blobstore.ErrNotFound) {
				t.Fatalf("BoundedGet: %v", err)
			}
			page, err := store.List(ctx, blobstore.ListOptions{Prefix: "staged/"})
			if err != nil || len(page.Objects) != 0 {
				t.Fatalf("List: %+v %v", page, err)
			}
			created, err := store.Create(ctx, key, bytes.NewReader([]byte("second attempt")), 14)
			if err != nil || created.Outcome != blobstore.Created {
				t.Fatalf("later Create: %+v %v", created, err)
			}
			got, err := store.BoundedGet(ctx, key, 64)
			if err != nil || string(got.Body) != "second attempt" {
				t.Fatalf("later Create adopted stale blocks: %q %v", got.Body, err)
			}
			service.assertBodiesClosed(t)
		})
	}
	t.Run("size-limit", func(t *testing.T) {
		service := newFakeService()
		result, err := service.store(t).Create(context.Background(), "staged/huge", bytes.NewReader(nil), MaxCreateBytes+1)
		if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, blobstore.ErrInvalidRequest) || service.mark() != 0 {
			t.Fatalf("oversized exactSize: %+v %v", result, err)
		}
	})
}

func TestCommitClassification(t *testing.T) {
	for name, tc := range map[string]struct {
		act  fakeAct
		want blobstore.CreateOutcome
	}{
		"409-BlobAlreadyExists":     {actStatus(409, "BlobAlreadyExists"), blobstore.AlreadyExists},
		"412-ConditionNotMet":       {actStatus(412, "ConditionNotMet"), blobstore.AlreadyExists},
		"500-InternalError":         {actStatus(500, "InternalError"), blobstore.CreateIndeterminate},
		"503-ServerBusy":            {actStatus(503, "ServerBusy"), blobstore.CreateIndeterminate},
		"429-TooManyRequests":       {actStatus(429, "TooManyRequests"), blobstore.CreateIndeterminate},
		"lost-response":             {actLostResponse, blobstore.CreateIndeterminate},
		"409-LeaseIdMissing":        {actStatus(409, "LeaseIdMissing"), blobstore.CreateIndeterminate},
		"412-LeaseIdMissing":        {actStatus(412, "LeaseIdMissing"), blobstore.CreateIndeterminate},
		"409-ContainerBeingDeleted": {actStatus(409, "ContainerBeingDeleted"), blobstore.CreateIndeterminate},
		"400-InvalidBlockList":      {actStatus(400, "InvalidBlockList"), blobstore.CreateIndeterminate},
		"404-ContainerNotFound":     {actStatus(404, "ContainerNotFound"), blobstore.CreateIndeterminate},
	} {
		t.Run(name, func(t *testing.T) {
			service := newFakeService()
			store := service.store(t)
			fault := service.arm(isCommit, tc.act)
			result, err := store.Create(context.Background(), "commit/"+name, bytes.NewReader([]byte("payload")), 7)
			if result.Outcome != tc.want || err == nil {
				t.Fatalf("Create: %+v %v", result, err)
			}
			if tc.want == blobstore.AlreadyExists != errors.Is(err, blobstore.ErrAlreadyExists) ||
				tc.want == blobstore.CreateIndeterminate != errors.Is(err, blobstore.ErrIndeterminate) {
				t.Fatalf("sentinels do not match outcome %v: %v", result.Outcome, err)
			}
			if responseError(err) == nil && !errors.Is(err, storetest.ErrInjected) {
				t.Fatalf("provider cause not retained: %v", err)
			}
			if n := service.seen(fault); n != 1 {
				t.Fatalf("%d commit requests, want exactly one", n)
			}
			service.assertBodiesClosed(t)
		})
	}
}

// sinkTransport accepts Put Block / Put Block List and retains nothing.
type sinkTransport struct {
	mu       sync.Mutex
	maxBlock int64
	total    int64
	blocks   int
	commits  int
	peakHeap uint64
}

func (s *sinkTransport) Do(req *http.Request) (*http.Response, error) {
	n, err := io.Copy(io.Discard, req.Body)
	if err = errors.Join(err, req.Body.Close()); err != nil {
		return nil, err
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	s.mu.Lock()
	s.peakHeap = max(s.peakHeap, stats.HeapAlloc)
	if req.URL.Query().Get("comp") == "block" {
		s.blocks++
		s.total += n
		s.maxBlock = max(s.maxBlock, n)
	} else {
		s.commits++
	}
	s.mu.Unlock()
	return &http.Response{StatusCode: http.StatusCreated, Request: req, Body: http.NoBody,
		Header: http.Header{"Etag": {`"0x8DSINK"`}}}, nil
}

func TestCreateBoundedUploadBuffering(t *testing.T) {
	const size = int64(40<<20 + 123)
	sink := &sinkTransport{}
	client, err := container.NewClientWithNoCredential(fakeContainerURL,
		&container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: sink}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := store.Create(context.Background(), "big/run", &patternReader{size: size}, size)
	runtime.ReadMemStats(&after)
	if err != nil || result.Outcome != blobstore.Created || result.Identity.Size != size {
		t.Fatalf("Create: %+v %v", result, err)
	}
	if sink.maxBlock > stageBlockBytes || sink.total != size || sink.blocks != 6 || sink.commits != 1 {
		t.Fatalf("blocks=%d commits=%d max=%d total=%d", sink.blocks, sink.commits, sink.maxBlock, sink.total)
	}
	// One stageBlockBytes buffer plus small per-request overhead; an
	// implementation that materialized or re-copied blocks would exceed this.
	const budget = stageBlockBytes + 4<<20
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > budget {
		t.Fatalf("Create allocated %d bytes in total for a %d byte object (budget %d)", allocated, size, budget)
	}
	if sink.peakHeap > before.HeapAlloc && sink.peakHeap-before.HeapAlloc > budget {
		t.Fatalf("heap grew by %d bytes (budget %d)", sink.peakHeap-before.HeapAlloc, budget)
	}
}

func TestCreateCancellation(t *testing.T) {
	const size = int64(stageBlockBytes + 100)
	t.Run("before-send", func(t *testing.T) {
		service := newFakeService()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		producer := &patternReader{size: size}
		result, err := service.store(t).Create(ctx, "cancel/before", producer, size)
		if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) || service.mark() != 0 || producer.position != 0 {
			t.Fatalf("Create: %+v %v requests=%d", result, err, service.mark())
		}
	})
	t.Run("between-blocks", func(t *testing.T) {
		service := newFakeService()
		store := service.store(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		producer := &patternReader{size: size, onRead: func(position int64) {
			if position >= stageBlockBytes {
				cancel()
			}
		}}
		result, err := store.Create(ctx, "cancel/between", producer, size)
		if result.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) {
			t.Fatalf("Create: %+v %v", result, err)
		}
		if blocks, commits := len(service.requests(0, isBlock)), len(service.requests(0, isCommit)); blocks != 1 || commits != 0 {
			t.Fatalf("blocks=%d commits=%d", blocks, commits)
		}
		if _, err := store.Stat(context.Background(), "cancel/between"); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("Stat: %v", err)
		}
		service.assertBodiesClosed(t)
	})
	t.Run("during-commit", func(t *testing.T) {
		service := newFakeService()
		store := service.store(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fault := service.arm(isCommit, func(_ *fakeService, r *fakeRequest, _ func() *http.Response) (*http.Response, error) {
			cancel()
			return nil, r.raw.Context().Err()
		})
		result, err := store.Create(ctx, "cancel/commit", &patternReader{size: size}, size)
		if result.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Create: %+v %v", result, err)
		}
		if n := service.seen(fault); n != 1 {
			t.Fatalf("%d commit requests", n)
		}
		service.assertBodiesClosed(t)
	})
}

func createRun(t *testing.T, store *Store, key string, data []byte) blobstore.RunIdentity {
	t.Helper()
	result, err := store.Create(context.Background(), key, bytes.NewReader(data), int64(len(data)))
	if err != nil || result.Outcome != blobstore.Created {
		t.Fatalf("Create: %+v %v", result, err)
	}
	return result.Identity
}

func TestOpenRangeAndDeleteRequestShape(t *testing.T) {
	for _, versioning := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioning=%v", versioning), func(t *testing.T) {
			service := newFakeService()
			service.versioning = versioning
			store := service.store(t)
			ctx := context.Background()
			data := patternBytes(5000)
			id := createRun(t, store, "shape/run", data)

			var fields runToken
			if err := idtoken.Decode(provider, id.Token, &fields); err != nil {
				t.Fatal(err)
			}
			current := service.blobs["shape/run"]
			if fields.ETag != current.etag || fields.VersionID != current.version || (fields.VersionID != "") != versioning {
				t.Fatalf("token fields %+v vs blob %+v", fields, current)
			}
			stat, err := store.Stat(ctx, "shape/run")
			if err != nil || !stat.Equal(id) {
				t.Fatalf("Stat token differs from Create token: %+v vs %+v (%v)", stat, id, err)
			}

			start := service.mark()
			body, err := store.OpenRange(ctx, "shape/run", id, 1000, 250)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(body)
			if err = errors.Join(err, body.Close(), body.Close()); err != nil || !bytes.Equal(got, data[1000:1250]) {
				t.Fatalf("range: %v", err)
			}
			requests := service.requests(start, nil)
			if len(requests) != 1 || !isRange(requests[0]) || requests[0].header.Get("x-ms-range") != "bytes=1000-1249" ||
				requests[0].header.Get("If-Match") != current.etag || requests[0].versionID != "" {
				t.Fatalf("OpenRange requests: %d %+v", len(requests), requests[0].header)
			}

			if versioning {
				service.arm(isRange, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
					response := apply()
					response.Header.Set("x-ms-version-id", "2020-01-01T00:00:00.0000000Z")
					return response, nil
				})
				if _, err := store.OpenRange(ctx, "shape/run", id, 0, 10); !errors.Is(err, blobstore.ErrRunChanged) {
					t.Fatalf("OpenRange with a different version id: %v", err)
				}
			}

			// Replacement between Stat and the pinned operations.
			service.replace("shape/run", patternBytes(5000)[1:])
			if _, err := store.OpenRange(ctx, "shape/run", id, 0, 10); !errors.Is(err, blobstore.ErrRunChanged) || errors.Is(err, blobstore.ErrNotFound) {
				t.Fatalf("OpenRange after replacement: %v", err)
			}
			start = service.mark()
			if err := store.DeleteIfIdentity(ctx, "shape/run", id); !errors.Is(err, blobstore.ErrRunChanged) {
				t.Fatalf("DeleteIfIdentity after replacement: %v", err)
			}
			deletes := service.requests(start, isDelete)
			if len(deletes) != 1 || !isDelete(deletes[0]) || deletes[0].header.Get("If-Match") != current.etag || deletes[0].versionID != "" {
				t.Fatalf("delete requests: %d", len(deletes))
			}
			if _, err := store.Stat(ctx, "shape/run"); err != nil {
				t.Fatalf("replacement was deleted: %v", err)
			}
			service.assertBodiesClosed(t)
		})
	}
}

func TestOpenRangeBodyFaults(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	ctx := context.Background()
	id := createRun(t, store, "range/run", patternBytes(4096))

	t.Run("truncated", func(t *testing.T) {
		service.arm(isRange, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
			response := apply()
			response.Body.(*trackedBody).reader = bytes.NewReader(patternBytes(100))
			return response, nil
		})
		body, err := store.OpenRange(ctx, "range/run", id, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		if !errors.Is(err, io.ErrUnexpectedEOF) || len(got) != 100 {
			t.Fatalf("truncated read: %d bytes, %v", len(got), err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong-length-or-etag", func(t *testing.T) {
		for _, header := range []string{"Content-Length", "Etag"} {
			service.arm(isRange, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
				response := apply()
				response.Header.Set(header, "77")
				return response, nil
			})
			if _, err := store.OpenRange(ctx, "range/run", id, 0, 4096); !errors.Is(err, blobstore.ErrRunChanged) {
				t.Fatalf("%s mismatch: %v", header, err)
			}
		}
	})
	t.Run("cancelled-read", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		body, err := store.OpenRange(cancelled, "range/run", id, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err := body.Read(make([]byte, 8)); !errors.Is(err, context.Canceled) {
			t.Fatalf("read after cancel: %v", err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("errors-are-not-identity-changes", func(t *testing.T) {
		for _, act := range []fakeAct{actStatus(503, "ServerBusy"), actStatus(404, "ContainerNotFound"), actBeforeSend} {
			fault := service.arm(isRange, act)
			_, err := store.OpenRange(ctx, "range/run", id, 0, 16)
			if err == nil || errors.Is(err, blobstore.ErrRunChanged) || errors.Is(err, blobstore.ErrNotFound) || service.seen(fault) != 1 {
				t.Fatalf("OpenRange: %v (%d requests)", err, service.seen(fault))
			}
		}
	})
	service.assertBodiesClosed(t)
}

func TestIdentityTokens(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	ctx := context.Background()
	id := createRun(t, store, "token/run", []byte("payload"))

	roundTrip, err := encodeIdentity("k", 9, ptr(azcore.ETag(`"0x1"`)), ptr("2026-01-01T00:00:00.0000001Z"))
	if err != nil {
		t.Fatal(err)
	}
	fields, err := decodeIdentity(roundTrip)
	if err != nil || fields.ETag != `"0x1"` || fields.VersionID != "2026-01-01T00:00:00.0000001Z" {
		t.Fatalf("round trip: %+v %v", fields, err)
	}
	if _, err := encodeIdentity("k", 9, nil, nil); err == nil {
		t.Fatal("identity without ETag accepted")
	}

	mustToken := func(providerName string, value any) string {
		token, err := idtoken.Encode(providerName, value)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	start := service.mark()
	for name, token := range map[string]string{
		"garbage":       "garbage",
		"foreign":       mustToken("s3", runToken{ETag: `"abc"`}),
		"wrong-version": "azure.v2.e30",
		"empty-etag":    mustToken(provider, runToken{}),
		"wildcard-etag": mustToken(provider, runToken{ETag: "*"}),
		"control-etag":  mustToken(provider, runToken{ETag: "a\r\nIf-Match: *"}),
		"unknown-field": mustToken(provider, struct {
			ETag  string `json:"etag"`
			Extra string `json:"extra"`
		}{`"abc"`, "x"}),
	} {
		bad := blobstore.RunIdentity{Key: id.Key, Size: id.Size, Token: token}
		if _, err := store.OpenRange(ctx, id.Key, bad, 0, 1); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("%s OpenRange: %v", name, err)
		}
		if err := store.DeleteIfIdentity(ctx, id.Key, bad); !errors.Is(err, blobstore.ErrInvalidIdentity) {
			t.Fatalf("%s DeleteIfIdentity: %v", name, err)
		}
	}
	if n := service.mark() - start; n != 0 {
		t.Fatalf("invalid identities caused %d requests", n)
	}
	for _, token := range []string{"*", "a\nb", string(make([]byte, maxTokenBytes+1))} {
		result, err := store.CompareAndSwap(ctx, "token/meta", token, []byte("x"))
		if result.Outcome != blobstore.CASUnknown || !errors.Is(err, blobstore.ErrInvalidRequest) || service.mark() != start {
			t.Fatalf("malformed CAS token %q: %+v %v", token, result, err)
		}
	}
}

func ptr[T any](value T) *T { return &value }

func TestBoundedGetRejectsOversizedBeforeRead(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	ctx := context.Background()
	if _, err := store.Put(ctx, "meta/large", patternBytes(1<<20)); err != nil {
		t.Fatal(err)
	}
	bodies := len(service.bodies)
	if _, err := store.BoundedGet(ctx, "meta/large", 1<<20-1); !errors.Is(err, blobstore.ErrTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	if len(service.bodies) != bodies+1 || service.bodies[bodies].read.Load() != 0 {
		t.Fatalf("oversized body was read: %d bytes", service.bodies[bodies].read.Load())
	}
	// A service that under-reports the length is still bounded by the limiter.
	service.arm(isGet, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
		response := apply()
		response.Header.Set("Content-Length", "10")
		return response, nil
	})
	if _, err := store.BoundedGet(ctx, "meta/large", 1000); !errors.Is(err, blobstore.ErrTooLarge) {
		t.Fatalf("under-reported length: %v", err)
	}
	if read := service.bodies[len(service.bodies)-1].read.Load(); read > 1001 {
		t.Fatalf("limiter let %d bytes through a 1000 byte bound", read)
	}
	// Fewer bytes than the reported length is an uncertain read.
	service.arm(isGet, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
		response := apply()
		response.Body.(*trackedBody).reader = bytes.NewReader(patternBytes(5))
		return response, nil
	})
	if _, err := store.BoundedGet(ctx, "meta/large", 1<<20); !errors.Is(err, blobstore.ErrIndeterminate) {
		t.Fatalf("short body: %v", err)
	}
	service.arm(isGet, actCloseBody)
	if _, err := store.BoundedGet(ctx, "meta/large", 1<<20); !errors.Is(err, blobstore.ErrCleanup) || !errors.Is(err, storetest.ErrInjected) {
		t.Fatalf("close failure: %v", err)
	}
	service.assertBodiesClosed(t)
}

func TestThrottlingAndFaultsAreNeverConflicts(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	ctx := context.Background()
	id := createRun(t, store, "busy/run", []byte("payload"))
	seed, err := store.Put(ctx, "busy/meta", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, act := range []fakeAct{actStatus(503, "ServerBusy"), actStatus(429, "TooManyRequests"), actStatus(500, "InternalError"),
		actStatus(409, "LeaseIdMissing"), actStatus(412, "LeaseIdMissing"), actStatus(404, "ContainerNotFound")} {
		fault := service.arm(isPutBlob, act)
		result, err := store.CompareAndSwap(ctx, "busy/meta", seed.Token, []byte("v2"))
		if result.Outcome != blobstore.CASUnknown || err == nil || responseError(err) == nil || service.seen(fault) != 1 {
			t.Fatalf("CAS: %+v %v", result, err)
		}
		fault = service.arm(isPutBlob, act)
		if _, err := store.Put(ctx, "busy/meta", []byte("other")); !errors.Is(err, blobstore.ErrIndeterminate) ||
			errors.Is(err, blobstore.ErrImmutableConflict) || service.seen(fault) != 1 {
			t.Fatalf("Put: %v", err)
		}
		fault = service.arm(isDelete, act)
		if err := store.DeleteIfIdentity(ctx, "busy/run", id); !errors.Is(err, blobstore.ErrIndeterminate) ||
			errors.Is(err, blobstore.ErrRunChanged) || errors.Is(err, blobstore.ErrNotFound) || service.seen(fault) != 1 {
			t.Fatalf("DeleteIfIdentity: %v", err)
		}
		fault = service.arm(isHead, act)
		if _, err := store.Stat(ctx, "busy/run"); err == nil || errors.Is(err, blobstore.ErrNotFound) || service.seen(fault) != 1 {
			t.Fatalf("Stat: %v", err)
		}
		fault = service.arm(isGet, act)
		if _, err := store.BoundedGet(ctx, "busy/meta", 16); err == nil || errors.Is(err, blobstore.ErrNotFound) || service.seen(fault) != 1 {
			t.Fatalf("BoundedGet: %v", err)
		}
		fault = service.arm(isDelete, act)
		if err := store.Delete(ctx, "busy/meta"); err == nil || service.seen(fault) != 1 {
			t.Fatalf("Delete: %v", err)
		}
		fault = service.arm(isList, act)
		if _, err := store.List(ctx, blobstore.ListOptions{Prefix: "busy/"}); err == nil || service.seen(fault) != 1 {
			t.Fatalf("List: %v", err)
		}
	}
	got, err := store.BoundedGet(ctx, "busy/meta", 16)
	if err != nil || string(got.Body) != "v1" || got.Token != seed.Token {
		t.Fatalf("faulted writes changed the object: %q %v", got.Body, err)
	}
	service.assertBodiesClosed(t)
}

func TestCompareAndSwapEdges(t *testing.T) {
	ctx := context.Background()
	t.Run("if-match-on-missing-404", func(t *testing.T) {
		service := newFakeService()
		service.putIfMatchMissing = http.StatusNotFound
		result, err := service.store(t).CompareAndSwap(ctx, "cas/missing", `"0xDEAD"`, []byte("x"))
		if err != nil || result.Outcome != blobstore.CASConflict || result.CurrentKnown {
			t.Fatalf("CAS: %+v %v", result, err)
		}
		if n := len(service.requests(0, isHead)); n != 1 {
			t.Fatalf("%d follow-up stats", n)
		}
		service.assertBodiesClosed(t)
	})
	t.Run("create-404-is-not-a-conflict", func(t *testing.T) {
		service := newFakeService()
		service.arm(isPutBlob, actStatus(404, "BlobNotFound"))
		result, err := service.store(t).CompareAndSwap(ctx, "cas/odd", "", []byte("x"))
		if err == nil || result.Outcome != blobstore.CASUnknown {
			t.Fatalf("CAS: %+v %v", result, err)
		}
	})
	t.Run("conflict-wins-over-stat-failure", func(t *testing.T) {
		service := newFakeService()
		store := service.store(t)
		if _, err := store.Put(ctx, "cas/held", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		fault := service.arm(isHead, actStatus(503, "ServerBusy"))
		result, err := store.CompareAndSwap(ctx, "cas/held", "", []byte("v2"))
		if err != nil || result.Outcome != blobstore.CASConflict || result.CurrentKnown || result.CurrentToken != "" || service.seen(fault) != 1 {
			t.Fatalf("CAS: %+v %v", result, err)
		}
		service.assertBodiesClosed(t)
	})
	t.Run("applied-without-etag", func(t *testing.T) {
		service := newFakeService()
		store := service.store(t)
		service.arm(isPutBlob, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
			response := apply()
			response.Header.Del("Etag")
			return response, nil
		})
		result, err := store.CompareAndSwap(ctx, "cas/no-etag", "", []byte("x"))
		if result.Outcome != blobstore.CASUnknown || !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("CAS: %+v %v", result, err)
		}
		service.arm(isCommit, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
			response := apply()
			response.Header.Del("Etag")
			return response, nil
		})
		created, err := store.Create(ctx, "cas/run-no-etag", bytes.NewReader([]byte("x")), 1)
		if created.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("Create: %+v %v", created, err)
		}
		service.arm(isHead, func(_ *fakeService, _ *fakeRequest, apply func() *http.Response) (*http.Response, error) {
			response := apply()
			response.Header.Del("Etag")
			return response, nil
		})
		if _, err := store.Stat(ctx, "cas/run-no-etag"); !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("Stat: %v", err)
		}
		service.assertBodiesClosed(t)
	})
}

func TestListRejectsDisorderedProvider(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	listing := func(names ...string) fakeAct {
		return func(f *fakeService, r *fakeRequest, _ func() *http.Response) (*http.Response, error) {
			var out bytes.Buffer
			out.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>`)
			for _, name := range names {
				fmt.Fprintf(&out, "<Blob><Name>%s</Name><Properties><Content-Length>1</Content-Length></Properties></Blob>", name)
			}
			out.WriteString("</Blobs><NextMarker></NextMarker></EnumerationResults>")
			return f.respond(r, http.StatusOK, http.Header{"Content-Type": {"application/xml"}}, out.Bytes()), nil
		}
	}
	for name, act := range map[string]fakeAct{
		"descending":     listing("p/b", "p/a"),
		"duplicate":      listing("p/a", "p/a"),
		"outside-prefix": listing("p/a", "q/a"),
	} {
		service.arm(isList, act)
		if page, err := store.List(context.Background(), blobstore.ListOptions{Prefix: "p/"}); !errors.Is(err, blobstore.ErrIndeterminate) {
			t.Fatalf("%s: %+v %v", name, page, err)
		}
	}
	service.arm(isList, listing("p/a", "p/dir/", "p/z"))
	page, err := store.List(context.Background(), blobstore.ListOptions{Prefix: "p/"})
	if err != nil || len(page.Objects) != 2 || page.Objects[1].Key != "p/z" {
		t.Fatalf("unaddressable names must be skipped: %+v %v", page, err)
	}
	service.assertBodiesClosed(t)
}

func TestListScanIsBounded(t *testing.T) {
	service := newFakeService()
	store := service.store(t)
	for i := 0; i < 30; i++ {
		service.replace(fmt.Sprintf("scan/%03d", i), []byte("x"))
	}
	start := service.mark()
	page, err := store.List(context.Background(), blobstore.ListOptions{Prefix: "scan/", Limit: 5})
	if err != nil || len(page.Objects) != 5 || !page.HasMore || page.NextAfterKey != "scan/004" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	requests := service.requests(start, isList)
	if len(requests) != 1 || requests[0].raw.URL.Query().Get("maxresults") != "6" {
		t.Fatalf("first page issued %d list requests", len(requests))
	}
	start = service.mark()
	page, err = store.List(context.Background(), blobstore.ListOptions{Prefix: "scan/", AfterKey: "scan/004", Limit: 5})
	if err != nil || len(page.Objects) != 5 || page.Objects[0].Key != "scan/005" {
		t.Fatalf("second page: %+v %v", page, err)
	}
	if requests = service.requests(start, isList); len(requests) != 1 || requests[0].raw.URL.Query().Get("maxresults") != "1000" {
		t.Fatalf("second page issued %d list requests", len(requests))
	}
}
