// Package storetest holds the conformance suites every blobstore backend
// runs. A backend passes only when both suites pass against the same store.
package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/isledb/blobstore"
)

// Op names the logical operation a fault targets.
type Op string

const (
	OpPut              Op = "Put"
	OpCompareAndSwap   Op = "CompareAndSwap"
	OpCreate           Op = "Create"
	OpOpenRange        Op = "OpenRange"
	OpDeleteIfIdentity Op = "DeleteIfIdentity"
)

type FaultKind uint8

const (
	// FaultBeforeSend fails the provider request before it has any effect.
	FaultBeforeSend FaultKind = iota + 1
	// FaultLostResponse lets the provider apply the request, then loses the
	// response in transport.
	FaultLostResponse
	// FaultCloseBody makes the OpenRange response body fail on Close.
	FaultCloseBody
	// FaultThrottle answers with the provider's throttling response.
	FaultThrottle
)

// ErrInjected is joined into every injected failure so suites can prove that
// the underlying cause is preserved.
var ErrInjected = errors.New("storetest: injected provider fault")

// Harness adapts one backend to the suites. Optional members may be nil; the
// dependent subtests are then skipped and reported as skipped, never passed.
type Harness struct {
	Metadata blobstore.MetadataStore
	Runs     blobstore.RunStore
	// Prefix isolates this run's keys inside a shared bucket. It ends in "/".
	Prefix string
	// Reopen returns stores over the same durable data, as after a restart.
	Reopen func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore)
	// Replace overwrites key out of band, as a foreign writer would.
	Replace func(t *testing.T, key string, body []byte)
	// Inject arms one fault for the next matching operation and returns the
	// number of provider requests observed for that operation since arming.
	Inject func(t *testing.T, op Op, kind FaultKind) (requests func() int)
	// ListObjects is how many objects the pagination test creates. Zero means
	// 2,100, which crosses two full 1,000-entry pages.
	ListObjects int
}

func (h Harness) key(parts string) string { return h.Prefix + parts }

func mustBody(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = seed + byte(i*31)
	}
	return out
}

// MetadataSuite verifies the metadata-object contract.
func MetadataSuite(t *testing.T, h Harness) {
	ctx := context.Background()
	m := h.Metadata

	t.Run("BoundedGet", func(t *testing.T) {
		key := h.key("meta/bounded")
		if _, err := m.BoundedGet(ctx, key, 16); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		body := mustBody(100, 1)
		if _, err := m.Put(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		got, err := m.BoundedGet(ctx, key, 100)
		if err != nil || !bytes.Equal(got.Body, body) || got.Token == "" || got.Key != key {
			t.Fatalf("exact bound: %v", err)
		}
		if _, err := m.BoundedGet(ctx, key, 99); !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("oversized: %v", err)
		}
		for _, bad := range []int64{0, -1, blobstore.MaxMetadataBytes + 1} {
			if _, err := m.BoundedGet(ctx, key, bad); !errors.Is(err, blobstore.ErrInvalidRequest) {
				t.Fatalf("bound %d: %v", bad, err)
			}
		}
		for _, bad := range []string{"", "/a", "a/", "a//b", "a/../b", "a/./b", "a\x00b"} {
			if _, err := m.BoundedGet(ctx, bad, 10); !errors.Is(err, blobstore.ErrInvalidRequest) {
				t.Fatalf("key %q: %v", bad, err)
			}
		}
		var nilCtx context.Context
		if _, err := m.BoundedGet(nilCtx, key, 10); !errors.Is(err, blobstore.ErrInvalidRequest) {
			t.Fatalf("nil context: %v", err)
		}
	})

	t.Run("PutImmutableReplay", func(t *testing.T) {
		key := h.key("meta/immutable")
		body := mustBody(64, 2)
		first, err := m.Put(ctx, key, body)
		if err != nil || first.Token == "" {
			t.Fatal(err)
		}
		again, err := m.Put(ctx, key, body)
		if err != nil || again.Token != first.Token {
			t.Fatalf("replay: %v", err)
		}
		if _, err := m.Put(ctx, key, mustBody(64, 3)); !errors.Is(err, blobstore.ErrImmutableConflict) {
			t.Fatalf("different bytes: %v", err)
		}
		if _, err := m.Put(ctx, key, mustBody(65, 2)); !errors.Is(err, blobstore.ErrImmutableConflict) {
			t.Fatalf("longer bytes: %v", err)
		}
		got, err := m.BoundedGet(ctx, key, 64)
		if err != nil || !bytes.Equal(got.Body, body) {
			t.Fatalf("conflict changed object: %v", err)
		}
	})

	t.Run("CompareAndSwap", func(t *testing.T) {
		key := h.key("meta/cas")
		created, err := m.CompareAndSwap(ctx, key, "", []byte("v1"))
		if err != nil || created.Outcome != blobstore.CASApplied || created.Object.Token == "" {
			t.Fatalf("create-if-absent: %v %v", created.Outcome, err)
		}
		lost, err := m.CompareAndSwap(ctx, key, "", []byte("other"))
		if err != nil || lost.Outcome != blobstore.CASConflict || !lost.CurrentKnown ||
			lost.CurrentToken != created.Object.Token || lost.Current.Size != 2 {
			t.Fatalf("second create: %+v %v", lost, err)
		}
		updated, err := m.CompareAndSwap(ctx, key, created.Object.Token, []byte("v2!"))
		if err != nil || updated.Outcome != blobstore.CASApplied || updated.Object.Token == created.Object.Token {
			t.Fatalf("swap: %+v %v", updated, err)
		}
		stale, err := m.CompareAndSwap(ctx, key, created.Object.Token, []byte("v3"))
		if err != nil || stale.Outcome != blobstore.CASConflict || stale.CurrentToken != updated.Object.Token {
			t.Fatalf("stale: %+v %v", stale, err)
		}
		got, err := m.BoundedGet(ctx, key, 16)
		if err != nil || string(got.Body) != "v2!" || got.Token != updated.Object.Token {
			t.Fatalf("read back: %q %v", got.Body, err)
		}
		if err := m.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		gone, err := m.CompareAndSwap(ctx, key, updated.Object.Token, []byte("v4"))
		if err != nil || gone.Outcome != blobstore.CASConflict || gone.CurrentKnown {
			t.Fatalf("swap on missing: %+v %v", gone, err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		res, err := m.CompareAndSwap(cancelled, key, "", []byte("v5"))
		if !errors.Is(err, context.Canceled) || res.Outcome != blobstore.CASUnknown {
			t.Fatalf("cancelled: %+v %v", res, err)
		}
		if _, err := m.BoundedGet(ctx, key, 16); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("cancelled CAS applied: %v", err)
		}
	})

	t.Run("CompareAndSwapUnknown", func(t *testing.T) {
		if h.Inject == nil {
			t.Skip("backend harness has no fault injection")
		}
		key := h.key("meta/cas-unknown")
		requests := h.Inject(t, OpCompareAndSwap, FaultLostResponse)
		res, err := m.CompareAndSwap(ctx, key, "", []byte("applied"))
		if err == nil || res.Outcome != blobstore.CASUnknown || !errors.Is(err, ErrInjected) {
			t.Fatalf("lost response: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden CAS retry: %d requests", n)
		}
		got, err := m.BoundedGet(ctx, key, 16)
		if err != nil || string(got.Body) != "applied" {
			t.Fatalf("lost response must have applied: %v", err)
		}
		requests = h.Inject(t, OpCompareAndSwap, FaultBeforeSend)
		res, err = m.CompareAndSwap(ctx, key, got.Token, []byte("never"))
		if err == nil || res.Outcome != blobstore.CASUnknown || !errors.Is(err, ErrInjected) {
			t.Fatalf("before send: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden CAS retry: %d requests", n)
		}
		requests = h.Inject(t, OpCompareAndSwap, FaultThrottle)
		res, err = m.CompareAndSwap(ctx, key, got.Token, []byte("never"))
		if err == nil || res.Outcome != blobstore.CASUnknown {
			t.Fatalf("throttle must not be a conflict: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden throttle retry: %d requests", n)
		}
		after, err := m.BoundedGet(ctx, key, 16)
		if err != nil || string(after.Body) != "applied" {
			t.Fatalf("failed CAS changed object: %q %v", after.Body, err)
		}
		requests = h.Inject(t, OpPut, FaultLostResponse)
		putKey := h.key("meta/put-lost")
		if _, err := m.Put(ctx, putKey, []byte("page")); !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, ErrInjected) {
			t.Fatalf("lost Put response: %v", err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden Put retry: %d requests", n)
		}
		if _, err := m.Put(ctx, putKey, []byte("page")); err != nil {
			t.Fatalf("replay after lost Put: %v", err)
		}
	})

	t.Run("ListPagination", func(t *testing.T) {
		total := h.ListObjects
		if total == 0 {
			total = 2100
		}
		prefix := h.key("list/")
		want := make([]string, 0, total)
		for i := 0; i < total; i++ {
			key := fmt.Sprintf("%sobj-%05d", prefix, i)
			if _, err := m.Put(ctx, key, []byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
			want = append(want, key)
		}
		if _, err := m.Put(ctx, h.key("list-sibling"), []byte("x")); err != nil {
			t.Fatal(err)
		}
		sort.Strings(want)
		var got []string
		after := ""
		for pages := 0; ; pages++ {
			if pages > total/blobstore.MaxListLimit+2 {
				t.Fatal("listing did not terminate")
			}
			// An over-ceiling limit must clamp to the common 1,000 maximum.
			page, err := m.List(ctx, blobstore.ListOptions{Prefix: prefix, AfterKey: after, Limit: 5000})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Objects) > blobstore.MaxListLimit {
				t.Fatalf("page of %d exceeds ceiling", len(page.Objects))
			}
			for _, object := range page.Objects {
				if object.Size != 1 {
					t.Fatalf("size %d for %s", object.Size, object.Key)
				}
				got = append(got, object.Key)
			}
			if !page.HasMore {
				break
			}
			if len(page.Objects) != blobstore.MaxListLimit || page.NextAfterKey != page.Objects[len(page.Objects)-1].Key {
				t.Fatalf("short non-final page: %d next=%q", len(page.Objects), page.NextAfterKey)
			}
			after = page.NextAfterKey
		}
		if len(got) != len(want) {
			t.Fatalf("listed %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("order at %d: %q want %q", i, got[i], want[i])
			}
		}
		// A cursor naming a deleted or never-existing key stays valid.
		if err := m.Delete(ctx, want[4]); err != nil {
			t.Fatal(err)
		}
		page, err := m.List(ctx, blobstore.ListOptions{Prefix: prefix, AfterKey: want[4], Limit: 2})
		if err != nil || len(page.Objects) != 2 || page.Objects[0].Key != want[5] || !page.HasMore {
			t.Fatalf("cursor over deleted key: %+v %v", page, err)
		}
		page, err = m.List(ctx, blobstore.ListOptions{Prefix: prefix, AfterKey: want[5] + "-between", Limit: 1})
		if err != nil || len(page.Objects) != 1 || page.Objects[0].Key != want[6] {
			t.Fatalf("cursor between keys: %+v %v", page, err)
		}
		page, err = m.List(ctx, blobstore.ListOptions{Prefix: prefix, AfterKey: want[len(want)-1]})
		if err != nil || len(page.Objects) != 0 || page.HasMore || page.NextAfterKey != "" {
			t.Fatalf("after last: %+v %v", page, err)
		}
	})

	t.Run("DeleteIdempotent", func(t *testing.T) {
		key := h.key("meta/delete")
		if err := m.Delete(ctx, key); err != nil {
			t.Fatalf("absent: %v", err)
		}
		if _, err := m.Put(ctx, key, []byte("x")); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := m.Delete(ctx, key); err != nil {
				t.Fatalf("delete %d: %v", i, err)
			}
		}
		if _, err := m.BoundedGet(ctx, key, 4); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("Cancellation", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		key := h.key("meta/cancelled")
		if _, err := m.Put(cancelled, key, []byte("x")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put: %v", err)
		}
		if _, err := m.BoundedGet(cancelled, key, 4); !errors.Is(err, context.Canceled) {
			t.Fatalf("BoundedGet: %v", err)
		}
		if _, err := m.List(cancelled, blobstore.ListOptions{Prefix: h.Prefix}); !errors.Is(err, context.Canceled) {
			t.Fatalf("List: %v", err)
		}
		if err := m.Delete(cancelled, key); !errors.Is(err, context.Canceled) {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := m.BoundedGet(ctx, key, 4); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("cancelled Put applied: %v", err)
		}
	})
}

// forwardOnly hides every optional interface so a backend cannot seek, size,
// or re-read the producer.
type forwardOnly struct {
	r     io.Reader
	reads atomic.Int64
}

func (f *forwardOnly) Read(p []byte) (int, error) {
	f.reads.Add(1)
	return f.r.Read(p)
}

type failingReader struct {
	data []byte
	err  error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if len(f.data) == 0 {
		return 0, f.err
	}
	n := copy(p, f.data)
	f.data = f.data[n:]
	return n, nil
}

func readAll(t *testing.T, r blobstore.RunStore, id blobstore.RunIdentity, offset, length int64) []byte {
	t.Helper()
	body, err := r.OpenRange(context.Background(), id.Key, id, offset, length)
	if err != nil {
		t.Fatalf("OpenRange(%d,%d): %v", offset, length, err)
	}
	data, readErr := io.ReadAll(body)
	if err := errors.Join(readErr, body.Close()); err != nil {
		t.Fatalf("read range: %v", err)
	}
	return data
}

// RunSuite verifies the run-object contract.
func RunSuite(t *testing.T, h Harness) {
	ctx := context.Background()
	r := h.Runs
	// Larger than every backend's copy chunk multiple-of-anything assumption
	// while staying cheap for emulators.
	payload := mustBody(3<<20+17, 7)

	create := func(t *testing.T, key string, data []byte) blobstore.RunIdentity {
		t.Helper()
		res, err := r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(data)}, int64(len(data)))
		if err != nil || res.Outcome != blobstore.Created || !res.Identity.Valid() ||
			res.Identity.Key != key || res.Identity.Size != int64(len(data)) {
			t.Fatalf("Create: %+v %v", res, err)
		}
		return res.Identity
	}

	t.Run("CreateStatOpenRange", func(t *testing.T) {
		key := h.key("run/exact")
		if _, err := r.Stat(ctx, key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("Stat missing: %v", err)
		}
		id := create(t, key, payload)
		for i := 0; i < 2; i++ {
			stat, err := r.Stat(ctx, key)
			if err != nil || !stat.Equal(id) {
				t.Fatalf("Stat identity unstable: %+v vs %+v (%v)", stat, id, err)
			}
		}
		if got := readAll(t, r, id, 0, id.Size); !bytes.Equal(got, payload) {
			t.Fatal("full range differs")
		}
		if got := readAll(t, r, id, 1<<20+3, 4099); !bytes.Equal(got, payload[1<<20+3:1<<20+3+4099]) {
			t.Fatal("inner range differs")
		}
		if got := readAll(t, r, id, id.Size-1, 1); got[0] != payload[len(payload)-1] {
			t.Fatal("last byte differs")
		}
		for _, bad := range [][2]int64{{-1, 1}, {0, 0}, {0, -1}, {id.Size, 1}, {0, id.Size + 1}, {1 << 62, 1 << 62}, {2, id.Size - 1}} {
			if _, err := r.OpenRange(ctx, key, id, bad[0], bad[1]); !errors.Is(err, blobstore.ErrInvalidRequest) {
				t.Fatalf("range %v: %v", bad, err)
			}
		}
	})

	t.Run("AlreadyExists", func(t *testing.T) {
		key := h.key("run/exists")
		id := create(t, key, payload[:4096])
		res, err := r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(payload[:4096])}, 4096)
		if res.Outcome != blobstore.AlreadyExists || !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("second create: %+v %v", res, err)
		}
		res, err = r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(payload[:8192])}, 8192)
		if res.Outcome != blobstore.AlreadyExists || !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("second create, different bytes: %+v %v", res, err)
		}
		stat, err := r.Stat(ctx, key)
		if err != nil || !stat.Equal(id) {
			t.Fatalf("rejected create changed the object: %v", err)
		}
	})

	t.Run("ProducerLengthAndErrors", func(t *testing.T) {
		injected := errors.New("producer failed")
		for name, tc := range map[string]struct {
			body io.Reader
			size int64
			want error
		}{
			"short":    {bytes.NewReader(payload[:1000]), 1001, io.ErrUnexpectedEOF},
			"long":     {bytes.NewReader(payload[:1002]), 1001, blobstore.ErrInvalidRequest},
			"error":    {&failingReader{data: bytes.Clone(payload[:500]), err: injected}, 1001, injected},
			"late-err": {&failingReader{data: bytes.Clone(payload[:1001]), err: injected}, 1001, injected},
		} {
			key := h.key("run/producer-" + name)
			res, err := r.Create(ctx, key, &forwardOnly{r: tc.body}, tc.size)
			if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, tc.want) {
				t.Fatalf("%s: %+v %v", name, res, err)
			}
			if _, err := r.Stat(ctx, key); !errors.Is(err, blobstore.ErrNotFound) {
				t.Fatalf("%s: object visible after rejected producer: %v", name, err)
			}
			// The key is not poisoned: a correct producer still succeeds.
			create(t, key, payload[:1001])
		}
		for _, size := range []int64{0, -1} {
			res, err := r.Create(ctx, h.key("run/bad-size"), bytes.NewReader(nil), size)
			if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, blobstore.ErrInvalidRequest) {
				t.Fatalf("size %d: %+v %v", size, res, err)
			}
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		key := h.key("run/cancelled")
		res, err := r.Create(cancelled, key, bytes.NewReader(payload[:64]), 64)
		if res.Outcome != blobstore.DefinitelyAbsent || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled before send: %+v %v", res, err)
		}
		if _, err := r.Stat(ctx, key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("cancelled create visible: %v", err)
		}
	})

	t.Run("CreateFaults", func(t *testing.T) {
		if h.Inject == nil {
			t.Skip("backend harness has no fault injection")
		}
		key := h.key("run/lost-response")
		requests := h.Inject(t, OpCreate, FaultLostResponse)
		res, err := r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(payload[:2048])}, 2048)
		if res.Outcome != blobstore.CreateIndeterminate || !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, ErrInjected) {
			t.Fatalf("lost response: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden Create retry: %d commit requests", n)
		}
		id, err := r.Stat(ctx, key)
		if err != nil || id.Size != 2048 {
			t.Fatalf("lost response must have created: %v", err)
		}
		if got := readAll(t, r, id, 0, 2048); !bytes.Equal(got, payload[:2048]) {
			t.Fatal("reconciled bytes differ")
		}
		key = h.key("run/before-send")
		requests = h.Inject(t, OpCreate, FaultBeforeSend)
		res, err = r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(payload[:2048])}, 2048)
		if res.Outcome == blobstore.Created || res.Outcome == blobstore.AlreadyExists || err == nil || !errors.Is(err, ErrInjected) {
			t.Fatalf("before send: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden Create retry: %d requests", n)
		}
		if _, err := r.Stat(ctx, key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("failed create visible: %v", err)
		}
		requests = h.Inject(t, OpCreate, FaultThrottle)
		res, err = r.Create(ctx, key, &forwardOnly{r: bytes.NewReader(payload[:2048])}, 2048)
		if res.Outcome == blobstore.Created || res.Outcome == blobstore.AlreadyExists || err == nil {
			t.Fatalf("throttle must not be success or conflict: %+v %v", res, err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden throttle retry: %d requests", n)
		}
	})

	t.Run("ReplacementDetection", func(t *testing.T) {
		if h.Replace == nil {
			t.Skip("backend harness cannot replace out of band")
		}
		key := h.key("run/replaced")
		id := create(t, key, payload[:4096])
		// Same length, different bytes: only the identity can tell.
		h.Replace(t, key, mustBody(4096, 99))
		if _, err := r.OpenRange(ctx, key, id, 0, 16); !errors.Is(err, blobstore.ErrRunChanged) {
			t.Fatalf("OpenRange after replacement: %v", err)
		}
		if err := r.DeleteIfIdentity(ctx, key, id); !errors.Is(err, blobstore.ErrRunChanged) {
			t.Fatalf("DeleteIfIdentity after replacement: %v", err)
		}
		current, err := r.Stat(ctx, key)
		if err != nil || current.Equal(id) {
			t.Fatalf("replacement deleted or identity reused: %v", err)
		}
		if got := readAll(t, r, current, 0, 4096); !bytes.Equal(got, mustBody(4096, 99)) {
			t.Fatal("replacement bytes differ")
		}
	})

	t.Run("DeleteIfIdentity", func(t *testing.T) {
		key := h.key("run/delete")
		id := create(t, key, payload[:512])
		// Different bytes: a provider whose identity is content-derived (an
		// unversioned S3 ETag is the content MD5) legitimately gives two
		// byte-identical objects the same token.
		other := create(t, h.key("run/delete-other"), mustBody(512, 123))
		wrongKey := other
		wrongKey.Key = key
		// Another object's token must never authorize this delete.
		if err := r.DeleteIfIdentity(ctx, key, wrongKey); err == nil {
			t.Fatal("foreign identity deleted the object")
		}
		for _, bad := range []blobstore.RunIdentity{{}, {Key: key, Size: 512}, {Key: key, Size: 512, Token: "garbage"},
			{Key: key, Size: 512, Token: "other.v1.e30"}, other} {
			if err := r.DeleteIfIdentity(ctx, key, bad); !errors.Is(err, blobstore.ErrInvalidIdentity) {
				t.Fatalf("identity %+v: %v", bad, err)
			}
			if _, err := r.OpenRange(ctx, key, bad, 0, 1); !errors.Is(err, blobstore.ErrInvalidIdentity) {
				t.Fatalf("OpenRange identity %+v: %v", bad, err)
			}
		}
		if _, err := r.Stat(ctx, key); err != nil {
			t.Fatalf("invalid identity deleted the object: %v", err)
		}
		if err := r.DeleteIfIdentity(ctx, key, id); err != nil {
			t.Fatal(err)
		}
		if err := r.DeleteIfIdentity(ctx, key, id); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("second delete: %v", err)
		}
		if _, err := r.OpenRange(ctx, key, id, 0, 1); !errors.Is(err, blobstore.ErrRunChanged) || !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("OpenRange after delete: %v", err)
		}
	})

	t.Run("RangeFaults", func(t *testing.T) {
		if h.Inject == nil {
			t.Skip("backend harness has no fault injection")
		}
		key := h.key("run/range-faults")
		id := create(t, key, payload[:4096])
		h.Inject(t, OpOpenRange, FaultCloseBody)
		body, err := r.OpenRange(ctx, key, id, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(body); err != nil {
			t.Fatal(err)
		}
		if err := body.Close(); !errors.Is(err, blobstore.ErrCleanup) {
			t.Fatalf("close failure not reported: %v", err)
		}
		requests := h.Inject(t, OpDeleteIfIdentity, FaultLostResponse)
		if err := r.DeleteIfIdentity(ctx, key, id); !errors.Is(err, blobstore.ErrIndeterminate) || !errors.Is(err, ErrInjected) {
			t.Fatalf("lost delete response: %v", err)
		}
		if n := requests(); n != 1 {
			t.Fatalf("hidden delete retry: %d requests", n)
		}
	})

	t.Run("IdentityRoundTripAcrossReopen", func(t *testing.T) {
		key := h.key("run/persisted")
		id := create(t, key, payload[:9000])
		encoded, err := json.Marshal(id)
		if err != nil {
			t.Fatal(err)
		}
		var generic map[string]any
		if err := json.Unmarshal(encoded, &generic); err != nil || len(generic) != 3 {
			t.Fatalf("identity is not a three-member JSON object: %s", encoded)
		}
		runs := r
		if h.Reopen != nil {
			_, runs = h.Reopen(t)
		}
		var restored blobstore.RunIdentity
		if err := json.Unmarshal(encoded, &restored); err != nil || !restored.Equal(id) {
			t.Fatalf("round trip: %v", err)
		}
		stat, err := runs.Stat(ctx, key)
		if err != nil || !stat.Equal(restored) {
			t.Fatalf("identity not durable across reopen: %+v vs %+v (%v)", stat, restored, err)
		}
		if got := readAll(t, runs, restored, 10, 100); !bytes.Equal(got, payload[10:110]) {
			t.Fatal("range after reopen differs")
		}
		if err := runs.DeleteIfIdentity(ctx, key, restored); err != nil {
			t.Fatalf("conditional delete with persisted identity: %v", err)
		}
	})
}
