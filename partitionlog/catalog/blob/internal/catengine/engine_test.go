package catengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestEngineReconcilesLostAppendResponseAndRemainsUsable(t *testing.T) {
	backend := newFaultStore()
	engine := testEngine(t, backend, 4, 2)
	initializeEngine(t, engine)
	session, err := engine.OpenWriter(context.Background(), 7, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	backend.failNextAppliedCAS(errors.New("lost CAS response"))
	segment := testSegment(session.config, session.head, 0)
	state, err := session.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if state.NextLSN != 1 || session.IsStale() {
		t.Fatalf("reconciled state next=%d stale=%v", state.NextLSN, session.IsStale())
	}
	if _, err := session.AppendSegment(context.Background(), testSegment(session.config, session.head, 1)); err != nil {
		t.Fatalf("AppendSegment after reconciliation error = %v", err)
	}
}

func TestEngineReconcilesOldCommitAfterSuccessorAndFencesBeforeUpload(t *testing.T) {
	backend := newFaultStore()
	engine := testEngine(t, backend, 2, 2)
	initializeEngine(t, engine)
	writerA, err := engine.OpenWriter(context.Background(), 7, filled16(0xa1))
	if err != nil {
		t.Fatal(err)
	}

	landed := make(chan struct{})
	release := make(chan struct{})
	backend.afterNextAppliedCAS(func() error {
		close(landed)
		<-release
		return errors.New("lost CAS response")
	})
	type appendResult struct {
		head pmeta.PartitionHead
		err  error
	}
	resultCh := make(chan appendResult, 1)
	go func() {
		head, err := writerA.AppendSegment(context.Background(), testSegment(writerA.config, writerA.head, 0))
		resultCh <- appendResult{head: head, err: err}
	}()
	<-landed

	writerB, err := engine.OpenWriter(context.Background(), 7, filled16(0xb1))
	if err != nil {
		t.Fatalf("OpenWriter(B) error = %v", err)
	}
	if _, err := writerB.AppendSegment(context.Background(), testSegment(writerB.config, writerB.head, 1)); err != nil {
		t.Fatalf("writer B AppendSegment() error = %v", err)
	}
	close(release)
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("writer A reconciliation error = %v", result.err)
	}
	if result.head.NextLSN != 1 || result.head.WriterEpoch != writerA.Epoch() {
		t.Fatalf("writer A acknowledgement = %+v, want its committed post-state", result.head)
	}
	if !writerA.IsStale() {
		t.Fatal("writer A did not become stale after observing successor fence")
	}
	if cached := writerA.Head(); cached.NextLSN != 2 || cached.WriterEpoch != writerB.Epoch() {
		t.Fatalf("writer A cached authoritative head = %+v, want successor state", cached)
	}

	putsBefore := backend.putCount()
	if _, err := writerA.AppendSegment(context.Background(), pmeta.SegmentRef{}); !errors.Is(err, csession.ErrStaleWriter) {
		t.Fatalf("stale AppendSegment() error = %v, want ErrStaleWriter", err)
	}
	if got := backend.putCount(); got != putsBefore {
		t.Fatalf("stale append performed %d immutable PUTs", got-putsBefore)
	}
}

func TestEngineReturnsDefiniteCASFailureWhenHeadDidNotMove(t *testing.T) {
	backend := newFaultStore()
	engine := testEngine(t, backend, 4, 2)
	initializeEngine(t, engine)
	session, err := engine.OpenWriter(context.Background(), 7, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("CAS unavailable")
	backend.failCASWithoutApply(engine.opts.CASAttempts, injected)
	if _, err := session.AppendSegment(context.Background(), testSegment(session.config, session.head, 0)); !errors.Is(err, injected) {
		t.Fatalf("AppendSegment() error = %v, want injected error", err)
	} else if errors.Is(err, csession.ErrCommitIndeterminate) {
		t.Fatalf("unchanged authoritative head was reported indeterminate: %v", err)
	}
	head, err := engine.LoadPartition(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if head.NextLSN != 0 {
		t.Fatalf("head NextLSN=%d after unapplied CAS", head.NextLSN)
	}
}

func TestEngineMissingHeadUsesOneLookup(t *testing.T) {
	backend := newFaultStore()
	engine := testEngine(t, backend, 4, 2)

	head, _, err := engine.Load(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if head.Header.NextLSN != 0 {
		t.Fatalf("Load() next_lsn = %d, want 0", head.Header.NextLSN)
	}
	if got := backend.getCount(); got != 1 {
		t.Fatalf("Load() GET count = %d, want 1", got)
	}

	initializeStore := newFaultStore()
	initializeCatalog := testEngine(t, initializeStore, 4, 2)
	if _, created, err := initializeCatalog.Initialize(context.Background(), 7, 0); err != nil {
		t.Fatal(err)
	} else if !created {
		t.Fatal("Initialize() did not create the missing head")
	}
	if got := initializeStore.getCount(); got != 0 {
		t.Fatalf("Initialize() GET count = %d, want 0", got)
	}
}

func testEngine(t *testing.T, backend blobstore.Store, leafLimit, indexLimit uint32) *Engine {
	t.Helper()
	engine, err := NewEngine(backend, EngineOptions{
		CatalogPrefix: "catalog-format",
		DataRoot:      "data-root",
		StreamID:      "tenant/events",
		LeafLimit:     leafLimit,
		IndexLimit:    indexLimit,
		HashAlgo:      segformat.HashXXH64,
		CASAttempts:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func initializeEngine(t *testing.T, engine *Engine) {
	t.Helper()
	state, created, err := engine.Initialize(context.Background(), 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !created || state.NextLSN != 0 {
		t.Fatalf("Initialize() = (%#v,%v)", state, created)
	}
}

type faultStore struct {
	mu      sync.Mutex
	objects map[string]blobstore.Object
	next    uint64
	gets    int
	puts    int

	casFailures int
	casErr      error
	afterApply  func() error
}

func newFaultStore() *faultStore {
	return &faultStore{objects: make(map[string]blobstore.Object)}
}

func (s *faultStore) Get(ctx context.Context, key string) (blobstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	object, ok := s.objects[key]
	if !ok {
		return blobstore.Object{}, blobstore.ErrObjectNotFound
	}
	object.Body = append([]byte(nil), object.Body...)
	return object, nil
}

func (s *faultStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func (s *faultStore) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.objects[key]; ok {
		if bytes.Equal(existing.Body, body) {
			existing.Body = append([]byte(nil), existing.Body...)
			return existing, nil
		}
		return blobstore.Object{}, blobstore.ErrImmutableConflict
	}
	s.puts++
	object := s.newObjectLocked(key, body)
	s.objects[key] = object
	return cloneObject(object), nil
}

func (s *faultStore) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, false, err
	}
	s.mu.Lock()
	current, exists := s.objects[key]
	matches := !exists && expectedToken == "" || exists && current.Token == expectedToken
	if !matches {
		s.mu.Unlock()
		if !exists {
			return blobstore.Object{}, false, blobstore.ErrObjectNotFound
		}
		return cloneObject(current), false, nil
	}
	if s.casFailures > 0 {
		s.casFailures--
		err := s.casErr
		s.mu.Unlock()
		return blobstore.Object{}, false, err
	}
	object := s.newObjectLocked(key, body)
	s.objects[key] = object
	hook := s.afterApply
	s.afterApply = nil
	s.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return blobstore.Object{}, false, err
		}
	}
	return cloneObject(object), true, nil
}

func (s *faultStore) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, opts.Prefix) && key > opts.AfterKey {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	limit := opts.NormalizedLimit()
	page := blobstore.ObjectPage{}
	for i, key := range keys {
		if i == limit {
			page.HasMore = true
			page.NextAfterKey = keys[i-1]
			break
		}
		object := s.objects[key]
		page.Objects = append(page.Objects, blobstore.ObjectInfo{Key: key, Token: object.Token, SizeBytes: len(object.Body), CreatedAt: object.CreatedAt})
	}
	return page, nil
}

func (s *faultStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

func (s *faultStore) failNextAppliedCAS(err error) {
	s.afterNextAppliedCAS(func() error { return err })
}

func (s *faultStore) afterNextAppliedCAS(hook func() error) {
	s.mu.Lock()
	s.afterApply = hook
	s.mu.Unlock()
}

func (s *faultStore) failCASWithoutApply(count int, err error) {
	s.mu.Lock()
	s.casFailures = count
	s.casErr = err
	s.mu.Unlock()
}

func (s *faultStore) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func (s *faultStore) newObjectLocked(key string, body []byte) blobstore.Object {
	s.next++
	return blobstore.Object{Key: key, Body: append([]byte(nil), body...), Token: fmt.Sprintf("t-%d", s.next), CreatedAt: time.Unix(0, int64(s.next))}
}

func cloneObject(object blobstore.Object) blobstore.Object {
	object.Body = append([]byte(nil), object.Body...)
	return object
}
