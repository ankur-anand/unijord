package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStoreCachesRangeAndCopiesBytes(t *testing.T) {
	t.Parallel()

	inner := newCountingStore(map[string][]byte{"segment": []byte("hello cached world")})
	store, err := NewStore(inner, NewLRU(1<<20))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	first, err := store.ReadAt(context.Background(), "segment", 6, 6)
	if err != nil {
		t.Fatalf("ReadAt(first) error = %v", err)
	}
	first[0] = 'X'
	second, err := store.ReadAt(context.Background(), "segment", 6, 6)
	if err != nil {
		t.Fatalf("ReadAt(second) error = %v", err)
	}
	if string(second) != "cached" {
		t.Fatalf("ReadAt(second) = %q, want cached", second)
	}
	if got := inner.count(Key{URI: "segment", Off: 6, N: 6}); got != 1 {
		t.Fatalf("inner reads = %d, want 1", got)
	}
}

func TestStoreDoesNotCacheErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	inner := &flakyStore{
		body: []byte("abcdef"),
		errs: []error{wantErr, nil},
	}
	store, err := NewStore(inner, NewLRU(1024))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := store.ReadAt(context.Background(), "segment", 1, 3); !errors.Is(err, wantErr) {
		t.Fatalf("ReadAt(first) error = %v, want %v", err, wantErr)
	}
	got, err := store.ReadAt(context.Background(), "segment", 1, 3)
	if err != nil {
		t.Fatalf("ReadAt(second) error = %v", err)
	}
	if string(got) != "bcd" {
		t.Fatalf("ReadAt(second) = %q, want bcd", got)
	}
	if inner.reads() != 2 {
		t.Fatalf("inner reads = %d, want 2", inner.reads())
	}
}

func TestStoreCoalescesConcurrentReads(t *testing.T) {
	t.Parallel()

	inner := newCountingStore(map[string][]byte{"segment": bytes.Repeat([]byte("x"), 128)})
	inner.delay = 25 * time.Millisecond
	store, err := NewStore(inner, NewLRU(1<<20))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := store.ReadAt(context.Background(), "segment", 4, 32)
			if err != nil {
				errs <- err
				return
			}
			if len(got) != 32 {
				errs <- fmt.Errorf("len=%d want=32", len(got))
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := inner.count(Key{URI: "segment", Off: 4, N: 32}); got != 1 {
		t.Fatalf("inner reads = %d, want 1", got)
	}
}

func TestLRUEvictsByByteBudget(t *testing.T) {
	t.Parallel()

	cache := NewLRU(8)
	k1 := Key{URI: "a", Off: 0, N: 4}
	k2 := Key{URI: "b", Off: 0, N: 4}
	k3 := Key{URI: "c", Off: 0, N: 4}
	cache.Set(k1, []byte("1111"))
	cache.Set(k2, []byte("2222"))
	if _, ok := cache.Get(k1); !ok {
		t.Fatal("k1 missing before eviction")
	}
	cache.Set(k3, []byte("3333"))

	if _, ok := cache.Get(k2); ok {
		t.Fatal("k2 present after eviction, want evicted")
	}
	if got, ok := cache.Get(k1); !ok || string(got) != "1111" {
		t.Fatalf("k1 = %q ok=%v, want 1111 true", got, ok)
	}
	if got, ok := cache.Get(k3); !ok || string(got) != "3333" {
		t.Fatalf("k3 = %q ok=%v, want 3333 true", got, ok)
	}
	if cache.Bytes() > cache.MaxBytes() {
		t.Fatalf("cache bytes=%d max=%d", cache.Bytes(), cache.MaxBytes())
	}
}

func TestLRURejectsOversizedAndZeroLengthValues(t *testing.T) {
	t.Parallel()

	cache := NewLRU(4)
	cache.Set(Key{URI: "big", N: 5}, []byte("12345"))
	cache.Set(Key{URI: "zero"}, nil)
	if cache.Len() != 0 || cache.Bytes() != 0 {
		t.Fatalf("cache len=%d bytes=%d, want empty", cache.Len(), cache.Bytes())
	}
}

type countingStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	counts  map[Key]int
	delay   time.Duration
}

func newCountingStore(objects map[string][]byte) *countingStore {
	out := &countingStore{
		objects: make(map[string][]byte, len(objects)),
		counts:  make(map[Key]int),
	}
	for uri, body := range objects {
		out.objects[uri] = append([]byte(nil), body...)
	}
	return out
}

func (s *countingStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	key := Key{URI: uri, Off: off, N: n}
	s.mu.Lock()
	s.counts[key]++
	body := append([]byte(nil), s.objects[uri]...)
	s.mu.Unlock()
	if off > uint64(len(body)) || n > uint64(len(body))-off {
		return nil, fmt.Errorf("range outside object")
	}
	return append([]byte(nil), body[off:off+n]...), nil
}

func (s *countingStore) count(key Key) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

type flakyStore struct {
	mu    sync.Mutex
	body  []byte
	errs  []error
	count int
}

func (s *flakyStore) ReadAt(ctx context.Context, _ string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.count++
	var err error
	if len(s.errs) > 0 {
		err = s.errs[0]
		s.errs = s.errs[1:]
	}
	body := append([]byte(nil), s.body...)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), body[off:off+n]...), nil
}

func (s *flakyStore) reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
