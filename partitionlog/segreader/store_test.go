package segreader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestReadAtExact(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := SegmentStoreFunc(func(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
		if uri != "segment" || off != 2 || n != 3 {
			t.Fatalf("ReadAt args = uri=%q off=%d n=%d", uri, off, n)
		}
		return []byte("abc"), nil
	})

	got, err := readAtExact(ctx, store, "segment", 2, 3)
	if err != nil {
		t.Fatalf("readAtExact() error = %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("readAtExact() = %q, want abc", got)
	}
}

func TestReadAtExactRejectsShortRead(t *testing.T) {
	t.Parallel()

	store := SegmentStoreFunc(func(context.Context, string, uint64, uint64) ([]byte, error) {
		return []byte("ab"), nil
	})
	_, err := readAtExact(context.Background(), store, "segment", 0, 3)
	if !errors.Is(err, ErrStoreRead) {
		t.Fatalf("readAtExact(short) error = %v, want %v", err, ErrStoreRead)
	}
}

func TestReadAtExactWrapsStoreError(t *testing.T) {
	t.Parallel()

	want := errors.New("boom")
	store := SegmentStoreFunc(func(context.Context, string, uint64, uint64) ([]byte, error) {
		return nil, want
	})
	_, err := readAtExact(context.Background(), store, "segment", 1, 2)
	if !errors.Is(err, ErrStoreRead) || !errors.Is(err, want) {
		t.Fatalf("readAtExact(store error) = %v, want %v wrapping %v", err, ErrStoreRead, want)
	}
}

func TestMemoryStoreReadAtAndDefensiveCopy(t *testing.T) {
	t.Parallel()

	body := []byte("hello world")
	store := newMemoryStore(map[string][]byte{"segment": body})
	body[0] = 'x'

	got, err := store.ReadAt(context.Background(), "segment", 6, 5)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if string(got) != "world" {
		t.Fatalf("ReadAt() = %q, want world", got)
	}

	got[0] = 'x'
	again, err := store.ReadAt(context.Background(), "segment", 6, 5)
	if err != nil {
		t.Fatalf("ReadAt(again) error = %v", err)
	}
	if string(again) != "world" {
		t.Fatalf("ReadAt returned mutable backing array: %q", again)
	}
}

func TestMemoryStoreReadAtRejectsBadRange(t *testing.T) {
	t.Parallel()

	store := newMemoryStore(map[string][]byte{"segment": []byte("abc")})
	if _, err := store.ReadAt(context.Background(), "missing", 0, 1); err == nil {
		t.Fatal("ReadAt(missing) error = nil, want error")
	}
	if _, err := store.ReadAt(context.Background(), "segment", 4, 0); err == nil {
		t.Fatal("ReadAt(offset beyond end) error = nil, want error")
	}
	if _, err := store.ReadAt(context.Background(), "segment", 2, 2); err == nil {
		t.Fatal("ReadAt(range beyond end) error = nil, want error")
	}
}

type memoryStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMemoryStore(objects map[string][]byte) *memoryStore {
	s := &memoryStore{objects: make(map[string][]byte, len(objects))}
	for uri, body := range objects {
		s.objects[uri] = append([]byte(nil), body...)
	}
	return s
}

func (s *memoryStore) put(uri string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[uri] = append([]byte(nil), body...)
}

func (s *memoryStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	body, ok := s.objects[uri]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("object not found: %s", uri)
	}
	if off > uint64(len(body)) {
		return nil, fmt.Errorf("offset=%d beyond object size=%d", off, len(body))
	}
	if n > uint64(len(body))-off {
		return nil, fmt.Errorf("range offset=%d length=%d beyond object size=%d", off, n, len(body))
	}
	start := int(off)
	end := start + int(n)
	return append([]byte(nil), body[start:end]...), nil
}
