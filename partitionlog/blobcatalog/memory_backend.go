package blobcatalog

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryBackend is an in-memory Backend for tests and local development. It is
// not durable and should not be used as a production catalog backend.
type MemoryBackend struct {
	mu      sync.Mutex
	objects map[string]memoryObject
	next    uint64
	clock   func() time.Time
}

type memoryObject struct {
	body      []byte
	token     string
	createdAt time.Time
}

var _ Backend = (*MemoryBackend)(nil)

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		objects: make(map[string]memoryObject),
		clock:   func() time.Time { return time.Now().UTC() },
	}
}

func (s *MemoryBackend) Get(ctx context.Context, key string) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if key == "" {
		return Object{}, fmt.Errorf("%w: empty key", ErrCorruptCatalog)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	if !ok {
		return Object{}, ErrObjectNotFound
	}
	return objectFromMemory(key, obj), nil
}

func (s *MemoryBackend) Put(ctx context.Context, key string, body []byte) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if key == "" {
		return Object{}, fmt.Errorf("%w: empty key", ErrCorruptCatalog)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLocked()

	if existing, ok := s.objects[key]; ok {
		if !bytes.Equal(existing.body, body) {
			return Object{}, fmt.Errorf("%w: %s", ErrImmutableConflict, key)
		}
		return objectFromMemory(key, existing), nil
	}
	obj := memoryObject{
		body:      bytes.Clone(body),
		token:     s.nextTokenLocked(),
		createdAt: s.clock(),
	}
	s.objects[key] = obj
	return objectFromMemory(key, obj), nil
}

func (s *MemoryBackend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	if key == "" {
		return Object{}, false, fmt.Errorf("%w: empty key", ErrCorruptCatalog)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLocked()

	current, ok := s.objects[key]
	if !ok {
		if expectedToken != "" {
			return Object{}, false, nil
		}
		next := memoryObject{
			body:      bytes.Clone(body),
			token:     s.nextTokenLocked(),
			createdAt: s.clock(),
		}
		s.objects[key] = next
		return objectFromMemory(key, next), true, nil
	}
	if current.token != expectedToken {
		return objectFromMemory(key, current), false, nil
	}
	next := memoryObject{
		body:      bytes.Clone(body),
		token:     s.nextTokenLocked(),
		createdAt: current.createdAt,
	}
	s.objects[key] = next
	return objectFromMemory(key, next), true, nil
}

func (s *MemoryBackend) List(ctx context.Context, opts ListOptions) (ObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return ObjectPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	infos := make([]ObjectInfo, 0)
	for key, obj := range s.objects {
		if !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		if opts.Cursor != "" && key <= opts.Cursor {
			continue
		}
		infos = append(infos, ObjectInfo{
			Key:       key,
			Token:     obj.token,
			SizeBytes: len(obj.body),
			CreatedAt: obj.createdAt,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Key < infos[j].Key
	})
	limit := opts.normalizedLimit()
	page := ObjectPage{Objects: infos}
	if len(page.Objects) > limit {
		page.Objects = page.Objects[:limit]
		page.HasMore = true
		page.NextCursor = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (s *MemoryBackend) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrCorruptCatalog)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *MemoryBackend) ensureLocked() {
	if s.objects == nil {
		s.objects = make(map[string]memoryObject)
	}
	if s.clock == nil {
		s.clock = func() time.Time { return time.Now().UTC() }
	}
}

func (s *MemoryBackend) nextTokenLocked() string {
	s.next++
	return fmt.Sprintf("%020d", s.next)
}

func objectFromMemory(key string, obj memoryObject) Object {
	return Object{
		Key:       key,
		Body:      bytes.Clone(obj.body),
		Token:     obj.token,
		CreatedAt: obj.createdAt,
	}
}
