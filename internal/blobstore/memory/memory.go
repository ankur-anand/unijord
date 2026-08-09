// Package memory provides an in-memory blobstore for tests and local tools.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
)

type Store struct {
	mu      sync.Mutex
	objects map[string]object
	next    uint64
}

type object struct {
	body      []byte
	token     string
	createdAt time.Time
}

var _ blobstore.Store = (*Store)(nil)

func New() *Store {
	return &Store{objects: make(map[string]object)}
}

func (s *Store) Get(ctx context.Context, key string) (blobstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, err
	}
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.objects[key]
	if !ok {
		return blobstore.Object{}, blobstore.ErrObjectNotFound
	}
	return cloneObject(key, stored), nil
}

func (s *Store) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, err
	}
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLocked()
	if current, ok := s.objects[key]; ok {
		if !bytes.Equal(current.body, body) {
			return blobstore.Object{}, fmt.Errorf("%w: %s", blobstore.ErrImmutableConflict, key)
		}
		return cloneObject(key, current), nil
	}
	stored := s.newObjectLocked(body)
	s.objects[key] = stored
	return cloneObject(key, stored), nil
}

func (s *Store) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.Object{}, false, err
	}
	if key == "" {
		return blobstore.Object{}, false, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLocked()
	current, exists := s.objects[key]
	if !exists {
		if expectedToken != "" {
			return blobstore.Object{}, false, nil
		}
		stored := s.newObjectLocked(body)
		s.objects[key] = stored
		return cloneObject(key, stored), true, nil
	}
	if current.token != expectedToken {
		return cloneObject(key, current), false, nil
	}
	stored := s.newObjectLocked(body)
	stored.createdAt = current.createdAt
	s.objects[key] = stored
	return cloneObject(key, stored), true, nil
}

func (s *Store) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	objects := make([]blobstore.ObjectInfo, 0)
	for key, stored := range s.objects {
		if !strings.HasPrefix(key, opts.Prefix) || opts.Cursor != "" && key <= opts.Cursor {
			continue
		}
		objects = append(objects, blobstore.ObjectInfo{
			Key:       key,
			Token:     stored.token,
			SizeBytes: len(stored.body),
			CreatedAt: stored.createdAt,
		})
	}
	sort.Slice(objects, func(i int, j int) bool { return objects[i].Key < objects[j].Key })
	page := blobstore.ObjectPage{Objects: objects}
	if limit := opts.NormalizedLimit(); len(page.Objects) > limit {
		page.Objects = page.Objects[:limit]
		page.HasMore = true
		page.NextCursor = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *Store) ensureLocked() {
	if s.objects == nil {
		s.objects = make(map[string]object)
	}
}

func (s *Store) newObjectLocked(body []byte) object {
	s.next++
	return object{
		body:      bytes.Clone(body),
		token:     fmt.Sprintf("%020d", s.next),
		createdAt: time.Now().UTC(),
	}
}

func cloneObject(key string, stored object) blobstore.Object {
	return blobstore.Object{
		Key:       key,
		Body:      bytes.Clone(stored.body),
		Token:     stored.token,
		CreatedAt: stored.createdAt,
	}
}
