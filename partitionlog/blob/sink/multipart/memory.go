package multipart

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
)

type MemoryStore struct {
	mu         sync.RWMutex
	objects    map[string]memoryObject
	generation uint64
}

type memoryObject struct {
	body  []byte
	attrs ObjectAttrs
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]memoryObject)}
}

func (s *MemoryStore) BeginMultipart(ctx context.Context, key string, opts Options) (Upload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	return &memoryUpload{
		store: s,
		key:   key,
		opts:  opts,
		parts: make(map[int]memoryPart),
	}, nil
}

func (s *MemoryStore) Read(ctx context.Context, key string) ([]byte, ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectAttrs{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[key]
	if !ok {
		return nil, ObjectAttrs{}, fmt.Errorf("%w: missing object %q", ErrInvalidStore, key)
	}
	body := append([]byte(nil), obj.body...)
	return body, obj.attrs, nil
}

func (s *MemoryStore) List(ctx context.Context, prefix string) ([]ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ObjectAttrs, 0)
	for key, obj := range s.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, obj.attrs)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

type memoryUpload struct {
	mu sync.Mutex

	store *MemoryStore
	key   string
	opts  Options
	parts map[int]memoryPart

	aborted   bool
	completed bool
}

type memoryPart struct {
	bytes []byte
	token string
}

func (u *memoryUpload) UploadPart(ctx context.Context, part Part) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	if err := ValidatePart(part); err != nil {
		return Receipt{}, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		return Receipt{}, ErrAborted
	}
	if u.completed {
		return Receipt{}, ErrCompleted
	}
	if _, ok := u.parts[part.Number]; ok {
		return Receipt{}, fmt.Errorf("%w: duplicate part %d", ErrInvalidStore, part.Number)
	}

	token := fmt.Sprintf("memory-part-%06d-%d", part.Number, len(part.Bytes))
	u.parts[part.Number] = memoryPart{
		bytes: append([]byte(nil), part.Bytes...),
		token: token,
	}
	return Receipt{
		Number:    part.Number,
		Token:     token,
		SizeBytes: uint64(len(part.Bytes)),
	}, nil
}

func (u *memoryUpload) Complete(ctx context.Context, receipts []Receipt) (ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return ObjectAttrs{}, err
	}
	if err := ValidateReceipts(receipts); err != nil {
		return ObjectAttrs{}, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		return ObjectAttrs{}, ErrAborted
	}
	if u.completed {
		return ObjectAttrs{}, ErrCompleted
	}

	body := bytes.Buffer{}
	for _, receipt := range receipts {
		part, ok := u.parts[receipt.Number]
		if !ok {
			return ObjectAttrs{}, fmt.Errorf("%w: missing part %d", ErrInvalidStore, receipt.Number)
		}
		if receipt.Token != "" && receipt.Token != part.token {
			return ObjectAttrs{}, fmt.Errorf("%w: token mismatch for part %d", ErrInvalidStore, receipt.Number)
		}
		body.Write(part.bytes)
	}

	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if _, ok := u.store.objects[u.key]; ok {
		return ObjectAttrs{}, ErrPreconditionFailed
	}
	u.store.generation++
	attrs := ObjectAttrs{
		Key:       u.key,
		SizeBytes: uint64(body.Len()),
		Token:     fmt.Sprintf("memory-generation-%d", u.store.generation),
	}
	u.store.objects[u.key] = memoryObject{
		body:  append([]byte(nil), body.Bytes()...),
		attrs: attrs,
	}
	u.completed = true
	u.parts = nil
	return attrs, nil
}

func (u *memoryUpload) Abort(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.completed || u.aborted {
		return nil
	}
	u.aborted = true
	u.parts = nil
	return nil
}

func StagingPartKey(stagingPrefix string, number int) string {
	return path.Join(stagingPrefix, fmt.Sprintf("part-%06d", number))
}
