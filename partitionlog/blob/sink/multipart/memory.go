package multipart

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

var memoryLimits = Limits{
	MaxPartSize:   uint64(math.MaxInt),
	MaxPartCount:  math.MaxInt,
	MaxObjectSize: math.MaxUint64,
}

type MemoryStore struct {
	mu         sync.RWMutex
	objects    map[string]memoryObject
	generation uint64
}

type memoryObject struct {
	body     []byte
	attrs    ObjectAttrs
	metadata map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]memoryObject)}
}

func (s *MemoryStore) Limits() Limits { return memoryLimits }

func (s *MemoryStore) Begin(ctx context.Context, key string, opts Options) (Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	return &memorySession{
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

type memorySession struct {
	mu sync.Mutex

	store *MemoryStore
	key   string
	opts  Options
	parts map[int]memoryPart

	cleaned    bool
	committing bool
	committed  *ObjectAttrs
}

type memoryPart struct {
	bytes   []byte
	receipt Receipt
}

func (u *memorySession) Limits() Limits { return memoryLimits }

func (u *memorySession) PutPart(ctx context.Context, part Part) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	if err := ValidatePartLimits(part, memoryLimits); err != nil {
		return Receipt{}, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned {
		return Receipt{}, ErrCleaned
	}
	if u.committed != nil {
		return Receipt{}, ErrCommitted
	}
	if u.committing {
		return Receipt{}, ErrCommitInProgress
	}
	if existing, ok := u.parts[part.Number]; ok {
		if existing.receipt.ChecksumSHA256 != part.ChecksumSHA256 || !bytes.Equal(existing.bytes, part.Bytes) {
			return Receipt{}, fmt.Errorf("%w: part %d", ErrPartConflict, part.Number)
		}
		return existing.receipt, nil
	}

	receipt := Receipt{
		Number:         part.Number,
		Token:          fmt.Sprintf("memory-part-%06d-%x", part.Number, part.ChecksumSHA256[:8]),
		SizeBytes:      uint64(len(part.Bytes)),
		ChecksumSHA256: part.ChecksumSHA256,
	}
	u.parts[part.Number] = memoryPart{bytes: append([]byte(nil), part.Bytes...), receipt: receipt}
	return receipt, nil
}

func (u *memorySession) Commit(ctx context.Context, request CommitRequest) (ObjectAttrs, error) {
	if err := ctx.Err(); err != nil {
		return ObjectAttrs{}, err
	}
	if err := ValidateCommitRequest(request, memoryLimits); err != nil {
		return ObjectAttrs{}, err
	}

	u.mu.Lock()
	if u.cleaned {
		u.mu.Unlock()
		return ObjectAttrs{}, ErrCleaned
	}
	if u.committed != nil {
		attrs := *u.committed
		u.mu.Unlock()
		if attrs.SizeBytes != request.SizeBytes || request.ObjectSHA256 != ([sha256.Size]byte{}) && attrs.ObjectSHA256 != request.ObjectSHA256 {
			return ObjectAttrs{}, ErrPreconditionFailed
		}
		return attrs, nil
	}
	if u.committing {
		u.mu.Unlock()
		return ObjectAttrs{}, ErrCommitInProgress
	}

	body := bytes.Buffer{}
	for _, receipt := range request.Receipts {
		part, ok := u.parts[receipt.Number]
		if !ok {
			u.mu.Unlock()
			return ObjectAttrs{}, fmt.Errorf("%w: missing part %d", ErrInvalidStore, receipt.Number)
		}
		if receipt != part.receipt {
			u.mu.Unlock()
			return ObjectAttrs{}, fmt.Errorf("%w: receipt mismatch for part %d", ErrPartConflict, receipt.Number)
		}
		_, _ = body.Write(part.bytes)
	}
	assembled := append([]byte(nil), body.Bytes()...)
	if request.ObjectSHA256 != ([sha256.Size]byte{}) && sha256.Sum256(assembled) != request.ObjectSHA256 {
		u.mu.Unlock()
		return ObjectAttrs{}, fmt.Errorf("%w: final object checksum mismatch", ErrPartConflict)
	}
	u.committing = true
	u.mu.Unlock()

	metadata := CommitMetadata(u.opts, request)
	u.store.mu.Lock()
	if existing, ok := u.store.objects[u.key]; ok {
		u.store.mu.Unlock()
		u.mu.Lock()
		u.committing = false
		u.mu.Unlock()
		if MatchesCommittedObject(uint64(len(existing.body)), existing.metadata, u.opts, request, true) {
			attrs := existing.attrs
			u.mu.Lock()
			u.committed = &attrs
			u.parts = nil
			u.mu.Unlock()
			return attrs, nil
		}
		return ObjectAttrs{}, ErrPreconditionFailed
	}
	u.store.generation++
	attrs := ObjectAttrs{
		Key:          u.key,
		SizeBytes:    uint64(len(assembled)),
		Token:        fmt.Sprintf("memory-generation-%d", u.store.generation),
		SessionID:    u.opts.SessionID,
		ObjectSHA256: request.ObjectSHA256,
	}
	u.store.objects[u.key] = memoryObject{body: assembled, attrs: attrs, metadata: metadata}
	u.store.mu.Unlock()

	u.mu.Lock()
	u.committing = false
	u.committed = &attrs
	u.parts = nil
	u.mu.Unlock()
	return attrs, nil
}

func (u *memorySession) Cleanup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.committed != nil || u.cleaned {
		return nil
	}
	if u.committing {
		return ErrCommitInProgress
	}
	u.cleaned = true
	u.parts = nil
	return nil
}
