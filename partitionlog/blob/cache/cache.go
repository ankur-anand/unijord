package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

type Key struct {
	URI string
	Off uint64
	N   uint64
}

type Cache interface {
	Get(Key) ([]byte, bool)
	Set(Key, []byte)
}

// Store decorates a segreader.SegmentStore with immutable range caching.
type Store struct {
	inner segreader.SegmentStore
	cache Cache

	mu       sync.Mutex
	inflight map[Key]*inflightRead
}

var _ segreader.SegmentStore = (*Store)(nil)

type inflightRead struct {
	done chan struct{}
	body []byte
	err  error
}

func NewStore(inner segreader.SegmentStore, cache Cache) (*Store, error) {
	if inner == nil {
		return nil, fmt.Errorf("blob/cache: nil inner store")
	}
	if cache == nil {
		return nil, fmt.Errorf("blob/cache: nil cache")
	}
	return &Store{
		inner:    inner,
		cache:    cache,
		inflight: make(map[Key]*inflightRead),
	}, nil
}

func MustNewStore(inner segreader.SegmentStore, cache Cache) *Store {
	store, err := NewStore(inner, cache)
	if err != nil {
		panic(err)
	}
	return store
}

func (s *Store) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}
	key := Key{URI: uri, Off: off, N: n}
	if body, ok := s.cache.Get(key); ok {
		return body, nil
	}

	call, leader := s.begin(key)
	if !leader {
		select {
		case <-call.done:
			if call.err != nil {
				return nil, call.err
			}
			return append([]byte(nil), call.body...), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	body, err := s.inner.ReadAt(ctx, uri, off, n)
	if err == nil {
		s.cache.Set(key, body)
	}
	s.finish(key, call, body, err)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), body...), nil
}

func (s *Store) begin(key Key) (*inflightRead, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call, ok := s.inflight[key]; ok {
		return call, false
	}
	call := &inflightRead{done: make(chan struct{})}
	s.inflight[key] = call
	return call, true
}

func (s *Store) finish(key Key, call *inflightRead, body []byte, err error) {
	s.mu.Lock()
	if current := s.inflight[key]; current == call {
		delete(s.inflight, key)
	}
	s.mu.Unlock()

	call.body = append([]byte(nil), body...)
	call.err = err
	close(call.done)
}

type LRU struct {
	mu       sync.Mutex
	maxBytes uint64
	bytes    uint64
	ll       *list.List
	items    map[Key]*list.Element
}

type entry struct {
	key   Key
	value []byte
	size  uint64
}

func NewLRU(maxBytes uint64) *LRU {
	return &LRU{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[Key]*list.Element),
	}
}

func (c *LRU) Get(key Key) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	ent := elem.Value.(*entry)
	return append([]byte(nil), ent.value...), true
}

func (c *LRU) Set(key Key, value []byte) {
	if c == nil {
		return
	}
	size := uint64(len(value))
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[key]; ok {
		c.removeElement(existing)
	}
	if c.maxBytes == 0 || size == 0 || size > c.maxBytes {
		return
	}
	ent := &entry{
		key:   key,
		value: append([]byte(nil), value...),
		size:  size,
	}
	c.items[key] = c.ll.PushFront(ent)
	c.bytes += size
	c.evict()
}

func (c *LRU) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *LRU) Bytes() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

func (c *LRU) MaxBytes() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxBytes
}

func (c *LRU) evict() {
	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			return
		}
		c.removeElement(back)
	}
}

func (c *LRU) removeElement(elem *list.Element) {
	c.ll.Remove(elem)
	ent := elem.Value.(*entry)
	delete(c.items, ent.key)
	c.bytes -= ent.size
}
