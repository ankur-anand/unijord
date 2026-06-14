package reader

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

// SegmentReaderCache caches opened immutable segment readers.
//
// It avoids repeatedly fetching/parsing the trailer, file preamble, and block
// index for hot segments. Segment objects are immutable, so entries do not need
// invalidation; eviction is by entry count.
type SegmentReaderCache struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List
	items      map[segmentCacheKey]*list.Element
	inflight   map[segmentCacheKey]*segmentOpenCall
}

type segmentCacheKey struct {
	Ref  pmeta.SegmentRef
	Opts segreader.Options
}

type segmentCacheEntry struct {
	key    segmentCacheKey
	reader *segreader.Reader
}

type segmentOpenCall struct {
	done   chan struct{}
	reader *segreader.Reader
	err    error
}

func NewSegmentReaderCache(maxEntries int) (*SegmentReaderCache, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("%w: segment cache max_entries=%d", ErrInvalidOptions, maxEntries)
	}
	return &SegmentReaderCache{
		maxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[segmentCacheKey]*list.Element),
		inflight:   make(map[segmentCacheKey]*segmentOpenCall),
	}, nil
}

func MustNewSegmentReaderCache(maxEntries int) *SegmentReaderCache {
	cache, err := NewSegmentReaderCache(maxEntries)
	if err != nil {
		panic(err)
	}
	return cache
}

func (c *SegmentReaderCache) Open(ctx context.Context, store segreader.SegmentStore, ref pmeta.SegmentRef, opts segreader.Options) (*segreader.Reader, error) {
	if c == nil {
		return segreader.Open(ctx, store, ref, opts)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := segmentCacheKey{Ref: ref, Opts: opts}
	if reader, ok := c.get(key); ok {
		return reader, nil
	}

	call, leader := c.begin(key)
	if !leader {
		select {
		case <-call.done:
			return call.reader, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	reader, err := segreader.Open(ctx, store, ref, opts)
	if err == nil {
		c.set(key, reader)
	}
	c.finish(key, call, reader, err)
	return reader, err
}

func (c *SegmentReaderCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *SegmentReaderCache) MaxEntries() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxEntries
}

func (c *SegmentReaderCache) get(key segmentCacheKey) (*segreader.Reader, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	return elem.Value.(*segmentCacheEntry).reader, true
}

func (c *SegmentReaderCache) set(key segmentCacheKey, reader *segreader.Reader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		elem.Value.(*segmentCacheEntry).reader = reader
		return
	}
	c.items[key] = c.ll.PushFront(&segmentCacheEntry{key: key, reader: reader})
	for len(c.items) > c.maxEntries {
		back := c.ll.Back()
		if back == nil {
			return
		}
		c.ll.Remove(back)
		entry := back.Value.(*segmentCacheEntry)
		delete(c.items, entry.key)
	}
}

func (c *SegmentReaderCache) begin(key segmentCacheKey) (*segmentOpenCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		return &segmentOpenCall{
			done:   closedChannel(),
			reader: elem.Value.(*segmentCacheEntry).reader,
		}, false
	}
	if call, ok := c.inflight[key]; ok {
		return call, false
	}
	call := &segmentOpenCall{done: make(chan struct{})}
	c.inflight[key] = call
	return call, true
}

func (c *SegmentReaderCache) finish(key segmentCacheKey, call *segmentOpenCall, reader *segreader.Reader, err error) {
	c.mu.Lock()
	if current := c.inflight[key]; current == call {
		delete(c.inflight, key)
	}
	c.mu.Unlock()

	call.reader = reader
	call.err = err
	close(call.done)
}

func closedChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
