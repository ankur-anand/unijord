package bench

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
)

// Sample collects durations and is safe for concurrent use.
type Sample struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *Sample) Add(d time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, d)
	s.mu.Unlock()
}

// Time runs fn and records its duration.
func (s *Sample) Time(fn func()) {
	t0 := time.Now()
	fn()
	s.Add(time.Since(t0))
}

func (s *Sample) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.d)
}

// Drain returns and clears the collected samples.
func (s *Sample) Drain() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.d
	s.d = nil
	return out
}

func (s *Sample) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Summarize(s.d)
}

// Stats is the summary of one latency sample.
type Stats struct {
	N                  int
	P50, P90, P99, Max time.Duration
	Mean               time.Duration
}

func Summarize(d []time.Duration) Stats {
	if len(d) == 0 {
		return Stats{}
	}
	d = slices.Clone(d)
	slices.Sort(d)
	pct := func(p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }
	var sum time.Duration
	for _, v := range d {
		sum += v
	}
	return Stats{N: len(d), P50: pct(.5), P90: pct(.9), P99: pct(.99), Max: d[len(d)-1], Mean: sum / time.Duration(len(d))}
}

// CountingStore wraps a backend and counts GET requests and bytes so a
// catalog operation's exact request profile can be asserted.
type CountingStore struct {
	blobstore.Store
	gets  atomic.Int64
	bytes atomic.Int64
}

func NewCountingStore(inner blobstore.Store) *CountingStore {
	return &CountingStore{Store: inner}
}

func (c *CountingStore) Get(ctx context.Context, key string) (blobstore.Object, error) {
	obj, err := c.Store.Get(ctx, key)
	c.gets.Add(1)
	c.bytes.Add(int64(len(obj.Body)))
	return obj, err
}

func (c *CountingStore) Reset()                 { c.gets.Store(0); c.bytes.Store(0) }
func (c *CountingStore) Gets() int64            { return c.gets.Load() }
func (c *CountingStore) Bytes() int64           { return c.bytes.Load() }
func (c *CountingStore) Inner() blobstore.Store { return c.Store }
