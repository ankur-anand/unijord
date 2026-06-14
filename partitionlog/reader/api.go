package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"golang.org/x/sync/singleflight"
)

const (
	defaultPollInterval           = time.Second
	defaultMaxConcurrentRefreshes = 16
)

var ErrWatchClosed = errors.New("partitionlog/reader: watch closed")

// Partition returns a passive reader view for one partition.
func (r *Reader) Partition(partition uint32) *PartitionReader {
	return &PartitionReader{reader: r, partition: partition}
}

// Watch starts explicit background catalog refresh for the configured
// partitions. Close the returned Watch to stop polling.
func (r *Reader) Watch(ctx context.Context, opts WatchOptions) (*Watch, error) {
	if r == nil || r.refresh == nil {
		return nil, fmt.Errorf("%w: nil reader", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(ctx)
	w := &Watch{
		reader:     r,
		ctx:        wctx,
		cancel:     cancel,
		partitions: make(map[uint32]struct{}, len(opts.Partitions)),
	}
	for _, partition := range opts.Partitions {
		if err := w.addPartitionLocked(partition); err != nil {
			cancel()
			return nil, err
		}
	}
	go func() {
		<-wctx.Done()
		_ = w.Close()
	}()
	return w, nil
}

// PartitionReader is a per-partition read view. It is cheap to create and
// shares the parent Reader runtime, caches, and refresh coordinator.
type PartitionReader struct {
	reader    *Reader
	partition uint32
}

// Head returns the cached head if available, otherwise it loads the partition
// head once.
func (p *PartitionReader) Head(ctx context.Context) (pmeta.PartitionHead, error) {
	return p.reader.refresh.head(ctx, p.partition)
}

// Read returns a bounded non-blocking batch from committed data. It may refresh
// the catalog according to req.Freshness, but it never starts background
// polling and it never waits for future data.
func (p *PartitionReader) Read(ctx context.Context, req ReadRequest) (ReadResult, error) {
	head, err := p.reader.refresh.headForRead(ctx, p.partition, req.StartLSN, req.Freshness)
	if err != nil {
		return ReadResult{}, err
	}
	return p.reader.consumeWithHead(ctx, head, ConsumeRequest{
		Partition: p.partition,
		StartLSN:  req.StartLSN,
		Limit:     req.Limit,
	})
}

// Cursor returns a passive replay cursor over this partition.
func (p *PartitionReader) Cursor(opts CursorOptions) (*Cursor, error) {
	if opts.Limit < 0 {
		return nil, fmt.Errorf("%w: limit=%d", ErrInvalidRequest, opts.Limit)
	}
	return &Cursor{
		partition: p,
		nextLSN:   opts.StartLSN,
		limit:     opts.Limit,
	}, nil
}

// Cursor is a passive stateful replay cursor. It is not safe for concurrent
// use.
type Cursor struct {
	partition *PartitionReader
	nextLSN   uint64
	limit     int
	closed    bool
}

// Next reads from the current cursor position and advances only when records
// are returned.
func (c *Cursor) Next(ctx context.Context) (ReadResult, error) {
	if c.closed {
		return ReadResult{}, fmt.Errorf("%w: cursor closed", ErrInvalidRequest)
	}
	result, err := c.partition.Read(ctx, ReadRequest{
		StartLSN:  c.nextLSN,
		Limit:     c.limit,
		Freshness: FreshnessOnTail,
	})
	if err != nil {
		return ReadResult{}, err
	}
	if len(result.Records) > 0 {
		c.nextLSN = result.NextLSN
	}
	return result, nil
}

// Seek moves the cursor to lsn.
func (c *Cursor) Seek(lsn uint64) {
	c.nextLSN = lsn
}

// Position returns the next LSN the cursor will try to read.
func (c *Cursor) Position() uint64 {
	return c.nextLSN
}

// Fork returns another cursor with the same position and limit.
func (c *Cursor) Fork() *Cursor {
	return &Cursor{
		partition: c.partition,
		nextLSN:   c.nextLSN,
		limit:     c.limit,
	}
}

// Close marks the cursor closed. It does not close the shared Reader runtime.
func (c *Cursor) Close() error {
	c.closed = true
	return nil
}

// Watch owns explicit background refresh for a set of partitions.
type Watch struct {
	reader *Reader
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	closed     bool
	partitions map[uint32]struct{}
}

// AddPartition adds a partition to this Watch's background refresh set.
func (w *Watch) AddPartition(partition uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWatchClosed
	}
	return w.addPartitionLocked(partition)
}

// RemovePartition removes a partition from this Watch's background refresh set.
func (w *Watch) RemovePartition(partition uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.partitions[partition]; !ok {
		return
	}
	delete(w.partitions, partition)
	w.reader.refresh.unwatchPartition(partition)
}

// Tail returns a blocking tail cursor for a partition already registered with
// this Watch.
func (w *Watch) Tail(opts TailOptions) (*Tailer, error) {
	if opts.Limit < 0 {
		return nil, fmt.Errorf("%w: limit=%d", ErrInvalidRequest, opts.Limit)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrWatchClosed
	}
	if _, ok := w.partitions[opts.Partition]; !ok {
		return nil, fmt.Errorf("%w: partition %d is not watched", ErrInvalidRequest, opts.Partition)
	}
	return &Tailer{
		watch:     w,
		partition: opts.Partition,
		nextLSN:   opts.StartLSN,
		limit:     opts.Limit,
	}, nil
}

// Close stops this Watch's background polling. It does not close the shared
// Reader runtime.
func (w *Watch) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	partitions := make([]uint32, 0, len(w.partitions))
	for partition := range w.partitions {
		partitions = append(partitions, partition)
	}
	w.partitions = nil
	w.mu.Unlock()
	w.cancel()
	for _, partition := range partitions {
		w.reader.refresh.unwatchPartition(partition)
	}
	return nil
}

func (w *Watch) addPartitionLocked(partition uint32) error {
	if _, ok := w.partitions[partition]; ok {
		return nil
	}
	w.partitions[partition] = struct{}{}
	w.reader.refresh.watchPartition(partition)
	return nil
}

// Tailer is a blocking cursor attached to an explicit Watch. It is not safe for
// concurrent use.
type Tailer struct {
	watch     *Watch
	partition uint32
	nextLSN   uint64
	limit     int
	closed    bool
}

// Next returns available records immediately. If the tailer is at the
// committed tail, it waits until the Watch observes the partition head advance
// or ctx is cancelled.
func (t *Tailer) Next(ctx context.Context) (ReadResult, error) {
	if t.closed {
		return ReadResult{}, fmt.Errorf("%w: tailer closed", ErrInvalidRequest)
	}
	partition := t.watch.reader.Partition(t.partition)
	for {
		result, err := partition.Read(ctx, ReadRequest{
			StartLSN:  t.nextLSN,
			Limit:     t.limit,
			Freshness: FreshnessOnTail,
		})
		if err != nil {
			return ReadResult{}, err
		}
		if len(result.Records) > 0 {
			t.nextLSN = result.NextLSN
			return result, nil
		}

		head, generation, ok := t.watch.reader.refresh.snapshot(t.partition)
		if ok && head.NextLSN > t.nextLSN {
			continue
		}
		if err := t.watch.waitForAdvance(ctx, t.partition, generation); err != nil {
			return ReadResult{}, err
		}
	}
}

// Position returns the next LSN the tailer will try to read.
func (t *Tailer) Position() uint64 {
	return t.nextLSN
}

// Close marks the tailer closed. It does not close the Watch.
func (t *Tailer) Close() error {
	t.closed = true
	return nil
}

func (w *Watch) waitForAdvance(ctx context.Context, partition uint32, generation uint64) error {
	ch, wait := w.reader.refresh.waitChannel(partition, generation)
	if !wait {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return ErrWatchClosed
	case <-ch:
		return nil
	}
}

type refreshCoordinator struct {
	catalog catalog.Reader
	policy  RefreshPolicy
	group   singleflight.Group

	mu         sync.Mutex
	partitions map[uint32]*partitionState
	watched    map[uint32]int
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

type partitionState struct {
	head       pmeta.PartitionHead
	hasHead    bool
	generation uint64
	changed    chan struct{}
}

type headSnapshot struct {
	head       pmeta.PartitionHead
	generation uint64
}

func newRefreshCoordinator(cat catalog.Reader, policy RefreshPolicy) *refreshCoordinator {
	return &refreshCoordinator{
		catalog:    cat,
		policy:     normalizeRefreshPolicy(policy, RefreshPolicy{}),
		partitions: make(map[uint32]*partitionState),
		watched:    make(map[uint32]int),
	}
}

func normalizeRefreshPolicy(policy RefreshPolicy, fallback RefreshPolicy) RefreshPolicy {
	if policy.PollInterval <= 0 {
		policy.PollInterval = fallback.PollInterval
	}
	if policy.PollInterval <= 0 {
		policy.PollInterval = defaultPollInterval
	}
	if policy.MaxConcurrentRefreshes <= 0 {
		policy.MaxConcurrentRefreshes = fallback.MaxConcurrentRefreshes
	}
	if policy.MaxConcurrentRefreshes <= 0 {
		policy.MaxConcurrentRefreshes = defaultMaxConcurrentRefreshes
	}
	if policy.RefreshTimeout == 0 {
		policy.RefreshTimeout = fallback.RefreshTimeout
	}
	return policy
}

func (c *refreshCoordinator) head(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	if head, _, ok := c.snapshot(partition); ok {
		return head, nil
	}
	return c.refresh(ctx, partition)
}

func (c *refreshCoordinator) headForRead(ctx context.Context, partition uint32, startLSN uint64, freshness Freshness) (pmeta.PartitionHead, error) {
	switch freshness {
	case FreshnessDefault:
		freshness = FreshnessOnTail
	case FreshnessCached, FreshnessOnTail, FreshnessLatest:
	default:
		return pmeta.PartitionHead{}, fmt.Errorf("%w: unknown freshness=%d", ErrInvalidRequest, freshness)
	}

	head, _, ok := c.snapshot(partition)
	switch freshness {
	case FreshnessLatest:
		return c.refresh(ctx, partition)
	case FreshnessCached:
		if ok {
			return head, nil
		}
		return c.refresh(ctx, partition)
	case FreshnessOnTail:
		if !ok || startLSN >= head.NextLSN {
			return c.refresh(ctx, partition)
		}
		return head, nil
	default:
		panic("unreachable freshness")
	}
}

func (c *refreshCoordinator) refresh(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	v, err, _ := c.group.Do(fmt.Sprintf("%d", partition), func() (any, error) {
		if err := ctx.Err(); err != nil {
			return headSnapshot{}, err
		}
		head, err := c.catalog.LoadPartition(ctx, partition)
		if err != nil {
			return headSnapshot{}, err
		}
		generation := c.updateHead(partition, head)
		return headSnapshot{head: head, generation: generation}, nil
	})
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	snapshot := v.(headSnapshot)
	return snapshot.head, nil
}

func (c *refreshCoordinator) watchPartition(partition uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watched[partition]++
	if c.loopCancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		c.loopCancel = cancel
		c.loopDone = done
		go c.loop(ctx, done)
	}
}

func (c *refreshCoordinator) unwatchPartition(partition uint32) {
	var cancel context.CancelFunc
	var done chan struct{}
	c.mu.Lock()
	if count := c.watched[partition]; count <= 1 {
		delete(c.watched, partition)
	} else {
		c.watched[partition] = count - 1
	}
	if len(c.watched) == 0 && c.loopCancel != nil {
		cancel = c.loopCancel
		done = c.loopDone
		c.loopCancel = nil
		c.loopDone = nil
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (c *refreshCoordinator) loop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(c.policy.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollWatched(ctx)
		}
	}
}

func (c *refreshCoordinator) pollWatched(ctx context.Context) {
	partitions := c.snapshotWatched()
	if len(partitions) == 0 {
		return
	}
	concurrency := c.policy.MaxConcurrentRefreshes
	if concurrency <= 0 || concurrency > len(partitions) {
		concurrency = len(partitions)
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, partition := range partitions {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(partition uint32) {
			defer wg.Done()
			defer func() { <-sem }()
			refreshCtx := ctx
			var cancel context.CancelFunc
			if c.policy.RefreshTimeout > 0 {
				refreshCtx, cancel = context.WithTimeout(ctx, c.policy.RefreshTimeout)
				defer cancel()
			}
			_, _ = c.refresh(refreshCtx, partition)
		}(partition)
	}
	wg.Wait()
}

func (c *refreshCoordinator) snapshotWatched() []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uint32, 0, len(c.watched))
	for partition := range c.watched {
		out = append(out, partition)
	}
	return out
}

func (c *refreshCoordinator) snapshot(partition uint32) (pmeta.PartitionHead, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.partitions[partition]
	if !ok || !state.hasHead {
		return pmeta.PartitionHead{}, 0, false
	}
	return state.head, state.generation, true
}

func (c *refreshCoordinator) waitChannel(partition uint32, generation uint64) (<-chan struct{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.stateLocked(partition)
	if state.generation != generation {
		return nil, false
	}
	return state.changed, true
}

func (c *refreshCoordinator) updateHead(partition uint32, head pmeta.PartitionHead) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.stateLocked(partition)
	if state.hasHead && state.head == head {
		return state.generation
	}
	state.head = head
	state.hasHead = true
	state.generation++
	close(state.changed)
	state.changed = make(chan struct{})
	return state.generation
}

func (c *refreshCoordinator) stateLocked(partition uint32) *partitionState {
	state, ok := c.partitions[partition]
	if ok {
		return state
	}
	state = &partitionState{changed: make(chan struct{})}
	c.partitions[partition] = state
	return state
}
