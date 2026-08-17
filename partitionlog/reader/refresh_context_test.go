package reader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func TestRefreshCallerCancellationDoesNotPoisonJoinedCaller(t *testing.T) {
	const partition = 7
	cat := &blockingRefreshCatalog{
		head: pmeta.PartitionHead{
			Partition:   partition,
			WriterEpoch: 1,
			NextLSN:     42,
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, DefaultMaxCachedPartitionHeads, nil)

	winnerCtx, cancelWinner := context.WithCancel(context.Background())
	winnerDone := make(chan refreshCallResult, 1)
	go func() {
		head, err := coordinator.refresh(winnerCtx, partition)
		winnerDone <- refreshCallResult{head: head, err: err}
	}()

	select {
	case <-cat.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for winning catalog refresh")
	}

	joinedCtx, cancelJoined := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelJoined()
	joinedStarted := make(chan struct{})
	joinedDone := make(chan refreshCallResult, 1)
	go func() {
		close(joinedStarted)
		head, err := coordinator.refresh(joinedCtx, partition)
		joinedDone <- refreshCallResult{head: head, err: err}
	}()
	<-joinedStarted

	// Give the second call time to join the already-blocked singleflight. The
	// catalog call-count assertion below also rejects a missed join.
	time.Sleep(25 * time.Millisecond)
	cancelWinner()

	select {
	case result := <-winnerDone:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("winning refresh error = %v, want %v", result.err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled winning refresh")
	}

	close(cat.release)
	select {
	case result := <-joinedDone:
		if result.err != nil {
			t.Fatalf("joined refresh error = %v; winner cancellation poisoned an unrelated caller", result.err)
		}
		if result.head.NextLSN != cat.head.NextLSN {
			t.Fatalf("joined refresh head = %+v, want next_lsn=%d", result.head, cat.head.NextLSN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for joined refresh")
	}
	if calls := cat.calls.Load(); calls != 1 {
		t.Fatalf("LoadPartition() calls = %d, want one shared refresh", calls)
	}
}

func TestRefreshJoinedCallerCanCancelWithoutStoppingSharedWork(t *testing.T) {
	const partition = 7
	cat := &blockingRefreshCatalog{
		head: pmeta.PartitionHead{
			Partition:   partition,
			WriterEpoch: 1,
			NextLSN:     42,
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, DefaultMaxCachedPartitionHeads, nil)

	winnerDone := make(chan refreshCallResult, 1)
	go func() {
		head, err := coordinator.refresh(context.Background(), partition)
		winnerDone <- refreshCallResult{head: head, err: err}
	}()
	select {
	case <-cat.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for winning catalog refresh")
	}

	joinedCtx, cancelJoined := context.WithCancel(context.Background())
	joinedStarted := make(chan struct{})
	joinedDone := make(chan refreshCallResult, 1)
	go func() {
		close(joinedStarted)
		head, err := coordinator.refresh(joinedCtx, partition)
		joinedDone <- refreshCallResult{head: head, err: err}
	}()
	<-joinedStarted
	time.Sleep(25 * time.Millisecond)
	cancelJoined()

	select {
	case result := <-joinedDone:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("joined refresh error = %v, want %v", result.err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled joined refresh")
	}

	close(cat.release)
	select {
	case result := <-winnerDone:
		if result.err != nil {
			t.Fatalf("winning refresh error = %v", result.err)
		}
		if result.head.NextLSN != cat.head.NextLSN {
			t.Fatalf("winning refresh head = %+v, want next_lsn=%d", result.head, cat.head.NextLSN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for winning refresh")
	}
	if calls := cat.calls.Load(); calls != 1 {
		t.Fatalf("LoadPartition() calls = %d, want one shared refresh", calls)
	}
}

func TestRefreshPolicyHasBoundedDefaultTimeout(t *testing.T) {
	policy := normalizeRefreshPolicy(RefreshPolicy{}, RefreshPolicy{})
	if policy.RefreshTimeout != defaultRefreshTimeout {
		t.Fatalf("RefreshTimeout = %v, want %v", policy.RefreshTimeout, defaultRefreshTimeout)
	}
}

func TestRefreshSharedWorkHonorsRefreshTimeout(t *testing.T) {
	cat := &blockingRefreshCatalog{
		head:    pmeta.PartitionHead{Partition: 7, WriterEpoch: 1},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{RefreshTimeout: 20 * time.Millisecond}, DefaultMaxCachedPartitionHeads, nil)

	_, err := coordinator.refresh(context.Background(), 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if calls := cat.calls.Load(); calls != 1 {
		t.Fatalf("LoadPartition() calls = %d, want 1", calls)
	}
}

type refreshCallResult struct {
	head pmeta.PartitionHead
	err  error
}

type blockingRefreshCatalog struct {
	head pmeta.PartitionHead

	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (c *blockingRefreshCatalog) LoadPartition(ctx context.Context, _ uint32) (pmeta.PartitionHead, error) {
	c.calls.Add(1)
	c.once.Do(func() { close(c.entered) })
	select {
	case <-c.release:
		return c.head, nil
	case <-ctx.Done():
		return pmeta.PartitionHead{}, ctx.Err()
	}
}

func (c *blockingRefreshCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *blockingRefreshCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return pmeta.SegmentPage{}, nil
}
