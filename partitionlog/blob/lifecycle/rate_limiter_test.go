package lifecycle

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
)

func TestExecuteDeletesChargesNativeBatchesByObjectCount(t *testing.T) {
	t.Parallel()

	backend := &nativeBatchBackend{Store: blobmemory.New()}
	limiter := &recordingDeleteLimiter{}
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, segmentsink.NewLayout("root"), newFakeClock(time.Now()), Options{
		DeleteBatchSize:   2,
		DeleteRateLimiter: limiter,
	})
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	candidates := []deleteCandidate{{key: "a"}, {key: "b"}, {key: "c"}, {key: "d"}, {key: "e"}}
	if _, err := r.executeDeletes(context.Background(), activeTestLease(r), candidates, &budget); err != nil {
		t.Fatalf("executeDeletes() error = %v", err)
	}
	if calls := limiter.Calls(); !equalInts(calls, []int{2, 2, 1}) {
		t.Fatalf("limiter calls = %v, want [2 2 1]", calls)
	}
}

func TestExecuteDeletesChargesFallbackDeletesIndividually(t *testing.T) {
	t.Parallel()

	limiter := &recordingDeleteLimiter{}
	r := newTestReclaimer(t, blobmemory.New(), &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, segmentsink.NewLayout("root"), newFakeClock(time.Now()), Options{
		DeleteBatchSize:   5,
		DeleteConcurrency: 3,
		DeleteRateLimiter: limiter,
	})
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	candidates := []deleteCandidate{{key: "a"}, {key: "b"}, {key: "c"}, {key: "d"}, {key: "e"}}
	if _, err := r.executeDeletes(context.Background(), activeTestLease(r), candidates, &budget); err != nil {
		t.Fatalf("executeDeletes() error = %v", err)
	}
	calls := limiter.Calls()
	if len(calls) != len(candidates) {
		t.Fatalf("limiter calls = %v, want %d calls", calls, len(candidates))
	}
	for _, count := range calls {
		if count != 1 {
			t.Fatalf("limiter calls = %v, want only single-object charges", calls)
		}
	}
}

func TestDeleteRateLimiterFailurePreservesCheckpointAndSkipsProvider(t *testing.T) {
	t.Parallel()

	backend := &nativeBatchBackend{Store: blobmemory.New()}
	limiter := &recordingDeleteLimiter{err: context.DeadlineExceeded}
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, segmentsink.NewLayout("root"), newFakeClock(time.Now()), Options{
		DeleteBatchSize:   2,
		DeleteRateLimiter: limiter,
	})
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	candidates := []deleteCandidate{
		{key: "a", beforeKey: "start"},
		{key: "b", beforeKey: "a"},
	}
	checkpoint, err := r.executeDeletes(context.Background(), activeTestLease(r), candidates, &budget)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("executeDeletes() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if checkpoint != "start" || result.DeletedObjects != 0 {
		t.Fatalf("checkpoint=%q deleted=%d, want start/0", checkpoint, result.DeletedObjects)
	}
	backend.mu.Lock()
	waves := len(backend.waves)
	backend.mu.Unlock()
	if waves != 0 {
		t.Fatalf("provider delete waves = %d, want 0", waves)
	}
}

func TestTokenBucketDeleteLimiterValidationAndCancellation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		rate  float64
		burst int
	}{
		{0, 1},
		{-1, 1},
		{math.NaN(), 1},
		{math.Inf(1), 1},
		{1, 0},
		{1, -1},
	} {
		if _, err := NewTokenBucketDeleteLimiter(tc.rate, tc.burst); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("NewTokenBucketDeleteLimiter(%v, %d) error = %v, want %v", tc.rate, tc.burst, err, ErrInvalidOptions)
		}
	}

	limiter, err := NewTokenBucketDeleteLimiter(1, 1)
	if err != nil {
		t.Fatalf("NewTokenBucketDeleteLimiter() error = %v", err)
	}
	if err := limiter.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait(first token) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait(canceled) error = %v, want %v", err, context.Canceled)
	}
	if err := limiter.Wait(context.Background(), -1); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Wait(negative) error = %v, want %v", err, ErrInvalidOptions)
	}
}

type recordingDeleteLimiter struct {
	mu    sync.Mutex
	calls []int
	err   error
}

func (l *recordingDeleteLimiter) Wait(_ context.Context, objects int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, objects)
	return l.err
}

func (l *recordingDeleteLimiter) Calls() []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int(nil), l.calls...)
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
