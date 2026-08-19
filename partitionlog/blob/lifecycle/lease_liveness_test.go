package lifecycle

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
)

func TestReclaimerDerivesLeaseFromMaxPassDuration(t *testing.T) {
	t.Parallel()

	r := newTestReclaimer(
		t,
		blobmemory.New(),
		&fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)},
		segmentsink.NewLayout("root"),
		newFakeClock(time.Now()),
		Options{MaxPassDuration: 10 * time.Second},
	)
	if got, want := r.leaseDuration(), 30*time.Second; got != want {
		t.Fatalf("lease duration = %v, want %v", got, want)
	}

	for _, duration := range []time.Duration{-1, time.Microsecond, time.Duration(1<<63-1)/3 + 1} {
		_, err := newReclaimer(
			blobmemory.New(),
			&fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)},
			segmentsink.NewLayout("root"),
			Options{
				StreamID:        testStreamID,
				CatalogPrefix:   "root/catalog",
				MaxPassDuration: duration,
			},
			time.Now,
		)
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("newReclaimer(MaxPassDuration=%v) error = %v, want %v", duration, err, ErrInvalidOptions)
		}
	}
}

func TestRunPartitionBoundsDirectCallerAndReleasesLease(t *testing.T) {
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	cat := &fakeCatalog{snapshot: maintenanceSnapshot(100, 200, 2, 0)}
	putSegments(t, backend, layout, 0)
	limiter := newBlockingDeleteLimiter()
	r := newTestReclaimer(t, backend, cat, layout, clock, Options{
		OwnerID:           [16]byte{1},
		MaxPassDuration:   20 * time.Millisecond,
		DeleteRateLimiter: limiter,
	})

	if _, err := r.RunPartition(context.Background(), 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)

	started := time.Now()
	_, err := r.RunPartition(context.Background(), 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunPartition(blocked limiter) error = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("RunPartition(blocked limiter) took %v, want a bounded pass", elapsed)
	}

	other := newTestReclaimer(t, backend, cat, layout, clock, Options{
		OwnerID:         [16]byte{2},
		MaxPassDuration: 20 * time.Millisecond,
	})
	if _, err := other.RunPartition(context.Background(), 7); errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("RunPartition(after timeout) error = %v; timed-out owner did not release its lease", err)
	} else if err != nil {
		t.Fatalf("RunPartition(after timeout) error = %v", err)
	}
}

func TestExecuteDeletesStopsWhenLeaseExpiresDuringRateLimitWait(t *testing.T) {
	backend := &nativeBatchBackend{Store: blobmemory.New()}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	limiter := newBlockingDeleteLimiter()
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, layout, clock, Options{
		OwnerID:           [16]byte{1},
		MaxPassDuration:   time.Second,
		DeleteRateLimiter: limiter,
	})
	state := stateFile{
		Version:      stateVersion,
		StreamID:     testStreamID,
		Partition:    7,
		OwnerID:      hex.EncodeToString(r.opts.OwnerID[:]),
		LeaseUntilMS: clock.Now().Add(r.leaseDuration()).UnixMilli(),
	}
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	done := make(chan error, 1)
	go func() {
		_, err := r.executeDeletes(context.Background(), &state, []deleteCandidate{{key: "a"}}, &budget)
		done <- err
	}()

	select {
	case <-limiter.entered:
	case <-time.After(time.Second):
		t.Fatal("delete limiter was not entered")
	}
	clock.Advance(r.leaseDuration() + time.Millisecond)
	close(limiter.release)

	select {
	case err := <-done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("executeDeletes() error = %v, want %v", err, ErrLeaseLost)
		}
	case <-time.After(time.Second):
		t.Fatal("executeDeletes() did not return after rate limiter release")
	}
	backend.mu.Lock()
	waves := len(backend.waves)
	backend.mu.Unlock()
	if waves != 0 || result.DeletedObjects != 0 {
		t.Fatalf("provider waves=%d deleted=%d, want no deletes after lease expiry", waves, result.DeletedObjects)
	}
}

func TestFallbackDeleteStopsWhenLeaseExpiresDuringRateLimitWait(t *testing.T) {
	backend := blobmemory.New()
	if _, err := backend.Put(context.Background(), "a", []byte("value")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	limiter := newBlockingDeleteLimiter()
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, layout, clock, Options{
		OwnerID:           [16]byte{1},
		MaxPassDuration:   time.Second,
		DeleteRateLimiter: limiter,
	})
	state := activeTestLease(r)
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	done := make(chan error, 1)
	go func() {
		_, err := r.executeDeletes(context.Background(), state, []deleteCandidate{{key: "a"}}, &budget)
		done <- err
	}()

	select {
	case <-limiter.entered:
	case <-time.After(time.Second):
		t.Fatal("delete limiter was not entered")
	}
	clock.Advance(r.leaseDuration() + time.Millisecond)
	close(limiter.release)

	select {
	case err := <-done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("executeDeletes() error = %v, want %v", err, ErrLeaseLost)
		}
	case <-time.After(time.Second):
		t.Fatal("executeDeletes() did not return after rate limiter release")
	}
	if _, err := backend.Get(context.Background(), "a"); err != nil {
		t.Fatalf("Get(a) error = %v, want object retained after lease expiry", err)
	}
	if result.DeletedObjects != 0 {
		t.Fatalf("deleted=%d, want 0", result.DeletedObjects)
	}
}

func TestDeleteSkipsProviderWhenLimiterReturnsAfterCancellation(t *testing.T) {
	backend := &nativeBatchBackend{Store: blobmemory.New()}
	clock := newFakeClock(time.Now().UTC())
	ctx, cancel := context.WithCancel(context.Background())
	r := newTestReclaimer(
		t,
		backend,
		&fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)},
		segmentsink.NewLayout("root"),
		clock,
		Options{DeleteRateLimiter: cancelingDeleteLimiter{cancel: cancel}},
	)
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}

	_, err := r.executeDeletes(ctx, activeTestLease(r), []deleteCandidate{{key: "a"}}, &budget)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("executeDeletes() error = %v, want %v", err, context.Canceled)
	}
	backend.mu.Lock()
	waves := len(backend.waves)
	backend.mu.Unlock()
	if waves != 0 {
		t.Fatalf("provider waves=%d, want 0 after pass cancellation", waves)
	}
}

type blockingDeleteLimiter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingDeleteLimiter() *blockingDeleteLimiter {
	return &blockingDeleteLimiter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func activeTestLease(r *Reclaimer) *stateFile {
	return &stateFile{
		Version:      stateVersion,
		StreamID:     testStreamID,
		Partition:    7,
		OwnerID:      hex.EncodeToString(r.opts.OwnerID[:]),
		LeaseUntilMS: r.now().UTC().Add(r.leaseDuration()).UnixMilli(),
	}
}

func (l *blockingDeleteLimiter) Wait(ctx context.Context, _ int) error {
	l.once.Do(func() { close(l.entered) })
	select {
	case <-l.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type cancelingDeleteLimiter struct {
	cancel context.CancelFunc
}

func (l cancelingDeleteLimiter) Wait(context.Context, int) error {
	l.cancel()
	return nil
}
