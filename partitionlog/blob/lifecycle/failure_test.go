package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

var errProviderThrottled = errors.New("provider throttled")
var errDeleteFailed = errors.New("delete failed")
var errCatalogLoadFailed = errors.New("catalog load failed")

func TestDeleteFailureJoinsCheckpointLeaseLoss(t *testing.T) {
	memory := blobmemory.New()
	backend := &deleteAndCheckpointFailureBackend{Store: memory}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(100, 200, 2, 0)}
	putSegments(t, memory, layout, 0)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})

	state := activeTestLease(r)
	state.SafeFloorLSN = 100
	token := "state-token"
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	err := r.reclaimSegments(context.Background(), state, &token, &budget)
	if !errors.Is(err, errDeleteFailed) {
		t.Fatalf("reclaimSegments() error = %v, want %v", err, errDeleteFailed)
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("reclaimSegments() error = %v, checkpoint %v was discarded", err, ErrLeaseLost)
	}
}

func TestPrimaryFailureJoinsLeaseReleaseLoss(t *testing.T) {
	backend := &releaseConflictBackend{Store: blobmemory.New()}
	r := newTestReclaimer(
		t,
		backend,
		&loadFailureCatalog{},
		segmentsink.NewLayout("root"),
		newFakeClock(time.Now().UTC()),
		Options{},
	)

	_, err := r.RunPartition(context.Background(), 7)
	if !errors.Is(err, errCatalogLoadFailed) {
		t.Fatalf("RunPartition() error = %v, want %v", err, errCatalogLoadFailed)
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("RunPartition() error = %v, release %v was discarded", err, ErrLeaseLost)
	}
}

func TestReclaimerRetriesAfterListThrottleWithoutSkippingObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memory := blobmemory.New()
	backend := &transientFailureBackend{Store: memory}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(300, 400, 2, 0)}
	keys := putSegments(t, memory, layout, 0, 100, 200, 300)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{ListPageSize: 4})

	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	backend.failNextList()
	if _, err := r.RunPartition(ctx, 7); !errors.Is(err, errProviderThrottled) {
		t.Fatalf("RunPartition(throttled LIST) error = %v, want %v", err, errProviderThrottled)
	}
	assertExists(t, memory, keys...)

	state := loadLifecycleState(t, memory, 7)
	if state.SegmentAfterKey != "" || state.SegmentReclaimedThroughLSN != 0 {
		t.Fatalf("state advanced after LIST failure: %+v", state)
	}

	result, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(retry) error = %v", err)
	}
	if result.ReclaimedThroughLSN != 300 {
		t.Fatalf("retry result = %+v, want reclaimed through 300", result)
	}
	assertMissing(t, memory, keys[0], keys[1], keys[2])
	assertExists(t, memory, keys[3])
}

func TestReclaimerRetriesAfterStateCASThrottle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memory := blobmemory.New()
	backend := &transientFailureBackend{Store: memory}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(200, 300, 2, 0)}
	keys := putSegments(t, memory, layout, 0, 100, 200)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{ListPageSize: 3})

	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	backend.failCASCall(2)
	if _, err := r.RunPartition(ctx, 7); !errors.Is(err, errProviderThrottled) {
		t.Fatalf("RunPartition(throttled CAS) error = %v, want %v", err, errProviderThrottled)
	}
	assertExists(t, memory, keys...)

	result, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(retry) error = %v", err)
	}
	if result.ReclaimedThroughLSN != 200 {
		t.Fatalf("retry result = %+v, want reclaimed through 200", result)
	}
	assertMissing(t, memory, keys[0], keys[1])
	assertExists(t, memory, keys[2])
}

func TestReclaimerResumesAfterDeleteContextTimeout(t *testing.T) {
	t.Parallel()

	memory := blobmemory.New()
	backend := &transientFailureBackend{Store: memory}
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(300, 400, 2, 0)}
	keys := putSegments(t, memory, layout, 0, 100, 200, 300)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{
		ListPageSize: 4, DeleteConcurrency: 2,
	})

	if _, err := r.RunPartition(context.Background(), 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	backend.setBlockDeletes(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := r.RunPartition(ctx, 7); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunPartition(timeout) error = %v, want %v", err, context.DeadlineExceeded)
	}
	assertExists(t, memory, keys...)

	backend.setBlockDeletes(false)
	result, err := r.RunPartition(context.Background(), 7)
	if err != nil {
		t.Fatalf("RunPartition(retry) error = %v", err)
	}
	if result.ReclaimedThroughLSN != 300 {
		t.Fatalf("retry result = %+v, want reclaimed through 300", result)
	}
	assertMissing(t, memory, keys[0], keys[1], keys[2])
	assertExists(t, memory, keys[3])
}

func loadLifecycleState(t testing.TB, backend *blobmemory.Store, partition uint32) stateFile {
	t.Helper()
	object, err := backend.Get(context.Background(), catalogblob.GCStatePath("root/catalog", testStreamID, partition))
	if err != nil {
		t.Fatalf("Get(GC state) error = %v", err)
	}
	state, err := decodeState(object.Body, testStreamID, partition)
	if err != nil {
		t.Fatalf("decodeState() error = %v", err)
	}
	return state
}

type transientFailureBackend struct {
	*blobmemory.Store

	mu           sync.Mutex
	listFailures int
	casCalls     int
	failCASAt    int
	blockDeletes bool
}

type deleteAndCheckpointFailureBackend struct {
	*blobmemory.Store
}

func (b *deleteAndCheckpointFailureBackend) Delete(context.Context, string) error {
	return errDeleteFailed
}

func (b *deleteAndCheckpointFailureBackend) CompareAndSwap(context.Context, string, string, []byte) (blobstore.Object, bool, error) {
	return blobstore.Object{}, false, nil
}

type releaseConflictBackend struct {
	*blobmemory.Store
	casCalls int
}

func (b *releaseConflictBackend) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	b.casCalls++
	if b.casCalls > 1 {
		return blobstore.Object{}, false, nil
	}
	return b.Store.CompareAndSwap(ctx, key, expectedToken, body)
}

type loadFailureCatalog struct {
	Catalog
}

func (c *loadFailureCatalog) LoadMaintenanceSnapshot(context.Context, uint32) (catalogblob.MaintenanceSnapshot, error) {
	return catalogblob.MaintenanceSnapshot{}, errCatalogLoadFailed
}

func (b *transientFailureBackend) failNextList() {
	b.mu.Lock()
	b.listFailures++
	b.mu.Unlock()
}

func (b *transientFailureBackend) failCASCall(call int) {
	b.mu.Lock()
	b.casCalls = 0
	b.failCASAt = call
	b.mu.Unlock()
}

func (b *transientFailureBackend) setBlockDeletes(block bool) {
	b.mu.Lock()
	b.blockDeletes = block
	b.mu.Unlock()
}

func (b *transientFailureBackend) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	b.mu.Lock()
	if b.listFailures > 0 {
		b.listFailures--
		b.mu.Unlock()
		return blobstore.ObjectPage{}, errProviderThrottled
	}
	b.mu.Unlock()
	return b.Store.List(ctx, opts)
}

func (b *transientFailureBackend) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	b.mu.Lock()
	b.casCalls++
	fail := b.failCASAt > 0 && b.casCalls == b.failCASAt
	if fail {
		b.failCASAt = 0
	}
	b.mu.Unlock()
	if fail {
		return blobstore.Object{}, false, errProviderThrottled
	}
	return b.Store.CompareAndSwap(ctx, key, expectedToken, body)
}

func (b *transientFailureBackend) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	block := b.blockDeletes
	b.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return b.Store.Delete(ctx, key)
}
