package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
)

func TestExecuteDeletesUsesBoundedConcurrency(t *testing.T) {
	t.Parallel()

	backend := &concurrentDeleteBackend{Store: blobmemory.New(), delay: 5 * time.Millisecond}
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, segmentsink.NewLayout("root"), newFakeClock(time.Now()), Options{
		DeleteConcurrency: 3,
	})
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	candidates := make([]deleteCandidate, 12)
	for i := range candidates {
		candidates[i] = deleteCandidate{key: string(rune('a' + i)), size: 1}
	}
	if _, err := r.executeDeletes(context.Background(), activeTestLease(r), candidates, &budget); err != nil {
		t.Fatalf("executeDeletes() error = %v", err)
	}
	if result.DeletedObjects != len(candidates) {
		t.Fatalf("deleted = %d, want %d", result.DeletedObjects, len(candidates))
	}
	backend.mu.Lock()
	maxActive := backend.maxActive
	backend.mu.Unlock()
	if maxActive < 2 || maxActive > 3 {
		t.Fatalf("max active deletes = %d, want 2..3", maxActive)
	}
}

func TestExecuteDeletesUsesNativeBatchesAndReportsFirstFailureCheckpoint(t *testing.T) {
	t.Parallel()

	backend := &nativeBatchBackend{Store: blobmemory.New(), failKey: "c"}
	r := newTestReclaimer(t, backend, &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}, segmentsink.NewLayout("root"), newFakeClock(time.Now()), Options{
		DeleteBatchSize: 2,
	})
	result := Result{}
	budget := runBudget{opts: r.opts, result: &result}
	candidates := []deleteCandidate{
		{key: "a", size: 1, beforeKey: "start"},
		{key: "b", size: 1, beforeKey: "a"},
		{key: "c", size: 1, beforeKey: "b"},
		{key: "d", size: 1, beforeKey: "c"},
		{key: "e", size: 1, beforeKey: "d"},
	}
	checkpoint, err := r.executeDeletes(context.Background(), activeTestLease(r), candidates, &budget)
	if !errors.Is(err, errNativeBatchDelete) {
		t.Fatalf("executeDeletes() error = %v, want %v", err, errNativeBatchDelete)
	}
	if checkpoint != "b" {
		t.Fatalf("checkpoint = %q, want b", checkpoint)
	}
	if result.DeletedObjects != 4 {
		t.Fatalf("deleted = %d, want 4", result.DeletedObjects)
	}
	backend.mu.Lock()
	waves := append([]int(nil), backend.waves...)
	backend.mu.Unlock()
	if len(waves) != 3 || waves[0] != 2 || waves[1] != 2 || waves[2] != 1 {
		t.Fatalf("batch waves = %v, want [2 2 1]", waves)
	}
}

func TestRunBudgetAllowsOneOversizedDeleteForProgress(t *testing.T) {
	t.Parallel()

	result := Result{}
	budget := runBudget{
		opts: Options{
			MaxDeletesPerRun: 10,
			MaxDeleteBytes:   100,
		},
		result: &result,
	}

	if !budget.canScheduleDelete(250, 0, 0) {
		t.Fatal("first oversized delete was rejected; GC cannot make progress")
	}
	if budget.canScheduleDelete(1, 1, 250) {
		t.Fatal("second delete was accepted after the byte budget was exceeded")
	}
}

func TestNewRejectsInvalidDeleteExecutionOptions(t *testing.T) {
	t.Parallel()

	for _, opts := range []Options{
		{DeleteBatchSize: -1},
		{DeleteBatchSize: 1001},
		{DeleteConcurrency: -1},
	} {
		opts.StreamID = testStreamID
		opts.CatalogPrefix = "root/catalog"
		_, err := newReclaimer(
			blobmemory.New(),
			&fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)},
			segmentsink.NewLayout("root"),
			opts,
			time.Now,
		)
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("newReclaimer(%+v) error = %v, want %v", opts, err, ErrInvalidOptions)
		}
	}
}

type concurrentDeleteBackend struct {
	*blobmemory.Store
	mu        sync.Mutex
	active    int
	maxActive int
	delay     time.Duration
}

func (b *concurrentDeleteBackend) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	b.active++
	b.maxActive = max(b.maxActive, b.active)
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.active--
		b.mu.Unlock()
	}()
	select {
	case <-time.After(b.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var errNativeBatchDelete = errors.New("native batch delete failure")

type nativeBatchBackend struct {
	*blobmemory.Store
	mu      sync.Mutex
	failKey string
	waves   []int
}

func (b *nativeBatchBackend) Delete(context.Context, string) error {
	panic("individual Delete called for native batch backend")
}

func (b *nativeBatchBackend) DeleteBatch(_ context.Context, keys []string) []error {
	b.mu.Lock()
	b.waves = append(b.waves, len(keys))
	b.mu.Unlock()
	errs := make([]error, len(keys))
	for i, key := range keys {
		if key == b.failKey {
			errs[i] = errNativeBatchDelete
		}
	}
	return errs
}
