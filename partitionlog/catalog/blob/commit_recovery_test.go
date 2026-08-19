package blob

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

var errInjectedCAS = errors.New("injected CAS transport failure")

type casFaultMode uint8

const (
	casFaultNone casFaultMode = iota
	casFaultBeforeApplyOnce
	casFaultAfterApplyOnce
	casFaultBeforeApplyAlways
	casFaultReturnCurrent
)

type casFaultBackend struct {
	Backend

	mu          sync.Mutex
	mode        casFaultMode
	counting    bool
	casCalls    int
	getCalls    int
	failGets    bool
	afterCommit func() error
	callbackErr error
}

func (b *casFaultBackend) arm(mode casFaultMode, failGets bool, afterCommit func() error) {
	b.mu.Lock()
	b.mode = mode
	b.counting = true
	b.casCalls = 0
	b.getCalls = 0
	b.failGets = failGets
	b.afterCommit = afterCommit
	b.callbackErr = nil
	b.mu.Unlock()
}

func (b *casFaultBackend) Get(ctx context.Context, key string) (Object, error) {
	b.mu.Lock()
	if b.counting {
		b.getCalls++
	}
	fail := b.counting && b.failGets && b.casCalls > 0
	b.mu.Unlock()
	if fail {
		return Object{}, errInjectedCAS
	}
	return b.Backend.Get(ctx, key)
}

func (b *casFaultBackend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (Object, bool, error) {
	b.mu.Lock()
	mode := b.mode
	if b.counting {
		b.casCalls++
	}
	if mode == casFaultBeforeApplyOnce || mode == casFaultAfterApplyOnce {
		b.mode = casFaultNone
	}
	afterCommit := b.afterCommit
	if mode == casFaultAfterApplyOnce {
		b.afterCommit = nil
	}
	b.mu.Unlock()

	switch mode {
	case casFaultBeforeApplyOnce, casFaultBeforeApplyAlways:
		return Object{}, false, errInjectedCAS
	case casFaultAfterApplyOnce:
		obj, swapped, err := b.Backend.CompareAndSwap(ctx, key, expectedToken, body)
		if err != nil || !swapped {
			return obj, swapped, err
		}
		if afterCommit != nil {
			if callbackErr := afterCommit(); callbackErr != nil {
				b.mu.Lock()
				b.callbackErr = callbackErr
				b.mu.Unlock()
			}
		}
		return Object{}, false, errInjectedCAS
	case casFaultReturnCurrent:
		obj, err := b.Backend.Get(ctx, key)
		return obj, false, err
	default:
		return b.Backend.CompareAndSwap(ctx, key, expectedToken, body)
	}
}

func (b *casFaultBackend) getCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.getCalls
}

func (b *casFaultBackend) stats() (casCalls int, callbackErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.casCalls, b.callbackErr
}

func TestAppendSegmentRecoversWhenCASAppliedButResponseWasLost(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	segment := testSegmentRef(1, 0, 9, ws.Epoch())
	backend.arm(casFaultAfterApplyOnce, false, nil)
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if state.SegmentCount != 1 || state.LastSegment != segment || state.NextLSN != 10 {
		t.Fatalf("state = %+v", state)
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls = %d, want 2", calls)
	}

	loaded, err := cat.LoadPartition(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.SegmentCount != 1 || loaded.LastSegment != segment {
		t.Fatalf("loaded state = %+v", loaded)
	}
}

func TestAppendSegmentRetriesWhenCASWasNotApplied(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	segment := testSegmentRef(1, 0, 9, ws.Epoch())
	backend.arm(casFaultBeforeApplyOnce, false, nil)
	state, err := ws.AppendSegment(context.Background(), segment)
	if err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if state.SegmentCount != 1 || state.LastSegment != segment {
		t.Fatalf("state = %+v", state)
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls = %d, want 2", calls)
	}
}

func TestAppendSegmentReconcilesHistoricalCommitAfterHeadAdvances(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	firstSegment := testSegmentRef(1, 0, 9, first.Epoch())

	backend.arm(casFaultAfterApplyOnce, false, func() error {
		second, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
		if err != nil {
			return fmt.Errorf("open second writer: %w", err)
		}
		_, err = second.AppendSegment(context.Background(), testSegmentRef(1, 10, 19, second.Epoch()))
		return err
	})
	state, err := first.AppendSegment(context.Background(), firstSegment)
	if err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}
	if state.SegmentCount != 1 || state.LastSegment != firstSegment || state.NextLSN != 10 {
		t.Fatalf("reconciled state = %+v", state)
	}
	if first.Epoch() != firstSegment.WriterEpoch || first.WriterID() != ([16]byte{1}) {
		t.Fatalf("first writer identity changed epoch=%d id=%v", first.Epoch(), first.WriterID())
	}
	if _, callbackErr := backend.stats(); callbackErr != nil {
		t.Fatalf("advance head callback error = %v", callbackErr)
	}

	loaded, err := cat.LoadPartition(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.SegmentCount != 2 || loaded.NextLSN != 20 {
		t.Fatalf("loaded state = %+v", loaded)
	}
	if _, err := first.AppendSegment(context.Background(), testSegmentRef(1, 10, 19, first.Epoch())); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("AppendSegment(stale session) error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}

func TestAppendSegmentRetryReconcilesAfterIndeterminateResultAndFenceMoves(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	firstSegment := testSegmentRef(1, 0, 9, first.Epoch())

	backend.arm(casFaultAfterApplyOnce, true, nil)
	if _, err := first.AppendSegment(context.Background(), firstSegment); !errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatalf("AppendSegment(first attempt) error = %v, want %v", err, pcatalog.ErrCommitIndeterminate)
	}

	backend.arm(casFaultNone, false, nil)
	second, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
	if err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	if _, err := second.AppendSegment(context.Background(), testSegmentRef(1, 10, 19, second.Epoch())); err != nil {
		t.Fatalf("AppendSegment(second) error = %v", err)
	}
	third, err := cat.OpenWriter(context.Background(), 1, [16]byte{3})
	if err != nil {
		t.Fatalf("OpenWriter(third) error = %v", err)
	}
	if _, err := third.AppendSegment(context.Background(), testSegmentRef(1, 20, 29, third.Epoch())); err != nil {
		t.Fatalf("AppendSegment(third) error = %v", err)
	}

	reconciled, err := first.AppendSegment(context.Background(), firstSegment)
	if err != nil {
		t.Fatalf("AppendSegment(retry) error = %v", err)
	}
	if reconciled.NextLSN != 10 || reconciled.SegmentCount != 1 || reconciled.LastSegment != firstSegment {
		t.Fatalf("reconciled state = %+v", reconciled)
	}

	current, err := cat.LoadPartition(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if current.NextLSN != 30 || current.SegmentCount != 3 || current.WriterEpoch != third.Epoch() {
		t.Fatalf("current state = %+v", current)
	}
}

func TestAppendSegmentReturnsIndeterminateWhenCommitCannotBeObserved(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	backend.arm(casFaultBeforeApplyAlways, true, nil)
	_, err = ws.AppendSegment(context.Background(), testSegmentRef(1, 0, 9, ws.Epoch()))
	if !errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatalf("AppendSegment() error = %v, want %v", err, pcatalog.ErrCommitIndeterminate)
	}
}

func TestAppendSegmentReturnsDefiniteErrorWhenUnchangedHeadIsObserved(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	backend.arm(casFaultBeforeApplyAlways, false, nil)
	_, err = ws.AppendSegment(context.Background(), testSegmentRef(1, 0, 9, ws.Epoch()))
	if !errors.Is(err, errInjectedCAS) {
		t.Fatalf("AppendSegment() error = %v, want injected error", err)
	}
	if errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatalf("AppendSegment() error = %v, outcome was observable", err)
	}
}

func TestAppendSegmentDoesNotRereadHeadAfterFinalCASConflict(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}

	backend.arm(casFaultReturnCurrent, false, nil)
	_, err = ws.AppendSegment(context.Background(), testSegmentRef(1, 0, 9, ws.Epoch()))
	if !errors.Is(err, pcatalog.ErrConflict) {
		t.Fatalf("AppendSegment() error = %v, want %v", err, pcatalog.ErrConflict)
	}
	if gets := backend.getCount(); gets != 0 {
		t.Fatalf("catalog Get calls after CAS response = %d, want 0", gets)
	}
}

func commitRecoveryTestOptions() Options {
	return Options{
		LeafSegmentLimit:           2,
		IndexRefLimit:              2,
		WriterCommitMaxAttempts:    2,
		WriterCommitInitialBackoff: time.Nanosecond,
		WriterCommitMaxBackoff:     time.Nanosecond,
	}
}
