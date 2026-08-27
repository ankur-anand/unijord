package blob

import (
	"context"
	"errors"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func TestOpenWriterRecoversWhenFenceCASAppliedButResponseWasLost(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	backend.arm(casFaultAfterApplyOnce, false, nil)
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if ws.Epoch() != 1 || ws.WriterID() != ([16]byte{1}) {
		t.Fatalf("writer identity epoch=%d id=%v", ws.Epoch(), ws.WriterID())
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls = %d, want 2", calls)
	}

	segment := testSegmentRef(1, 0, 9, ws.Epoch())
	if _, err := ws.AppendSegment(context.Background(), segment); err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
}

func TestOpenWriterRetriesWhenFenceCASWasNotApplied(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	backend.arm(casFaultBeforeApplyOnce, false, nil)
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if ws.Epoch() != 1 {
		t.Fatalf("writer epoch = %d, want 1", ws.Epoch())
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls = %d, want 2", calls)
	}
}

func TestOpenWriterReacquiresAfterAmbiguousFenceWasSuperseded(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(4))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var competing pcatalog.WriterSession
	backend.arm(casFaultAfterApplyOnce, false, func() error {
		var openErr error
		competing, openErr = cat.OpenWriter(context.Background(), 1, [16]byte{2})
		return openErr
	})
	ws, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, callbackErr := backend.stats(); callbackErr != nil {
		t.Fatalf("competing OpenWriter() error = %v", callbackErr)
	}
	if competing == nil || competing.Epoch() != 2 {
		t.Fatalf("competing writer = %#v", competing)
	}
	if ws.Epoch() != 3 || ws.WriterID() != ([16]byte{1}) {
		t.Fatalf("reacquired identity epoch=%d id=%v, want epoch=3", ws.Epoch(), ws.WriterID())
	}

	if _, err := competing.AppendSegment(context.Background(), testSegmentRef(1, 0, 9, competing.Epoch())); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("competing AppendSegment() error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}

func TestOpenWriterReturnsDefiniteErrorWhenFenceWasNotApplied(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	backend.arm(casFaultBeforeApplyAlways, false, nil)
	_, err = cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if !errors.Is(err, errInjectedCAS) {
		t.Fatalf("OpenWriter() error = %v, want injected error", err)
	}
	if errors.Is(err, pcatalog.ErrFenceIndeterminate) {
		t.Fatalf("OpenWriter() error = %v, fence was observable", err)
	}
}

func TestOpenWriterReturnsIndeterminateWhenFenceCannotBeObserved(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	backend.arm(casFaultBeforeApplyAlways, true, nil)
	_, err = cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if !errors.Is(err, pcatalog.ErrFenceIndeterminate) {
		t.Fatalf("OpenWriter() error = %v, want %v", err, pcatalog.ErrFenceIndeterminate)
	}
}

func TestOpenWriterCancellationAfterAmbiguousCASCanBeRecoveredByNewWriter(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	backend.arm(casFaultAfterApplyOnce, false, func() error {
		cancel()
		return nil
	})

	_, err = cat.OpenWriter(ctx, 1, [16]byte{1})
	if !errors.Is(err, pcatalog.ErrFenceIndeterminate) || !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenWriter() error = %v, want indeterminate canceled fence", err)
	}

	replacement, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
	if err != nil {
		t.Fatalf("OpenWriter(replacement) error = %v", err)
	}
	if replacement.Epoch() != 2 || replacement.WriterID() != ([16]byte{2}) {
		t.Fatalf("replacement identity epoch=%d id=%v", replacement.Epoch(), replacement.WriterID())
	}
}

func TestOpenWriterReusesCASConflictHeadWithoutAnotherGet(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, fenceRecoveryTestOptions(2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, _, err := cat.InitializePartition(context.Background(), 1, 0); err != nil {
		t.Fatalf("InitializePartition() error = %v", err)
	}

	backend.arm(casFaultReturnCurrent, false, nil)
	_, err = cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if !errors.Is(err, pcatalog.ErrConflict) {
		t.Fatalf("OpenWriter() error = %v, want %v", err, pcatalog.ErrConflict)
	}
	if gets := backend.getCount(); gets != 1 {
		t.Fatalf("catalog Get calls = %d, want one initial head read", gets)
	}
}

func fenceRecoveryTestOptions(attempts int) Options {
	return Options{
		LeafSegmentLimit:            2,
		IndexRefLimit:               2,
		WriterAcquireMaxAttempts:    attempts,
		WriterAcquireInitialBackoff: time.Nanosecond,
		WriterAcquireMaxBackoff:     time.Nanosecond,
	}
}
