package blob

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func TestBlobCatalogRetentionTrimsMultiLevelHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for base := uint64(0); base < 160; base += 10 {
		if _, err := ws.AppendSegment(ctx, testSegmentRef(1, base, base+9, ws.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", base, err)
		}
	}
	before, err := cat.LoadMaintenanceSnapshot(ctx, 1)
	if err != nil {
		t.Fatalf("LoadMaintenanceSnapshot(before) error = %v", err)
	}
	if before.MaxIndexLevel < 2 {
		t.Fatalf("max index level = %d, want multi-level tree", before.MaxIndexLevel)
	}

	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 75, CreatedUnixMS: 10}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	result, err := ws.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if !result.Applied || result.Head.OldestLSN != 70 || result.Head.AppliedRetentionLSN != 75 || result.Head.AppliedRetentionVersion != 1 {
		t.Fatalf("retention result = %+v", result)
	}
	if result.Head.NextLSN != 160 || result.Head.SegmentCount != 16 || result.Head.ReachableSegmentCount != 9 || result.Head.LastSegment.BaseLSN != 150 {
		t.Fatalf("retention changed append history = %+v", result.Head)
	}
	if _, ok, err := cat.FindSegment(ctx, 1, 69); err != nil || ok {
		t.Fatalf("FindSegment(expired) ok=%v err=%v", ok, err)
	}
	segment, ok, err := cat.FindSegment(ctx, 1, 75)
	if err != nil || !ok || segment.BaseLSN != 70 {
		t.Fatalf("FindSegment(retained) = %+v ok=%v err=%v", segment, ok, err)
	}
	page, err := cat.ListSegments(ctx, pcatalog.ListSegmentsRequest{Partition: 1, FromLSN: 0, Limit: 32})
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	if len(page.Segments) != 9 || page.Segments[0].BaseLSN != 70 || page.Segments[8].BaseLSN != 150 {
		t.Fatalf("retained segments = %+v", page.Segments)
	}

	after, err := cat.LoadMaintenanceSnapshot(ctx, 1)
	if err != nil {
		t.Fatalf("LoadMaintenanceSnapshot(after) error = %v", err)
	}
	if after.Head != result.Head || after.Generation <= before.Generation {
		t.Fatalf("maintenance snapshot after retention = %+v, before=%+v", after, before)
	}
}

func TestBlobCatalogApplyPendingRetentionLoadsHeadOnceAfterMailbox(t *testing.T) {
	ctx := context.Background()
	backend := &countingGetBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, Options{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.AppendSegment(ctx, testSegmentRef(1, 0, 9, writer.Epoch())); err != nil {
		t.Fatal(err)
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 5}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatal(err)
	}

	backend.gets.Store(0)
	result, err := writer.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied {
		t.Fatal("ApplyPendingRetention() applied = false, want true")
	}
	if got := backend.gets.Load(); got != 2 {
		t.Fatalf("ApplyPendingRetention() GET count = %d, want mailbox + one head refresh", got)
	}
}

func TestBlobCatalogRetentionRefreshAfterMailboxObservesTakeover(t *testing.T) {
	ctx := context.Background()
	backend := &afterGetBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 1}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatal(err)
	}

	backend.key = RetentionRequestPath(cat.opts.Prefix, cat.opts.StreamID, 1)
	backend.after = func() error {
		_, err := cat.OpenWriter(ctx, 1, [16]byte{2})
		return err
	}
	if _, err := first.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("ApplyPendingRetention() error = %v, want ErrStaleWriter", err)
	}
	if backend.afterErr != nil {
		t.Fatalf("takeover after mailbox read: %v", backend.afterErr)
	}
}

func TestBlobCatalogRetentionCanTrimEverythingAndContinueAppending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for base := uint64(0); base < 40; base += 10 {
		if _, err := ws.AppendSegment(ctx, testSegmentRef(1, base, base+9, ws.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", base, err)
		}
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 100, CreatedUnixMS: 10}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	result, err := ws.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if result.Head.OldestLSN != 40 || result.Head.AppliedRetentionLSN != 40 || !result.Head.HasLastSegment || result.Head.SegmentCount != 4 || result.Head.ReachableSegmentCount != 0 {
		t.Fatalf("fully trimmed head = %+v", result.Head)
	}
	snapshot, pages, err := cat.ListMaintenancePages(ctx, MaintenancePageRequest{Partition: 1, Level: 0, Limit: 10})
	if err != nil {
		t.Fatalf("ListMaintenancePages() error = %v", err)
	}
	if snapshot.Head != result.Head || len(pages.Paths) != 0 {
		t.Fatalf("fully trimmed topology snapshot=%+v pages=%+v", snapshot, pages)
	}
	lookup, err := cat.LookupTimestamp(ctx, pcatalog.TimestampLookupRequest{Partition: 1, TimestampMS: 0})
	if err != nil {
		t.Fatalf("LookupTimestamp(fully trimmed) error = %v", err)
	}
	if lookup.Found || lookup.Head != result.Head {
		t.Fatalf("LookupTimestamp(fully trimmed) = %+v, want not found with committed head", lookup)
	}

	state, err := ws.AppendSegment(ctx, testSegmentRef(1, 40, 49, ws.Epoch()))
	if err != nil {
		t.Fatalf("AppendSegment(after trim) error = %v", err)
	}
	if state.OldestLSN != 40 || state.NextLSN != 50 || state.SegmentCount != 5 || state.ReachableSegmentCount != 1 {
		t.Fatalf("state after append = %+v", state)
	}
}

func TestBlobCatalogRetentionMailboxIsMonotonicAndFenced(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 3, BeforeLSN: 50, CreatedUnixMS: 1}
	stored, err := cat.RequestRetention(ctx, 1, request)
	if err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	retry := request
	retry.CreatedUnixMS = 2
	storedRetry, err := cat.RequestRetention(ctx, 1, retry)
	if err != nil || storedRetry != stored {
		t.Fatalf("RequestRetention(retry) = %+v err=%v, want %+v", storedRetry, err, stored)
	}
	if _, err := cat.RequestRetention(ctx, 1, pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 4, BeforeLSN: 49}); !errors.Is(err, pcatalog.ErrRetentionRegression) {
		t.Fatalf("RequestRetention(regression) error = %v", err)
	}

	first, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	if _, err := cat.OpenWriter(ctx, 1, [16]byte{2}); err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	_, err = first.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("ApplyPendingRetention(stale) error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}

func TestBlobCatalogStaleWriterCannotReportRetentionNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	if _, err := cat.OpenWriter(ctx, 1, [16]byte{2}); err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}

	_, err = first.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("ApplyPendingRetention(stale no-op) error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
}

func TestBlobCatalogRetentionRequestRecoversLostCASResponse(t *testing.T) {
	t.Parallel()

	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 10, CreatedUnixMS: 1}
	backend.arm(casFaultAfterApplyOnce, false, nil)
	stored, err := cat.RequestRetention(context.Background(), 1, request)
	if err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	if stored != request {
		t.Fatalf("stored request = %+v, want %+v", stored, request)
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("retention CAS calls = %d, want 2", calls)
	}
}

func TestBlobCatalogRetentionCancellationAfterAmbiguousCASRemainsIndeterminate(t *testing.T) {
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitInitialBackoff = time.Hour
	opts.WriterCommitMaxBackoff = time.Hour
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := pcatalog.RetentionRequest{
		Version:       pcatalog.RetentionRequestVersion,
		PolicyVersion: 1,
		BeforeLSN:     10,
		CreatedUnixMS: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	backend.arm(casFaultAfterApplyOnce, false, func() error {
		cancel()
		return nil
	})

	_, err = cat.RequestRetention(ctx, 1, request)
	stored, found, loadErr := cat.LoadRetentionRequest(context.Background(), 1)
	if loadErr != nil {
		t.Fatalf("LoadRetentionRequest() error = %v", loadErr)
	}
	if !found || stored != request {
		t.Fatalf("durable retention request = %+v found=%v, want %+v", stored, found, request)
	}
	if !errors.Is(err, errInjectedCAS) || !errors.Is(err, context.Canceled) {
		t.Fatalf("RequestRetention() error = %v, want transport and context errors", err)
	}
	if !errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatalf("RequestRetention() error = %v, committed outcome must be ErrCommitIndeterminate", err)
	}
}

func TestBlobCatalogRetentionReturnsIndeterminateWhenCommitCannotBeObserved(t *testing.T) {
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := pcatalog.RetentionRequest{
		Version:       pcatalog.RetentionRequestVersion,
		PolicyVersion: 1,
		BeforeLSN:     10,
		CreatedUnixMS: 1,
	}
	backend.arm(casFaultAfterApplyOnce, true, nil)

	_, err = cat.RequestRetention(context.Background(), 1, request)
	if !errors.Is(err, errInjectedCAS) || !errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatalf("RequestRetention() error = %v, want transport error and ErrCommitIndeterminate", err)
	}
}

func TestBlobCatalogRetentionApplyRecoversLostHeadCASResponse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ws, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for base := uint64(0); base < 20; base += 10 {
		if _, err := ws.AppendSegment(ctx, testSegmentRef(1, base, base+9, ws.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", base, err)
		}
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 15, CreatedUnixMS: 1}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	backend.arm(casFaultAfterApplyOnce, false, nil)
	result, err := ws.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if !result.Applied || result.Head.OldestLSN != 10 || result.Head.AppliedRetentionVersion != 1 {
		t.Fatalf("retention result = %+v", result)
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls = %d, want 2", calls)
	}
}

func TestBlobCatalogRetentionReconcilesHistoricalCommitAfterFenceMoves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, commitRecoveryTestOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter(first) error = %v", err)
	}
	if _, err := first.AppendSegment(ctx, testSegmentRef(1, 0, 9, first.Epoch())); err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 5, CreatedUnixMS: 1}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}

	backend.arm(casFaultAfterApplyOnce, false, func() error {
		_, err := cat.OpenWriter(ctx, 1, [16]byte{2})
		if err != nil {
			return fmt.Errorf("open replacement writer: %w", err)
		}
		return nil
	})
	result, err := first.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if !result.Applied || result.Head.WriterEpoch != first.Epoch() || result.Head.AppliedRetentionVersion != 1 || result.Head.AppliedRetentionLSN != 5 {
		t.Fatalf("historical retention result = %+v", result)
	}
	if _, callbackErr := backend.stats(); callbackErr != nil {
		t.Fatalf("advance fence callback error = %v", callbackErr)
	}
	loaded, err := cat.LoadPartition(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if loaded.WriterEpoch <= first.Epoch() || loaded.AppliedRetentionVersion != 1 || loaded.AppliedRetentionLSN != 5 {
		t.Fatalf("authoritative head = %+v", loaded)
	}
	getsBefore := backend.getCount()
	if _, err := first.(pcatalog.RetentionWriterSession).ApplyPendingRetention(ctx); !errors.Is(err, pcatalog.ErrStaleWriter) {
		t.Fatalf("ApplyPendingRetention(stale retry) error = %v, want %v", err, pcatalog.ErrStaleWriter)
	}
	if getsAfter := backend.getCount(); getsAfter != getsBefore {
		t.Fatalf("ApplyPendingRetention(stale retry) head reads = %d, want unchanged %d", getsAfter, getsBefore)
	}
}

func TestBlobCatalogHeadRejectsRegressedRestoredRetentionRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ws, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := ws.AppendSegment(ctx, testSegmentRef(1, 0, 9, ws.Epoch())); err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	first := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 7}
	if _, err := cat.RequestRetention(ctx, 1, first); err != nil {
		t.Fatalf("RequestRetention(first) error = %v", err)
	}
	retention := ws.(pcatalog.RetentionWriterSession)
	if _, err := retention.ApplyPendingRetention(ctx); err != nil {
		t.Fatalf("ApplyPendingRetention(first) error = %v", err)
	}

	if err := cat.backend.Delete(ctx, RetentionRequestPath(cat.opts.Prefix, cat.opts.StreamID, 1)); err != nil {
		t.Fatalf("Delete(retention request) error = %v", err)
	}
	regressed := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 2, BeforeLSN: 6}
	if _, err := cat.RequestRetention(ctx, 1, regressed); err != nil {
		t.Fatalf("RequestRetention(restored) error = %v", err)
	}
	if _, err := retention.ApplyPendingRetention(ctx); !errors.Is(err, pcatalog.ErrRetentionRegression) {
		t.Fatalf("ApplyPendingRetention(regressed) error = %v, want %v", err, pcatalog.ErrRetentionRegression)
	}
}

type afterGetBackend struct {
	Backend
	key      string
	after    func() error
	afterErr error
}

func (b *afterGetBackend) Get(ctx context.Context, key string) (Object, error) {
	object, err := b.Backend.Get(ctx, key)
	if err != nil || key != b.key || b.after == nil {
		return object, err
	}
	after := b.after
	b.after = nil
	b.afterErr = after()
	return object, nil
}
