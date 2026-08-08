package catalog

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryCatalogRetentionIsMonotonicAndSegmentGranular(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 1, 1)
	for base := uint64(0); base < 30; base += 10 {
		if _, err := ws.AppendSegment(ctx, testSegment(1, base, base+9, ws.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", base, err)
		}
	}

	request := RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 15, CreatedUnixMS: 10}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	retention := ws.(RetentionWriterSession)
	result, err := retention.ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if !result.Applied || result.Head.OldestLSN != 10 || result.Head.AppliedRetentionLSN != 15 || result.Head.AppliedRetentionVersion != 1 {
		t.Fatalf("retention result = %+v", result)
	}
	if result.Head.SegmentCount != 3 || result.Head.LastSegment.BaseLSN != 20 {
		t.Fatalf("retention changed append history = %+v", result.Head)
	}
	if _, ok, err := cat.FindSegment(ctx, 1, 5); err != nil || ok {
		t.Fatalf("FindSegment(expired) ok=%v err=%v", ok, err)
	}
	segment, ok, err := cat.FindSegment(ctx, 1, 15)
	if err != nil || !ok || segment.BaseLSN != 10 {
		t.Fatalf("FindSegment(retained) = %+v ok=%v err=%v", segment, ok, err)
	}

	noOp, err := retention.ApplyPendingRetention(ctx)
	if err != nil || noOp.Applied {
		t.Fatalf("ApplyPendingRetention(no-op) = %+v err=%v", noOp, err)
	}

	full := RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 2, BeforeLSN: 100, CreatedUnixMS: 20}
	if _, err := cat.RequestRetention(ctx, 1, full); err != nil {
		t.Fatalf("RequestRetention(full) error = %v", err)
	}
	result, err = retention.ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyPendingRetention(full) error = %v", err)
	}
	if result.Head.OldestLSN != 30 || result.Head.AppliedRetentionLSN != 30 || !result.Head.HasLastSegment || result.Head.SegmentCount != 3 {
		t.Fatalf("fully trimmed head = %+v", result.Head)
	}
	page, err := cat.ListSegments(ctx, ListSegmentsRequest{Partition: 1})
	if err != nil || len(page.Segments) != 0 {
		t.Fatalf("ListSegments(after full trim) = %+v err=%v", page, err)
	}

	state, err := ws.AppendSegment(ctx, testSegment(1, 30, 39, ws.Epoch()))
	if err != nil {
		t.Fatalf("AppendSegment(after trim) error = %v", err)
	}
	if state.OldestLSN != 30 || state.NextLSN != 40 || state.SegmentCount != 4 {
		t.Fatalf("state after append = %+v", state)
	}
}

func TestMemoryCatalogRetentionRequestRejectsRegressionAndAcceptsRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat := NewMemoryCatalog()
	first := RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 7, BeforeLSN: 100, CreatedUnixMS: 1}
	if _, err := cat.RequestRetention(ctx, 2, first); err != nil {
		t.Fatalf("RequestRetention(first) error = %v", err)
	}
	retry := first
	retry.CreatedUnixMS = 2
	stored, err := cat.RequestRetention(ctx, 2, retry)
	if err != nil {
		t.Fatalf("RequestRetention(retry) error = %v", err)
	}
	if stored.CreatedUnixMS != first.CreatedUnixMS {
		t.Fatalf("retry replaced durable request: %+v", stored)
	}
	if _, err := cat.RequestRetention(ctx, 2, RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 6, BeforeLSN: 100}); !errors.Is(err, ErrRetentionRegression) {
		t.Fatalf("older policy error = %v, want %v", err, ErrRetentionRegression)
	}
	if _, err := cat.RequestRetention(ctx, 2, RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 8, BeforeLSN: 99}); !errors.Is(err, ErrRetentionRegression) {
		t.Fatalf("older boundary error = %v, want %v", err, ErrRetentionRegression)
	}
}

func TestMemoryCatalogStaleWriterCannotApplyRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat := NewMemoryCatalog()
	first := mustOpenWriter(t, cat, 3, 1)
	if _, err := cat.OpenWriter(ctx, 3, [16]byte{2}); err != nil {
		t.Fatalf("OpenWriter(second) error = %v", err)
	}
	if _, err := cat.RequestRetention(ctx, 3, RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 1}); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	_, err := first.(RetentionWriterSession).ApplyPendingRetention(ctx)
	if !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("ApplyPendingRetention(stale) error = %v, want %v", err, ErrStaleWriter)
	}
}

func TestMemoryCatalogHeadRejectsRegressedRestoredRetentionRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cat := NewMemoryCatalog()
	ws := mustOpenWriter(t, cat, 4, 1)
	if _, err := ws.AppendSegment(ctx, testSegment(4, 0, 9, ws.Epoch())); err != nil {
		t.Fatalf("AppendSegment() error = %v", err)
	}
	if _, err := cat.RequestRetention(ctx, 4, RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 7}); err != nil {
		t.Fatalf("RequestRetention(first) error = %v", err)
	}
	retention := ws.(RetentionWriterSession)
	if _, err := retention.ApplyPendingRetention(ctx); err != nil {
		t.Fatalf("ApplyPendingRetention(first) error = %v", err)
	}

	cat.mu.Lock()
	delete(cat.retention, 4)
	cat.mu.Unlock()
	if _, err := cat.RequestRetention(ctx, 4, RetentionRequest{Version: RetentionRequestVersion, PolicyVersion: 2, BeforeLSN: 6}); err != nil {
		t.Fatalf("RequestRetention(restored) error = %v", err)
	}
	if _, err := retention.ApplyPendingRetention(ctx); !errors.Is(err, ErrRetentionRegression) {
		t.Fatalf("ApplyPendingRetention(regressed) error = %v, want %v", err, ErrRetentionRegression)
	}
}
