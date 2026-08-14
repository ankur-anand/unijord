package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

func TestReclaimerStartsAtPersistedHighLSN(t *testing.T) {
	t.Parallel()

	const (
		startLSN = uint64(900_000_000)
		expired  = uint64(2_500)
	)
	r, backend := newSyntheticReclaimer(t, startLSN, expired)
	result, err := r.RunPartition(context.Background(), 7)
	if err != nil {
		t.Fatalf("RunPartition() error = %v", err)
	}
	if result.DeletedObjects != int(expired) || result.ReclaimedThroughLSN != startLSN+expired {
		t.Fatalf("result = %+v", result)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.segmentLists) != 3 {
		t.Fatalf("segment LIST calls = %d, want 3", len(backend.segmentLists))
	}
	wantAfter := backend.layout.SegmentLowerBound(testStreamID, 7, startLSN)
	if got := backend.segmentLists[0].AfterKey; got != wantAfter {
		t.Fatalf("first AfterKey = %q, want %q", got, wantAfter)
	}
	if backend.deleted != expired {
		t.Fatalf("deleted = %d, want %d", backend.deleted, expired)
	}
}

func BenchmarkReclaimerHighLSN(b *testing.B) {
	for _, expired := range []uint64{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("expired_%d", expired), func(b *testing.B) {
			var listCalls uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, backend := newSyntheticReclaimer(b, 1_000_000_000, expired)
				b.StartTimer()
				result, err := r.RunPartition(context.Background(), 7)
				if err != nil {
					b.Fatal(err)
				}
				if result.DeletedObjects != int(expired) {
					b.Fatalf("deleted=%d want=%d", result.DeletedObjects, expired)
				}
				backend.mu.Lock()
				listCalls += uint64(len(backend.segmentLists))
				backend.mu.Unlock()
			}
			b.StopTimer()
			b.ReportMetric(float64(expired), "objects/op")
			b.ReportMetric(float64(listCalls)/float64(b.N), "segment-lists/op")
			b.ReportMetric(float64(expired)*float64(b.N)/b.Elapsed().Seconds(), "objects/s")
		})
	}
}

func BenchmarkScrubSegmentOrphansHighLSN(b *testing.B) {
	for _, objects := range []uint64{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("orphans_%d", objects), func(b *testing.B) {
			var listCalls uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, backend := newSyntheticScrubber(b, 1_000_000_000, objects)
				b.StartTimer()
				result, err := r.ScrubPartition(context.Background(), 7)
				if err != nil {
					b.Fatal(err)
				}
				if result.DeletedObjects != int(objects) {
					b.Fatalf("deleted=%d want=%d", result.DeletedObjects, objects)
				}
				backend.mu.Lock()
				listCalls += uint64(len(backend.segmentLists))
				backend.mu.Unlock()
			}
			b.StopTimer()
			b.ReportMetric(float64(objects), "objects/op")
			b.ReportMetric(float64(listCalls)/float64(b.N), "segment-lists/op")
			b.ReportMetric(float64(objects)*float64(b.N)/b.Elapsed().Seconds(), "objects/s")
		})
	}
}

func TestScrubSegmentCatalogReadsArePageBounded(t *testing.T) {
	t.Parallel()

	const objects = uint64(2_500)
	r, _ := newSyntheticScrubber(t, 1_000_000_000, objects)
	result, err := r.ScrubPartition(context.Background(), 7)
	if err != nil {
		t.Fatalf("ScrubPartition() error = %v", err)
	}
	if result.DeletedObjects != int(objects) {
		t.Fatalf("deleted=%d want=%d", result.DeletedObjects, objects)
	}
	cat := r.catalog.(*fakeCatalog)
	cat.mu.Lock()
	loads := cat.loads
	cat.mu.Unlock()
	// One initial maintenance snapshot plus one catalog page read for each
	// physical LIST page. The empty catalog does not add history pages.
	if loads != 4 {
		t.Fatalf("catalog loads=%d want=4 for %d objects", loads, objects)
	}
}

func TestScrubPageCatalogReadsArePageBounded(t *testing.T) {
	t.Parallel()

	const objects = 2_500
	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Unix(1_800_000_000, 0).UTC())
	snapshot := maintenanceSnapshot(0, 0, 2, 0)
	snapshot.Generation = 10
	cat := &fakeCatalog{snapshot: snapshot, reachablePages: make(map[string]bool)}
	r := newTestReclaimer(t, backend, cat, layout, clock, Options{
		DryRun:           true,
		ListPageSize:     1_000,
		MaxObjectsPerRun: objects,
	})
	keys := make([]string, objects)
	for i := range keys {
		base := uint64(i * 10)
		keys[i] = catalogblob.LeafPagePath("root/catalog", testStreamID, 7, base, base+9, 1, fmt.Sprintf("%032x", i+1))
	}
	putKeys(t, backend, keys)

	result, err := r.ScrubPartition(ctx, 7)
	if err != nil {
		t.Fatalf("ScrubPartition() error = %v", err)
	}
	if result.ScannedObjects != objects || result.CandidateObjects != objects {
		t.Fatalf("result=%+v want scanned/candidates=%d", result, objects)
	}
	cat.mu.Lock()
	loads := cat.loads
	cat.mu.Unlock()
	// One initial maintenance snapshot plus one reachable-page query for each
	// physical LIST page.
	if loads != 4 {
		t.Fatalf("catalog loads=%d want=4 for %d objects", loads, objects)
	}
}

func TestQuarantineRecheckCatalogReadsArePageBounded(t *testing.T) {
	t.Parallel()

	const candidates = 256
	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Unix(1_800_000_000, 0).UTC())
	snapshot := maintenanceSnapshot(0, 0, 2, 0)
	snapshot.Generation = 10
	cat := &fakeCatalog{snapshot: snapshot, reachablePages: make(map[string]bool)}
	r := newTestReclaimer(t, backend, cat, layout, clock, Options{
		MaxObjectsPerRun: candidates,
		MaxDeletesPerRun: candidates,
		MaxDeleteBytes:   ^uint64(0),
		MaxQuarantine:    candidates,
	})
	state := stateFile{
		Version: stateVersion, StreamID: testStreamID, Partition: 7,
		UpdatedMS: clock.Now().UnixMilli(),
	}
	for i := 0; i < candidates; i++ {
		base := uint64(i * 10)
		key := catalogblob.LeafPagePath("root/catalog", testStreamID, 7, base, base+9, 1, fmt.Sprintf("%032x", i+1))
		putKeys(t, backend, []string{key})
		state.PageQuarantine = append(state.PageQuarantine, quarantineObject{
			Key: key, SizeBytes: 1, ObservedGeneration: 9,
			ObservedMS: clock.Now().Add(-DefaultDeleteDelay - time.Second).UnixMilli(),
		})
	}
	body, err := marshalState(state, testStreamID, 7)
	if err != nil {
		t.Fatalf("marshalState() error = %v", err)
	}
	statePath := catalogblob.GCStatePath("root/catalog", testStreamID, 7)
	if _, swapped, err := backend.CompareAndSwap(ctx, statePath, "", body); err != nil || !swapped {
		t.Fatalf("seed state swapped=%v error=%v", swapped, err)
	}

	result, err := r.ScrubPartition(ctx, 7)
	if err != nil {
		t.Fatalf("ScrubPartition() error = %v", err)
	}
	if result.DeletedObjects != candidates || result.PendingQuarantine != 0 {
		t.Fatalf("result=%+v want deleted=%d pending=0", result, candidates)
	}
	cat.mu.Lock()
	loads := cat.loads
	cat.mu.Unlock()
	if loads != 2 {
		t.Fatalf("catalog loads=%d want=2 for %d quarantine candidates", loads, candidates)
	}
}

func newSyntheticReclaimer(t testing.TB, startLSN, expired uint64) (*Reclaimer, *syntheticSegmentBackend) {
	t.Helper()
	layout := segmentsink.NewLayout("root")
	floor := startLSN + expired
	backend := &syntheticSegmentBackend{
		Store: blobmemory.New(), layout: layout, startLSN: startLSN, retainedLSN: floor, writerEpoch: 2,
	}
	clock := newFakeClock(time.Unix(1_800_000_000, 0).UTC())
	state := stateFile{
		Version:                    stateVersion,
		StreamID:                   testStreamID,
		Partition:                  7,
		RetentionVersion:           1,
		SafeFloorLSN:               floor,
		SegmentReclaimedThroughLSN: startLSN,
		PageReclaimedThroughLSN:    floor,
		StagingEpoch:               2,
		UpdatedMS:                  clock.Now().UnixMilli(),
	}
	body, err := marshalState(state, testStreamID, 7)
	if err != nil {
		t.Fatalf("marshalState() error = %v", err)
	}
	statePath := catalogblob.GCStatePath("root/catalog", testStreamID, 7)
	if _, swapped, err := backend.CompareAndSwap(context.Background(), statePath, "", body); err != nil || !swapped {
		t.Fatalf("seed state swapped=%v error=%v", swapped, err)
	}
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(floor, floor+1, 2, 0)}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{
		MaxObjectsPerRun: int(expired) + 1,
		MaxDeletesPerRun: int(expired) + 1,
		MaxDeleteBytes:   ^uint64(0),
	})
	return r, backend
}

func newSyntheticScrubber(t testing.TB, startLSN, objects uint64) (*Reclaimer, *syntheticSegmentBackend) {
	t.Helper()
	layout := segmentsink.NewLayout("root")
	backend := &syntheticSegmentBackend{
		Store: blobmemory.New(), layout: layout, startLSN: startLSN,
		retainedLSN: startLSN + objects - 1, writerEpoch: 1,
	}
	clock := newFakeClock(time.Unix(1_800_000_000, 0).UTC())
	snapshot := maintenanceSnapshot(startLSN, startLSN, 2, 0)
	snapshot.Generation = 10
	catalog := &fakeCatalog{snapshot: snapshot}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{
		DeleteDelay:      time.Millisecond,
		MaxObjectsPerRun: int(objects),
		MaxDeletesPerRun: int(objects) + 1,
		MaxDeleteBytes:   ^uint64(0),
	})
	return r, backend
}

// syntheticSegmentBackend models an object-store ordered index without
// allocating the complete retained history. It makes the benchmark sensitive
// to the requested lower bound and LIST page count, not an in-memory map scan.
type syntheticSegmentBackend struct {
	*blobmemory.Store

	mu           sync.Mutex
	layout       segmentsink.Layout
	startLSN     uint64
	retainedLSN  uint64
	writerEpoch  uint64
	segmentLists []blobstore.ListOptions
	deleted      uint64
}

func (b *syntheticSegmentBackend) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	segmentPrefix := b.layout.SegmentPrefix(testStreamID, 7)
	if opts.Prefix != segmentPrefix {
		return blobstore.ObjectPage{}, nil
	}
	b.mu.Lock()
	b.segmentLists = append(b.segmentLists, opts)
	b.mu.Unlock()

	next := b.startLSN
	if opts.AfterKey != segmentPrefix && opts.AfterKey != b.layout.SegmentLowerBound(testStreamID, 7, b.startLSN) {
		parsed, err := b.layout.ParseSegmentKey(testStreamID, 7, opts.AfterKey)
		if err != nil {
			return blobstore.ObjectPage{}, fmt.Errorf("synthetic list after key: %w", err)
		}
		next = parsed.BaseLSN + 1
	}
	if next > b.retainedLSN {
		return blobstore.ObjectPage{}, nil
	}

	limit := opts.NormalizedLimit()
	remaining := b.retainedLSN - next + 1
	count := min(uint64(limit), remaining)
	objects := make([]blobstore.ObjectInfo, 0, count)
	for i := uint64(0); i < count; i++ {
		baseLSN := next + i
		key := b.layout.SegmentKey(plwriter.SegmentInfo{
			StreamID: testStreamID, Partition: 7, BaseLSN: baseLSN,
			WriterEpoch: b.writerEpoch, SegmentUUID: [16]byte{1},
		})
		objects = append(objects, blobstore.ObjectInfo{
			Key: key, SizeBytes: 1 << 20, CreatedAt: time.Unix(1, 0).UTC(),
		})
	}
	hasMore := next+count <= b.retainedLSN
	page := blobstore.ObjectPage{Objects: objects, HasMore: hasMore}
	if hasMore {
		page.NextAfterKey = objects[len(objects)-1].Key
	}
	return page, nil
}

func (b *syntheticSegmentBackend) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := b.layout.ParseSegmentKey(testStreamID, 7, key); err != nil {
		return b.Store.Delete(ctx, key)
	}
	b.mu.Lock()
	b.deleted++
	b.mu.Unlock()
	return nil
}
