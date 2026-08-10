package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	blobmemory "github.com/ankur-anand/unijord/internal/blobstore/memory"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

const testStreamID = "hosts/test/events"

func TestReclaimerRetainsUntilDelayedFloorThenDeletesSegmentsPagesAndStaleStaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC().Add(48 * time.Hour))
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(200, 300, 2, 1)}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})

	segmentKeys := putSegments(t, backend, layout, 0, 100, 200)
	pageID := "0123456789abcdef0123456789abcdef"
	pageKeys := []string{
		catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 0, 99, 1, pageID),
		catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 100, 199, 2, pageID),
		catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 200, 299, 3, pageID),
		catalogblob.IndexPagePath("root/catalog", testStreamID, 7, 1, 0, 199, 2, pageID),
		catalogblob.IndexPagePath("root/catalog", testStreamID, 7, 1, 200, 299, 3, pageID),
	}
	putKeys(t, backend, pageKeys)
	staleStaging := stagingKey(layout, 250, 1, 1)
	currentStaging := stagingKey(layout, 300, 2, 2)
	putKeys(t, backend, []string{staleStaging, currentStaging})

	first, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	if first.SafeFloorLSN != 0 || !first.HasMore {
		t.Fatalf("first result = %+v, want pending floor", first)
	}
	assertExists(t, backend, segmentKeys...)
	assertExists(t, backend, pageKeys...)
	assertMissing(t, backend, staleStaging)
	assertExists(t, backend, currentStaging)

	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	second, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(reclaim) error = %v", err)
	}
	if second.SafeFloorLSN != 200 || second.ReclaimedThroughLSN != 200 || second.DeletedObjects != 5 || second.HasMore {
		t.Fatalf("second result = %+v", second)
	}
	assertMissing(t, backend, segmentKeys[0], segmentKeys[1], pageKeys[0], pageKeys[1], pageKeys[3])
	assertExists(t, backend, segmentKeys[2], pageKeys[2], pageKeys[4], currentStaging)
}

func TestReclaimerResumesAfterDeleteFailureUsingLastObjectKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memory := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(300, 400, 2, 0)}
	keys := putSegments(t, memory, layout, 0, 100, 200, 300)
	backend := &faultBackend{Store: memory, failDeleteKey: keys[1], failDeleteCount: 1}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{ListPageSize: 4})

	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	if _, err := r.RunPartition(ctx, 7); err == nil || err.Error() != "injected delete failure" {
		t.Fatalf("RunPartition(failure) error = %v", err)
	}
	assertMissing(t, memory, keys[0])
	assertExists(t, memory, keys[1], keys[2], keys[3])

	backend.mu.Lock()
	backend.listCalls = nil
	backend.mu.Unlock()
	result, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(resume) error = %v", err)
	}
	if result.ReclaimedThroughLSN != 300 {
		t.Fatalf("resume result = %+v", result)
	}
	backend.mu.Lock()
	firstAfter := backend.listCalls[0].AfterKey
	backend.mu.Unlock()
	if firstAfter != keys[0] {
		t.Fatalf("resume AfterKey = %q, want deleted checkpoint %q", firstAfter, keys[0])
	}
	assertMissing(t, memory, keys[1], keys[2])
	assertExists(t, memory, keys[3])
}

func TestReclaimerRechecksHeadBeforeDeleting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(200, 300, 2, 0)}
	keys := putSegments(t, backend, layout, 0, 100, 200)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})
	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	catalog.onLoad = func(load int, snapshot *catalogblob.MaintenanceSnapshot) {
		if load >= 3 {
			snapshot.Head.OldestLSN = 100
		}
	}
	if _, err := r.RunPartition(ctx, 7); err == nil {
		t.Fatal("RunPartition(regressed head) error = nil")
	}
	assertExists(t, backend, keys...)
}

func TestReclaimerAcceptsWriterAdvancingHeadDuringDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(200, 300, 2, 0)}
	keys := putSegments(t, backend, layout, 0, 100, 200, 300)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})
	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	catalog.onLoad = func(load int, snapshot *catalogblob.MaintenanceSnapshot) {
		if load >= 3 {
			snapshot.Head.OldestLSN = 300
			snapshot.Head.NextLSN = 400
		}
	}
	if _, err := r.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(advanced head) error = %v", err)
	}
	assertMissing(t, backend, keys[0], keys[1])
	assertExists(t, backend, keys[2], keys[3])
}

func TestReclaimerDryRunDoesNotAdvanceOrDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(200, 300, 2, 0)}
	keys := putSegments(t, backend, layout, 0, 100, 200)
	observer := newTestReclaimer(t, backend, catalog, layout, clock, Options{})
	if _, err := observer.RunPartition(ctx, 7); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{DryRun: true, OwnerID: [16]byte{2}})
	result, err := r.RunPartition(ctx, 7)
	if err != nil {
		t.Fatalf("RunPartition(dry reclaim) error = %v", err)
	}
	if result.DeletedObjects != 0 || result.CandidateObjects != 2 || result.SafeFloorLSN != 200 {
		t.Fatalf("dry result = %+v", result)
	}
	assertExists(t, backend, keys...)
	stateObject, err := backend.Get(ctx, catalogblob.GCStatePath("root/catalog", testStreamID, 7))
	if err != nil {
		t.Fatalf("Get(state) error = %v", err)
	}
	state, err := decodeState(stateObject.Body, testStreamID, 7)
	if err != nil {
		t.Fatalf("decodeState() error = %v", err)
	}
	if state.SafeFloorLSN != 0 || !state.HasPendingFloor {
		t.Fatalf("persisted state after dry run = %+v", state)
	}
}

func TestReclaimerRejectsConcurrentLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC())
	catalog := &fakeCatalog{snapshot: maintenanceSnapshot(0, 0, 1, 0)}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})
	state := stateFile{
		Version: stateVersion, StreamID: testStreamID, Partition: 7,
		OwnerID: "ffffffffffffffffffffffffffffffff", LeaseUntilMS: clock.Now().Add(time.Hour).UnixMilli(),
		UpdatedMS: clock.Now().UnixMilli(),
	}
	body, err := marshalState(state, testStreamID, 7)
	if err != nil {
		t.Fatalf("marshalState() error = %v", err)
	}
	if _, swapped, err := backend.CompareAndSwap(ctx, catalogblob.GCStatePath("root/catalog", testStreamID, 7), "", body); err != nil || !swapped {
		t.Fatalf("seed state swapped=%v error=%v", swapped, err)
	}
	if _, err := r.RunPartition(ctx, 7); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("RunPartition() error = %v, want %v", err, ErrLeaseHeld)
	}
}

func TestScrubPartitionDeletesOnlyProvenSegmentOrphansAndQuarantinedPages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := blobmemory.New()
	layout := segmentsink.NewLayout("root")
	clock := newFakeClock(time.Now().UTC().Add(48 * time.Hour))
	snapshot := maintenanceSnapshot(0, 200, 2, 0)
	snapshot.Generation = 10
	catalog := &fakeCatalog{
		snapshot:       snapshot,
		segments:       make(map[uint64]pmeta.SegmentRef),
		reachablePages: make(map[string]bool),
	}
	r := newTestReclaimer(t, backend, catalog, layout, clock, Options{})

	committed := putSegmentInfo(t, backend, layout, plwriter.SegmentInfo{
		StreamID: testStreamID, Partition: 7, BaseLSN: 0, WriterEpoch: 1, SegmentUUID: [16]byte{1},
	})
	orphanAtCommittedBase := putSegmentInfo(t, backend, layout, plwriter.SegmentInfo{
		StreamID: testStreamID, Partition: 7, BaseLSN: 0, WriterEpoch: 1, SegmentUUID: [16]byte{9},
	})
	inFlight := putSegmentInfo(t, backend, layout, plwriter.SegmentInfo{
		StreamID: testStreamID, Partition: 7, BaseLSN: 200, WriterEpoch: 2, SegmentUUID: [16]byte{2},
	})
	fencedFuture := putSegmentInfo(t, backend, layout, plwriter.SegmentInfo{
		StreamID: testStreamID, Partition: 7, BaseLSN: 300, WriterEpoch: 1, SegmentUUID: [16]byte{3},
	})
	catalog.segments[0] = pmeta.SegmentRef{URI: committed, BaseLSN: 0, LastLSN: 99}

	pageID := "0123456789abcdef0123456789abcdef"
	reachablePage := catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 0, 99, 8, pageID)
	orphanPage := catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 100, 199, 9, pageID)
	currentPage := catalogblob.LeafPagePath("root/catalog", testStreamID, 7, 200, 299, 10, pageID)
	putKeys(t, backend, []string{reachablePage, orphanPage, currentPage})
	catalog.reachablePages[reachablePage] = true

	first, err := r.ScrubPartition(ctx, 7)
	if err != nil {
		t.Fatalf("ScrubPartition(discover) error = %v", err)
	}
	if first.DeletedObjects != 2 || first.QuarantinedObjects != 1 || first.PendingQuarantine != 1 {
		t.Fatalf("first result = %+v", first)
	}
	assertExists(t, backend, committed, inFlight, reachablePage, orphanPage, currentPage)
	assertMissing(t, backend, orphanAtCommittedBase, fencedFuture)

	clock.Advance(DefaultDeleteDelay + time.Millisecond)
	second, err := r.ScrubPartition(ctx, 7)
	if err != nil {
		t.Fatalf("ScrubPartition(recheck) error = %v", err)
	}
	if second.DeletedObjects != 1 || second.PendingQuarantine != 0 {
		t.Fatalf("second result = %+v", second)
	}
	assertMissing(t, backend, orphanPage)
	assertExists(t, backend, committed, inFlight, reachablePage, currentPage)
}

func newTestReclaimer(t testing.TB, backend Backend, catalog Catalog, layout segmentsink.Layout, clock *fakeClock, extra Options) *Reclaimer {
	t.Helper()
	extra.StreamID = testStreamID
	extra.CatalogPrefix = "root/catalog"
	if extra.OwnerID == ([16]byte{}) {
		extra.OwnerID = [16]byte{1}
	}
	r, err := newReclaimer(backend, catalog, layout, extra, clock.Now)
	if err != nil {
		t.Fatalf("newReclaimer() error = %v", err)
	}
	return r
}

func maintenanceSnapshot(oldest, next, epoch uint64, maxLevel uint8) catalogblob.MaintenanceSnapshot {
	return catalogblob.MaintenanceSnapshot{
		Head: pmeta.PartitionHead{
			StreamID: testStreamID, Partition: 7, OldestLSN: oldest, NextLSN: next, WriterEpoch: epoch,
			AppliedRetentionVersion: 1,
		},
		Generation: 10, MaxIndexLevel: maxLevel,
	}
}

func putSegments(t testing.TB, backend interface {
	Put(context.Context, string, []byte) (blobstore.Object, error)
}, layout segmentsink.Layout, bases ...uint64) []string {
	t.Helper()
	keys := make([]string, 0, len(bases))
	for i, base := range bases {
		info := plwriter.SegmentInfo{
			StreamID: testStreamID, Partition: 7, BaseLSN: base, WriterEpoch: 1, SegmentUUID: [16]byte{byte(i + 1)},
		}
		key := layout.SegmentKey(info)
		if _, err := backend.Put(context.Background(), key, []byte(fmt.Sprintf("segment-%d", base))); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

func putSegmentInfo(t testing.TB, backend interface {
	Put(context.Context, string, []byte) (blobstore.Object, error)
}, layout segmentsink.Layout, info plwriter.SegmentInfo) string {
	t.Helper()
	key := layout.SegmentKey(info)
	if _, err := backend.Put(context.Background(), key, []byte(key)); err != nil {
		t.Fatalf("Put(%q) error = %v", key, err)
	}
	return key
}

func stagingKey(layout segmentsink.Layout, base, epoch uint64, id byte) string {
	info := plwriter.SegmentInfo{
		StreamID: testStreamID, Partition: 7, BaseLSN: base, WriterEpoch: epoch, SegmentUUID: [16]byte{id},
	}
	return layout.StagingPrefix(info) + "/part-000001"
}

func putKeys(t testing.TB, backend interface {
	Put(context.Context, string, []byte) (blobstore.Object, error)
}, keys []string) {
	t.Helper()
	for _, key := range keys {
		if _, err := backend.Put(context.Background(), key, []byte(key)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}
}

func assertExists(t testing.TB, backend interface {
	Get(context.Context, string) (blobstore.Object, error)
}, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := backend.Get(context.Background(), key); err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
	}
}

func assertMissing(t testing.TB, backend interface {
	Get(context.Context, string) (blobstore.Object, error)
}, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := backend.Get(context.Background(), key); !errors.Is(err, blobstore.ErrObjectNotFound) {
			t.Fatalf("Get(%q) error = %v, want not found", key, err)
		}
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type fakeCatalog struct {
	mu             sync.Mutex
	snapshot       catalogblob.MaintenanceSnapshot
	loads          int
	onLoad         func(load int, snapshot *catalogblob.MaintenanceSnapshot)
	segments       map[uint64]pmeta.SegmentRef
	reachablePages map[string]bool
}

func (c *fakeCatalog) LoadMaintenanceSnapshot(context.Context, uint32) (catalogblob.MaintenanceSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	snapshot := c.snapshot
	if c.onLoad != nil {
		c.onLoad(c.loads, &snapshot)
	}
	return snapshot, nil
}

func (c *fakeCatalog) FindSegment(_ context.Context, _ uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, segment := range c.segments {
		if lsn >= segment.BaseLSN && lsn <= segment.LastLSN {
			return segment, true, nil
		}
	}
	return pmeta.SegmentRef{}, false, nil
}

func (c *fakeCatalog) IsPageReachable(_ context.Context, _ uint32, path string) (catalogblob.MaintenanceSnapshot, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot, c.reachablePages[path], nil
}

type faultBackend struct {
	*blobmemory.Store
	mu              sync.Mutex
	failDeleteKey   string
	failDeleteCount int
	listCalls       []blobstore.ListOptions
}

func (b *faultBackend) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	b.mu.Lock()
	b.listCalls = append(b.listCalls, opts)
	b.mu.Unlock()
	return b.Store.List(ctx, opts)
}

func (b *faultBackend) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	if key == b.failDeleteKey && b.failDeleteCount > 0 {
		b.failDeleteCount--
		b.mu.Unlock()
		return errors.New("injected delete failure")
	}
	b.mu.Unlock()
	return b.Store.Delete(ctx, key)
}
