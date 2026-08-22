package reader

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func TestCursorCheckpointRoundTripAndResume(t *testing.T) {
	t.Parallel()

	cat := &checkpointCatalog{head: pmeta.PartitionHead{
		StreamID:  "hosts/host-a/events",
		Partition: 7,
		OldestLSN: 10,
		NextLSN:   20,
	}}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(7)
	cursor, err := partition.Cursor(CursorOptions{StartLSN: 12, Limit: 4})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}

	checkpoint, err := cursor.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	want := CursorCheckpoint{
		Version:   CursorCheckpointVersion,
		StreamID:  "hosts/host-a/events",
		Partition: 7,
		NextLSN:   12,
	}
	if checkpoint != want {
		t.Fatalf("Checkpoint() = %+v, want %+v", checkpoint, want)
	}
	body, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var decoded CursorCheckpoint
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded != want {
		t.Fatalf("decoded checkpoint = %+v, want %+v", decoded, want)
	}

	resumed, err := partition.ResumeCursor(context.Background(), decoded, CursorResumeOptions{Limit: 8})
	if err != nil {
		t.Fatalf("ResumeCursor() error = %v", err)
	}
	if resumed.Position() != 12 || resumed.limit != 8 || resumed.streamID != want.StreamID || !resumed.bound {
		t.Fatalf("resumed cursor = %+v", resumed)
	}
	if cat.Loads() != 2 {
		t.Fatalf("catalog loads = %d, want 2", cat.Loads())
	}
}

func TestResumeCursorValidatesCheckpoint(t *testing.T) {
	t.Parallel()

	head := pmeta.PartitionHead{
		StreamID:  "stream-a",
		Partition: 3,
		OldestLSN: 10,
		NextLSN:   20,
	}
	tests := []struct {
		name       string
		checkpoint CursorCheckpoint
		wantErr    error
		wantLoads  int
	}{
		{
			name:       "unsupported version",
			checkpoint: CursorCheckpoint{Version: 99, StreamID: "stream-a", Partition: 3, NextLSN: 10},
			wantErr:    ErrCheckpointInvalid,
		},
		{
			name:       "wrong partition",
			checkpoint: CursorCheckpoint{Version: CursorCheckpointVersion, StreamID: "stream-a", Partition: 4, NextLSN: 10},
			wantErr:    ErrCheckpointMismatch,
		},
		{
			name:       "wrong stream",
			checkpoint: CursorCheckpoint{Version: CursorCheckpointVersion, StreamID: "stream-b", Partition: 3, NextLSN: 10},
			wantErr:    ErrCheckpointMismatch,
			wantLoads:  1,
		},
		{
			name:       "expired position",
			checkpoint: CursorCheckpoint{Version: CursorCheckpointVersion, StreamID: "stream-a", Partition: 3, NextLSN: 9},
			wantErr:    ErrLSNExpired,
			wantLoads:  1,
		},
		{
			name:       "ahead of head",
			checkpoint: CursorCheckpoint{Version: CursorCheckpointVersion, StreamID: "stream-a", Partition: 3, NextLSN: 21},
			wantErr:    ErrCheckpointAhead,
			wantLoads:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cat := &checkpointCatalog{head: head}
			r, err := New(cat, newTestSegmentStore(nil), Options{})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_, err = r.Partition(3).ResumeCursor(context.Background(), test.checkpoint, CursorResumeOptions{})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ResumeCursor() error = %v, want %v", err, test.wantErr)
			}
			if cat.Loads() != test.wantLoads {
				t.Fatalf("catalog loads = %d, want %d", cat.Loads(), test.wantLoads)
			}
		})
	}
}

func TestCursorNextBindsIdentityAndForkPreservesIt(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	fixture.appendSegment(t, 0, 2)
	r := fixture.openReader(t, Options{MaxRecordsPerBatch: 1})
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{Limit: 1})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if !cursor.bound || cursor.Position() != 1 {
		t.Fatalf("cursor bound=%v position=%d, want true/1", cursor.bound, cursor.Position())
	}

	fork := cursor.Fork()
	if !fork.bound || fork.streamID != cursor.streamID || fork.Position() != cursor.Position() {
		t.Fatalf("fork = %+v, cursor = %+v", fork, cursor)
	}
	fork.Seek(2)
	if cursor.Position() != 1 {
		t.Fatalf("fork Seek changed original position to %d", cursor.Position())
	}
}

func TestCachedReadRechecksRetentionFloorOnlyOnAnomaly(t *testing.T) {
	t.Parallel()

	retained := validSegmentRef(3, 10, 19)
	cat := &retentionRaceCatalog{
		heads: []pmeta.PartitionHead{
			{
				StreamID:       "stream-a",
				Partition:      3,
				OldestLSN:      0,
				NextLSN:        20,
				HasLastSegment: true,
				LastSegment:    validSegmentRef(3, 0, 19),
			},
			{
				StreamID:       "stream-a",
				Partition:      3,
				OldestLSN:      10,
				NextLSN:        20,
				HasLastSegment: true,
				LastSegment:    retained,
			},
		},
		page: pmeta.SegmentPage{Segments: []pmeta.SegmentRef{retained}},
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(3)
	if _, err := partition.Head(context.Background()); err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	_, err = partition.Read(context.Background(), ReadRequest{
		StartLSN:  5,
		Limit:     1,
		Freshness: FreshnessCached,
	})
	if !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Read() error = %v, want %v", err, ErrLSNExpired)
	}
	var expired LSNExpiredError
	if !errors.As(err, &expired) || expired.Requested != 5 || expired.Oldest != 10 || expired.HeadNext != 20 {
		t.Fatalf("expired = %+v", expired)
	}
	if cat.Loads() != 2 {
		t.Fatalf("catalog loads = %d, want initial plus anomaly refresh", cat.Loads())
	}
}

func TestFetchRechecksRetentionFloorOnCatalogMiss(t *testing.T) {
	t.Parallel()

	cat := &retentionRaceCatalog{
		heads: []pmeta.PartitionHead{
			{
				StreamID:       "stream-a",
				Partition:      3,
				OldestLSN:      0,
				NextLSN:        20,
				HasLastSegment: true,
				LastSegment:    validSegmentRef(3, 0, 19),
			},
			{
				StreamID:       "stream-a",
				Partition:      3,
				OldestLSN:      10,
				NextLSN:        20,
				HasLastSegment: true,
				LastSegment:    validSegmentRef(3, 10, 19),
			},
		},
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = r.Fetch(context.Background(), FetchRequest{Partition: 3, LSN: 5})
	if !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Fetch() error = %v, want %v", err, ErrLSNExpired)
	}
	var expired LSNExpiredError
	if !errors.As(err, &expired) || expired.Requested != 5 || expired.Oldest != 10 || expired.HeadNext != 20 {
		t.Fatalf("expired = %+v", expired)
	}
	if cat.Loads() != 2 {
		t.Fatalf("catalog loads = %d, want initial plus anomaly refresh", cat.Loads())
	}
}

func TestExpiredReadsDoNotRefreshTwice(t *testing.T) {
	t.Parallel()

	cat := &checkpointCatalog{head: pmeta.PartitionHead{
		StreamID:  "stream-a",
		Partition: 3,
		OldestLSN: 10,
		NextLSN:   20,
	}}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := r.Fetch(context.Background(), FetchRequest{Partition: 3, LSN: 9}); !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Fetch() error = %v, want %v", err, ErrLSNExpired)
	}
	if cat.Loads() != 1 {
		t.Fatalf("Fetch() catalog loads = %d, want 1", cat.Loads())
	}

	cat.mu.Lock()
	cat.loads = 0
	cat.mu.Unlock()
	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: 3, StartLSN: 9, Limit: 1}); !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrLSNExpired)
	}
	if cat.Loads() != 1 {
		t.Fatalf("Consume() catalog loads = %d, want 1", cat.Loads())
	}
}

func TestCachedReadRetriesOnceAfterCatalogTopologyChanges(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	head, err := fixture.catalog.LoadPartition(context.Background(), fixture.partition)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	cat := &topologyRetryCatalog{
		head:             head,
		segment:          segment,
		listFailuresLeft: 1,
		listErr:          errors.New("stale leaf page"),
	}
	r, err := New(cat, fixture.store, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(fixture.partition)
	if _, err := partition.Head(context.Background()); err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	result, err := partition.Read(context.Background(), ReadRequest{
		StartLSN:  0,
		Limit:     1,
		Freshness: FreshnessCached,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[:1])
	loads, lists, _, _ := cat.Calls()
	if loads != 2 || lists != 2 {
		t.Fatalf("catalog calls = loads:%d lists:%d, want loads:2 lists:2", loads, lists)
	}
}

func TestFetchRetriesOnceAfterCatalogTopologyChanges(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	head, err := fixture.catalog.LoadPartition(context.Background(), fixture.partition)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	cat := &topologyRetryCatalog{
		head:           head,
		segment:        segment,
		findMissesLeft: 1,
	}
	r, err := New(cat, fixture.store, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := r.Fetch(context.Background(), FetchRequest{Partition: fixture.partition, LSN: 2})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !result.Found {
		t.Fatal("Fetch() found=false, want true")
	}
	assertRecordsEqual(t, []Record{result.Record}, fixture.records[2:3])
	loads, _, finds, _ := cat.Calls()
	if loads != 2 || finds != 2 {
		t.Fatalf("catalog calls = loads:%d finds:%d, want loads:2 finds:2", loads, finds)
	}
}

func TestTimestampReadRetriesOnceAfterCatalogTopologyChanges(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 5)
	head, err := fixture.catalog.LoadPartition(context.Background(), fixture.partition)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	cat := &topologyRetryCatalog{
		head:               head,
		segment:            segment,
		lookupFailuresLeft: 1,
		lookupErr:          errors.New("stale timestamp index page"),
	}
	r, err := New(cat, fixture.store, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := r.ConsumeFromTimestamp(context.Background(), ConsumeFromTimestampRequest{
		Partition:   fixture.partition,
		TimestampMS: fixture.records[2].TimestampMS,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("ConsumeFromTimestamp() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[2:3])
	loads, lists, _, lookups := cat.Calls()
	if loads != 1 || lists != 1 || lookups != 2 {
		t.Fatalf("catalog calls = loads:%d lists:%d lookups:%d, want loads:1 lists:1 lookups:2", loads, lists, lookups)
	}
}

func TestCachedReadRetriesCatalogAnomalyAtMostOnce(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("catalog page unavailable")
	cat := &topologyRetryCatalog{
		head: pmeta.PartitionHead{
			StreamID:       "stream-a",
			Partition:      3,
			NextLSN:        10,
			HasLastSegment: true,
			LastSegment:    validSegmentRef(3, 0, 9),
		},
		listFailuresLeft: 2,
		listErr:          wantErr,
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(3)
	if _, err := partition.Head(context.Background()); err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	_, err = partition.Read(context.Background(), ReadRequest{
		StartLSN:  0,
		Limit:     1,
		Freshness: FreshnessCached,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Read() error = %v, want %v", err, wantErr)
	}
	loads, lists, _, _ := cat.Calls()
	if loads != 2 || lists != 2 {
		t.Fatalf("catalog calls = loads:%d lists:%d, want loads:2 lists:2", loads, lists)
	}
}

func TestCachedReadAtTailDoesNotRefresh(t *testing.T) {
	t.Parallel()

	cat := &checkpointCatalog{head: pmeta.PartitionHead{
		StreamID:  "stream-a",
		Partition: 3,
		NextLSN:   10,
	}}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(3)
	if _, err := partition.Head(context.Background()); err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if _, err := partition.Read(context.Background(), ReadRequest{
		StartLSN:  10,
		Limit:     1,
		Freshness: FreshnessCached,
	}); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if cat.Loads() != 1 {
		t.Fatalf("catalog loads = %d, want 1", cat.Loads())
	}
}

type checkpointCatalog struct {
	mu    sync.Mutex
	head  pmeta.PartitionHead
	loads int
}

func (c *checkpointCatalog) LoadPartition(_ context.Context, _ uint32) (pmeta.PartitionHead, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	return c.head, nil
}

func (c *checkpointCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *checkpointCatalog) LookupTimestamp(context.Context, catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	return catalog.TimestampLookupResult{Head: c.head}, nil
}

func (c *checkpointCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return pmeta.SegmentPage{}, nil
}

func (c *checkpointCatalog) Loads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}

type retentionRaceCatalog struct {
	mu    sync.Mutex
	heads []pmeta.PartitionHead
	loads int
	page  pmeta.SegmentPage
}

type topologyRetryCatalog struct {
	mu sync.Mutex

	head    pmeta.PartitionHead
	segment pmeta.SegmentRef

	listFailuresLeft   int
	listErr            error
	findMissesLeft     int
	lookupFailuresLeft int
	lookupErr          error

	loads   int
	lists   int
	finds   int
	lookups int
}

func (c *topologyRetryCatalog) LoadPartition(_ context.Context, _ uint32) (pmeta.PartitionHead, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	return c.head, nil
}

func (c *topologyRetryCatalog) FindSegment(_ context.Context, _ uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finds++
	if c.findMissesLeft > 0 {
		c.findMissesLeft--
		return pmeta.SegmentRef{}, false, nil
	}
	if lsn < c.segment.BaseLSN || lsn > c.segment.LastLSN {
		return pmeta.SegmentRef{}, false, nil
	}
	return c.segment, true, nil
}

func (c *topologyRetryCatalog) LookupTimestamp(_ context.Context, req catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups++
	if c.lookupFailuresLeft > 0 {
		c.lookupFailuresLeft--
		return catalog.TimestampLookupResult{}, c.lookupErr
	}
	result := catalog.TimestampLookupResult{Head: c.head}
	if c.segment.MaxTimestampMS >= req.TimestampMS {
		result.Segment = c.segment
		result.Found = true
	}
	return result, nil
}

func (c *topologyRetryCatalog) ListSegments(_ context.Context, req catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists++
	if c.listFailuresLeft > 0 {
		c.listFailuresLeft--
		return pmeta.SegmentPage{}, c.listErr
	}
	if req.FromLSN > c.segment.LastLSN {
		return pmeta.SegmentPage{}, nil
	}
	return pmeta.SegmentPage{Segments: []pmeta.SegmentRef{c.segment}}, nil
}

func (c *topologyRetryCatalog) Calls() (loads int, lists int, finds int, lookups int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.lists, c.finds, c.lookups
}

func (c *retentionRaceCatalog) LoadPartition(_ context.Context, _ uint32) (pmeta.PartitionHead, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	index := c.loads
	if index >= len(c.heads) {
		index = len(c.heads) - 1
	}
	c.loads++
	return c.heads[index], nil
}

func (c *retentionRaceCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *retentionRaceCatalog) LookupTimestamp(context.Context, catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	index := c.loads
	if index >= len(c.heads) {
		index = len(c.heads) - 1
	}
	return catalog.TimestampLookupResult{Head: c.heads[index]}, nil
}

func (c *retentionRaceCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.page.Clone(), nil
}

func (c *retentionRaceCatalog) Loads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}
