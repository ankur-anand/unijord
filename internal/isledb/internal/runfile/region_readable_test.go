package runfile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage"
)

func TestRegionReadableBoundsAndReadBefore(t *testing.T) {
	object := append(bytes.Repeat([]byte{0xaa}, 16), []byte("0123456789abcdef")...)
	object = append(object, bytes.Repeat([]byte{0xbb}, 16)...)
	source := &memoryRangeSource{data: object}
	readable, err := NewRegionReadable(source, "run", int64(len(object)), RegionDescriptor{
		Kind:   RegionKindEventsSST,
		Offset: 16,
		Length: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 4)
	if err := readable.ReadAt(context.Background(), data, 12); err != nil || string(data) != "cdef" {
		t.Fatalf("exact-end read data=%q err=%v", data, err)
	}
	requestCount := len(source.requests)
	if err := readable.ReadAt(context.Background(), nil, 16); err != nil {
		t.Fatalf("empty exact-end read: %v", err)
	}
	if len(source.requests) != requestCount {
		t.Fatal("empty exact-end read reached the range provider")
	}
	for _, offset := range []int64{-1, 13, 16} {
		if err := readable.ReadAt(context.Background(), make([]byte, 4), offset); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("offset=%d error=%v", offset, err)
		}
	}

	handle := readable.NewReadHandle(objstorage.ReadBeforeSize(12))
	if err := handle.ReadAt(context.Background(), data, 10); err != nil || string(data) != "abcd" {
		t.Fatalf("read-before data=%q err=%v", data, err)
	}
	last := source.requests[len(source.requests)-1]
	if last != [2]int64{18, 12} {
		t.Fatalf("physical read-before range=%v, want [18 12]", last)
	}
	if err := handle.ReadAt(context.Background(), data, 6); err != nil || string(data) != "6789" {
		t.Fatalf("buffered read data=%q err=%v", data, err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.ReadAt(context.Background(), data, 0); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed handle error=%v", err)
	}
	for _, request := range source.requests {
		if request[0] < 16 || request[0]+request[1] > 32 {
			t.Fatalf("request crossed sibling boundary: %v", request)
		}
	}
}

func TestRegionReadableRejectsShortSuccessfulProviderRead(t *testing.T) {
	source := &memoryRangeSource{data: bytes.Repeat([]byte{1}, 64), short: true}
	readable, err := NewRegionReadable(source, "run", 64, RegionDescriptor{Kind: RegionKindHeadsSST, Offset: 8, Length: 32})
	if err != nil {
		t.Fatal(err)
	}
	if err := readable.ReadAt(context.Background(), make([]byte, 4), 0); !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corrupt unexpected EOF", err)
	}
}

func TestRegionReadablePreservesTransportError(t *testing.T) {
	transport := errors.New("temporary object-store failure")
	readable, err := NewRegionReadable(
		&errorRangeSource{err: transport},
		"run",
		64,
		RegionDescriptor{Kind: RegionKindEventsSST, Offset: 8, Length: 32},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = readable.ReadAt(context.Background(), make([]byte, 4), 0)
	if !errors.Is(err, transport) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("transport error classification=%v", err)
	}
}

func TestRegionReadableRejectsInvalidGeometryAndOverflow(t *testing.T) {
	source := &memoryRangeSource{data: bytes.Repeat([]byte{1}, 64)}
	tests := []struct {
		name       string
		objectSize int64
		region     RegionDescriptor
		want       error
	}{
		{name: "negative object", objectSize: -1, region: RegionDescriptor{Kind: RegionKindEventsSST, Length: 1}, want: ErrInvalidRun},
		{name: "large object", objectSize: int64(MaxRunObjectBytes + 1), region: RegionDescriptor{Kind: RegionKindEventsSST, Length: 1}, want: ErrRunTooLarge},
		{name: "zero length", objectSize: 64, region: RegionDescriptor{Kind: RegionKindEventsSST, Offset: 8}, want: ErrInvalidRun},
		{name: "past object", objectSize: 64, region: RegionDescriptor{Kind: RegionKindEventsSST, Offset: 56, Length: 16}, want: ErrInvalidRun},
		{name: "unsigned overflow", objectSize: 64, region: RegionDescriptor{Kind: RegionKindEventsSST, Offset: ^uint64(0) - 3, Length: 8}, want: ErrInvalidRun},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRegionReadable(source, "run", test.objectSize, test.region); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}

	readable, err := NewRegionReadable(source, "run", 64, RegionDescriptor{
		Kind: RegionKindEventsSST, Offset: 8, Length: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	requestCount := len(source.requests)
	if err := readable.ReadAt(context.Background(), make([]byte, 4), math.MaxInt64-1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("overflowing logical read error=%v", err)
	}
	if len(source.requests) != requestCount {
		t.Fatal("out-of-bounds logical read reached the range provider")
	}
}

func TestRegionReadableCacheKeyIncludesRunID(t *testing.T) {
	object := append(bytes.Repeat([]byte{0xaa}, 8), []byte("first-value-rest")...)
	source := &memoryRangeSource{data: object}
	cache := newRegionTestCache()
	region := RegionDescriptor{Kind: RegionKindEventsSST, Offset: 8, Length: 16}
	firstID := testRunID()
	secondID := firstID
	secondID[0]++

	first, err := NewRegionReadable(source, "run-1", int64(len(object)), region, RegionReadOptions{RunID: firstID, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRegionReadable(source, "run-2", int64(len(object)), region, RegionReadOptions{RunID: secondID, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 5)
	if err := first.ReadAt(context.Background(), data, 0); err != nil || string(data) != "first" {
		t.Fatalf("first run read data=%q err=%v", data, err)
	}
	data[0] = 'X'
	if err := first.ReadAt(context.Background(), data, 0); err != nil || string(data) != "first" {
		t.Fatalf("first run cache hit data=%q err=%v", data, err)
	}
	if len(source.requests) != 1 {
		t.Fatalf("first run provider requests=%d, want one cached load", len(source.requests))
	}
	copy(source.data[8:13], "other")
	if err := second.ReadAt(context.Background(), data, 0); err != nil || string(data) != "other" {
		t.Fatalf("second run read data=%q err=%v", data, err)
	}
	if len(source.requests) != 2 {
		t.Fatalf("provider requests=%d, want 2 isolated run loads", len(source.requests))
	}
	if cache.len() != 2 {
		t.Fatalf("cache entries=%d, want one per run ID", cache.len())
	}
	if regionCacheKey(firstID, region.Kind, 0, 5) == regionCacheKey(secondID, region.Kind, 0, 5) {
		t.Fatal("different run IDs produced the same cache key")
	}
}

func TestRegionReadableConcurrentSiblingReadsStayIsolated(t *testing.T) {
	object := append(bytes.Repeat([]byte{'p'}, 8), bytes.Repeat([]byte{'E'}, 16)...)
	object = append(object, bytes.Repeat([]byte{'H'}, 16)...)
	object = append(object, bytes.Repeat([]byte{'D'}, 16)...)
	source := newGatedRangeSource(object)
	cache := newRegionTestCache()
	loads := &RegionLoadGroup{}
	defer loads.Close(nil)
	defer close(source.release)
	options := RegionReadOptions{RunID: testRunID(), Cache: cache, Coalescer: loads}
	events, err := NewRegionReadable(source, "run", int64(len(object)), RegionDescriptor{
		Kind: RegionKindEventsSST, Offset: 8, Length: 16,
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := NewRegionReadable(source, "run", int64(len(object)), RegionDescriptor{
		Kind: RegionKindHeadsSST, Offset: 24, Length: 16,
	}, options)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		kind RegionKind
		data string
		err  error
	}
	results := make(chan result, 2)
	read := func(kind RegionKind, readable objstorage.Readable) {
		handle := readable.NewReadHandle(objstorage.ReadBeforeSize(12))
		defer handle.Close()
		data := make([]byte, 4)
		err := handle.ReadAt(context.Background(), data, 12)
		results <- result{kind: kind, data: string(data), err: err}
	}
	go read(RegionKindEventsSST, events)
	go read(RegionKindHeadsSST, heads)

	requests := receiveRegionRequests(t, source.requests, 2)
	for _, request := range requests {
		if request != [2]int64{12, 12} && request != [2]int64{28, 12} {
			t.Fatalf("range request %v escaped an SST region", request)
		}
	}
	if requests[0] == requests[1] {
		t.Fatalf("sibling reads were coalesced into one physical range: %v", requests[0])
	}
	close(source.proceed)
	for range 2 {
		got := <-results
		want := "EEEE"
		if got.kind == RegionKindHeadsSST {
			want = "HHHH"
		}
		if got.err != nil || got.data != want {
			t.Fatalf("region kind %d data=%q err=%v, want %q", got.kind, got.data, got.err, want)
		}
	}
	if source.calls.Load() != 2 {
		t.Fatalf("provider calls=%d, want one per sibling", source.calls.Load())
	}
	if cache.len() != 2 {
		t.Fatalf("cache entries=%d, want one per region kind", cache.len())
	}

	// Fresh handles request the identical read-before windows. Both must hit
	// their own cache entries without touching the provider.
	for kind, readable := range map[RegionKind]objstorage.Readable{
		RegionKindEventsSST: events,
		RegionKindHeadsSST:  heads,
	} {
		handle := readable.NewReadHandle(objstorage.ReadBeforeSize(12))
		data := make([]byte, 4)
		if err := handle.ReadAt(context.Background(), data, 12); err != nil {
			t.Fatalf("cached region kind %d: %v", kind, err)
		}
		_ = handle.Close()
	}
	if source.calls.Load() != 2 {
		t.Fatalf("cache hits caused provider calls=%d, want 2", source.calls.Load())
	}
}

func TestRegionReadableCoalescesOnlyIdenticalRegionRange(t *testing.T) {
	object := append(bytes.Repeat([]byte{'p'}, 8), bytes.Repeat([]byte{'E'}, 32)...)
	source := newGatedRangeSource(object)
	loads := &RegionLoadGroup{}
	defer loads.Close(nil)
	defer close(source.release)
	options := RegionReadOptions{RunID: testRunID(), Coalescer: loads}
	readable, err := NewRegionReadable(source, "run", int64(len(object)), RegionDescriptor{
		Kind: RegionKindEventsSST, Offset: 8, Length: 32,
	}, options)
	if err != nil {
		t.Fatal(err)
	}

	const readers = 24
	start := make(chan struct{})
	errs := make(chan error, readers)
	for range readers {
		go func() {
			<-start
			data := make([]byte, 8)
			err := readable.ReadAt(context.Background(), data, 4)
			if err == nil && !bytes.Equal(data, bytes.Repeat([]byte{'E'}, 8)) {
				err = errors.New("coalesced read returned wrong bytes")
			}
			errs <- err
		}()
	}
	close(start)
	key := regionCacheKey(options.RunID, RegionKindEventsSST, 4, 8)
	waitForRegionLoadWaiters(t, loads, key, readers)
	requests := receiveRegionRequests(t, source.requests, 1)
	if requests[0] != [2]int64{12, 8} {
		t.Fatalf("physical coalesced request=%v, want [12 8]", requests[0])
	}
	close(source.proceed)
	for range readers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatalf("provider calls=%d, want 1 coalesced load", source.calls.Load())
	}
}

func TestRegionReadableCacheRequiresRunID(t *testing.T) {
	cache := newRegionTestCache()
	_, err := NewRegionReadable(
		&memoryRangeSource{data: bytes.Repeat([]byte{1}, 16)},
		"run",
		16,
		RegionDescriptor{Kind: RegionKindEventsSST, Length: 16},
		RegionReadOptions{Cache: cache},
	)
	if !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("error=%v, want invalid run", err)
	}
}

func TestOpenTableWithReadOptionsUsesRecoveredRunIdentity(t *testing.T) {
	buildOptions, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, buildOptions, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &memoryRangeSource{data: object.Bytes()}
	cache := newRegionTestCache()
	loads := &RegionLoadGroup{}
	defer loads.Close(nil)
	reader, err := OpenTableWithReadOptions(
		context.Background(),
		source,
		"run",
		ref,
		RegionKindEventsSST,
		RegionReadOptions{Cache: cache, Coalescer: loads},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if cache.len() == 0 {
		t.Fatal("opening the table did not populate the configured region cache")
	}

	mismatch := ref.RunID
	mismatch[0]++
	requestCount := len(source.requests)
	_, err = OpenTableWithReadOptions(
		context.Background(),
		source,
		"run",
		ref,
		RegionKindEventsSST,
		RegionReadOptions{RunID: mismatch, Cache: cache},
	)
	if !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("mismatched run identity error=%v, want invalid run", err)
	}
	if len(source.requests) != requestCount {
		t.Fatal("mismatched run identity reached the range provider")
	}
}

func TestOpenTablePreservesTransportError(t *testing.T) {
	buildOptions, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, buildOptions, input)
	if err != nil {
		t.Fatal(err)
	}
	transport := errors.New("temporary object-store failure")
	_, err = OpenTable(
		context.Background(),
		&errorRangeSource{err: transport},
		"run",
		ref,
		RegionKindEventsSST,
	)
	if !errors.Is(err, transport) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("table-open transport error classification=%v", err)
	}
}

func TestRegionReadBeforeSize(t *testing.T) {
	tests := []struct {
		regionSize int64
		requested  objstorage.ReadBeforeSize
		want       int64
	}{
		{regionSize: 0, requested: 512 << 10, want: 0},
		{regionSize: 16 << 10, requested: 512 << 10, want: 16 << 10},
		{regionSize: 4 << 20, requested: 512 << 10, want: 32 << 10},
		{regionSize: 4<<20 + 1, requested: 512 << 10, want: 64 << 10},
		{regionSize: 16 << 20, requested: 96 << 10, want: 96 << 10},
		{regionSize: 64 << 20, requested: 1 << 20, want: 512 << 10},
	}
	for _, test := range tests {
		if got := regionReadBeforeSize(test.regionSize, test.requested); got != test.want {
			t.Fatalf("regionReadBeforeSize(%d, %d)=%d, want %d", test.regionSize, test.requested, got, test.want)
		}
	}
}

type regionTestCache struct {
	mu     sync.Mutex
	values map[string][]byte
}

type errorRangeSource struct {
	err error
}

func (s *errorRangeSource) Size(context.Context, string) (int64, error) {
	return 0, s.err
}

func (s *errorRangeSource) ReadRange(context.Context, string, int64, int64) ([]byte, error) {
	return nil, s.err
}

func newRegionTestCache() *regionTestCache {
	return &regionTestCache{values: make(map[string][]byte)}
}

func (c *regionTestCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

func (c *regionTestCache) Set(key string, value []byte, _ int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = bytes.Clone(value)
	return true
}

func (c *regionTestCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.values)
}

type gatedRangeSource struct {
	data     []byte
	requests chan [2]int64
	proceed  chan struct{}
	release  chan struct{}
	calls    atomic.Int64
}

func newGatedRangeSource(data []byte) *gatedRangeSource {
	return &gatedRangeSource{
		data:     data,
		requests: make(chan [2]int64, 64),
		proceed:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (s *gatedRangeSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *gatedRangeSource) ReadRange(ctx context.Context, _ string, offset, length int64) ([]byte, error) {
	s.calls.Add(1)
	select {
	case s.requests <- [2]int64{offset, length}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.proceed:
	case <-s.release:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func receiveRegionRequests(t *testing.T, requests <-chan [2]int64, count int) [][2]int64 {
	t.Helper()
	got := make([][2]int64, 0, count)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(got) < count {
		select {
		case request := <-requests:
			got = append(got, request)
		case <-deadline.C:
			t.Fatalf("received %d of %d provider requests", len(got), count)
		}
	}
	return got
}

func waitForRegionLoadWaiters(t *testing.T, group *RegionLoadGroup, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		call := group.calls[key]
		got := 0
		if call != nil {
			got = call.waiters
		}
		group.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("coalesced load did not reach %d waiters", want)
}
