package reader

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	blobcache "github.com/ankur-anand/unijord/partitionlog/blob/cache"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

func TestReaderCloseReleasesOwnedRuntime(t *testing.T) {
	segmentCache := MustNewSegmentReaderCache(1)
	segmentCache.set(segmentCacheKey{}, &segreader.Reader{})

	rangeCache := blobcache.NewLRU(1024)
	rangeCache.Set(blobcache.Key{URI: "segment", N: 4}, []byte("data"))
	cachedStore := blobcache.MustNewStore(newTestSegmentStore(nil), rangeCache)

	r, err := New(newHeadCacheCatalog(), cachedStore, Options{SegmentCache: segmentCache})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{7}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	if _, err := r.Head(context.Background(), 8); err != nil {
		t.Fatalf("Head() error = %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if segmentCache.Len() != 0 {
		t.Fatalf("segment cache entries after Close = %d, want 0", segmentCache.Len())
	}
	if rangeCache.Bytes() != 0 {
		t.Fatalf("range cache bytes after Close = %d, want 0", rangeCache.Bytes())
	}
	r.refresh.mu.Lock()
	cachedHeads := len(r.refresh.cachedHeads)
	openWatches := len(r.refresh.watches)
	loopRunning := r.refresh.loopCancel != nil
	r.refresh.mu.Unlock()
	if cachedHeads != 0 || openWatches != 0 || loopRunning {
		t.Fatalf("refresh state after Close: cached=%d watches=%d loop_running=%t", cachedHeads, openWatches, loopRunning)
	}
	if _, err := watch.partitionMembership(7); !errors.Is(err, ErrWatchClosed) {
		t.Fatalf("Watch error after Reader.Close = %v, want %v", err, ErrWatchClosed)
	}
	if _, err := r.Head(context.Background(), 7); !errors.Is(err, ErrClosed) {
		t.Fatalf("Head() after Reader.Close error = %v, want %v", err, ErrClosed)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestReaderCloseCancelsAndDrainsActiveRefresh(t *testing.T) {
	cat := &readerCloseCatalog{
		entered:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	r, err := New(cat, newTestSegmentStore(nil), Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	headDone := make(chan error, 1)
	go func() {
		_, err := r.Head(context.Background(), 7)
		headDone <- err
	}()
	waitForLoadSignal(t, cat.entered, "catalog refresh")

	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()
	waitForLoadSignal(t, cat.canceled, "refresh cancellation")
	select {
	case err := <-closeDone:
		t.Fatalf("Reader.Close returned before refresh drained: %v", err)
	default:
	}
	close(cat.release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Reader.Close() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reader.Close did not return after refresh drained")
	}
	if err := receiveLoadError(t, headDone); !errors.Is(err, ErrClosed) {
		t.Fatalf("Head() error = %v, want ErrClosed", err)
	}
}

type readerCloseCatalog struct {
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *readerCloseCatalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	close(c.canceled)
	<-c.release
	return pmeta.PartitionHead{Partition: partition}, ctx.Err()
}

func (*readerCloseCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *readerCloseCatalog) LookupTimestamp(ctx context.Context, req catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	head, err := c.LoadPartition(ctx, req.Partition)
	return catalog.TimestampLookupResult{Head: head}, err
}

func (*readerCloseCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return pmeta.SegmentPage{}, nil
}
