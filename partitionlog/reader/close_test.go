package reader

import (
	"context"
	"errors"
	"testing"

	blobcache "github.com/ankur-anand/unijord/partitionlog/blob/cache"
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
