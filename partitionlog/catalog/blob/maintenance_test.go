package blob

import (
	"context"
	"testing"
)

func TestIsPageReachableDistinguishesSupersededIndexCandidate(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	writer, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for i := uint64(0); i < 6; i++ {
		base := i * 10
		if _, err := writer.AppendSegment(context.Background(), testSegmentRef(1, base, base+9, writer.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", i, err)
		}
	}

	page, err := cat.backend.List(context.Background(), ListOptions{Prefix: PagePrefix(cat.opts.Prefix, cat.opts.StreamID, 1)})
	if err != nil {
		t.Fatalf("List(pages) error = %v", err)
	}
	if len(page.Objects) != 5 {
		t.Fatalf("page objects = %d, want 5", len(page.Objects))
	}
	reachableCount := 0
	unreachableCount := 0
	for _, object := range page.Objects {
		snapshot, reachable, err := cat.IsPageReachable(context.Background(), 1, object.Key)
		if err != nil {
			t.Fatalf("IsPageReachable(%q) error = %v", object.Key, err)
		}
		if snapshot.Generation == 0 || snapshot.Head.NextLSN != 60 {
			t.Fatalf("snapshot = %+v", snapshot)
		}
		if reachable {
			reachableCount++
		} else {
			unreachableCount++
		}
	}
	if reachableCount != 4 || unreachableCount != 1 {
		t.Fatalf("reachable=%d unreachable=%d, want 4/1", reachableCount, unreachableCount)
	}
}
