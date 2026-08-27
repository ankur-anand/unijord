package blob

import (
	"cmp"
	"context"
	"slices"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
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
	if _, err := cat.RequestRetention(context.Background(), 1, pcatalog.RetentionRequest{
		Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 20,
	}); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	if _, err := writer.(pcatalog.RetentionWriterSession).ApplyPendingRetention(context.Background()); err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
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
	reachableIndexes := 0
	unreachableIndexes := 0
	for _, object := range page.Objects {
		parsed, err := ParsePagePath(cat.opts.Prefix, cat.opts.StreamID, 1, object.Key)
		if err != nil {
			t.Fatalf("ParsePagePath(%q) error = %v", object.Key, err)
		}
		snapshot, reachable, err := cat.IsPageReachable(context.Background(), 1, object.Key)
		if err != nil {
			t.Fatalf("IsPageReachable(%q) error = %v", object.Key, err)
		}
		if snapshot.Generation == 0 || snapshot.Head.NextLSN != 60 {
			t.Fatalf("snapshot = %+v", snapshot)
		}
		if reachable {
			reachableCount++
			if parsed.Kind == PageObjectIndex {
				reachableIndexes++
			}
		} else {
			unreachableCount++
			if parsed.Kind == PageObjectIndex {
				unreachableIndexes++
			}
		}
	}
	if reachableCount != 3 || unreachableCount != 2 || reachableIndexes != 1 || unreachableIndexes != 1 {
		t.Fatalf("reachable=%d unreachable=%d reachable_indexes=%d unreachable_indexes=%d, want 3/2/1/1", reachableCount, unreachableCount, reachableIndexes, unreachableIndexes)
	}
}

func TestListMaintenancePagesPaginatesReachableTreeByLevel(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	writer, err := cat.OpenWriter(context.Background(), 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	for i := uint64(0); i < 18; i++ {
		base := i * 10
		if _, err := writer.AppendSegment(context.Background(), testSegmentRef(1, base, base+9, writer.Epoch())); err != nil {
			t.Fatalf("AppendSegment(%d) error = %v", i, err)
		}
	}

	objects, err := cat.backend.List(context.Background(), ListOptions{Prefix: PagePrefix(cat.opts.Prefix, cat.opts.StreamID, 1)})
	if err != nil {
		t.Fatalf("List(pages) error = %v", err)
	}
	wantByLevel := make(map[uint8][]string)
	for _, object := range objects.Objects {
		parsed, err := ParsePagePath(cat.opts.Prefix, cat.opts.StreamID, 1, object.Key)
		if err != nil {
			t.Fatalf("ParsePagePath(%q) error = %v", object.Key, err)
		}
		_, reachable, err := cat.IsPageReachable(context.Background(), 1, object.Key)
		if err != nil {
			t.Fatalf("IsPageReachable(%q) error = %v", object.Key, err)
		}
		if reachable {
			wantByLevel[parsed.Level] = append(wantByLevel[parsed.Level], object.Key)
		}
	}
	if len(wantByLevel[2]) == 0 {
		t.Fatalf("reachable levels = %+v, want l02 coverage", wantByLevel)
	}

	for level, want := range wantByLevel {
		slices.SortFunc(want, func(a, b string) int {
			pa, _ := ParsePagePath(cat.opts.Prefix, cat.opts.StreamID, 1, a)
			pb, _ := ParsePagePath(cat.opts.Prefix, cat.opts.StreamID, 1, b)
			if order := cmp.Compare(pa.SeqLo, pb.SeqLo); order != 0 {
				return order
			}
			return cmp.Compare(a, b)
		})
		var got []string
		from := uint64(0)
		for {
			snapshot, page, err := cat.ListMaintenancePages(context.Background(), MaintenancePageRequest{
				Partition: 1,
				Level:     level,
				FromSeqLo: from,
				Limit:     1,
			})
			if err != nil {
				t.Fatalf("ListMaintenancePages(level=%d from=%d) error = %v", level, from, err)
			}
			if snapshot.Head.NextLSN != 180 {
				t.Fatalf("snapshot = %+v", snapshot)
			}
			got = append(got, page.Paths...)
			if !page.HasMore {
				break
			}
			if page.NextSeqLo <= from {
				t.Fatalf("non-advancing page = %+v from=%d", page, from)
			}
			from = page.NextSeqLo
		}
		if !slices.Equal(got, want) {
			t.Fatalf("level %d paths = %v, want %v", level, got, want)
		}
	}
}
