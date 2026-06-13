package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"testing"
)

func TestBuildNextPageSetBuffersActiveSegmentInHead(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	segment := testSegmentRef(1, 100, 199, 1)
	next, err := cat.buildNextPageSet(context.Background(), headFile{}, segment, 2)
	if err != nil {
		t.Fatalf("buildNextPageSet() error = %v", err)
	}
	if next.LeafFrontier != nil {
		t.Fatalf("LeafFrontier = %+v, want nil", next.LeafFrontier)
	}
	if len(next.IndexFrontier) != 0 {
		t.Fatalf("IndexFrontier = %+v, want empty", next.IndexFrontier)
	}
	if len(next.ActiveSegments) != 1 || next.ActiveSegments[0] != segment {
		t.Fatalf("ActiveSegments = %+v, want segment", next.ActiveSegments)
	}
	objects, err := cat.backend.List(context.Background(), ListOptions{Prefix: PagePrefix("catalog", 1)})
	if err != nil {
		t.Fatalf("List(pages) error = %v", err)
	}
	if len(objects.Objects) != 0 {
		t.Fatalf("page objects = %+v, want none", objects.Objects)
	}
}

func TestBuildNextPageSetSealsFullActiveSegmentsIntoLeafFrontier(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first := testSegmentRef(1, 100, 199, 1)
	head := headFile{
		ActiveSegments: []pmeta.SegmentRef{first},
	}
	second := testSegmentRef(1, 200, 299, 1)
	next, err := cat.buildNextPageSet(context.Background(), head, second, 3)
	if err != nil {
		t.Fatalf("buildNextPageSet() error = %v", err)
	}
	if next.LeafFrontier == nil || next.LeafFrontier.SeqLo != 100 || next.LeafFrontier.SeqHi != 299 || next.LeafFrontier.Count != 2 {
		t.Fatalf("LeafFrontier = %+v", next.LeafFrontier)
	}
	if len(next.IndexFrontier) != 0 {
		t.Fatalf("IndexFrontier = %+v, want empty", next.IndexFrontier)
	}
	if len(next.ActiveSegments) != 0 {
		t.Fatalf("ActiveSegments = %+v, want empty after seal", next.ActiveSegments)
	}
	leaf, err := cat.loadLeaf(context.Background(), *next.LeafFrontier)
	if err != nil {
		t.Fatalf("loadLeaf(leaf frontier) error = %v", err)
	}
	if len(leaf.Segments) != 2 || leaf.Segments[0] != first || leaf.Segments[1] != second {
		t.Fatalf("leaf segments = %+v", leaf.Segments)
	}
}

func TestBuildNextPageSetCarriesOldLeafFrontierWhenSealingNewLeaf(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first := testSegmentRef(1, 100, 199, 1)
	oldLeaf, _, err := cat.writeLeaf(context.Background(), leafPage{
		Partition:  1,
		Generation: 2,
		Segments:   []pmeta.SegmentRef{first},
	})
	if err != nil {
		t.Fatalf("writeLeaf() error = %v", err)
	}
	second := testSegmentRef(1, 200, 299, 1)
	third := testSegmentRef(1, 300, 399, 1)
	head := headFile{
		LeafFrontier:   oldLeaf,
		ActiveSegments: []pmeta.SegmentRef{second},
	}
	next, err := cat.buildNextPageSet(context.Background(), head, third, 3)
	if err != nil {
		t.Fatalf("buildNextPageSet() error = %v", err)
	}
	if next.LeafFrontier == nil || next.LeafFrontier.SeqLo != 200 || next.LeafFrontier.SeqHi != 399 || next.LeafFrontier.Count != 2 {
		t.Fatalf("LeafFrontier = %+v", next.LeafFrontier)
	}
	if len(next.ActiveSegments) != 0 {
		t.Fatalf("ActiveSegments = %+v, want empty after seal", next.ActiveSegments)
	}
	if len(next.IndexFrontier) != 1 || next.IndexFrontier[0].Level != 1 || next.IndexFrontier[0].SeqLo != 100 || next.IndexFrontier[0].SeqHi != 199 {
		t.Fatalf("IndexFrontier = %+v, want l01 for old leaf frontier", next.IndexFrontier)
	}
	index, err := cat.loadIndex(context.Background(), next.IndexFrontier[0])
	if err != nil {
		t.Fatalf("loadIndex(frontier[0]) error = %v", err)
	}
	if len(index.Refs) != 1 || index.Refs[0] != *oldLeaf {
		t.Fatalf("index refs = %+v, want old leaf frontier %+v", index.Refs, *oldLeaf)
	}
}

func TestCarryPageRefCreatesIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	child := testPageRef(0, 100, 199, 2, "leaf-a", 1)
	frontier, err := cat.carryPageRef(context.Background(), 1, nil, child, 3)
	if err != nil {
		t.Fatalf("carryPageRef() error = %v", err)
	}
	if len(frontier) != 1 || frontier[0].Level != 1 || frontier[0].SeqLo != 100 || frontier[0].SeqHi != 199 || frontier[0].Count != 1 {
		t.Fatalf("frontier = %+v", frontier)
	}
	page, err := cat.loadIndex(context.Background(), frontier[0])
	if err != nil {
		t.Fatalf("loadIndex(frontier[0]) error = %v", err)
	}
	if len(page.Refs) != 1 || page.Refs[0] != child {
		t.Fatalf("index refs = %+v, want child %+v", page.Refs, child)
	}
}

func TestCarryPageRefAppendsToExistingIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{IndexRefLimit: 3})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	first := testPageRef(0, 100, 199, 2, "leaf-a", 1)
	frontier, err := cat.carryPageRef(context.Background(), 1, nil, first, 3)
	if err != nil {
		t.Fatalf("carryPageRef(first) error = %v", err)
	}
	second := testPageRef(0, 200, 299, 4, "leaf-b", 1)
	frontier, err = cat.carryPageRef(context.Background(), 1, frontier, second, 5)
	if err != nil {
		t.Fatalf("carryPageRef(second) error = %v", err)
	}
	if len(frontier) != 1 || frontier[0].SeqLo != 100 || frontier[0].SeqHi != 299 || frontier[0].Count != 2 {
		t.Fatalf("frontier = %+v", frontier)
	}
	page, err := cat.loadIndex(context.Background(), frontier[0])
	if err != nil {
		t.Fatalf("loadIndex(frontier[0]) error = %v", err)
	}
	if len(page.Refs) != 2 || page.Refs[0] != first || page.Refs[1] != second {
		t.Fatalf("index refs = %+v", page.Refs)
	}
}

func TestCarryPageRefCarriesFullIndexUp(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{IndexRefLimit: 2})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	var frontier []pageRef
	for i := 0; i < 3; i++ {
		child := testPageRef(0, uint64(100+i*100), uint64(199+i*100), uint64(2+i), fmt.Sprintf("leaf-%d", i), 1)
		frontier, err = cat.carryPageRef(context.Background(), 1, frontier, child, uint64(10+i))
		if err != nil {
			t.Fatalf("carryPageRef(%d) error = %v", i, err)
		}
	}
	if len(frontier) != 2 {
		t.Fatalf("frontier len = %d, want 2: %+v", len(frontier), frontier)
	}
	if frontier[0].Level != 1 || frontier[0].SeqLo != 300 || frontier[0].SeqHi != 399 {
		t.Fatalf("frontier[0] = %+v, want new l01 for 300-399", frontier[0])
	}
	if frontier[1].Level != 2 || frontier[1].SeqLo != 100 || frontier[1].SeqHi != 299 {
		t.Fatalf("frontier[1] = %+v, want l02 for old l01 100-299", frontier[1])
	}
}

func TestWriteLeaf(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{Prefix: "test-catalog"})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref, stored, err := cat.writeLeaf(context.Background(), leafPage{
		Partition:  1,
		Generation: 2,
		Segments: []pmeta.SegmentRef{
			testSegmentRef(1, 100, 199, 1),
			testSegmentRef(1, 200, 299, 1),
		},
	})
	if err != nil {
		t.Fatalf("writeLeaf() error = %v", err)
	}
	if ref == nil {
		t.Fatal("writeLeaf() ref = nil")
	}
	if ref.Level != 0 || ref.SeqLo != 100 || ref.SeqHi != 299 || ref.Generation != 2 || ref.PageID == "" || ref.Count != 2 {
		t.Fatalf("ref = %+v", ref)
	}
	if ref.Path != LeafPagePath("test-catalog", 1, 100, 299, 2, ref.PageID) {
		t.Fatalf("ref path = %q", ref.Path)
	}
	if stored.PageID != ref.PageID || stored.SeqLo != 100 || stored.SeqHi != 299 {
		t.Fatalf("stored leaf = %+v ref = %+v", stored, ref)
	}

	obj, err := cat.backend.Get(context.Background(), ref.Path)
	if err != nil {
		t.Fatalf("Get(written leaf) error = %v", err)
	}
	var decoded leafPage
	if err := json.Unmarshal(obj.Body, &decoded); err != nil {
		t.Fatalf("decode written leaf error = %v", err)
	}
	if err := validateLeafPage(decoded); err != nil {
		t.Fatalf("written leaf validation error = %v", err)
	}
	if err := verifyLeafRef(decoded, *ref); err != nil {
		t.Fatalf("written leaf ref verification error = %v", err)
	}
	if err := verifyPageID(ref.PageID, decoded); err != nil {
		t.Fatalf("written leaf page ID verification error = %v", err)
	}
}

func TestWriteLeafRejectsEmptyLeaf(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	if _, _, err := cat.writeLeaf(context.Background(), leafPage{Partition: 1, Generation: 1}); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("writeLeaf(empty) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestWriteLeafImmutableReplay(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	page := leafPage{
		Partition:  1,
		Generation: 2,
		Segments:   []pmeta.SegmentRef{testSegmentRef(1, 100, 199, 1)},
	}
	first, _, err := cat.writeLeaf(context.Background(), page)
	if err != nil {
		t.Fatalf("writeLeaf(first) error = %v", err)
	}
	second, _, err := cat.writeLeaf(context.Background(), page)
	if err != nil {
		t.Fatalf("writeLeaf(replay) error = %v", err)
	}
	if *second != *first {
		t.Fatalf("replay ref = %+v, want %+v", second, first)
	}
}

func TestWriteIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{Prefix: "test-catalog"})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref, err := cat.writeIndex(context.Background(), indexPage{
		Level:      1,
		Partition:  1,
		Generation: 3,
		Refs: []pageRef{
			testPageRef(0, 100, 199, 2, "a", 1),
			testPageRef(0, 200, 299, 2, "b", 1),
		},
	})
	if err != nil {
		t.Fatalf("writeIndex() error = %v", err)
	}
	if ref == nil {
		t.Fatal("writeIndex() ref = nil")
	}
	if ref.Level != 1 || ref.SeqLo != 100 || ref.SeqHi != 299 || ref.Generation != 3 || ref.PageID == "" || ref.Count != 2 {
		t.Fatalf("ref = %+v", ref)
	}
	if ref.Path != IndexPagePath("test-catalog", 1, 1, 100, 299, 3, ref.PageID) {
		t.Fatalf("ref path = %q", ref.Path)
	}

	obj, err := cat.backend.Get(context.Background(), ref.Path)
	if err != nil {
		t.Fatalf("Get(written index) error = %v", err)
	}
	var decoded indexPage
	if err := json.Unmarshal(obj.Body, &decoded); err != nil {
		t.Fatalf("decode written index error = %v", err)
	}
	if err := validateIndexPage(decoded); err != nil {
		t.Fatalf("written index validation error = %v", err)
	}
	if err := verifyIndexRef(decoded, *ref); err != nil {
		t.Fatalf("written index ref verification error = %v", err)
	}
	if err := verifyPageID(ref.PageID, decoded); err != nil {
		t.Fatalf("written index page ID verification error = %v", err)
	}
}

func TestWriteIndexRejectsEmptyIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	if _, err := cat.writeIndex(context.Background(), indexPage{Level: 1, Partition: 1, Generation: 1}); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("writeIndex(empty) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestWriteIndexImmutableReplay(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	page := indexPage{
		Level:      1,
		Partition:  1,
		Generation: 3,
		Refs:       []pageRef{testPageRef(0, 100, 199, 2, "a", 1)},
	}
	first, err := cat.writeIndex(context.Background(), page)
	if err != nil {
		t.Fatalf("writeIndex(first) error = %v", err)
	}
	second, err := cat.writeIndex(context.Background(), page)
	if err != nil {
		t.Fatalf("writeIndex(replay) error = %v", err)
	}
	if *second != *first {
		t.Fatalf("replay ref = %+v, want %+v", second, first)
	}
}

func TestLoadLeaf(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref, want, err := cat.writeLeaf(context.Background(), leafPage{
		Partition:  1,
		Generation: 2,
		Segments:   []pmeta.SegmentRef{testSegmentRef(1, 100, 199, 1)},
	})
	if err != nil {
		t.Fatalf("writeLeaf() error = %v", err)
	}
	got, err := cat.loadLeaf(context.Background(), *ref)
	if err != nil {
		t.Fatalf("loadLeaf() error = %v", err)
	}
	if got.PageID != want.PageID || got.SeqLo != want.SeqLo || got.SeqHi != want.SeqHi || len(got.Segments) != len(want.Segments) {
		t.Fatalf("loadLeaf() = %+v, want %+v", got, want)
	}

	badRef := *ref
	badRef.SeqHi = 200
	if _, err := cat.loadLeaf(context.Background(), badRef); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("loadLeaf(bad ref) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestLoadIndex(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref, err := cat.writeIndex(context.Background(), indexPage{
		Level:      1,
		Partition:  1,
		Generation: 3,
		Refs:       []pageRef{testPageRef(0, 100, 199, 2, "a", 1)},
	})
	if err != nil {
		t.Fatalf("writeIndex() error = %v", err)
	}
	got, err := cat.loadIndex(context.Background(), *ref)
	if err != nil {
		t.Fatalf("loadIndex() error = %v", err)
	}
	if got.PageID != ref.PageID || got.SeqLo != ref.SeqLo || got.SeqHi != ref.SeqHi || len(got.Refs) != ref.Count {
		t.Fatalf("loadIndex() = %+v ref = %+v", got, ref)
	}

	badRef := *ref
	badRef.PageID = "different"
	if _, err := cat.loadIndex(context.Background(), badRef); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("loadIndex(bad ref) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestLoadPageRejectsBadJSON(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref := testPageRef(0, 100, 199, 2, "bad", 1)
	if _, err := cat.backend.Put(context.Background(), ref.Path, []byte("{bad json")); err != nil {
		t.Fatalf("Put(bad json) error = %v", err)
	}
	if _, err := cat.loadLeaf(context.Background(), ref); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("loadLeaf(bad json) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestCloneRefs(t *testing.T) {
	t.Parallel()

	refs := []pageRef{testPageRef(0, 100, 199, 1, "a", 1)}
	cloned := cloneRefs(refs)
	if len(cloned) != 1 || cloned[0] != refs[0] {
		t.Fatalf("cloneRefs() = %+v, want %+v", cloned, refs)
	}
	cloned[0].SeqHi = 999
	if refs[0].SeqHi != 199 {
		t.Fatalf("cloneRefs shared backing array; original = %+v", refs[0])
	}
}

func TestCloneLeafPage(t *testing.T) {
	t.Parallel()

	page := leafPage{Segments: []pmeta.SegmentRef{testSegmentRef(1, 100, 199, 1)}}
	cloned := cloneLeafPage(page)
	if len(cloned.Segments) != 1 || cloned.Segments[0] != page.Segments[0] {
		t.Fatalf("cloneLeafPage() = %+v, want %+v", cloned, page)
	}
	cloned.Segments[0].LastLSN = 999
	if page.Segments[0].LastLSN != 199 {
		t.Fatalf("cloneLeafPage shared segment backing array; original = %+v", page.Segments[0])
	}
}

func TestEnsureAndTrimFrontierLevel(t *testing.T) {
	t.Parallel()

	frontier := ensureFrontierLevel(nil, 3)
	if len(frontier) != 3 {
		t.Fatalf("ensureFrontierLevel len = %d, want 3", len(frontier))
	}
	frontier[0] = testPageRef(1, 0, 99, 1, "a", 1)
	frontier = trimFrontier(frontier)
	if len(frontier) != 1 {
		t.Fatalf("trimFrontier len = %d, want 1", len(frontier))
	}
}
