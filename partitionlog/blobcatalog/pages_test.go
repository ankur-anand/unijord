package blobcatalog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func TestWriteLeaf(t *testing.T) {
	t.Parallel()

	cat, err := NewMemory(Options{Prefix: "test-catalog"})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	ref, stored, err := cat.writeLeaf(context.Background(), leafPage{
		Partition:  1,
		Generation: 2,
		Segments: []pcatalog.SegmentRef{
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
		Segments:   []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)},
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

	page := leafPage{Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)}}
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
