package blobcatalog

import (
	"errors"
	"fmt"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestValidatePageRef(t *testing.T) {
	t.Parallel()

	valid := pageRef{
		Level:      1,
		SeqLo:      100,
		SeqHi:      199,
		Generation: 2,
		PageID:     "abc",
		Path:       "catalog/p00000001/pages/l01/index.json",
		Count:      3,
	}
	if err := validatePageRef(valid, 1); err != nil {
		t.Fatalf("validatePageRef(valid) error = %v", err)
	}

	cases := []pageRef{
		{Level: 2, SeqLo: 100, SeqHi: 199, Generation: 2, PageID: "abc", Path: "x", Count: 1},
		{Level: 1, SeqLo: 200, SeqHi: 199, Generation: 2, PageID: "abc", Path: "x", Count: 1},
		{Level: 1, SeqLo: 100, SeqHi: 199, Generation: 0, PageID: "abc", Path: "x", Count: 1},
		{Level: 1, SeqLo: 100, SeqHi: 199, Generation: 2, PageID: "", Path: "x", Count: 1},
		{Level: 1, SeqLo: 100, SeqHi: 199, Generation: 2, PageID: "abc", Path: "", Count: 1},
		{Level: 1, SeqLo: 100, SeqHi: 199, Generation: 2, PageID: "abc", Path: "x", Count: 0},
	}
	for i, ref := range cases {
		if err := validatePageRef(ref, 1); !errors.Is(err, ErrCorruptCatalog) {
			t.Fatalf("case %d validatePageRef() error = %v, want %v", i, err, ErrCorruptCatalog)
		}
	}
}

func TestValidateHeadFileEmpty(t *testing.T) {
	t.Parallel()

	head := headFile{Version: pageVersion, Partition: 1}
	if err := validateHeadFile(head, 1); err != nil {
		t.Fatalf("validateHeadFile(empty) error = %v", err)
	}

	head.WriterID = [16]byte{1}
	if err := validateHeadFile(head, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("validateHeadFile(writer without epoch) error = %v, want %v", err, ErrCorruptCatalog)
	}

	head = headFile{Version: pageVersion, Partition: 1, HasLastSegment: false, NextLSN: 1}
	if err := validateHeadFile(head, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("validateHeadFile(empty with state) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestValidateHeadFileWithActiveSegments(t *testing.T) {
	t.Parallel()

	segment := testSegmentRef(1, 100, 199, 1)
	head := headFile{
		Version:        pageVersion,
		Partition:      1,
		NextLSN:        200,
		OldestLSN:      100,
		WriterEpoch:    1,
		WriterID:       [16]byte{1},
		SegmentCount:   1,
		LastSegment:    segment,
		HasLastSegment: true,
		ActiveSegments: []pcatalog.SegmentRef{segment},
		Generation:     2,
	}
	if err := validateHeadFile(head, 1); err != nil {
		t.Fatalf("validateHeadFile(active segments) error = %v", err)
	}

	head.ActiveSegments[0].LastLSN = 198
	if err := validateHeadFile(head, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("validateHeadFile(active segment mismatch) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestValidateHeadFileWithLeafFrontier(t *testing.T) {
	t.Parallel()

	head := headFile{
		Version:        pageVersion,
		Partition:      1,
		NextLSN:        200,
		OldestLSN:      100,
		WriterEpoch:    1,
		WriterID:       [16]byte{1},
		SegmentCount:   1,
		LastSegment:    testSegmentRef(1, 100, 199, 1),
		HasLastSegment: true,
		LeafFrontier:   refPtr(testPageRef(0, 100, 199, 2, "leaf", 1)),
		Generation:     2,
	}
	if err := validateHeadFile(head, 1); err != nil {
		t.Fatalf("validateHeadFile(leaf frontier) error = %v", err)
	}

	head.LeafFrontier.SeqHi = 198
	if err := validateHeadFile(head, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("validateHeadFile(leaf frontier mismatch) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestValidateHeadFileWithIndexFrontier(t *testing.T) {
	t.Parallel()

	head := headFile{
		Version:        pageVersion,
		Partition:      1,
		NextLSN:        500,
		OldestLSN:      100,
		WriterEpoch:    1,
		WriterID:       [16]byte{1},
		SegmentCount:   4,
		LastSegment:    testSegmentRef(1, 400, 499, 1),
		HasLastSegment: true,
		LeafFrontier:   refPtr(testPageRef(0, 400, 499, 5, "leaf", 1)),
		IndexFrontier: []pageRef{
			testPageRef(1, 100, 399, 4, "index", 3),
		},
		Generation: 5,
	}
	if err := validateHeadFile(head, 1); err != nil {
		t.Fatalf("validateHeadFile(index frontier) error = %v", err)
	}

	head.IndexFrontier[0].SeqHi = 398
	if err := validateHeadFile(head, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("validateHeadFile(frontier gap) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestValidateLeafPage(t *testing.T) {
	t.Parallel()

	valid := leafPage{
		Version:   pageVersion,
		Type:      "leaf",
		Partition: 1,
		SeqLo:     100,
		SeqHi:     299,
		Segments: []pcatalog.SegmentRef{
			testSegmentRef(1, 100, 199, 1),
			testSegmentRef(1, 200, 299, 1),
		},
	}
	if err := validateLeafPage(valid); err != nil {
		t.Fatalf("validateLeafPage(valid) error = %v", err)
	}

	cases := []struct {
		name string
		page leafPage
		want error
	}{
		{
			name: "bad header",
			page: leafPage{Version: pageVersion, Type: "index", Partition: 1, SeqLo: 100, SeqHi: 199, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)}},
			want: ErrCorruptCatalog,
		},
		{
			name: "empty",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 1},
			want: ErrCorruptCatalog,
		},
		{
			name: "partition mismatch",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 2, SeqLo: 100, SeqHi: 199, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)}},
			want: ErrCorruptCatalog,
		},
		{
			name: "seq lo mismatch",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 1, SeqLo: 99, SeqHi: 199, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)}},
			want: ErrCorruptCatalog,
		},
		{
			name: "gap",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 1, SeqLo: 100, SeqHi: 399, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1), testSegmentRef(1, 300, 399, 1)}},
			want: ErrCorruptCatalog,
		},
		{
			name: "timestamp regression",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 1, SeqLo: 100, SeqHi: 299, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1), testSegmentRefWithTime(1, 200, 299, 1, 50, 60)}},
			want: pcatalog.ErrTimestampOrder,
		},
		{
			name: "seq hi mismatch",
			page: leafPage{Version: pageVersion, Type: "leaf", Partition: 1, SeqLo: 100, SeqHi: 300, Segments: []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1), testSegmentRef(1, 200, 299, 1)}},
			want: ErrCorruptCatalog,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := validateLeafPage(tc.page); !errors.Is(err, tc.want) {
				t.Fatalf("validateLeafPage() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateIndexPage(t *testing.T) {
	t.Parallel()

	valid := indexPage{
		Version: pageVersion,
		Type:    "index",
		Level:   1,
		SeqLo:   100,
		SeqHi:   699,
		Refs: []pageRef{
			testPageRef(0, 100, 399, 2, "a", 3),
			testPageRef(0, 400, 699, 3, "b", 3),
		},
	}
	if err := validateIndexPage(valid); err != nil {
		t.Fatalf("validateIndexPage(valid) error = %v", err)
	}

	cases := []struct {
		name string
		page indexPage
	}{
		{
			name: "bad type",
			page: indexPage{Version: pageVersion, Type: "leaf", Level: 1, SeqLo: 100, SeqHi: 399, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3)}},
		},
		{
			name: "zero level",
			page: indexPage{Version: pageVersion, Type: "index", Level: 0, SeqLo: 100, SeqHi: 399, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3)}},
		},
		{
			name: "empty refs",
			page: indexPage{Version: pageVersion, Type: "index", Level: 1},
		},
		{
			name: "child wrong level",
			page: indexPage{Version: pageVersion, Type: "index", Level: 2, SeqLo: 100, SeqHi: 399, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3)}},
		},
		{
			name: "seq lo mismatch",
			page: indexPage{Version: pageVersion, Type: "index", Level: 1, SeqLo: 99, SeqHi: 399, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3)}},
		},
		{
			name: "gap",
			page: indexPage{Version: pageVersion, Type: "index", Level: 1, SeqLo: 100, SeqHi: 799, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3), testPageRef(0, 500, 799, 3, "b", 3)}},
		},
		{
			name: "seq hi mismatch",
			page: indexPage{Version: pageVersion, Type: "index", Level: 1, SeqLo: 100, SeqHi: 700, Refs: []pageRef{testPageRef(0, 100, 399, 2, "a", 3), testPageRef(0, 400, 699, 3, "b", 3)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := validateIndexPage(tc.page); !errors.Is(err, ErrCorruptCatalog) {
				t.Fatalf("validateIndexPage() error = %v, want %v", err, ErrCorruptCatalog)
			}
		})
	}
}

func TestVerifyLeafRef(t *testing.T) {
	t.Parallel()

	page := leafPage{
		Version:    pageVersion,
		Type:       "leaf",
		Partition:  1,
		SeqLo:      100,
		SeqHi:      199,
		Generation: 2,
		PageID:     "abc",
		Segments:   []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)},
	}
	ref := pageRef{
		Level:      0,
		SeqLo:      100,
		SeqHi:      199,
		Generation: 2,
		PageID:     "abc",
		Path:       "catalog/p00000001/pages/l00/leaf.json",
		Count:      1,
	}
	if err := verifyLeafRef(page, ref); err != nil {
		t.Fatalf("verifyLeafRef(valid) error = %v", err)
	}
	ref.SeqHi = 200
	if err := verifyLeafRef(page, ref); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("verifyLeafRef(mismatch) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestVerifyIndexRef(t *testing.T) {
	t.Parallel()

	page := indexPage{
		Version:    pageVersion,
		Type:       "index",
		Level:      1,
		Partition:  1,
		SeqLo:      100,
		SeqHi:      399,
		Generation: 2,
		PageID:     "abc",
		Refs:       []pageRef{testPageRef(0, 100, 399, 2, "leaf", 3)},
	}
	ref := pageRef{
		Level:      1,
		SeqLo:      100,
		SeqHi:      399,
		Generation: 2,
		PageID:     "abc",
		Path:       "catalog/p00000001/pages/l01/index.json",
		Count:      1,
	}
	if err := verifyIndexRef(page, ref); err != nil {
		t.Fatalf("verifyIndexRef(valid) error = %v", err)
	}
	ref.PageID = "different"
	if err := verifyIndexRef(page, ref); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("verifyIndexRef(mismatch) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func TestVerifyPageID(t *testing.T) {
	t.Parallel()

	page := leafPage{
		Version:    pageVersion,
		Type:       "leaf",
		Partition:  1,
		SeqLo:      100,
		SeqHi:      199,
		Generation: 2,
		Segments:   []pcatalog.SegmentRef{testSegmentRef(1, 100, 199, 1)},
	}
	pageID, err := canonicalPageID(page)
	if err != nil {
		t.Fatalf("canonicalPageID() error = %v", err)
	}
	page.PageID = pageID

	if err := verifyPageID(pageID, page); err != nil {
		t.Fatalf("verifyPageID(valid) error = %v", err)
	}
	if err := verifyPageID("different", page); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("verifyPageID(mismatch) error = %v, want %v", err, ErrCorruptCatalog)
	}
	if _, err := canonicalPageID(struct{}{}); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("canonicalPageID(unknown) error = %v, want %v", err, ErrCorruptCatalog)
	}
}

func testPageRef(level uint8, seqLo, seqHi, generation uint64, pageID string, count int) pageRef {
	return pageRef{
		Level:      level,
		SeqLo:      seqLo,
		SeqHi:      seqHi,
		Generation: generation,
		PageID:     pageID,
		Path:       fmt.Sprintf("catalog/p00000001/pages/l%02d/page-%s.json", level, pageID),
		Count:      count,
	}
}

func refPtr(ref pageRef) *pageRef {
	return &ref
}

func testSegmentRef(partition uint32, base, last, epoch uint64) pcatalog.SegmentRef {
	return testSegmentRefWithTime(partition, base, last, epoch, int64(base), int64(last))
}

func testSegmentRefWithTime(partition uint32, base, last, epoch uint64, minTS, maxTS int64) pcatalog.SegmentRef {
	return pcatalog.SegmentRef{
		URI:              fmt.Sprintf("object://p%08d/%020d-%020d", partition, base, last),
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      [16]byte{byte(partition), byte(base + 1), byte(last + 1), byte(epoch + 1)},
		WriterTag:        [16]byte{9, 8, 7},
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   minTS,
		MaxTimestampMS:   maxTS,
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        128,
		BlockIndexOffset: 64,
		BlockIndexLength: 64,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      base + 100,
		TrailerHash:      last + 100,
	}
}
