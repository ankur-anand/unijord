package blobcatalog

import (
	"errors"
	"testing"
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
