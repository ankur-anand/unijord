package blobcatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
)

func validatePageRef(ref pageRef, level uint8) error {
	if ref.Level != level {
		return fmt.Errorf("%w: page ref level=%d want=%d", ErrCorruptCatalog, ref.Level, level)
	}
	if ref.SeqLo > ref.SeqHi {
		return fmt.Errorf("%w: page ref seq_lo=%d seq_hi=%d", ErrCorruptCatalog, ref.SeqLo, ref.SeqHi)
	}
	if ref.Generation == 0 || ref.PageID == "" || ref.Path == "" || ref.Count <= 0 {
		return fmt.Errorf("%w: incomplete page ref", ErrCorruptCatalog)
	}
	return nil
}

func validateLeafPage(page leafPage) error {
	if page.Version != pageVersion || page.Type != "leaf" {
		return fmt.Errorf("%w: invalid leaf header", ErrCorruptCatalog)
	}
	if len(page.Segments) == 0 || len(page.Segments) > pcatalog.MaxSegmentPageLimit {
		return fmt.Errorf("%w: leaf segment count=%d", ErrCorruptCatalog, len(page.Segments))
	}
	for i, segment := range page.Segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, err)
		}
		if segment.Partition != page.Partition {
			return fmt.Errorf("%w: leaf partition=%d segment partition=%d", ErrCorruptCatalog, page.Partition, segment.Partition)
		}
		if i == 0 {
			if segment.BaseLSN != page.SeqLo {
				return fmt.Errorf("%w: leaf seq_lo=%d first=%d", ErrCorruptCatalog, page.SeqLo, segment.BaseLSN)
			}
			continue
		}
		prev := page.Segments[i-1]
		if segment.BaseLSN != prev.NextLSN() {
			return fmt.Errorf("%w: non-contiguous leaf segment", ErrCorruptCatalog)
		}
		if segment.MinTimestampMS < prev.MaxTimestampMS {
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, pcatalog.ErrTimestampOrder)
		}
	}
	last := page.Segments[len(page.Segments)-1]
	if last.LastLSN != page.SeqHi {
		return fmt.Errorf("%w: leaf seq_hi=%d last=%d", ErrCorruptCatalog, page.SeqHi, last.LastLSN)
	}
	return nil
}

func validateIndexPage(page indexPage) error {
	if page.Version != pageVersion || page.Type != "index" || page.Level == 0 {
		return fmt.Errorf("%w: invalid index header", ErrCorruptCatalog)
	}
	if len(page.Refs) == 0 || len(page.Refs) > pcatalog.MaxSegmentPageLimit {
		return fmt.Errorf("%w: index ref count=%d", ErrCorruptCatalog, len(page.Refs))
	}
	for i, ref := range page.Refs {
		if err := validatePageRef(ref, page.Level-1); err != nil {
			return err
		}
		if i == 0 {
			if ref.SeqLo != page.SeqLo {
				return fmt.Errorf("%w: index seq_lo=%d first=%d", ErrCorruptCatalog, page.SeqLo, ref.SeqLo)
			}
			continue
		}
		prev := page.Refs[i-1]
		if prev.SeqHi == ^uint64(0) || ref.SeqLo != prev.SeqHi+1 {
			return fmt.Errorf("%w: non-contiguous index refs", ErrCorruptCatalog)
		}
	}
	if page.Refs[len(page.Refs)-1].SeqHi != page.SeqHi {
		return fmt.Errorf("%w: index seq_hi mismatch", ErrCorruptCatalog)
	}
	return nil
}

func verifyLeafRef(page leafPage, ref pageRef) error {
	if ref.Level != 0 || ref.Count != len(page.Segments) || ref.SeqLo != page.SeqLo || ref.SeqHi != page.SeqHi || ref.Generation != page.Generation || ref.PageID != page.PageID {
		return fmt.Errorf("%w: leaf ref mismatch", ErrCorruptCatalog)
	}
	return nil
}

func verifyIndexRef(page indexPage, ref pageRef) error {
	if ref.Level != page.Level || ref.Count != len(page.Refs) || ref.SeqLo != page.SeqLo || ref.SeqHi != page.SeqHi || ref.Generation != page.Generation || ref.PageID != page.PageID {
		return fmt.Errorf("%w: index ref mismatch", ErrCorruptCatalog)
	}
	return nil
}

func verifyPageID(want string, page any) error {
	got, err := canonicalPageID(page)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: page_id=%s want=%s", ErrCorruptCatalog, got, want)
	}
	return nil
}

func canonicalPageID(page any) (string, error) {
	switch p := page.(type) {
	case leafPage:
		p.PageID = ""
		body, err := json.Marshal(p)
		if err != nil {
			return "", err
		}
		return shortPageID(body), nil
	case indexPage:
		p.PageID = ""
		body, err := json.Marshal(p)
		if err != nil {
			return "", err
		}
		return shortPageID(body), nil
	default:
		return "", fmt.Errorf("%w: unknown page type %T", ErrCorruptCatalog, page)
	}
}

func shortPageID(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:16])
}
