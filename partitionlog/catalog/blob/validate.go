package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func validateHeadFile(head headFile, streamID string, partition uint32) error {
	if head.Version != pageVersion {
		return fmt.Errorf("%w: head version=%d", ErrCorruptCatalog, head.Version)
	}
	if head.StreamID != streamID {
		return fmt.Errorf("%w: head stream_id=%q want=%q", ErrCorruptCatalog, head.StreamID, streamID)
	}
	if head.Partition != partition {
		return fmt.Errorf("%w: head partition=%d want=%d", ErrCorruptCatalog, head.Partition, partition)
	}
	if head.WriterEpoch == 0 && head.WriterID != ([16]byte{}) {
		return fmt.Errorf("%w: head has writer_id without writer_epoch", ErrCorruptCatalog)
	}
	if head.WriterEpoch > 0 && head.WriterID == ([16]byte{}) {
		return fmt.Errorf("%w: head has writer_epoch without writer_id", ErrCorruptCatalog)
	}
	if !head.HasLastSegment {
		if head.OldestLSN != head.NextLSN || head.SegmentCount != 0 || head.LeafFrontier != nil || len(head.IndexFrontier) != 0 || len(head.ActiveSegments) != 0 {
			return fmt.Errorf("%w: empty head carries segment state", ErrCorruptCatalog)
		}
		return nil
	}
	if head.SegmentCount == 0 {
		return fmt.Errorf("%w: head has last segment with zero segment_count", ErrCorruptCatalog)
	}
	if err := head.LastSegment.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrCorruptCatalog, err)
	}
	if head.LastSegment.Partition != head.Partition {
		return fmt.Errorf("%w: head partition=%d last segment partition=%d", ErrCorruptCatalog, head.Partition, head.LastSegment.Partition)
	}
	if head.LastSegment.StreamID != head.StreamID {
		return fmt.Errorf("%w: head stream_id=%q last segment stream_id=%q", ErrCorruptCatalog, head.StreamID, head.LastSegment.StreamID)
	}
	if head.LastSegment.WriterEpoch > head.WriterEpoch {
		return fmt.Errorf("%w: last segment writer_epoch=%d head writer_epoch=%d", ErrCorruptCatalog, head.LastSegment.WriterEpoch, head.WriterEpoch)
	}
	if head.NextLSN != head.LastSegment.NextLSN() {
		return fmt.Errorf("%w: head next_lsn=%d last next_lsn=%d", ErrCorruptCatalog, head.NextLSN, head.LastSegment.NextLSN())
	}
	if head.OldestLSN > head.LastSegment.BaseLSN {
		return fmt.Errorf("%w: head oldest_lsn=%d last base_lsn=%d", ErrCorruptCatalog, head.OldestLSN, head.LastSegment.BaseLSN)
	}
	for i, ref := range head.IndexFrontier {
		if ref.Path == "" {
			continue
		}
		wantLevel := uint8(i + 1)
		if err := validatePageRef(ref, wantLevel); err != nil {
			return err
		}
	}
	if head.LeafFrontier != nil {
		if err := validatePageRef(*head.LeafFrontier, 0); err != nil {
			return err
		}
	}
	if err := validateActiveSegments(head.StreamID, head.Partition, head.ActiveSegments); err != nil {
		return err
	}
	if len(head.ActiveSegments) > int(csession.MaxSegmentPageLimit) {
		return fmt.Errorf("%w: active segment count=%d", ErrCorruptCatalog, len(head.ActiveSegments))
	}

	roots := reachableRoots(head)
	if len(roots) == 0 && len(head.ActiveSegments) == 0 {
		return fmt.Errorf("%w: head has no reachable history", ErrCorruptCatalog)
	}
	for i, root := range roots {
		if root.Level > 0 {
			if err := validatePageRef(root, root.Level); err != nil {
				return err
			}
		}
		if i == 0 {
			continue
		}
		prev := roots[i-1]
		if prev.SeqHi == math.MaxUint64 || prev.SeqHi+1 != root.SeqLo {
			return fmt.Errorf("%w: reachable roots are not contiguous", ErrCorruptCatalog)
		}
	}
	var firstLSN uint64
	var lastLSN uint64
	switch {
	case len(roots) > 0:
		firstLSN = roots[0].SeqLo
		lastLSN = roots[len(roots)-1].SeqHi
		if len(head.ActiveSegments) > 0 {
			firstActive := head.ActiveSegments[0]
			if lastLSN == math.MaxUint64 || lastLSN+1 != firstActive.BaseLSN {
				return fmt.Errorf("%w: active segments are not contiguous with sealed roots", ErrCorruptCatalog)
			}
			lastLSN = head.ActiveSegments[len(head.ActiveSegments)-1].LastLSN
		}
	case len(head.ActiveSegments) > 0:
		firstLSN = head.ActiveSegments[0].BaseLSN
		lastLSN = head.ActiveSegments[len(head.ActiveSegments)-1].LastLSN
	}
	if firstLSN != head.OldestLSN {
		return fmt.Errorf("%w: first reachable lsn=%d oldest_lsn=%d", ErrCorruptCatalog, firstLSN, head.OldestLSN)
	}
	if lastLSN != head.LastSegment.LastLSN {
		return fmt.Errorf("%w: last reachable lsn=%d last_lsn=%d", ErrCorruptCatalog, lastLSN, head.LastSegment.LastLSN)
	}
	if len(head.ActiveSegments) > 0 && head.ActiveSegments[len(head.ActiveSegments)-1] != head.LastSegment {
		return fmt.Errorf("%w: last active segment does not match head last segment", ErrCorruptCatalog)
	}
	return nil
}

func validateActiveSegments(streamID string, partition uint32, segments []pmeta.SegmentRef) error {
	for i, segment := range segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, err)
		}
		if segment.StreamID != streamID {
			return fmt.Errorf("%w: active segment stream_id=%q head stream_id=%q", ErrCorruptCatalog, segment.StreamID, streamID)
		}
		if segment.Partition != partition {
			return fmt.Errorf("%w: active segment partition=%d head partition=%d", ErrCorruptCatalog, segment.Partition, partition)
		}
		if i == 0 {
			continue
		}
		prev := segments[i-1]
		if segment.BaseLSN != prev.NextLSN() {
			return fmt.Errorf("%w: non-contiguous active segments", ErrCorruptCatalog)
		}
		if segment.MinTimestampMS < prev.MaxTimestampMS {
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, csession.ErrTimestampOrder)
		}
	}
	return nil
}

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
	if len(page.Segments) == 0 || len(page.Segments) > csession.MaxSegmentPageLimit {
		return fmt.Errorf("%w: leaf segment count=%d", ErrCorruptCatalog, len(page.Segments))
	}
	for i, segment := range page.Segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, err)
		}
		if segment.Partition != page.Partition {
			return fmt.Errorf("%w: leaf partition=%d segment partition=%d", ErrCorruptCatalog, page.Partition, segment.Partition)
		}
		if segment.StreamID != page.StreamID {
			return fmt.Errorf("%w: leaf stream_id=%q segment stream_id=%q", ErrCorruptCatalog, page.StreamID, segment.StreamID)
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
			return fmt.Errorf("%w: %w", ErrCorruptCatalog, csession.ErrTimestampOrder)
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
	if len(page.Refs) == 0 || len(page.Refs) > csession.MaxSegmentPageLimit {
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

func reachableRoots(head headFile) []pageRef {
	roots := make([]pageRef, 0, len(head.IndexFrontier)+1)
	for i := len(head.IndexFrontier) - 1; i >= 0; i-- {
		if head.IndexFrontier[i].Path != "" {
			roots = append(roots, head.IndexFrontier[i])
		}
	}
	if head.LeafFrontier != nil {
		roots = append(roots, *head.LeafFrontier)
	}
	return roots
}
