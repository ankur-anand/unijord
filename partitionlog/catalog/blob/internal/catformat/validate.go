package catformat

import (
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidObject, fmt.Sprintf(format, args...))
}

func isZero16(value [16]byte) bool { return value == [16]byte{} }
func isZero32(value [32]byte) bool { return value == [32]byte{} }

func validateLeafEntry(entry LeafEntry) error {
	if entry.BaseLSN > entry.LastLSN || entry.LastLSN > MaxRecordLSN {
		return invalidf("invalid leaf LSN range [%d,%d]", entry.BaseLSN, entry.LastLSN)
	}
	span := entry.LastLSN - entry.BaseLSN + 1
	if span > math.MaxUint32 || entry.RecordCount == 0 || uint64(entry.RecordCount) != span {
		return invalidf("leaf record_count=%d does not match LSN span=%d", entry.RecordCount, span)
	}
	if entry.MinTimestampMS > entry.MaxTimestampMS {
		return invalidf("leaf timestamp range [%d,%d] is inverted", entry.MinTimestampMS, entry.MaxTimestampMS)
	}
	if entry.BlockCount == 0 {
		return invalidf("leaf block_count is zero")
	}
	if entry.SizeBytes == 0 {
		return invalidf("leaf size_bytes is zero")
	}
	if entry.BlockIndexLength == 0 {
		return invalidf("leaf block_index_length is zero")
	}
	if err := entry.Codec.Validate(); err != nil {
		return invalidf("leaf codec: %v", err)
	}
	if err := entry.HashAlgo.Validate(); err != nil {
		return invalidf("leaf hash algorithm: %v", err)
	}
	if entry.WriterEpoch == 0 {
		return invalidf("leaf writer_epoch is zero")
	}
	if isZero16(entry.SegmentUUID) {
		return invalidf("leaf segment_uuid is zero")
	}
	if isZero16(entry.WriterTag) {
		return invalidf("leaf writer_tag is zero")
	}
	return nil
}

func validateIndexEntry(entry IndexEntry) error {
	if entry.Level > MaxIndexLevel {
		return invalidf("index child level=%d exceeds maximum=%d", entry.Level, MaxIndexLevel)
	}
	if entry.SeqLo > entry.SeqHi || entry.SeqHi > MaxRecordLSN {
		return invalidf("invalid index LSN range [%d,%d]", entry.SeqLo, entry.SeqHi)
	}
	if entry.MinTimestampMS > entry.MaxTimestampMS {
		return invalidf("index timestamp range [%d,%d] is inverted", entry.MinTimestampMS, entry.MaxTimestampMS)
	}
	limit := uint32(MaxIndexEntries)
	if entry.Level == 0 {
		limit = MaxLeafEntries
	}
	if entry.EntryCount == 0 || entry.EntryCount > limit {
		return invalidf("index entry_count=%d outside [1,%d]", entry.EntryCount, limit)
	}
	if entry.Generation == 0 {
		return invalidf("index generation is zero")
	}
	if isZero16(entry.PageID) {
		return invalidf("index page_id is zero")
	}
	if entry.SegmentCount < uint64(entry.EntryCount) {
		return invalidf("index segment_count=%d is below entry_count=%d", entry.SegmentCount, entry.EntryCount)
	}
	if entry.Level == 0 && entry.SegmentCount != uint64(entry.EntryCount) {
		return invalidf("leaf reference segment_count=%d differs from entry_count=%d", entry.SegmentCount, entry.EntryCount)
	}
	span := entry.SeqHi - entry.SeqLo + 1
	if entry.SegmentCount > span {
		return invalidf("index segment_count=%d exceeds LSN span=%d", entry.SegmentCount, span)
	}
	return nil
}

func validateCommonPageHeader(header PageHeader) error {
	if header.Flags != 0 {
		return invalidf("page flags=%#x contain unknown bits", header.Flags)
	}
	if err := header.HashAlgo.Validate(); err != nil {
		return invalidf("page hash algorithm: %v", err)
	}
	if header.Generation == 0 {
		return invalidf("page generation is zero")
	}
	if header.WriterEpoch == 0 {
		return invalidf("page writer_epoch is zero")
	}
	if isZero32(header.StreamKey) {
		return invalidf("page stream_key is zero")
	}
	return nil
}

func validateLeafPage(page LeafPage) error {
	if err := validateCommonPageHeader(page.Header); err != nil {
		return err
	}
	if page.Header.Level != 0 {
		return invalidf("leaf page level=%d, want 0", page.Header.Level)
	}
	if page.Header.EntrySize != LeafEntrySize {
		return invalidf("leaf page entry_size=%d, want %d", page.Header.EntrySize, LeafEntrySize)
	}
	if len(page.Entries) == 0 || len(page.Entries) > MaxLeafEntries {
		return invalidf("leaf page entry count=%d outside [1,%d]", len(page.Entries), MaxLeafEntries)
	}
	if page.Header.EntryCount != uint32(len(page.Entries)) {
		return invalidf("leaf header entry_count=%d, actual=%d", page.Header.EntryCount, len(page.Entries))
	}
	if page.Header.SegmentCount != uint64(len(page.Entries)) {
		return invalidf("leaf header segment_count=%d, want %d", page.Header.SegmentCount, len(page.Entries))
	}
	for i, entry := range page.Entries {
		if err := validateLeafEntry(entry); err != nil {
			return invalidf("leaf entry %d: %v", i, err)
		}
		if i == 0 {
			continue
		}
		previous := page.Entries[i-1]
		if previous.LastLSN == MaxRecordLSN || entry.BaseLSN != previous.LastLSN+1 {
			return invalidf("leaf entries %d and %d are not contiguous", i-1, i)
		}
		if entry.MinTimestampMS < previous.MaxTimestampMS {
			return invalidf("leaf timestamps decrease between entries %d and %d", i-1, i)
		}
		if entry.WriterEpoch < previous.WriterEpoch {
			return invalidf("leaf writer epochs decrease between entries %d and %d", i-1, i)
		}
	}
	first, last := page.Entries[0], page.Entries[len(page.Entries)-1]
	if page.Header.SeqLo != first.BaseLSN || page.Header.SeqHi != last.LastLSN ||
		page.Header.MinTimestampMS != first.MinTimestampMS || page.Header.MaxTimestampMS != last.MaxTimestampMS {
		return invalidf("leaf page bounds do not match first and last entries")
	}
	if page.Header.WriterEpoch < last.WriterEpoch {
		return invalidf("page writer_epoch=%d is below last leaf epoch=%d", page.Header.WriterEpoch, last.WriterEpoch)
	}
	return nil
}

func validateIndexPage(page IndexPage) error {
	if err := validateCommonPageHeader(page.Header); err != nil {
		return err
	}
	if page.Header.Level == 0 || page.Header.Level > MaxIndexLevel {
		return invalidf("index page level=%d outside [1,%d]", page.Header.Level, MaxIndexLevel)
	}
	if page.Header.EntrySize != IndexEntrySize {
		return invalidf("index page entry_size=%d, want %d", page.Header.EntrySize, IndexEntrySize)
	}
	if len(page.Entries) == 0 || len(page.Entries) > MaxIndexEntries {
		return invalidf("index page entry count=%d outside [1,%d]", len(page.Entries), MaxIndexEntries)
	}
	if page.Header.EntryCount != uint32(len(page.Entries)) {
		return invalidf("index header entry_count=%d, actual=%d", page.Header.EntryCount, len(page.Entries))
	}
	var segmentCount uint64
	for i, entry := range page.Entries {
		if err := validateIndexEntry(entry); err != nil {
			return invalidf("index entry %d: %v", i, err)
		}
		if entry.Level+1 != page.Header.Level {
			return invalidf("index entry %d level=%d, want %d", i, entry.Level, page.Header.Level-1)
		}
		if entry.Generation > page.Header.Generation {
			return invalidf("index entry %d generation=%d exceeds page generation=%d", i, entry.Generation, page.Header.Generation)
		}
		if math.MaxUint64-segmentCount < entry.SegmentCount {
			return invalidf("index segment_count sum overflows")
		}
		segmentCount += entry.SegmentCount
		if i == 0 {
			continue
		}
		previous := page.Entries[i-1]
		if previous.SeqHi == MaxRecordLSN || entry.SeqLo != previous.SeqHi+1 {
			return invalidf("index entries %d and %d are not contiguous", i-1, i)
		}
		if entry.MinTimestampMS < previous.MaxTimestampMS {
			return invalidf("index timestamps decrease between entries %d and %d", i-1, i)
		}
	}
	if page.Header.SegmentCount != segmentCount {
		return invalidf("index header segment_count=%d, computed=%d", page.Header.SegmentCount, segmentCount)
	}
	first, last := page.Entries[0], page.Entries[len(page.Entries)-1]
	if page.Header.SeqLo != first.SeqLo || page.Header.SeqHi != last.SeqHi ||
		page.Header.MinTimestampMS != first.MinTimestampMS || page.Header.MaxTimestampMS != last.MaxTimestampMS {
		return invalidf("index page bounds do not match first and last entries")
	}
	return nil
}

type rootRange struct {
	seqLo, seqHi uint64
	minTS, maxTS int64
	segments     uint64
	leaf         *LeafEntry
}

func validateHead(head Head) error {
	header := head.Header
	if header.Flags&^FlagHasLastSegment != 0 {
		return invalidf("head flags=%#x contain unknown bits", header.Flags)
	}
	if err := header.HashAlgo.Validate(); err != nil {
		return invalidf("head hash algorithm: %v", err)
	}
	if header.SegmentLayoutVersion != SegmentLayoutV1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedLayout, header.SegmentLayoutVersion)
	}
	if header.LeafLimit == 0 || header.LeafLimit > MaxLeafEntries {
		return invalidf("head leaf_limit=%d outside [1,%d]", header.LeafLimit, MaxLeafEntries)
	}
	if header.IndexLimit < 2 || header.IndexLimit > MaxIndexEntries {
		return invalidf("head index_limit=%d outside [2,%d]", header.IndexLimit, MaxIndexEntries)
	}
	if len(head.Active) >= int(header.LeafLimit) || header.ActiveCount != uint32(len(head.Active)) {
		return invalidf("head active_count=%d actual=%d limit=%d", header.ActiveCount, len(head.Active), header.LeafLimit)
	}
	if len(head.Sections) > MaxOpenIndexLevel || header.LevelCount != uint32(len(head.Sections)) {
		return invalidf("head level_count=%d actual=%d maximum=%d", header.LevelCount, len(head.Sections), MaxOpenIndexLevel)
	}
	if header.NextLSN > ReservedLSN || header.OldestLSN > header.NextLSN || header.AppliedRetentionLSN > header.NextLSN {
		return invalidf("head LSN state next=%d oldest=%d retention=%d", header.NextLSN, header.OldestLSN, header.AppliedRetentionLSN)
	}
	if header.AppliedRetentionVersion == 0 && header.AppliedRetentionLSN != 0 {
		return invalidf("head has applied_retention_lsn=%d without a retention version", header.AppliedRetentionLSN)
	}
	if (header.WriterEpoch == 0) != isZero16(header.WriterID) {
		return invalidf("head writer epoch and writer ID zero states differ")
	}
	if header.ReachableSegmentCount > header.SegmentCount {
		return invalidf("head reachable_segment_count=%d exceeds segment_count=%d", header.ReachableSegmentCount, header.SegmentCount)
	}
	if isZero32(header.StreamKey) {
		return invalidf("head stream_key is zero")
	}
	if isZero32(header.DataRootKey) {
		return invalidf("head data_root_key is zero")
	}

	roots := make([]rootRange, 0, len(head.Active)+len(head.Sections)*int(header.IndexLimit))
	for i := range head.Sections {
		section := head.Sections[i]
		wantLevel := uint8(i + 1)
		if section.Level != wantLevel {
			return invalidf("head section %d level=%d, want %d", i, section.Level, wantLevel)
		}
		if len(section.Entries) >= int(header.IndexLimit) {
			return invalidf("head section level=%d entry count=%d reaches limit=%d", section.Level, len(section.Entries), header.IndexLimit)
		}
		for j, entry := range section.Entries {
			if err := validateIndexEntry(entry); err != nil {
				return invalidf("head section level=%d entry=%d: %v", section.Level, j, err)
			}
			if entry.Level+1 != section.Level {
				return invalidf("head section level=%d entry=%d child level=%d", section.Level, j, entry.Level)
			}
			if entry.Generation > header.Generation {
				return invalidf("head section level=%d entry=%d generation=%d exceeds head generation=%d", section.Level, j, entry.Generation, header.Generation)
			}
		}
	}
	for i := len(head.Sections) - 1; i >= 0; i-- {
		for j := range head.Sections[i].Entries {
			entry := &head.Sections[i].Entries[j]
			roots = append(roots, rootRange{entry.SeqLo, entry.SeqHi, entry.MinTimestampMS, entry.MaxTimestampMS, entry.SegmentCount, nil})
		}
	}
	for i := range head.Active {
		entry := &head.Active[i]
		if err := validateLeafEntry(*entry); err != nil {
			return invalidf("head active leaf %d: %v", i, err)
		}
		if entry.WriterEpoch > header.WriterEpoch {
			return invalidf("head active leaf %d writer_epoch=%d exceeds head writer_epoch=%d", i, entry.WriterEpoch, header.WriterEpoch)
		}
		if i > 0 {
			previous := head.Active[i-1]
			if entry.WriterEpoch < previous.WriterEpoch {
				return invalidf("head active writer epochs decrease between entries %d and %d", i-1, i)
			}
		}
		roots = append(roots, rootRange{entry.BaseLSN, entry.LastLSN, entry.MinTimestampMS, entry.MaxTimestampMS, 1, entry})
	}

	if !head.HasLastSegment() {
		if head.LastSegment != (LeafEntry{}) {
			return invalidf("head without last segment has non-zero last_segment bytes")
		}
		if header.OldestLSN != header.NextLSN || header.SegmentCount != 0 || header.ReachableSegmentCount != 0 || len(head.Active) != 0 || len(roots) != 0 {
			return invalidf("empty head has reachable history")
		}
		return nil
	}
	if err := validateLeafEntry(head.LastSegment); err != nil {
		return invalidf("head last_segment: %v", err)
	}
	if head.LastSegment.WriterEpoch > header.WriterEpoch {
		return invalidf("last_segment writer_epoch=%d exceeds head writer_epoch=%d", head.LastSegment.WriterEpoch, header.WriterEpoch)
	}
	if header.SegmentCount == 0 {
		return invalidf("head has last segment with zero segment_count")
	}
	if head.LastSegment.LastLSN == MaxRecordLSN || head.LastSegment.LastLSN+1 != header.NextLSN {
		return invalidf("last_segment last_lsn=%d does not precede next_lsn=%d", head.LastSegment.LastLSN, header.NextLSN)
	}
	if len(roots) == 0 {
		if header.OldestLSN != header.NextLSN || header.ReachableSegmentCount != 0 || len(head.Active) != 0 {
			return invalidf("head without reachable roots is not fully retained")
		}
		return nil
	}
	if roots[len(roots)-1].seqHi != head.LastSegment.LastLSN {
		return invalidf("last root seq_hi=%d differs from last_segment last_lsn=%d", roots[len(roots)-1].seqHi, head.LastSegment.LastLSN)
	}
	if len(head.Active) > 0 && head.Active[len(head.Active)-1] != head.LastSegment {
		return invalidf("rightmost active leaf differs from last_segment")
	}

	var segmentCount uint64
	for i, root := range roots {
		if math.MaxUint64-segmentCount < root.segments {
			return invalidf("head root segment_count sum overflows")
		}
		segmentCount += root.segments
		if i == 0 {
			if root.seqLo != header.OldestLSN {
				return invalidf("first root seq_lo=%d differs from oldest_lsn=%d", root.seqLo, header.OldestLSN)
			}
			continue
		}
		previous := roots[i-1]
		if previous.seqHi == MaxRecordLSN || root.seqLo != previous.seqHi+1 {
			return invalidf("head roots %d and %d are not contiguous", i-1, i)
		}
		if root.minTS < previous.maxTS {
			return invalidf("head timestamps decrease between roots %d and %d", i-1, i)
		}
	}
	last := roots[len(roots)-1]
	if last.seqHi == MaxRecordLSN || last.seqHi+1 != header.NextLSN {
		return invalidf("last root seq_hi=%d does not precede next_lsn=%d", last.seqHi, header.NextLSN)
	}
	if segmentCount != header.ReachableSegmentCount {
		return invalidf("head reachable_segment_count=%d, computed=%d", header.ReachableSegmentCount, segmentCount)
	}
	return nil
}

func validateHashAlgo(algo segformat.HashAlgo) error {
	if err := algo.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidObject, err)
	}
	return nil
}
