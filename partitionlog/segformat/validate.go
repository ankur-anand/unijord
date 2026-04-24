package segformat

import "fmt"

func (p FilePreamble) Validate() error {
	if p.Flags != 0 {
		return fmt.Errorf("%w: file preamble flags must be zero", ErrInvalidSegment)
	}
	if err := p.Codec.Validate(); err != nil {
		return err
	}
	if err := p.HashAlgo.Validate(); err != nil {
		return err
	}
	if err := p.RecordFormat.Validate(); err != nil {
		return err
	}
	return nil
}

func (p BlockPreamble) Validate() error {
	if p.Flags != 0 {
		return fmt.Errorf("%w: block preamble flags must be zero", ErrInvalidSegment)
	}
	if p.StoredSize == 0 {
		return fmt.Errorf("%w: stored_size must be positive", ErrInvalidSegment)
	}
	if p.StoredSize > MaxStoredBlockSize {
		return fmt.Errorf("%w: stored_size=%d max=%d", ErrBlockTooLarge, p.StoredSize, MaxStoredBlockSize)
	}
	if p.RawSize == 0 {
		return fmt.Errorf("%w: raw_size must be positive", ErrInvalidSegment)
	}
	if p.RawSize > MaxRawBlockSize {
		return fmt.Errorf("%w: raw_size=%d max=%d", ErrBlockTooLarge, p.RawSize, MaxRawBlockSize)
	}
	if p.RecordCount == 0 {
		return fmt.Errorf("%w: record_count must be positive", ErrInvalidSegment)
	}
	if p.RecordCount > p.RawSize/RecordHeaderSize {
		return fmt.Errorf("%w: record_count=%d cannot fit in raw_size=%d", ErrInvalidSegment, p.RecordCount, p.RawSize)
	}
	if _, err := lastLSN(p.BaseLSN, p.RecordCount); err != nil {
		return err
	}
	if p.MaxTimestampMS < p.MinTimestampMS {
		return fmt.Errorf("%w: block max timestamp < min timestamp", ErrInvalidSegment)
	}
	return nil
}

func (p IndexPreamble) Validate() error {
	if p.Flags != 0 {
		return fmt.Errorf("%w: index preamble flags must be zero", ErrInvalidSegment)
	}
	if p.EntrySize != BlockIndexEntrySize {
		return fmt.Errorf("%w: entry_size=%d want=%d", ErrInvalidSegment, p.EntrySize, BlockIndexEntrySize)
	}
	if p.EntryCount == 0 {
		return fmt.Errorf("%w: entry_count must be positive", ErrInvalidSegment)
	}
	if p.EntryCount > MaxBlockCount {
		return fmt.Errorf("%w: entry_count=%d max=%d", ErrInvalidSegment, p.EntryCount, MaxBlockCount)
	}
	return nil
}

func (e BlockIndexEntry) Validate() error {
	if e.StoredSize == 0 {
		return fmt.Errorf("%w: index stored_size must be positive", ErrInvalidSegment)
	}
	if e.StoredSize > MaxStoredBlockSize {
		return fmt.Errorf("%w: index stored_size=%d max=%d", ErrBlockTooLarge, e.StoredSize, MaxStoredBlockSize)
	}
	if e.RawSize == 0 {
		return fmt.Errorf("%w: index raw_size must be positive", ErrInvalidSegment)
	}
	if e.RawSize > MaxRawBlockSize {
		return fmt.Errorf("%w: index raw_size=%d max=%d", ErrBlockTooLarge, e.RawSize, MaxRawBlockSize)
	}
	if e.RecordCount == 0 {
		return fmt.Errorf("%w: index record_count must be positive", ErrInvalidSegment)
	}
	if e.RecordCount > e.RawSize/RecordHeaderSize {
		return fmt.Errorf("%w: index record_count=%d cannot fit in raw_size=%d", ErrInvalidSegment, e.RecordCount, e.RawSize)
	}
	if _, err := lastLSN(e.BaseLSN, e.RecordCount); err != nil {
		return err
	}
	if e.MaxTimestampMS < e.MinTimestampMS {
		return fmt.Errorf("%w: index max timestamp < min timestamp", ErrInvalidSegment)
	}
	return nil
}

func (t Trailer) Validate(objectSize uint64) error {
	if t.Flags != 0 {
		return fmt.Errorf("%w: trailer flags must be zero", ErrInvalidSegment)
	}
	if err := t.Codec.Validate(); err != nil {
		return err
	}
	if err := t.HashAlgo.Validate(); err != nil {
		return err
	}
	if err := t.RecordFormat.Validate(); err != nil {
		return err
	}
	if t.LastLSN < t.BaseLSN {
		return fmt.Errorf("%w: last_lsn < base_lsn", ErrInvalidSegment)
	}
	if t.RecordCount == 0 {
		return fmt.Errorf("%w: record_count must be positive", ErrInvalidSegment)
	}
	if t.BlockCount == 0 {
		return fmt.Errorf("%w: block_count must be positive", ErrInvalidSegment)
	}
	if t.BlockCount > MaxBlockCount {
		return fmt.Errorf("%w: block_count=%d max=%d", ErrInvalidSegment, t.BlockCount, MaxBlockCount)
	}
	if t.MaxTimestampMS < t.MinTimestampMS {
		return fmt.Errorf("%w: trailer max timestamp < min timestamp", ErrInvalidSegment)
	}
	expectedRecords := t.LastLSN - t.BaseLSN + 1
	if expectedRecords != uint64(t.RecordCount) {
		return fmt.Errorf("%w: record_count=%d lsn_span=%d", ErrInvalidSegment, t.RecordCount, expectedRecords)
	}
	expectedIndexLength := uint64(IndexPreambleSize) + uint64(t.BlockCount)*uint64(BlockIndexEntrySize)
	if expectedIndexLength > 1<<32-1 {
		return fmt.Errorf("%w: block index length overflows u32", ErrInvalidSegment)
	}
	if t.BlockIndexLength != uint32(expectedIndexLength) {
		return fmt.Errorf("%w: block_index_length=%d want=%d", ErrInvalidSegment, t.BlockIndexLength, expectedIndexLength)
	}
	if t.BlockIndexOffset < FilePreambleSize+BlockPreambleSize {
		return fmt.Errorf("%w: block_index_offset=%d too small", ErrInvalidSegment, t.BlockIndexOffset)
	}
	expectedTotal := t.BlockIndexOffset + uint64(t.BlockIndexLength) + TrailerSize
	if t.TotalSize != expectedTotal {
		return fmt.Errorf("%w: total_size=%d want=%d", ErrInvalidSegment, t.TotalSize, expectedTotal)
	}
	if objectSize != 0 && t.TotalSize != objectSize {
		return fmt.Errorf("%w: object size=%d trailer total_size=%d", ErrInvalidSegment, objectSize, t.TotalSize)
	}
	return nil
}

func ValidateIndex(entries []BlockIndexEntry, trailer Trailer) error {
	if len(entries) == 0 {
		return fmt.Errorf("%w: empty index", ErrBlockIndexMismatch)
	}
	if uint32(len(entries)) != trailer.BlockCount {
		return fmt.Errorf("%w: entries=%d block_count=%d", ErrBlockIndexMismatch, len(entries), trailer.BlockCount)
	}
	if entries[0].BlockOffset != FilePreambleSize {
		return fmt.Errorf("%w: first block_offset=%d want=%d", ErrBlockIndexMismatch, entries[0].BlockOffset, FilePreambleSize)
	}
	var recordCount uint64
	for i := range entries {
		if err := entries[i].Validate(); err != nil {
			return err
		}
		recordCount += uint64(entries[i].RecordCount)
		if i == 0 {
			if entries[i].BaseLSN != trailer.BaseLSN {
				return fmt.Errorf("%w: first base_lsn=%d trailer base_lsn=%d", ErrBlockIndexMismatch, entries[i].BaseLSN, trailer.BaseLSN)
			}
			if entries[i].MinTimestampMS != trailer.MinTimestampMS {
				return fmt.Errorf("%w: first min timestamp=%d trailer min timestamp=%d", ErrBlockIndexMismatch, entries[i].MinTimestampMS, trailer.MinTimestampMS)
			}
			continue
		}
		prev := entries[i-1]
		wantLSN, err := nextLSN(prev.BaseLSN, prev.RecordCount)
		if err != nil {
			return err
		}
		if entries[i].BaseLSN != wantLSN {
			return fmt.Errorf("%w: entry %d base_lsn=%d want=%d", ErrBlockIndexMismatch, i, entries[i].BaseLSN, wantLSN)
		}
		wantOffset, err := nextBlockOffset(prev.BlockOffset, prev.StoredSize)
		if err != nil {
			return err
		}
		if entries[i].BlockOffset != wantOffset {
			return fmt.Errorf("%w: entry %d block_offset=%d want=%d", ErrBlockIndexMismatch, i, entries[i].BlockOffset, wantOffset)
		}
		if entries[i].MinTimestampMS < prev.MaxTimestampMS {
			return fmt.Errorf("%w: entry %d timestamp regression", ErrBlockIndexMismatch, i)
		}
	}
	last := entries[len(entries)-1]
	lastLSN, err := lastLSN(last.BaseLSN, last.RecordCount)
	if err != nil {
		return err
	}
	if lastLSN != trailer.LastLSN {
		return fmt.Errorf("%w: last_lsn=%d trailer last_lsn=%d", ErrBlockIndexMismatch, lastLSN, trailer.LastLSN)
	}
	indexOffset, err := nextBlockOffset(last.BlockOffset, last.StoredSize)
	if err != nil {
		return err
	}
	if indexOffset != trailer.BlockIndexOffset {
		return fmt.Errorf("%w: index offset=%d trailer index offset=%d", ErrBlockIndexMismatch, indexOffset, trailer.BlockIndexOffset)
	}
	if last.MaxTimestampMS != trailer.MaxTimestampMS {
		return fmt.Errorf("%w: last max timestamp=%d trailer max timestamp=%d", ErrBlockIndexMismatch, last.MaxTimestampMS, trailer.MaxTimestampMS)
	}
	if recordCount != uint64(trailer.RecordCount) {
		return fmt.Errorf("%w: record sum=%d trailer record_count=%d", ErrBlockIndexMismatch, recordCount, trailer.RecordCount)
	}
	return nil
}

func ValidatePreambleTrailer(p FilePreamble, t Trailer) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := t.Validate(t.TotalSize); err != nil {
		return err
	}
	if p.Partition != t.Partition ||
		p.Codec != t.Codec ||
		p.HashAlgo != t.HashAlgo ||
		p.RecordFormat != t.RecordFormat ||
		p.BaseLSN != t.BaseLSN ||
		p.SegmentUUID != t.SegmentUUID ||
		p.WriterTag != t.WriterTag {
		return fmt.Errorf("%w: file preamble does not match trailer", ErrInvalidSegment)
	}
	return nil
}

func nextLSN(base uint64, count uint32) (uint64, error) {
	if count == 0 {
		return 0, fmt.Errorf("%w: record_count must be positive", ErrInvalidSegment)
	}
	if base > ^uint64(0)-uint64(count) {
		return 0, fmt.Errorf("%w: lsn range overflows uint64", ErrInvalidSegment)
	}
	return base + uint64(count), nil
}

func lastLSN(base uint64, count uint32) (uint64, error) {
	next, err := nextLSN(base, count)
	if err != nil {
		return 0, err
	}
	return next - 1, nil
}

func nextBlockOffset(offset uint64, storedSize uint32) (uint64, error) {
	if offset > ^uint64(0)-BlockPreambleSize {
		return 0, fmt.Errorf("%w: block offset overflows uint64", ErrInvalidSegment)
	}
	offset += BlockPreambleSize
	if offset > ^uint64(0)-uint64(storedSize) {
		return 0, fmt.Errorf("%w: block offset overflows uint64", ErrInvalidSegment)
	}
	return offset + uint64(storedSize), nil
}

func MatchBlockIndexEntry(p BlockPreamble, e BlockIndexEntry) error {
	if p.StoredSize != e.StoredSize ||
		p.RawSize != e.RawSize ||
		p.RecordCount != e.RecordCount ||
		p.BaseLSN != e.BaseLSN ||
		p.MinTimestampMS != e.MinTimestampMS ||
		p.MaxTimestampMS != e.MaxTimestampMS ||
		p.BlockHash != e.BlockHash {
		return fmt.Errorf("%w: block preamble does not match index entry", ErrBlockIndexMismatch)
	}
	return nil
}
