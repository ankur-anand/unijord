package segformat

import (
	"encoding/binary"
	"fmt"
)

func EncodeRawBlock(records []RawRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: raw block must contain at least one record", ErrInvalidSegment)
	}
	total := 0
	prevTS := records[0].TimestampMS
	for i, r := range records {
		if len(r.Value) > MaxRecordValueLen {
			return nil, fmt.Errorf("%w: value_len=%d max=%d", ErrRecordTooLarge, len(r.Value), MaxRecordValueLen)
		}
		if i > 0 && r.TimestampMS < prevTS {
			return nil, fmt.Errorf("%w: timestamp regression at record %d", ErrInvalidSegment, i)
		}
		prevTS = r.TimestampMS
		total += RecordHeaderSize + len(r.Value)
		if total > MaxRawBlockSize {
			return nil, fmt.Errorf("%w: raw block size=%d max=%d", ErrBlockTooLarge, total, MaxRawBlockSize)
		}
	}
	raw := make([]byte, total)
	off := 0
	for _, r := range records {
		binary.BigEndian.PutUint64(raw[off:off+8], uint64(r.TimestampMS))
		binary.BigEndian.PutUint32(raw[off+8:off+12], uint32(len(r.Value)))
		off += RecordHeaderSize
		copy(raw[off:off+len(r.Value)], r.Value)
		off += len(r.Value)
	}
	return raw, nil
}

func DecodeRawBlock(raw []byte, block BlockPreamble) ([]Record, error) {
	if err := block.Validate(); err != nil {
		return nil, err
	}
	if _, err := lastLSN(block.BaseLSN, block.RecordCount); err != nil {
		return nil, err
	}
	if len(raw) != int(block.RawSize) {
		return nil, fmt.Errorf("%w: raw_size=%d want=%d", ErrInvalidSegment, len(raw), block.RawSize)
	}
	if block.RecordCount > uint32(len(raw)/RecordHeaderSize) {
		return nil, fmt.Errorf("%w: record_count=%d cannot fit in raw_size=%d", ErrInvalidSegment, block.RecordCount, len(raw))
	}
	records := make([]Record, 0, block.RecordCount)
	off := 0
	var prevTS int64
	for i := uint32(0); i < block.RecordCount; i++ {
		if len(raw)-off < RecordHeaderSize {
			return nil, fmt.Errorf("%w: truncated raw record header", ErrInvalidSegment)
		}
		ts := int64(binary.BigEndian.Uint64(raw[off : off+8]))
		valueLen := binary.BigEndian.Uint32(raw[off+8 : off+12])
		off += RecordHeaderSize
		if valueLen > MaxRecordValueLen {
			return nil, fmt.Errorf("%w: value_len=%d max=%d", ErrRecordTooLarge, valueLen, MaxRecordValueLen)
		}
		if uint64(len(raw)-off) < uint64(valueLen) {
			return nil, fmt.Errorf("%w: truncated raw record value", ErrInvalidSegment)
		}
		if ts < block.MinTimestampMS || ts > block.MaxTimestampMS {
			return nil, fmt.Errorf("%w: timestamp=%d outside block range", ErrInvalidSegment, ts)
		}
		if i > 0 && ts < prevTS {
			return nil, fmt.Errorf("%w: timestamp regression at record %d", ErrInvalidSegment, i)
		}
		prevTS = ts
		records = append(records, Record{
			LSN:         block.BaseLSN + uint64(i),
			TimestampMS: ts,
			Value:       raw[off : off+int(valueLen)],
		})
		off += int(valueLen)
	}
	if off != len(raw) {
		return nil, fmt.Errorf("%w: raw block has %d trailing bytes", ErrInvalidSegment, len(raw)-off)
	}
	return records, nil
}
