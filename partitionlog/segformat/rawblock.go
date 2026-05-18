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
		size, err := RawRecordSize(r)
		if err != nil {
			return nil, err
		}
		if i > 0 && r.TimestampMS < prevTS {
			return nil, fmt.Errorf("%w: timestamp regression at record %d", ErrInvalidSegment, i)
		}
		prevTS = r.TimestampMS
		total += size
		if total > MaxRawBlockSize {
			return nil, fmt.Errorf("%w: raw block size=%d max=%d", ErrBlockTooLarge, total, MaxRawBlockSize)
		}
	}
	raw := make([]byte, 0, total)
	for _, r := range records {
		var err error
		raw, err = AppendRawRecord(raw, r)
		if err != nil {
			return nil, err
		}
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
		record, next, err := scanRawRecord(raw, off)
		if err != nil {
			return nil, err
		}
		ts := record.TimestampMS
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
			Headers:     record.Headers,
			Value:       record.Value,
		})
		off = next
	}
	if off != len(raw) {
		return nil, fmt.Errorf("%w: raw block has %d trailing bytes", ErrInvalidSegment, len(raw)-off)
	}
	return records, nil
}

func RecordSize(headers []Header, value []byte) (int, error) {
	return RawRecordSize(RawRecord{Headers: headers, Value: value})
}

func RawRecordSize(record RawRecord) (int, error) {
	if len(record.Value) > MaxRecordValueLen {
		return 0, fmt.Errorf("%w: value_len=%d max=%d", ErrRecordTooLarge, len(record.Value), MaxRecordValueLen)
	}
	headersLen, err := encodedHeadersLen(record.Headers)
	if err != nil {
		return 0, err
	}
	return RecordHeaderSize + headersLen + len(record.Value), nil
}

func AppendRawRecord(dst []byte, record RawRecord) ([]byte, error) {
	size, err := RawRecordSize(record)
	if err != nil {
		return nil, err
	}
	off := len(dst)
	dst = append(dst, make([]byte, size)...)
	binary.BigEndian.PutUint64(dst[off:off+8], uint64(record.TimestampMS))
	headersLen, _ := encodedHeadersLen(record.Headers)
	binary.BigEndian.PutUint32(dst[off+8:off+12], uint32(headersLen))
	binary.BigEndian.PutUint32(dst[off+12:off+16], uint32(len(record.Value)))
	pos := off + RecordHeaderSize
	pos = appendHeaders(dst, pos, record.Headers)
	copy(dst[pos:pos+len(record.Value)], record.Value)
	return dst, nil
}

func scanRawRecord(raw []byte, off int) (RawRecord, int, error) {
	ts := int64(binary.BigEndian.Uint64(raw[off : off+8]))
	headersLen := binary.BigEndian.Uint32(raw[off+8 : off+12])
	valueLen := binary.BigEndian.Uint32(raw[off+12 : off+16])
	if headersLen > MaxRecordHeaderBytes {
		return RawRecord{}, 0, fmt.Errorf("%w: headers_len=%d max=%d", ErrRecordTooLarge, headersLen, MaxRecordHeaderBytes)
	}
	if valueLen > MaxRecordValueLen {
		return RawRecord{}, 0, fmt.Errorf("%w: value_len=%d max=%d", ErrRecordTooLarge, valueLen, MaxRecordValueLen)
	}
	headersOff := off + RecordHeaderSize
	if uint64(len(raw)-headersOff) < uint64(headersLen)+uint64(valueLen) {
		return RawRecord{}, 0, fmt.Errorf("%w: truncated raw record", ErrInvalidSegment)
	}
	valueOff := headersOff + int(headersLen)
	next := valueOff + int(valueLen)
	headers, err := decodeHeaders(raw[headersOff:valueOff])
	if err != nil {
		return RawRecord{}, 0, err
	}
	return RawRecord{TimestampMS: ts, Headers: headers, Value: raw[valueOff:next]}, next, nil
}

func encodedHeadersLen(headers []Header) (int, error) {
	if len(headers) == 0 {
		return 0, nil
	}
	if len(headers) > MaxRecordHeaders {
		return 0, fmt.Errorf("%w: header_count=%d max=%d", ErrRecordTooLarge, len(headers), MaxRecordHeaders)
	}
	total := 2
	for i, h := range headers {
		if len(h.Key) == 0 {
			return 0, fmt.Errorf("%w: header %d key is empty", ErrInvalidSegment, i)
		}
		if len(h.Key) > MaxRecordHeaderKeyLen {
			return 0, fmt.Errorf("%w: header %d key_len=%d max=%d", ErrRecordTooLarge, i, len(h.Key), MaxRecordHeaderKeyLen)
		}
		if len(h.Value) > MaxRecordHeaderValueLen {
			return 0, fmt.Errorf("%w: header %d value_len=%d max=%d", ErrRecordTooLarge, i, len(h.Value), MaxRecordHeaderValueLen)
		}
		total += 4 + len(h.Key) + len(h.Value)
		if total > MaxRecordHeaderBytes {
			return 0, fmt.Errorf("%w: headers_len=%d max=%d", ErrRecordTooLarge, total, MaxRecordHeaderBytes)
		}
	}
	return total, nil
}

func appendHeaders(dst []byte, off int, headers []Header) int {
	if len(headers) == 0 {
		return off
	}
	binary.BigEndian.PutUint16(dst[off:off+2], uint16(len(headers)))
	off += 2
	for _, h := range headers {
		binary.BigEndian.PutUint16(dst[off:off+2], uint16(len(h.Key)))
		binary.BigEndian.PutUint16(dst[off+2:off+4], uint16(len(h.Value)))
		off += 4
		copy(dst[off:off+len(h.Key)], h.Key)
		off += len(h.Key)
		copy(dst[off:off+len(h.Value)], h.Value)
		off += len(h.Value)
	}
	return off
}

func decodeHeaders(buf []byte) ([]Header, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 2 {
		return nil, fmt.Errorf("%w: truncated record headers", ErrInvalidSegment)
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	if count == 0 {
		return nil, fmt.Errorf("%w: zero header count in non-empty headers", ErrInvalidSegment)
	}
	if count > MaxRecordHeaders {
		return nil, fmt.Errorf("%w: header_count=%d max=%d", ErrRecordTooLarge, count, MaxRecordHeaders)
	}
	headers := make([]Header, count)
	off := 2
	for i := 0; i < count; i++ {
		if len(buf)-off < 4 {
			return nil, fmt.Errorf("%w: truncated record header entry", ErrInvalidSegment)
		}
		keyLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
		valueLen := int(binary.BigEndian.Uint16(buf[off+2 : off+4]))
		off += 4
		if keyLen == 0 {
			return nil, fmt.Errorf("%w: header %d key is empty", ErrInvalidSegment, i)
		}
		if keyLen > MaxRecordHeaderKeyLen {
			return nil, fmt.Errorf("%w: header %d key_len=%d max=%d", ErrRecordTooLarge, i, keyLen, MaxRecordHeaderKeyLen)
		}
		if valueLen > MaxRecordHeaderValueLen {
			return nil, fmt.Errorf("%w: header %d value_len=%d max=%d", ErrRecordTooLarge, i, valueLen, MaxRecordHeaderValueLen)
		}
		if len(buf)-off < keyLen+valueLen {
			return nil, fmt.Errorf("%w: truncated record header bytes", ErrInvalidSegment)
		}
		headers[i] = Header{
			Key:   buf[off : off+keyLen],
			Value: buf[off+keyLen : off+keyLen+valueLen],
		}
		off += keyLen + valueLen
	}
	if off != len(buf) {
		return nil, fmt.Errorf("%w: record headers have %d trailing bytes", ErrInvalidSegment, len(buf)-off)
	}
	return headers, nil
}
