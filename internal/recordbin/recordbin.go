package recordbin

import (
	"encoding/binary"
	"io"
)

const (
	uint64Size           = 8
	storedValueHeaderLen = uint64Size
	walRecordHeaderLen   = 2 * uint64Size
)

type StoredValue struct {
	TimestampMS uint64
	Value       []byte
}

type WALRecord struct {
	LSN         uint64
	TimestampMS uint64
	Value       []byte
}

func EncodeStoredValue(timestampMS uint64, value []byte) []byte {
	buf := make([]byte, storedValueHeaderLen+len(value))
	binary.BigEndian.PutUint64(buf[:storedValueHeaderLen], timestampMS)
	copy(buf[storedValueHeaderLen:], value)
	return buf
}

func DecodeStoredValue(data []byte) (StoredValue, error) {
	if len(data) < storedValueHeaderLen {
		return StoredValue{}, io.ErrUnexpectedEOF
	}

	return StoredValue{
		TimestampMS: binary.BigEndian.Uint64(data[:storedValueHeaderLen]),
		Value:       cloneBytes(data[storedValueHeaderLen:]),
	}, nil
}

func EncodeWALRecord(lsn, timestampMS uint64, value []byte) []byte {
	buf := make([]byte, walRecordHeaderLen+len(value))
	binary.BigEndian.PutUint64(buf[:uint64Size], lsn)
	binary.BigEndian.PutUint64(buf[uint64Size:walRecordHeaderLen], timestampMS)
	copy(buf[walRecordHeaderLen:], value)
	return buf
}

func DecodeWALRecord(data []byte) (WALRecord, error) {
	if len(data) < walRecordHeaderLen {
		return WALRecord{}, io.ErrUnexpectedEOF
	}

	return WALRecord{
		LSN:         binary.BigEndian.Uint64(data[:uint64Size]),
		TimestampMS: binary.BigEndian.Uint64(data[uint64Size:walRecordHeaderLen]),
		Value:       cloneBytes(data[walRecordHeaderLen:]),
	}, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
