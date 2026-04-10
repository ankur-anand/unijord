package writer

import (
	"encoding/binary"
	"io"
)

const (
	walFileExt = ".wal"
	lsnKeySize = 8
)

func encodeWALRecord(lsn uint64, value []byte) []byte {
	buf := make([]byte, lsnKeySize+len(value))
	binary.BigEndian.PutUint64(buf[:lsnKeySize], lsn)
	copy(buf[lsnKeySize:], value)
	return buf
}

func decodeWALRecord(data []byte) (Record, error) {
	if len(data) < lsnKeySize {
		return Record{}, io.ErrUnexpectedEOF
	}

	return Record{
		LSN:   binary.BigEndian.Uint64(data[:lsnKeySize]),
		Value: cloneBytes(data[lsnKeySize:]),
	}, nil
}

func encodeLSNKey(lsn uint64) []byte {
	buf := make([]byte, lsnKeySize)
	binary.BigEndian.PutUint64(buf, lsn)
	return buf
}
