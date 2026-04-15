package writer

import (
	"encoding/binary"

	"github.com/ankur-anand/unijord/internal/recordbin"
)

const (
	walFileExt = ".wal"
	lsnKeySize = 8
)

func encodeWALRecord(lsn uint64, timestampMS uint64, value []byte) []byte {
	return recordbin.EncodeWALRecord(lsn, timestampMS, value)
}

func decodeWALRecord(data []byte) (Record, error) {
	record, err := recordbin.DecodeWALRecord(data)
	if err != nil {
		return Record{}, err
	}

	return Record{
		LSN:         record.LSN,
		TimestampMS: record.TimestampMS,
		Value:       cloneBytes(record.Value),
	}, nil
}

func encodeLSNKey(lsn uint64) []byte {
	buf := make([]byte, lsnKeySize)
	binary.BigEndian.PutUint64(buf, lsn)
	return buf
}
