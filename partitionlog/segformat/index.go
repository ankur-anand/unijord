package segformat

import (
	"encoding/binary"
	"fmt"
)

func MarshalBlockIndex(entries []BlockIndexEntry, trailer Trailer) ([]byte, IndexPreamble, error) {
	if len(entries) == 0 {
		return nil, IndexPreamble{}, fmt.Errorf("%w: empty index", ErrBlockIndexMismatch)
	}
	if len(entries) > MaxBlockCount {
		return nil, IndexPreamble{}, fmt.Errorf("%w: entries=%d max=%d", ErrBlockIndexMismatch, len(entries), MaxBlockCount)
	}
	if err := ValidateIndex(entries, trailer); err != nil {
		return nil, IndexPreamble{}, err
	}
	out := make([]byte, IndexPreambleSize+len(entries)*BlockIndexEntrySize)
	entriesBytes := out[IndexPreambleSize:]
	for i, e := range entries {
		if err := e.Validate(); err != nil {
			return nil, IndexPreamble{}, err
		}
		writeBlockIndexEntry(entriesBytes[i*BlockIndexEntrySize:(i+1)*BlockIndexEntrySize], e)
	}
	indexHash, err := HashBytes(trailer.HashAlgo, entriesBytes)
	if err != nil {
		return nil, IndexPreamble{}, err
	}
	preamble := IndexPreamble{
		EntrySize:  BlockIndexEntrySize,
		EntryCount: uint32(len(entries)),
		IndexHash:  indexHash,
	}
	writeIndexPreamble(out[:IndexPreambleSize], preamble)
	return out, preamble, nil
}

func ParseBlockIndex(buf []byte, algo HashAlgo) (IndexPreamble, []BlockIndexEntry, error) {
	if len(buf) < IndexPreambleSize {
		return IndexPreamble{}, nil, fmt.Errorf("%w: index too small", ErrInvalidSegment)
	}
	preamble, err := ParseIndexPreamble(buf[:IndexPreambleSize])
	if err != nil {
		return IndexPreamble{}, nil, err
	}
	wantLen := IndexPreambleSize + int(preamble.EntryCount)*BlockIndexEntrySize
	if len(buf) != wantLen {
		return IndexPreamble{}, nil, fmt.Errorf("%w: index size=%d want=%d", ErrInvalidSegment, len(buf), wantLen)
	}
	entriesBytes := buf[IndexPreambleSize:]
	gotHash, err := HashBytes(algo, entriesBytes)
	if err != nil {
		return IndexPreamble{}, nil, err
	}
	if gotHash != preamble.IndexHash {
		return IndexPreamble{}, nil, fmt.Errorf("%w: index hash got=%x want=%x", ErrIntegrityMismatch, gotHash, preamble.IndexHash)
	}
	entries := make([]BlockIndexEntry, preamble.EntryCount)
	for i := range entries {
		start := i * BlockIndexEntrySize
		entries[i], err = ParseBlockIndexEntry(entriesBytes[start : start+BlockIndexEntrySize])
		if err != nil {
			return IndexPreamble{}, nil, err
		}
	}
	return preamble, entries, nil
}

func writeIndexPreamble(buf []byte, p IndexPreamble) {
	copy(buf[0:4], indexMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], IndexPreambleSize)
	binary.BigEndian.PutUint16(buf[6:8], p.Flags)
	binary.BigEndian.PutUint32(buf[8:12], p.EntrySize)
	binary.BigEndian.PutUint32(buf[12:16], p.EntryCount)
	binary.BigEndian.PutUint64(buf[16:24], p.IndexHash)
}

func writeBlockIndexEntry(buf []byte, e BlockIndexEntry) {
	binary.BigEndian.PutUint64(buf[0:8], e.BlockOffset)
	binary.BigEndian.PutUint32(buf[8:12], e.StoredSize)
	binary.BigEndian.PutUint32(buf[12:16], e.RawSize)
	binary.BigEndian.PutUint32(buf[16:20], e.RecordCount)
	binary.BigEndian.PutUint64(buf[24:32], e.BaseLSN)
	binary.BigEndian.PutUint64(buf[32:40], uint64(e.MinTimestampMS))
	binary.BigEndian.PutUint64(buf[40:48], uint64(e.MaxTimestampMS))
	binary.BigEndian.PutUint64(buf[48:56], e.BlockHash)
}
