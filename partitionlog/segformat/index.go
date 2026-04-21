package segformat

import "fmt"

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
	entriesBytes := make([]byte, 0, len(entries)*BlockIndexEntrySize)
	for _, e := range entries {
		buf, err := e.MarshalBinary()
		if err != nil {
			return nil, IndexPreamble{}, err
		}
		entriesBytes = append(entriesBytes, buf...)
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
	preambleBytes, err := preamble.MarshalBinary()
	if err != nil {
		return nil, IndexPreamble{}, err
	}
	out := make([]byte, 0, IndexPreambleSize+len(entriesBytes))
	out = append(out, preambleBytes...)
	out = append(out, entriesBytes...)
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
		start := IndexPreambleSize + i*BlockIndexEntrySize
		entries[i], err = ParseBlockIndexEntry(buf[start : start+BlockIndexEntrySize])
		if err != nil {
			return IndexPreamble{}, nil, err
		}
	}
	return preamble, entries, nil
}
