package segformat

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func (p FilePreamble) MarshalBinary() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, FilePreambleSize)
	copy(buf[0:4], fileMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], Version)
	binary.BigEndian.PutUint16(buf[6:8], FilePreambleSize)
	binary.BigEndian.PutUint32(buf[8:12], p.Flags)
	binary.BigEndian.PutUint32(buf[12:16], p.Partition)
	binary.BigEndian.PutUint16(buf[16:18], uint16(p.Codec))
	binary.BigEndian.PutUint16(buf[18:20], uint16(p.HashAlgo))
	binary.BigEndian.PutUint16(buf[20:22], uint16(p.RecordFormat))
	binary.BigEndian.PutUint64(buf[24:32], p.BaseLSN)
	copy(buf[32:48], p.SegmentUUID[:])
	copy(buf[48:64], p.WriterTag[:])
	return buf, nil
}

func ParseFilePreamble(buf []byte) (FilePreamble, error) {
	var p FilePreamble
	if len(buf) != FilePreambleSize {
		return p, fmt.Errorf("%w: file preamble size=%d want=%d", ErrInvalidSegment, len(buf), FilePreambleSize)
	}
	if !bytes.Equal(buf[0:4], fileMagic[:]) {
		return p, fmt.Errorf("%w: bad file magic=%q", ErrInvalidSegment, buf[0:4])
	}
	if v := binary.BigEndian.Uint16(buf[4:6]); v != Version {
		return p, fmt.Errorf("%w: version=%d", ErrUnsupportedVersion, v)
	}
	if n := binary.BigEndian.Uint16(buf[6:8]); n != FilePreambleSize {
		return p, fmt.Errorf("%w: file preamble length=%d want=%d", ErrInvalidSegment, n, FilePreambleSize)
	}
	if !zero(buf[22:24]) {
		return p, fmt.Errorf("%w: file preamble reserved0 must be zero", ErrInvalidSegment)
	}

	p.Flags = binary.BigEndian.Uint32(buf[8:12])
	p.Partition = binary.BigEndian.Uint32(buf[12:16])
	p.Codec = Codec(binary.BigEndian.Uint16(buf[16:18]))
	p.HashAlgo = HashAlgo(binary.BigEndian.Uint16(buf[18:20]))
	p.RecordFormat = RecordFormat(binary.BigEndian.Uint16(buf[20:22]))
	p.BaseLSN = binary.BigEndian.Uint64(buf[24:32])
	copy(p.SegmentUUID[:], buf[32:48])
	copy(p.WriterTag[:], buf[48:64])
	if err := p.Validate(); err != nil {
		return FilePreamble{}, err
	}
	return p, nil
}

func (p BlockPreamble) MarshalBinary() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, BlockPreambleSize)
	copy(buf[0:4], blockMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], BlockPreambleSize)
	binary.BigEndian.PutUint16(buf[6:8], p.Flags)
	binary.BigEndian.PutUint32(buf[8:12], p.StoredSize)
	binary.BigEndian.PutUint32(buf[12:16], p.RawSize)
	binary.BigEndian.PutUint32(buf[16:20], p.RecordCount)
	binary.BigEndian.PutUint64(buf[24:32], p.BaseLSN)
	binary.BigEndian.PutUint64(buf[32:40], uint64(p.MinTimestampMS))
	binary.BigEndian.PutUint64(buf[40:48], uint64(p.MaxTimestampMS))
	binary.BigEndian.PutUint64(buf[48:56], p.BlockHash)
	return buf, nil
}

func ParseBlockPreamble(buf []byte) (BlockPreamble, error) {
	var p BlockPreamble
	if len(buf) != BlockPreambleSize {
		return p, fmt.Errorf("%w: block preamble size=%d want=%d", ErrInvalidSegment, len(buf), BlockPreambleSize)
	}
	if !bytes.Equal(buf[0:4], blockMagic[:]) {
		return p, fmt.Errorf("%w: bad block magic=%q", ErrInvalidSegment, buf[0:4])
	}
	if n := binary.BigEndian.Uint16(buf[4:6]); n != BlockPreambleSize {
		return p, fmt.Errorf("%w: block preamble length=%d want=%d", ErrInvalidSegment, n, BlockPreambleSize)
	}
	if !zero(buf[20:24]) || !zero(buf[56:64]) {
		return p, fmt.Errorf("%w: block preamble reserved fields must be zero", ErrInvalidSegment)
	}

	p.Flags = binary.BigEndian.Uint16(buf[6:8])
	p.StoredSize = binary.BigEndian.Uint32(buf[8:12])
	p.RawSize = binary.BigEndian.Uint32(buf[12:16])
	p.RecordCount = binary.BigEndian.Uint32(buf[16:20])
	p.BaseLSN = binary.BigEndian.Uint64(buf[24:32])
	p.MinTimestampMS = int64(binary.BigEndian.Uint64(buf[32:40]))
	p.MaxTimestampMS = int64(binary.BigEndian.Uint64(buf[40:48]))
	p.BlockHash = binary.BigEndian.Uint64(buf[48:56])
	if err := p.Validate(); err != nil {
		return BlockPreamble{}, err
	}
	return p, nil
}

func (p IndexPreamble) MarshalBinary() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, IndexPreambleSize)
	copy(buf[0:4], indexMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], IndexPreambleSize)
	binary.BigEndian.PutUint16(buf[6:8], p.Flags)
	binary.BigEndian.PutUint32(buf[8:12], p.EntrySize)
	binary.BigEndian.PutUint32(buf[12:16], p.EntryCount)
	binary.BigEndian.PutUint64(buf[16:24], p.IndexHash)
	return buf, nil
}

func ParseIndexPreamble(buf []byte) (IndexPreamble, error) {
	var p IndexPreamble
	if len(buf) != IndexPreambleSize {
		return p, fmt.Errorf("%w: index preamble size=%d want=%d", ErrInvalidSegment, len(buf), IndexPreambleSize)
	}
	if !bytes.Equal(buf[0:4], indexMagic[:]) {
		return p, fmt.Errorf("%w: bad index magic=%q", ErrInvalidSegment, buf[0:4])
	}
	if n := binary.BigEndian.Uint16(buf[4:6]); n != IndexPreambleSize {
		return p, fmt.Errorf("%w: index preamble length=%d want=%d", ErrInvalidSegment, n, IndexPreambleSize)
	}
	if !zero(buf[24:64]) {
		return p, fmt.Errorf("%w: index preamble reserved0 must be zero", ErrInvalidSegment)
	}

	p.Flags = binary.BigEndian.Uint16(buf[6:8])
	p.EntrySize = binary.BigEndian.Uint32(buf[8:12])
	p.EntryCount = binary.BigEndian.Uint32(buf[12:16])
	p.IndexHash = binary.BigEndian.Uint64(buf[16:24])
	if err := p.Validate(); err != nil {
		return IndexPreamble{}, err
	}
	return p, nil
}

func (e BlockIndexEntry) MarshalBinary() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, BlockIndexEntrySize)
	binary.BigEndian.PutUint64(buf[0:8], e.BlockOffset)
	binary.BigEndian.PutUint32(buf[8:12], e.StoredSize)
	binary.BigEndian.PutUint32(buf[12:16], e.RawSize)
	binary.BigEndian.PutUint32(buf[16:20], e.RecordCount)
	binary.BigEndian.PutUint64(buf[24:32], e.BaseLSN)
	binary.BigEndian.PutUint64(buf[32:40], uint64(e.MinTimestampMS))
	binary.BigEndian.PutUint64(buf[40:48], uint64(e.MaxTimestampMS))
	binary.BigEndian.PutUint64(buf[48:56], e.BlockHash)
	return buf, nil
}

func ParseBlockIndexEntry(buf []byte) (BlockIndexEntry, error) {
	var e BlockIndexEntry
	if len(buf) != BlockIndexEntrySize {
		return e, fmt.Errorf("%w: block index entry size=%d want=%d", ErrInvalidSegment, len(buf), BlockIndexEntrySize)
	}
	if !zero(buf[20:24]) || !zero(buf[56:64]) {
		return e, fmt.Errorf("%w: block index reserved fields must be zero", ErrInvalidSegment)
	}
	e.BlockOffset = binary.BigEndian.Uint64(buf[0:8])
	e.StoredSize = binary.BigEndian.Uint32(buf[8:12])
	e.RawSize = binary.BigEndian.Uint32(buf[12:16])
	e.RecordCount = binary.BigEndian.Uint32(buf[16:20])
	e.BaseLSN = binary.BigEndian.Uint64(buf[24:32])
	e.MinTimestampMS = int64(binary.BigEndian.Uint64(buf[32:40]))
	e.MaxTimestampMS = int64(binary.BigEndian.Uint64(buf[40:48]))
	e.BlockHash = binary.BigEndian.Uint64(buf[48:56])
	if err := e.Validate(); err != nil {
		return BlockIndexEntry{}, err
	}
	return e, nil
}

func (t Trailer) MarshalBinary() ([]byte, error) {
	if err := t.Validate(t.TotalSize); err != nil {
		return nil, err
	}
	buf := make([]byte, TrailerSize)
	copy(buf[0:4], trailerMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], TrailerSize)
	binary.BigEndian.PutUint16(buf[6:8], Version)
	binary.BigEndian.PutUint32(buf[8:12], t.Flags)
	binary.BigEndian.PutUint32(buf[12:16], t.Partition)
	binary.BigEndian.PutUint16(buf[16:18], uint16(t.Codec))
	binary.BigEndian.PutUint16(buf[18:20], uint16(t.HashAlgo))
	binary.BigEndian.PutUint16(buf[20:22], uint16(t.RecordFormat))
	binary.BigEndian.PutUint64(buf[24:32], t.BaseLSN)
	binary.BigEndian.PutUint64(buf[32:40], t.LastLSN)
	binary.BigEndian.PutUint64(buf[40:48], uint64(t.MinTimestampMS))
	binary.BigEndian.PutUint64(buf[48:56], uint64(t.MaxTimestampMS))
	binary.BigEndian.PutUint32(buf[56:60], t.RecordCount)
	binary.BigEndian.PutUint32(buf[60:64], t.BlockCount)
	binary.BigEndian.PutUint64(buf[64:72], t.BlockIndexOffset)
	binary.BigEndian.PutUint32(buf[72:76], t.BlockIndexLength)
	binary.BigEndian.PutUint64(buf[80:88], t.TotalSize)
	binary.BigEndian.PutUint64(buf[88:96], uint64(t.CreatedUnixMS))
	copy(buf[96:112], t.SegmentUUID[:])
	copy(buf[112:128], t.WriterTag[:])
	binary.BigEndian.PutUint64(buf[128:136], t.SegmentHash)

	trailerHash, err := HashBytes(t.HashAlgo, buf)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint64(buf[136:144], trailerHash)
	return buf, nil
}

// MarshalTrailer serializes t and returns the parsed trailer with the derived
// TrailerHash populated.
func MarshalTrailer(t Trailer) ([]byte, Trailer, error) {
	buf, err := t.MarshalBinary()
	if err != nil {
		return nil, Trailer{}, err
	}
	sealed, err := ParseTrailer(buf, t.TotalSize)
	if err != nil {
		return nil, Trailer{}, err
	}
	return buf, sealed, nil
}

func ParseTrailer(buf []byte, objectSize uint64) (Trailer, error) {
	var t Trailer
	if len(buf) != TrailerSize {
		return t, fmt.Errorf("%w: trailer size=%d want=%d", ErrInvalidSegment, len(buf), TrailerSize)
	}
	if !bytes.Equal(buf[0:4], trailerMagic[:]) {
		return t, fmt.Errorf("%w: bad trailer magic=%q", ErrInvalidSegment, buf[0:4])
	}
	if n := binary.BigEndian.Uint16(buf[4:6]); n != TrailerSize {
		return t, fmt.Errorf("%w: trailer length=%d want=%d", ErrInvalidSegment, n, TrailerSize)
	}
	if v := binary.BigEndian.Uint16(buf[6:8]); v != Version {
		return t, fmt.Errorf("%w: version=%d", ErrUnsupportedVersion, v)
	}
	if !zero(buf[22:24]) || !zero(buf[76:80]) || !zero(buf[144:192]) {
		return t, fmt.Errorf("%w: trailer reserved fields must be zero", ErrInvalidSegment)
	}

	t.Flags = binary.BigEndian.Uint32(buf[8:12])
	t.Partition = binary.BigEndian.Uint32(buf[12:16])
	t.Codec = Codec(binary.BigEndian.Uint16(buf[16:18]))
	t.HashAlgo = HashAlgo(binary.BigEndian.Uint16(buf[18:20]))
	t.RecordFormat = RecordFormat(binary.BigEndian.Uint16(buf[20:22]))
	t.BaseLSN = binary.BigEndian.Uint64(buf[24:32])
	t.LastLSN = binary.BigEndian.Uint64(buf[32:40])
	t.MinTimestampMS = int64(binary.BigEndian.Uint64(buf[40:48]))
	t.MaxTimestampMS = int64(binary.BigEndian.Uint64(buf[48:56]))
	t.RecordCount = binary.BigEndian.Uint32(buf[56:60])
	t.BlockCount = binary.BigEndian.Uint32(buf[60:64])
	t.BlockIndexOffset = binary.BigEndian.Uint64(buf[64:72])
	t.BlockIndexLength = binary.BigEndian.Uint32(buf[72:76])
	t.TotalSize = binary.BigEndian.Uint64(buf[80:88])
	t.CreatedUnixMS = int64(binary.BigEndian.Uint64(buf[88:96]))
	copy(t.SegmentUUID[:], buf[96:112])
	copy(t.WriterTag[:], buf[112:128])
	t.SegmentHash = binary.BigEndian.Uint64(buf[128:136])
	t.TrailerHash = binary.BigEndian.Uint64(buf[136:144])

	if err := t.Validate(objectSize); err != nil {
		return Trailer{}, err
	}
	var check [TrailerSize]byte
	copy(check[:], buf)
	for i := 136; i < 144; i++ {
		check[i] = 0
	}
	got, err := HashBytes(t.HashAlgo, check[:])
	if err != nil {
		return Trailer{}, err
	}
	if got != t.TrailerHash {
		return Trailer{}, fmt.Errorf("%w: trailer hash got=%x want=%x", ErrIntegrityMismatch, got, t.TrailerHash)
	}
	return t, nil
}

func zero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
