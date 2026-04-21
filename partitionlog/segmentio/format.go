package segmentio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Segment file layout, version 1. All integers are big-endian.
//
//	+------------------------------+
//	| Header (64 bytes)            |
//	+------------------------------+
//	| Block 0 stored bytes         |
//	+------------------------------+
//	| Block 1 stored bytes         |
//	+------------------------------+
//	| ...                          |
//	+------------------------------+
//	| Block index (N * 48 bytes)   |
//	+------------------------------+
//	| Footer (32 bytes)            |
//	+------------------------------+
//
// Header (64 bytes):
//
//	0  .. 3   magic              "PLSG"
//	4  .. 5   version            uint16
//	6  .. 7   header_len         uint16 (= 64)
//	8  .. 11  flags              uint32 (= 0 in v1)
//	12 .. 15  partition          uint32
//	16 .. 17  codec              uint16
//	18 .. 19  reserved0          zero
//	20 .. 27  base_lsn           uint64
//	28 .. 35  last_lsn           uint64
//	36 .. 43  min_timestamp_ms   int64
//	44 .. 51  max_timestamp_ms   int64
//	52 .. 55  record_count       uint32
//	56 .. 59  block_count        uint32
//	60 .. 63  reserved1          zero
//
// Raw block payload before compression:
//
//	repeated record:
//	  0  .. 7   timestamp_ms     int64
//	  8  .. 11  value_len        uint32
//	  12 .. ..  value            bytes
//
// LSN is not stored per record inside the block. The reader reconstructs it as
// block.base_lsn + record_index.
//
// Block index entry (48 bytes):
//
//	0  .. 7   block_offset       uint64
//	8  .. 11  stored_size        uint32
//	12 .. 15  raw_size           uint32
//	16 .. 23  base_lsn           uint64
//	24 .. 27  record_count       uint32
//	28 .. 31  block_crc32c       uint32
//	32 .. 39  min_timestamp_ms   int64
//	40 .. 47  max_timestamp_ms   int64
//
// Footer (32 bytes):
//
//	0  .. 7   block_index_offset uint64
//	8  .. 11  block_index_length uint32
//	12 .. 15  block_index_crc32c uint32
//	16 .. 19  magic              "PLFT"
//	20 .. 21  footer_len         uint16 (= 32)
//	22 .. 23  version            uint16 (= 1)
//	24 .. 31  reserved           zero
//
// Validity rules:
//
//   - block payload starts immediately after the 64-byte header
//   - blocks are stored contiguously; block_offset + stored_size of block i
//     must equal block_offset of block i+1
//   - block index starts immediately after the last stored block
//   - a segment is finalized only when the full footer is present and the
//     block index and block payload CRC32C checks succeed
const (
	// Version is the current on-disk segment format version.
	Version uint16 = 1

	HeaderSize          = 64
	FooterSize          = 32
	BlockIndexEntrySize = 48
	maxUint32Value      = uint64(^uint32(0))

	DefaultTargetBlockSize = 1 << 20

	// Hard safety limits for version 1. Readers and writers must reject data
	// above these bounds to avoid unbounded allocation from corrupt metadata.
	MaxBlockSize         = 16 << 20
	MaxRecordValueLen    = 4 << 20
	DefaultMaxRecordSize = MaxRecordValueLen + 12
)

var (
	segmentMagic = [4]byte{'P', 'L', 'S', 'G'}
	footerMagic  = [4]byte{'P', 'L', 'F', 'T'}
	crc32cTable  = crc32.MakeTable(crc32.Castagnoli)

	ErrEmptySegment       = errors.New("segmentio: empty segment")
	ErrInvalidSegment     = errors.New("segmentio: invalid segment")
	ErrUnsupportedCodec   = errors.New("segmentio: unsupported codec")
	ErrMixedPartition     = errors.New("segmentio: records must belong to one partition")
	ErrNonContiguousLSN   = errors.New("segmentio: records must have contiguous lsns")
	ErrTimestampOrder     = errors.New("segmentio: records must have non-decreasing timestamps")
	ErrRecordTooLarge     = errors.New("segmentio: record too large")
	ErrBlockIndexMismatch = errors.New("segmentio: block index does not match header")
)

// Codec is the segment-wide block compression codec.
type Codec uint16

const (
	CodecNone Codec = 0
	CodecZstd Codec = 1
)

func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecZstd:
		return "zstd"
	default:
		return fmt.Sprintf("unknown(%d)", uint16(c))
	}
}

func (c Codec) validate() error {
	switch c {
	case CodecNone, CodecZstd:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedCodec, uint16(c))
	}
}

// Record is the logical event shape encoded inside one segment.
type Record struct {
	Partition   uint32
	LSN         uint64
	TimestampMS int64
	Value       []byte
}

// Header is the fixed-width segment header.
type Header struct {
	Flags          uint32
	Partition      uint32
	Codec          Codec
	BaseLSN        uint64
	LastLSN        uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	RecordCount    uint32
	BlockCount     uint32
}

func (h Header) validate() error {
	if err := h.Codec.validate(); err != nil {
		return err
	}
	if h.Flags != 0 {
		return fmt.Errorf("%w: header flags must be zero", ErrInvalidSegment)
	}
	if h.RecordCount == 0 {
		return fmt.Errorf("%w: record_count must be positive", ErrInvalidSegment)
	}
	if h.BlockCount == 0 {
		return fmt.Errorf("%w: block_count must be positive", ErrInvalidSegment)
	}
	if h.LastLSN < h.BaseLSN {
		return fmt.Errorf("%w: last_lsn < base_lsn", ErrInvalidSegment)
	}
	if h.MaxTimestampMS < h.MinTimestampMS {
		return fmt.Errorf("%w: max_timestamp < min_timestamp", ErrInvalidSegment)
	}

	expectedRecordCount := h.LastLSN - h.BaseLSN + 1
	if expectedRecordCount != uint64(h.RecordCount) {
		return fmt.Errorf(
			"%w: record_count=%d does not match lsn span=%d",
			ErrInvalidSegment,
			h.RecordCount,
			expectedRecordCount,
		)
	}

	return nil
}

func (h Header) marshalBinary() []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[:4], segmentMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], Version)
	binary.BigEndian.PutUint16(buf[6:8], HeaderSize)
	binary.BigEndian.PutUint32(buf[8:12], h.Flags)
	binary.BigEndian.PutUint32(buf[12:16], h.Partition)
	binary.BigEndian.PutUint16(buf[16:18], uint16(h.Codec))
	binary.BigEndian.PutUint64(buf[20:28], h.BaseLSN)
	binary.BigEndian.PutUint64(buf[28:36], h.LastLSN)
	binary.BigEndian.PutUint64(buf[36:44], uint64(h.MinTimestampMS))
	binary.BigEndian.PutUint64(buf[44:52], uint64(h.MaxTimestampMS))
	binary.BigEndian.PutUint32(buf[52:56], h.RecordCount)
	binary.BigEndian.PutUint32(buf[56:60], h.BlockCount)
	return buf
}

func parseHeader(buf []byte) (Header, error) {
	var h Header
	if len(buf) != HeaderSize {
		return h, fmt.Errorf("%w: header size=%d want=%d", ErrInvalidSegment, len(buf), HeaderSize)
	}
	if !bytes.Equal(buf[:4], segmentMagic[:]) {
		return h, fmt.Errorf("%w: bad header magic=%q", ErrInvalidSegment, buf[:4])
	}
	if binary.BigEndian.Uint16(buf[4:6]) != Version {
		return h, fmt.Errorf("%w: unsupported header version=%d", ErrInvalidSegment, binary.BigEndian.Uint16(buf[4:6]))
	}
	if binary.BigEndian.Uint16(buf[6:8]) != HeaderSize {
		return h, fmt.Errorf("%w: unsupported header length=%d", ErrInvalidSegment, binary.BigEndian.Uint16(buf[6:8]))
	}
	if !bytes.Equal(buf[18:20], []byte{0, 0}) {
		return h, fmt.Errorf("%w: reserved0 must be zero", ErrInvalidSegment)
	}
	if !bytes.Equal(buf[60:64], []byte{0, 0, 0, 0}) {
		return h, fmt.Errorf("%w: reserved1 must be zero", ErrInvalidSegment)
	}

	h.Flags = binary.BigEndian.Uint32(buf[8:12])
	h.Partition = binary.BigEndian.Uint32(buf[12:16])
	h.Codec = Codec(binary.BigEndian.Uint16(buf[16:18]))
	h.BaseLSN = binary.BigEndian.Uint64(buf[20:28])
	h.LastLSN = binary.BigEndian.Uint64(buf[28:36])
	h.MinTimestampMS = int64(binary.BigEndian.Uint64(buf[36:44]))
	h.MaxTimestampMS = int64(binary.BigEndian.Uint64(buf[44:52]))
	h.RecordCount = binary.BigEndian.Uint32(buf[52:56])
	h.BlockCount = binary.BigEndian.Uint32(buf[56:60])

	if err := h.validate(); err != nil {
		return Header{}, err
	}

	return h, nil
}

// Footer is the fixed-width footer that points to the block index.
type Footer struct {
	BlockIndexOffset uint64
	BlockIndexLength uint32
	BlockIndexCRC    uint32
}

func (f Footer) validate(fileSize int64, blockCount uint32) error {
	if fileSize < HeaderSize+FooterSize {
		return fmt.Errorf("%w: file too small", ErrInvalidSegment)
	}

	expectedIndexLength := uint64(blockCount) * BlockIndexEntrySize
	if uint64(f.BlockIndexLength) != expectedIndexLength {
		return fmt.Errorf(
			"%w: block_index_length=%d want=%d",
			ErrInvalidSegment,
			f.BlockIndexLength,
			expectedIndexLength,
		)
	}

	if f.BlockIndexOffset < HeaderSize {
		return fmt.Errorf("%w: block_index_offset before header", ErrInvalidSegment)
	}

	expectedFileSize := f.BlockIndexOffset + uint64(f.BlockIndexLength) + FooterSize
	if expectedFileSize != uint64(fileSize) {
		return fmt.Errorf(
			"%w: file size=%d want=%d from footer",
			ErrInvalidSegment,
			fileSize,
			expectedFileSize,
		)
	}

	return nil
}

func (f Footer) marshalBinary() []byte {
	buf := make([]byte, FooterSize)
	binary.BigEndian.PutUint64(buf[0:8], f.BlockIndexOffset)
	binary.BigEndian.PutUint32(buf[8:12], f.BlockIndexLength)
	binary.BigEndian.PutUint32(buf[12:16], f.BlockIndexCRC)
	copy(buf[16:20], footerMagic[:])
	binary.BigEndian.PutUint16(buf[20:22], FooterSize)
	binary.BigEndian.PutUint16(buf[22:24], Version)
	return buf
}

func parseFooter(buf []byte) (Footer, error) {
	var f Footer
	if len(buf) != FooterSize {
		return f, fmt.Errorf("%w: footer size=%d want=%d", ErrInvalidSegment, len(buf), FooterSize)
	}
	if !bytes.Equal(buf[16:20], footerMagic[:]) {
		return f, fmt.Errorf("%w: bad footer magic=%q", ErrInvalidSegment, buf[16:20])
	}
	if binary.BigEndian.Uint16(buf[20:22]) != FooterSize {
		return f, fmt.Errorf("%w: unsupported footer length=%d", ErrInvalidSegment, binary.BigEndian.Uint16(buf[20:22]))
	}
	if binary.BigEndian.Uint16(buf[22:24]) != Version {
		return f, fmt.Errorf("%w: unsupported footer version=%d", ErrInvalidSegment, binary.BigEndian.Uint16(buf[22:24]))
	}
	if !bytes.Equal(buf[24:32], make([]byte, 8)) {
		return f, fmt.Errorf("%w: footer reserved bytes must be zero", ErrInvalidSegment)
	}

	f.BlockIndexOffset = binary.BigEndian.Uint64(buf[0:8])
	f.BlockIndexLength = binary.BigEndian.Uint32(buf[8:12])
	f.BlockIndexCRC = binary.BigEndian.Uint32(buf[12:16])
	return f, nil
}

// BlockIndexEntry describes one stored block.
type BlockIndexEntry struct {
	BlockOffset    uint64
	StoredSize     uint32
	RawSize        uint32
	BaseLSN        uint64
	RecordCount    uint32
	CRC32C         uint32
	MinTimestampMS int64
	MaxTimestampMS int64
}

func (e BlockIndexEntry) validate() error {
	if e.StoredSize == 0 {
		return fmt.Errorf("%w: stored_size must be positive", ErrInvalidSegment)
	}
	if e.StoredSize > MaxBlockSize {
		return fmt.Errorf("%w: stored_size=%d exceeds max=%d", ErrInvalidSegment, e.StoredSize, MaxBlockSize)
	}
	if e.RawSize == 0 {
		return fmt.Errorf("%w: raw_size must be positive", ErrInvalidSegment)
	}
	if e.RawSize > MaxBlockSize {
		return fmt.Errorf("%w: raw_size=%d exceeds max=%d", ErrInvalidSegment, e.RawSize, MaxBlockSize)
	}
	if e.RecordCount == 0 {
		return fmt.Errorf("%w: record_count must be positive", ErrInvalidSegment)
	}
	if e.MaxTimestampMS < e.MinTimestampMS {
		return fmt.Errorf("%w: max_timestamp < min_timestamp", ErrInvalidSegment)
	}
	return nil
}

func (e BlockIndexEntry) marshalTo(dst []byte) {
	binary.BigEndian.PutUint64(dst[0:8], e.BlockOffset)
	binary.BigEndian.PutUint32(dst[8:12], e.StoredSize)
	binary.BigEndian.PutUint32(dst[12:16], e.RawSize)
	binary.BigEndian.PutUint64(dst[16:24], e.BaseLSN)
	binary.BigEndian.PutUint32(dst[24:28], e.RecordCount)
	binary.BigEndian.PutUint32(dst[28:32], e.CRC32C)
	binary.BigEndian.PutUint64(dst[32:40], uint64(e.MinTimestampMS))
	binary.BigEndian.PutUint64(dst[40:48], uint64(e.MaxTimestampMS))
}

func parseBlockIndexEntry(src []byte) (BlockIndexEntry, error) {
	var e BlockIndexEntry
	if len(src) != BlockIndexEntrySize {
		return e, fmt.Errorf("%w: block index entry size=%d want=%d", ErrInvalidSegment, len(src), BlockIndexEntrySize)
	}

	e.BlockOffset = binary.BigEndian.Uint64(src[0:8])
	e.StoredSize = binary.BigEndian.Uint32(src[8:12])
	e.RawSize = binary.BigEndian.Uint32(src[12:16])
	e.BaseLSN = binary.BigEndian.Uint64(src[16:24])
	e.RecordCount = binary.BigEndian.Uint32(src[24:28])
	e.CRC32C = binary.BigEndian.Uint32(src[28:32])
	e.MinTimestampMS = int64(binary.BigEndian.Uint64(src[32:40]))
	e.MaxTimestampMS = int64(binary.BigEndian.Uint64(src[40:48]))

	if err := e.validate(); err != nil {
		return BlockIndexEntry{}, err
	}

	return e, nil
}

func (e BlockIndexEntry) lastLSN() uint64 {
	return e.BaseLSN + uint64(e.RecordCount) - 1
}

// Metadata is the validated segment summary exposed by the codec package.
type Metadata struct {
	Partition      uint32
	Codec          Codec
	BaseLSN        uint64
	LastLSN        uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	RecordCount    uint32
	BlockCount     uint32
	SizeBytes      int64
}

func metadataFromHeader(h Header, sizeBytes int64) Metadata {
	return Metadata{
		Partition:      h.Partition,
		Codec:          h.Codec,
		BaseLSN:        h.BaseLSN,
		LastLSN:        h.LastLSN,
		MinTimestampMS: h.MinTimestampMS,
		MaxTimestampMS: h.MaxTimestampMS,
		RecordCount:    h.RecordCount,
		BlockCount:     h.BlockCount,
		SizeBytes:      sizeBytes,
	}
}

func validateBlockIndex(index []BlockIndexEntry, header Header, blockIndexOffset uint64) error {
	if len(index) != int(header.BlockCount) {
		return fmt.Errorf("%w: block_count=%d index_entries=%d", ErrBlockIndexMismatch, header.BlockCount, len(index))
	}
	if len(index) == 0 {
		return fmt.Errorf("%w: empty block index", ErrBlockIndexMismatch)
	}

	expectedBlockOffset := uint64(HeaderSize)
	expectedBaseLSN := header.BaseLSN
	totalRecords := uint64(0)
	prevMaxTimestamp := index[0].MinTimestampMS

	for i, entry := range index {
		if err := entry.validate(); err != nil {
			return err
		}
		if entry.BlockOffset != expectedBlockOffset {
			return fmt.Errorf(
				"%w: block %d offset=%d want=%d",
				ErrBlockIndexMismatch,
				i,
				entry.BlockOffset,
				expectedBlockOffset,
			)
		}
		if entry.BaseLSN != expectedBaseLSN {
			return fmt.Errorf(
				"%w: block %d base_lsn=%d want=%d",
				ErrBlockIndexMismatch,
				i,
				entry.BaseLSN,
				expectedBaseLSN,
			)
		}
		if entry.BlockOffset+uint64(entry.StoredSize) > blockIndexOffset {
			return fmt.Errorf("%w: block %d overlaps block index", ErrBlockIndexMismatch, i)
		}
		if i > 0 && entry.MinTimestampMS < prevMaxTimestamp {
			return fmt.Errorf("%w: block %d timestamp range regresses", ErrBlockIndexMismatch, i)
		}

		expectedBlockOffset += uint64(entry.StoredSize)
		expectedBaseLSN += uint64(entry.RecordCount)
		totalRecords += uint64(entry.RecordCount)
		prevMaxTimestamp = entry.MaxTimestampMS
	}

	if expectedBlockOffset != blockIndexOffset {
		return fmt.Errorf("%w: block payload region does not end at block index", ErrBlockIndexMismatch)
	}
	if totalRecords != uint64(header.RecordCount) {
		return fmt.Errorf(
			"%w: summed record_count=%d want=%d",
			ErrBlockIndexMismatch,
			totalRecords,
			header.RecordCount,
		)
	}
	if index[0].BaseLSN != header.BaseLSN {
		return fmt.Errorf("%w: first block base_lsn does not match header", ErrBlockIndexMismatch)
	}
	if index[len(index)-1].lastLSN() != header.LastLSN {
		return fmt.Errorf("%w: last block last_lsn does not match header", ErrBlockIndexMismatch)
	}
	if index[0].MinTimestampMS != header.MinTimestampMS {
		return fmt.Errorf("%w: first block min_timestamp does not match header", ErrBlockIndexMismatch)
	}
	if index[len(index)-1].MaxTimestampMS != header.MaxTimestampMS {
		return fmt.Errorf("%w: last block max_timestamp does not match header", ErrBlockIndexMismatch)
	}

	return nil
}

func encodeRecordSize(valueLen int) int {
	return 12 + valueLen
}
