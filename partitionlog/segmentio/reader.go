package segmentio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// Reader opens and validates one immutable segment.
type Reader struct {
	r        io.ReaderAt
	header   Header
	index    []BlockIndexEntry
	metadata Metadata
}

// Open validates a segment from a random-access byte source.
func Open(r io.ReaderAt, size int64) (*Reader, error) {
	if size < HeaderSize+FooterSize+BlockIndexEntrySize {
		return nil, fmt.Errorf("%w: file too small=%d", ErrInvalidSegment, size)
	}

	headerBuf := make([]byte, HeaderSize)
	if err := readAtFull(r, headerBuf, 0); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	header, err := parseHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	footerBuf := make([]byte, FooterSize)
	if err := readAtFull(r, footerBuf, size-FooterSize); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	footer, err := parseFooter(footerBuf)
	if err != nil {
		return nil, err
	}
	if err := footer.validate(size, header.BlockCount); err != nil {
		return nil, err
	}

	blockIndexBytes := make([]byte, footer.BlockIndexLength)
	if err := readAtFull(r, blockIndexBytes, int64(footer.BlockIndexOffset)); err != nil {
		return nil, fmt.Errorf("read block index: %w", err)
	}
	if crc32Checksum(blockIndexBytes) != footer.BlockIndexCRC {
		return nil, fmt.Errorf("%w: block index crc mismatch", ErrInvalidSegment)
	}

	index := make([]BlockIndexEntry, int(header.BlockCount))
	for i := range index {
		entry, err := parseBlockIndexEntry(blockIndexBytes[i*BlockIndexEntrySize : (i+1)*BlockIndexEntrySize])
		if err != nil {
			return nil, fmt.Errorf("parse block index entry %d: %w", i, err)
		}
		index[i] = entry
	}
	if err := validateBlockIndex(index, header, footer.BlockIndexOffset); err != nil {
		return nil, err
	}

	return &Reader{
		r:        r,
		header:   header,
		index:    index,
		metadata: metadataFromHeader(header, size),
	}, nil
}

// OpenBytes is a convenience wrapper for in-memory segment bytes.
func OpenBytes(data []byte) (*Reader, error) {
	return Open(bytesReaderAt(data), int64(len(data)))
}

// Metadata returns the validated segment summary.
func (r *Reader) Metadata() Metadata {
	return r.metadata
}

// BlockIndex returns a copy of the validated block index.
func (r *Reader) BlockIndex() []BlockIndexEntry {
	return append([]BlockIndexEntry(nil), r.index...)
}

// ReadAll returns every record in the segment.
func (r *Reader) ReadAll() ([]Record, error) {
	return r.ReadFromLSN(r.metadata.BaseLSN)
}

// ReadFromLSN returns records starting from fromLSN, inclusive.
func (r *Reader) ReadFromLSN(fromLSN uint64) ([]Record, error) {
	if fromLSN > r.metadata.LastLSN {
		return nil, nil
	}
	if fromLSN < r.metadata.BaseLSN {
		fromLSN = r.metadata.BaseLSN
	}

	startBlock := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].lastLSN() >= fromLSN
	})
	if startBlock == len(r.index) {
		return nil, nil
	}

	out := make([]Record, 0, r.metadata.RecordCount)
	for i := startBlock; i < len(r.index); i++ {
		entry := r.index[i]
		raw, err := r.readRawBlock(entry)
		if err != nil {
			return nil, err
		}
		records, err := decodeRawBlock(raw, entry, r.header.Partition)
		if err != nil {
			return nil, err
		}

		if i == startBlock && fromLSN > entry.BaseLSN {
			skip := int(fromLSN - entry.BaseLSN)
			records = records[skip:]
		}
		out = append(out, records...)
	}

	return out, nil
}

func (r *Reader) readRawBlock(entry BlockIndexEntry) ([]byte, error) {
	stored := make([]byte, entry.StoredSize)
	if err := readAtFull(r.r, stored, int64(entry.BlockOffset)); err != nil {
		return nil, fmt.Errorf("read block at offset %d: %w", entry.BlockOffset, err)
	}

	raw, err := decodeBlock(stored, entry.RawSize, r.header.Codec)
	if err != nil {
		return nil, err
	}
	if len(raw) != int(entry.RawSize) {
		return nil, fmt.Errorf("%w: raw_size=%d got=%d", ErrInvalidSegment, entry.RawSize, len(raw))
	}
	if crc32Checksum(raw) != entry.CRC32C {
		return nil, fmt.Errorf("%w: block crc mismatch at base_lsn=%d", ErrInvalidSegment, entry.BaseLSN)
	}

	return raw, nil
}

func decodeRawBlock(raw []byte, entry BlockIndexEntry, partition uint32) ([]Record, error) {
	out := make([]Record, 0, entry.RecordCount)
	offset := 0
	prevTS := entry.MinTimestampMS

	for i := uint32(0); i < entry.RecordCount; i++ {
		if len(raw)-offset < 12 {
			return nil, fmt.Errorf("%w: truncated record header", ErrInvalidSegment)
		}

		ts := int64(binary.BigEndian.Uint64(raw[offset : offset+8]))
		valueLen := int(binary.BigEndian.Uint32(raw[offset+8 : offset+12]))
		offset += 12

		if valueLen > MaxRecordValueLen {
			return nil, fmt.Errorf("%w: record value length=%d exceeds max=%d", ErrInvalidSegment, valueLen, MaxRecordValueLen)
		}
		if len(raw)-offset < valueLen {
			return nil, fmt.Errorf("%w: truncated record value", ErrInvalidSegment)
		}
		if i > 0 && ts < prevTS {
			return nil, fmt.Errorf("%w: block timestamp regression", ErrInvalidSegment)
		}

		value := append([]byte(nil), raw[offset:offset+valueLen]...)
		offset += valueLen

		out = append(out, Record{
			Partition:   partition,
			LSN:         entry.BaseLSN + uint64(i),
			TimestampMS: ts,
			Value:       value,
		})
		prevTS = ts
	}

	if offset != len(raw) {
		return nil, fmt.Errorf("%w: trailing bytes in raw block", ErrInvalidSegment)
	}
	if len(out) > 0 {
		if out[0].TimestampMS != entry.MinTimestampMS {
			return nil, fmt.Errorf("%w: block min_timestamp mismatch", ErrInvalidSegment)
		}
		if out[len(out)-1].TimestampMS != entry.MaxTimestampMS {
			return nil, fmt.Errorf("%w: block max_timestamp mismatch", ErrInvalidSegment)
		}
	}

	return out, nil
}

func decodeBlock(stored []byte, rawSize uint32, codec Codec) ([]byte, error) {
	switch codec {
	case CodecNone:
		return append([]byte(nil), stored...), nil
	case CodecZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", err)
		}
		defer dec.Close()

		raw, err := dec.DecodeAll(stored, make([]byte, 0, rawSize))
		if err != nil {
			return nil, fmt.Errorf("decode zstd block: %w", err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCodec, uint16(codec))
	}
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func readAtFull(r io.ReaderAt, dst []byte, off int64) error {
	read := 0
	for read < len(dst) {
		n, err := r.ReadAt(dst[read:], off+int64(read))
		read += n
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) && read == len(dst) {
			return nil
		}
		return err
	}
	return nil
}
