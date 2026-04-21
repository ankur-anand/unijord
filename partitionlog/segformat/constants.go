package segformat

import "fmt"

const (
	Version uint16 = 2

	FilePreambleSize     = 64
	BlockPreambleSize    = 64
	IndexPreambleSize    = 64
	BlockIndexEntrySize  = 64
	TrailerSize          = 192
	RecordHeaderSize     = 12
	MaxRawBlockSize      = 16 << 20
	MaxStoredBlockSize   = 20 << 20
	MaxRecordValueLen    = 4 << 20
	MaxBlockCount        = (1<<32 - 1 - IndexPreambleSize) / BlockIndexEntrySize
	MaxRecordCount       = 1<<32 - 1
	DefaultRecordFormat  = RecordFormatV1
	DefaultHashAlgorithm = HashXXH64
)

var (
	fileMagic    = [4]byte{'P', 'L', 'S', 'G'}
	blockMagic   = [4]byte{'P', 'L', 'B', 'K'}
	indexMagic   = [4]byte{'P', 'L', 'I', 'X'}
	trailerMagic = [4]byte{'P', 'L', 'F', 'T'}
)

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

func (c Codec) Validate() error {
	switch c {
	case CodecNone, CodecZstd:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedCodec, uint16(c))
	}
}

type HashAlgo uint16

const (
	HashCRC32C HashAlgo = 0
	HashXXH64  HashAlgo = 1
)

func (h HashAlgo) String() string {
	switch h {
	case HashCRC32C:
		return "crc32c"
	case HashXXH64:
		return "xxh64"
	default:
		return fmt.Sprintf("unknown(%d)", uint16(h))
	}
}

func (h HashAlgo) Validate() error {
	switch h {
	case HashCRC32C, HashXXH64:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedHashAlgo, uint16(h))
	}
}

type RecordFormat uint16

const (
	RecordFormatV1 RecordFormat = 1
)

func (f RecordFormat) Validate() error {
	switch f {
	case RecordFormatV1:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedRecord, uint16(f))
	}
}
