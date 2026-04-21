package segblock

import (
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func Seal(codec segformat.Codec, hashAlgo segformat.HashAlgo, raw []byte, meta Meta) (Sealed, error) {
	return seal(codec, hashAlgo, raw, meta, false)
}

// SealOwned is equivalent to Seal, except the caller transfers ownership of
// raw to segblock. For CodecNone this lets the sealed block reuse raw as the
// stored payload instead of copying it.
func SealOwned(codec segformat.Codec, hashAlgo segformat.HashAlgo, raw []byte, meta Meta) (Sealed, error) {
	return seal(codec, hashAlgo, raw, meta, true)
}

func seal(codec segformat.Codec, hashAlgo segformat.HashAlgo, raw []byte, meta Meta, rawOwned bool) (Sealed, error) {
	if err := codec.Validate(); err != nil {
		return Sealed{}, err
	}
	if err := hashAlgo.Validate(); err != nil {
		return Sealed{}, err
	}
	if err := validateMeta(meta, len(raw)); err != nil {
		return Sealed{}, err
	}

	stored, err := encodeStored(codec, raw, rawOwned)
	if err != nil {
		return Sealed{}, err
	}
	if len(stored) > segformat.MaxStoredBlockSize {
		return Sealed{}, fmt.Errorf("%w: stored_size=%d max=%d", segformat.ErrBlockTooLarge, len(stored), segformat.MaxStoredBlockSize)
	}

	blockHash, err := segformat.HashBytes(hashAlgo, stored)
	if err != nil {
		return Sealed{}, err
	}
	preamble := segformat.BlockPreamble{
		StoredSize:     uint32(len(stored)),
		RawSize:        uint32(len(raw)),
		RecordCount:    meta.RecordCount,
		BaseLSN:        meta.BaseLSN,
		MinTimestampMS: meta.MinTimestampMS,
		MaxTimestampMS: meta.MaxTimestampMS,
		BlockHash:      blockHash,
	}
	if err := preamble.Validate(); err != nil {
		return Sealed{}, err
	}
	return Sealed{Preamble: preamble, Stored: stored}, nil
}

func encodeStored(codec segformat.Codec, raw []byte, rawOwned bool) ([]byte, error) {
	switch codec {
	case segformat.CodecNone:
		if rawOwned {
			return raw, nil
		}
		return append([]byte(nil), raw...), nil
	case segformat.CodecZstd:
		enc := getZstdEncoder()
		defer putZstdEncoder(enc)
		return enc.EncodeAll(raw, nil), nil
	default:
		return nil, fmt.Errorf("%w: %d", segformat.ErrUnsupportedCodec, uint16(codec))
	}
}

func validateMeta(meta Meta, rawSize int) error {
	if rawSize <= 0 {
		return fmt.Errorf("%w: raw block must be non-empty", segformat.ErrInvalidSegment)
	}
	if rawSize > segformat.MaxRawBlockSize {
		return fmt.Errorf("%w: raw_size=%d max=%d", segformat.ErrBlockTooLarge, rawSize, segformat.MaxRawBlockSize)
	}
	if meta.RecordCount == 0 {
		return fmt.Errorf("%w: record_count must be positive", segformat.ErrInvalidSegment)
	}
	if int64(meta.RecordCount) > int64(rawSize/segformat.RecordHeaderSize) {
		return fmt.Errorf("%w: record_count=%d cannot fit in raw_size=%d", segformat.ErrInvalidSegment, meta.RecordCount, rawSize)
	}
	if meta.MaxTimestampMS < meta.MinTimestampMS {
		return fmt.Errorf("%w: max timestamp < min timestamp", segformat.ErrInvalidSegment)
	}
	return nil
}
