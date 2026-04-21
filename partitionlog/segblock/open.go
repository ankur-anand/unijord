package segblock

import (
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func Open(codec segformat.Codec, hashAlgo segformat.HashAlgo, preamble segformat.BlockPreamble, stored []byte) ([]byte, error) {
	if err := codec.Validate(); err != nil {
		return nil, err
	}
	if err := hashAlgo.Validate(); err != nil {
		return nil, err
	}
	if err := preamble.Validate(); err != nil {
		return nil, err
	}
	if len(stored) != int(preamble.StoredSize) {
		return nil, fmt.Errorf("%w: stored_size=%d want=%d", segformat.ErrInvalidSegment, len(stored), preamble.StoredSize)
	}
	gotHash, err := segformat.HashBytes(hashAlgo, stored)
	if err != nil {
		return nil, err
	}
	if gotHash != preamble.BlockHash {
		return nil, fmt.Errorf("%w: block hash got=%x want=%x", segformat.ErrIntegrityMismatch, gotHash, preamble.BlockHash)
	}
	raw, err := decodeStored(codec, stored, preamble.RawSize)
	if err != nil {
		return nil, err
	}
	if len(raw) != int(preamble.RawSize) {
		return nil, fmt.Errorf("%w: raw_size=%d want=%d", segformat.ErrInvalidSegment, len(raw), preamble.RawSize)
	}
	return raw, nil
}

func decodeStored(codec segformat.Codec, stored []byte, rawSize uint32) ([]byte, error) {
	switch codec {
	case segformat.CodecNone:
		return append([]byte(nil), stored...), nil
	case segformat.CodecZstd:
		dec := getZstdDecoder()
		defer putZstdDecoder(dec)
		raw, err := dec.DecodeAll(stored, make([]byte, 0, rawSize))
		if err != nil {
			return nil, fmt.Errorf("decode zstd block: %w", err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: %d", segformat.ErrUnsupportedCodec, uint16(codec))
	}
}
