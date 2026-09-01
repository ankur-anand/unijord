package ujpk

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdEncoders = sync.Pool{New: func() any {
		encoder, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithZeroFrames(true),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			panic(fmt.Errorf("ujpk: initialize zstd encoder: %w", err))
		}
		return encoder
	}}
	zstdDecoders = sync.Pool{New: func() any {
		decoder, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(MaxRawPageBytes*2),
		)
		if err != nil {
			panic(fmt.Errorf("ujpk: initialize zstd decoder: %w", err))
		}
		return decoder
	}}
)

func encodePage(codec Codec, raw []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return append([]byte(nil), raw...), nil
	case CodecZstd:
		encoder := zstdEncoders.Get().(*zstd.Encoder)
		defer zstdEncoders.Put(encoder)
		return encoder.EncodeAll(raw, make([]byte, 0, len(raw)/2)), nil
	default:
		return nil, fmt.Errorf("%w: codec=%d", ErrUnsupported, codec)
	}
}

func decodePage(codec Codec, stored []byte, rawSize uint32) ([]byte, error) {
	switch codec {
	case CodecNone:
		if len(stored) != int(rawSize) {
			return nil, fmt.Errorf("%w: codec=none stored=%d raw=%d", ErrInvalidPack, len(stored), rawSize)
		}
		return stored, nil
	case CodecZstd:
		decoder := zstdDecoders.Get().(*zstd.Decoder)
		defer zstdDecoders.Put(decoder)
		raw, err := decoder.DecodeAll(stored, make([]byte, 0, rawSize))
		if err != nil {
			return nil, fmt.Errorf("%w: decompress page: %v", ErrInvalidPack, err)
		}
		if len(raw) != int(rawSize) {
			return nil, fmt.Errorf("%w: decompressed=%d raw=%d", ErrInvalidPack, len(raw), rawSize)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: codec=%d", ErrUnsupported, codec)
	}
}
