package segblock

import (
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/klauspost/compress/zstd"
)

var (
	zstdEncoderPool = sync.Pool{New: func() any {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithZeroFrames(true),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			panic(fmt.Errorf("segblock: zstd encoder init: %w", err))
		}
		return enc
	}}

	zstdDecoderPool = sync.Pool{New: func() any {
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(segformat.MaxRawBlockSize*2)),
		)
		if err != nil {
			panic(fmt.Errorf("segblock: zstd decoder init: %w", err))
		}
		return dec
	}}
)

func getZstdEncoder() *zstd.Encoder {
	return zstdEncoderPool.Get().(*zstd.Encoder)
}

func putZstdEncoder(enc *zstd.Encoder) {
	zstdEncoderPool.Put(enc)
}

func getZstdDecoder() *zstd.Decoder {
	return zstdDecoderPool.Get().(*zstd.Decoder)
}

func putZstdDecoder(dec *zstd.Decoder) {
	zstdDecoderPool.Put(dec)
}
