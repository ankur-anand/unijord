package ujpk

import (
	"context"
	"fmt"
	"math"
)

type bytesSource struct {
	buf []byte
}

func (s bytesSource) Size(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(len(s.buf)), nil
}

func (s bytesSource) ReadRange(ctx context.Context, offset, length uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset > uint64(len(s.buf)) || length > uint64(len(s.buf))-offset {
		return nil, fmt.Errorf("%w: range offset=%d length=%d size=%d", ErrInvalidPack, offset, length, len(s.buf))
	}
	return s.buf[offset : offset+length], nil
}

func readExactRange(ctx context.Context, source RangeSource, offset, length uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if length > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: range length=%d", ErrInvalidPack, length)
	}
	buf, err := source.ReadRange(ctx, offset, length)
	if err != nil {
		return nil, fmt.Errorf("ujpk: read range offset=%d length=%d: %w", offset, length, err)
	}
	if uint64(len(buf)) != length {
		return nil, fmt.Errorf("%w: short range offset=%d length=%d got=%d", ErrInvalidPack, offset, length, len(buf))
	}
	return buf, nil
}
