package rangeread

import (
	"fmt"
	"io"
	"math"
)

type Bounds struct {
	Offset int64
	Count  int64
	End    uint64
}

func Validate(off, n uint64) (Bounds, error) {
	if n == 0 {
		return Bounds{Offset: int64(off)}, nil
	}
	if off > math.MaxInt64 || n >= math.MaxInt64 || off > uint64(math.MaxInt64)-(n-1) {
		return Bounds{}, fmt.Errorf("blob/source: range overflows int64 offset=%d length=%d", off, n)
	}
	return Bounds{
		Offset: int64(off),
		Count:  int64(n),
		End:    off + n - 1,
	}, nil
}

// ReadExact bounds memory even when a provider ignores the requested range.
func ReadExact(body io.Reader, n uint64) ([]byte, error) {
	if n >= math.MaxInt64 {
		return nil, fmt.Errorf("blob/source: range length overflows int64: %d", n)
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(n)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != n {
		return nil, fmt.Errorf("blob/source: range length=%d got=%d", n, len(data))
	}
	return data, nil
}
