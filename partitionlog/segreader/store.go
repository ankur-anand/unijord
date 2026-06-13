package segreader

import (
	"context"
	"fmt"
)

// SegmentStore reads byte ranges from immutable segment objects.
type SegmentStore interface {
	ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error)
}

type SegmentStoreFunc func(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error)

func (f SegmentStoreFunc) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	return f(ctx, uri, off, n)
}

func readAtExact(ctx context.Context, store SegmentStore, uri string, off uint64, n uint64) ([]byte, error) {
	body, err := store.ReadAt(ctx, uri, off, n)
	if err != nil {
		return nil, fmt.Errorf("%w: uri=%s offset=%d length=%d: %w", ErrStoreRead, uri, off, n, err)
	}
	if uint64(len(body)) != n {
		return nil, fmt.Errorf("%w: uri=%s offset=%d length=%d got=%d", ErrStoreRead, uri, off, n, len(body))
	}
	return body, nil
}
