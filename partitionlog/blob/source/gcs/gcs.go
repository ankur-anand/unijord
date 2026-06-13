package gcs

import (
	"context"
	"fmt"
	"io"
	"math"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

type Store struct {
	client *storage.Client
	bucket string
}

var _ segreader.SegmentStore = (*Store)(nil)

func NewStore(client *storage.Client, bucket string) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("blob/source/gcs: nil client")
	}
	if bucket == "" {
		return nil, fmt.Errorf("blob/source/gcs: empty bucket")
	}
	return &Store{client: client, bucket: bucket}, nil
}

func (s *Store) ReadAt(ctx context.Context, key string, off uint64, n uint64) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("blob/source/gcs: empty key")
	}
	if n == 0 {
		return []byte{}, nil
	}
	if off > math.MaxInt64 || n > math.MaxInt64 {
		return nil, fmt.Errorf("blob/source/gcs: range overflows int64 offset=%d length=%d", off, n)
	}
	r, err := s.client.Bucket(s.bucket).Object(key).NewRangeReader(ctx, int64(off), int64(n))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
