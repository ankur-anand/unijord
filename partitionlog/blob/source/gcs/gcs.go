package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blob/source/internal/rangeread"
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("blob/source/gcs: empty key")
	}
	if n == 0 {
		return []byte{}, nil
	}
	bounds, err := rangeread.Validate(off, n)
	if err != nil {
		return nil, err
	}
	r, err := s.client.Bucket(s.bucket).Object(key).NewRangeReader(ctx, bounds.Offset, bounds.Count)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return rangeread.ReadExact(r, n)
}
