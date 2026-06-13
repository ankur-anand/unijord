package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/ankur-anand/unijord/partitionlog/segreader"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client *awss3.Client
	bucket string
}

var _ segreader.SegmentStore = (*Store)(nil)

func NewStore(client *awss3.Client, bucket string) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("blob/source/s3: nil client")
	}
	if bucket == "" {
		return nil, fmt.Errorf("blob/source/s3: empty bucket")
	}
	return &Store{client: client, bucket: bucket}, nil
}

func (s *Store) ReadAt(ctx context.Context, key string, off uint64, n uint64) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("blob/source/s3: empty key")
	}
	if n == 0 {
		return []byte{}, nil
	}
	end, err := rangeEnd(off, n)
	if err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func rangeEnd(off uint64, n uint64) (uint64, error) {
	if off > ^uint64(0)-n+1 {
		return 0, fmt.Errorf("blob/source/s3: range overflow offset=%d length=%d", off, n)
	}
	return off + n - 1, nil
}
