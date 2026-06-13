package azure

import (
	"context"
	"fmt"
	"io"
	"math"

	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

type Store struct {
	container *container.Client
}

var _ segreader.SegmentStore = (*Store)(nil)

func NewStore(container *container.Client) (*Store, error) {
	if container == nil {
		return nil, fmt.Errorf("blob/source/azure: nil container client")
	}
	return &Store{container: container}, nil
}

func (s *Store) ReadAt(ctx context.Context, key string, off uint64, n uint64) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("blob/source/azure: empty key")
	}
	if n == 0 {
		return []byte{}, nil
	}
	if off > math.MaxInt64 || n > math.MaxInt64 {
		return nil, fmt.Errorf("blob/source/azure: range overflows int64 offset=%d length=%d", off, n)
	}
	resp, err := s.container.NewBlobClient(key).DownloadStream(ctx, &azblobblob.DownloadStreamOptions{
		Range: azblobblob.HTTPRange{
			Offset: int64(off),
			Count:  int64(n),
		},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
