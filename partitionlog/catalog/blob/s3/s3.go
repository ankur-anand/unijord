package s3

import (
	"github.com/ankur-anand/unijord/internal/blobstore/s3"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Backend = s3.Backend
type Options = blob.Options

func New(client *awss3.Client, bucket string, opts Options) (*blob.Catalog, error) {
	backend, err := s3.New(client, bucket)
	if err != nil {
		return nil, err
	}
	return blob.New(backend, opts)
}

func NewBackend(client *awss3.Client, bucket string) (*Backend, error) {
	return s3.New(client, bucket)
}
