package gcs

import (
	"cloud.google.com/go/storage"
	blobstoregcs "github.com/ankur-anand/unijord/internal/blobstore/gcs"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

type Backend = blobstoregcs.Backend
type Options = blob.Options

func New(client *storage.Client, bucket string, opts Options) (*blob.Catalog, error) {
	backend, err := blobstoregcs.New(client, bucket)
	if err != nil {
		return nil, err
	}
	return blob.New(backend, opts)
}

func NewBackend(client *storage.Client, bucket string) (*Backend, error) {
	return blobstoregcs.New(client, bucket)
}
