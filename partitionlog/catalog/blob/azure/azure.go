package azure

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobstoreazure "github.com/ankur-anand/unijord/internal/blobstore/azure"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

type Backend = blobstoreazure.Backend
type Options = blob.Options

func New(container *container.Client, opts Options) (*blob.Catalog, error) {
	backend, err := blobstoreazure.New(container)
	if err != nil {
		return nil, err
	}
	return blob.New(backend, opts)
}

func NewBackend(container *container.Client) (*Backend, error) {
	return blobstoreazure.New(container)
}
