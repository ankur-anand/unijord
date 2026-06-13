package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

const maxListResults = 5000

// Backend stores catalog/blob JSON objects in one Azure Blob container.
type Backend struct {
	container *container.Client
}

var _ blob.Backend = (*Backend)(nil)

func NewBackend(container *container.Client) (*Backend, error) {
	if container == nil {
		return nil, fmt.Errorf("catalog/blob/azure: nil container client")
	}
	return &Backend{container: container}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (blob.Object, error) {
	if key == "" {
		return blob.Object{}, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	out, err := b.container.NewBlobClient(key).DownloadStream(ctx, nil)
	if err != nil {
		return blob.Object{}, mapError(err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return blob.Object{}, err
	}
	return blob.Object{
		Key:       key,
		Body:      body,
		Token:     etagString(out.ETag),
		CreatedAt: timeValue(out.LastModified),
	}, nil
}

func (b *Backend) Put(ctx context.Context, key string, body []byte) (blob.Object, error) {
	if key == "" {
		return blob.Object{}, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	none := azcore.ETag("*")
	obj, err := b.upload(ctx, key, body, &none, nil)
	if err == nil {
		return obj, nil
	}
	if !isPreconditionError(err) {
		return blob.Object{}, err
	}

	current, getErr := b.Get(ctx, key)
	if getErr != nil {
		return blob.Object{}, getErr
	}
	if !bytes.Equal(current.Body, body) {
		return blob.Object{}, fmt.Errorf("%w: %s", blob.ErrImmutableConflict, key)
	}
	return current, nil
}

func (b *Backend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (blob.Object, bool, error) {
	if key == "" {
		return blob.Object{}, false, fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}

	var ifNoneMatch *azcore.ETag
	var ifMatch *azcore.ETag
	if expectedToken == "" {
		none := azcore.ETag("*")
		ifNoneMatch = &none
	} else {
		match := azcore.ETag(expectedToken)
		ifMatch = &match
	}

	obj, err := b.upload(ctx, key, body, ifNoneMatch, ifMatch)
	if err == nil {
		return obj, true, nil
	}
	if !isPreconditionError(err) {
		return blob.Object{}, false, err
	}
	current, getErr := b.Get(ctx, key)
	if errors.Is(getErr, blob.ErrObjectNotFound) {
		return blob.Object{}, false, nil
	}
	if getErr != nil {
		return blob.Object{}, false, getErr
	}
	return current, false, nil
}

func (b *Backend) List(ctx context.Context, opts blob.ListOptions) (blob.ObjectPage, error) {
	limit := opts.NormalizedLimit()
	if limit > maxListResults {
		limit = maxListResults
	}
	maxResults := int32(limit)
	pager := b.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Marker:     stringPtr(opts.Cursor),
		MaxResults: &maxResults,
		Prefix:     stringPtr(opts.Prefix),
	})
	if !pager.More() {
		return blob.ObjectPage{}, nil
	}
	page, err := pager.NextPage(ctx)
	if err != nil {
		return blob.ObjectPage{}, mapError(err)
	}

	items := page.ListBlobsFlatSegmentResponse.Segment
	objects := make([]blob.ObjectInfo, 0)
	if items != nil {
		objects = make([]blob.ObjectInfo, 0, len(items.BlobItems))
		for _, item := range items.BlobItems {
			if item == nil || item.Name == nil || *item.Name == "" {
				continue
			}
			info := blob.ObjectInfo{Key: *item.Name}
			if props := item.Properties; props != nil {
				info.Token = etagString(props.ETag)
				info.CreatedAt = timeValue(props.LastModified)
				if props.ContentLength != nil {
					if *props.ContentLength > int64(math.MaxInt) {
						return blob.ObjectPage{}, fmt.Errorf("%w: object %s size=%d exceeds int", blob.ErrCorruptCatalog, info.Key, *props.ContentLength)
					}
					if *props.ContentLength > 0 {
						info.SizeBytes = int(*props.ContentLength)
					}
				}
			}
			objects = append(objects, info)
		}
	}

	nextCursor := ""
	if page.NextMarker != nil {
		nextCursor = *page.NextMarker
	}
	return blob.ObjectPage{
		Objects:    objects,
		NextCursor: nextCursor,
		HasMore:    nextCursor != "",
	}, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", blob.ErrCorruptCatalog)
	}
	_, err := b.container.NewBlobClient(key).Delete(ctx, nil)
	if isNotFoundError(err) {
		return nil
	}
	return mapError(err)
}

func (b *Backend) upload(ctx context.Context, key string, body []byte, ifNoneMatch *azcore.ETag, ifMatch *azcore.ETag) (blob.Object, error) {
	out, err := b.container.NewBlockBlobClient(key).Upload(ctx, readSeekCloser{Reader: bytes.NewReader(body)}, &blockblob.UploadOptions{
		HTTPHeaders: &azblobblob.HTTPHeaders{
			BlobContentType: stringPtr(blob.ObjectContentType),
		},
		AccessConditions: &azblobblob.AccessConditions{
			ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{
				IfNoneMatch: ifNoneMatch,
				IfMatch:     ifMatch,
			},
		},
	})
	if err != nil {
		return blob.Object{}, mapError(err)
	}
	return blob.Object{
		Key:       key,
		Body:      bytes.Clone(body),
		Token:     etagString(out.ETag),
		CreatedAt: timeValue(out.LastModified),
	}, nil
}

type readSeekCloser struct {
	*bytes.Reader
}

var _ io.ReadSeekCloser = readSeekCloser{}

func (r readSeekCloser) Close() error {
	return nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundError(err) {
		return fmt.Errorf("%w: %w", blob.ErrObjectNotFound, err)
	}
	if isPreconditionAPIError(err) {
		return fmt.Errorf("%w: %w", blob.ErrImmutableConflict, err)
	}
	return err
}

func isPreconditionError(err error) bool {
	return errors.Is(err, blob.ErrImmutableConflict) || isPreconditionAPIError(err)
}

func isPreconditionAPIError(err error) bool {
	var responseErr *azcore.ResponseError
	if !errors.As(err, &responseErr) {
		return false
	}
	switch responseErr.StatusCode {
	case 409, 412:
		return true
	default:
		return false
	}
}

func isNotFoundError(err error) bool {
	var responseErr *azcore.ResponseError
	if !errors.As(err, &responseErr) {
		return false
	}
	return responseErr.StatusCode == 404
}

func etagString(etag *azcore.ETag) string {
	if etag == nil {
		return ""
	}
	return string(*etag)
}

func timeValue(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
