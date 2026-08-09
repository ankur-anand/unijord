package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/internal/blobstore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type Backend struct {
	client *storage.Client
	bucket string
}

var _ blobstore.Store = (*Backend)(nil)

func New(client *storage.Client, bucket string) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("internal/blobstore/gcs: nil client")
	}
	if bucket == "" {
		return nil, fmt.Errorf("internal/blobstore/gcs: empty bucket")
	}
	return &Backend{client: client, bucket: bucket}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (blobstore.Object, error) {
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	r, err := b.client.Bucket(b.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return blobstore.Object{}, mapError(err)
	}
	defer r.Close()

	body, err := io.ReadAll(r)
	if err != nil {
		return blobstore.Object{}, err
	}
	attrs := r.Attrs
	return blobstore.Object{
		Key:       key,
		Body:      body,
		Token:     generationToken(attrs.Generation),
		CreatedAt: attrs.LastModified,
	}, nil
}

func (b *Backend) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if key == "" {
		return blobstore.Object{}, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	obj, err := b.upload(ctx, key, body, storage.Conditions{DoesNotExist: true})
	if err == nil {
		return obj, nil
	}
	if !isPreconditionError(err) {
		return blobstore.Object{}, err
	}

	current, getErr := b.Get(ctx, key)
	if getErr != nil {
		return blobstore.Object{}, getErr
	}
	if !bytes.Equal(current.Body, body) {
		return blobstore.Object{}, fmt.Errorf("%w: %s", blobstore.ErrImmutableConflict, key)
	}
	return current, nil
}

func (b *Backend) CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (blobstore.Object, bool, error) {
	if key == "" {
		return blobstore.Object{}, false, fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}

	conds := storage.Conditions{DoesNotExist: true}
	if expectedToken != "" {
		generation, err := parseGeneration(expectedToken)
		if err != nil {
			return blobstore.Object{}, false, err
		}
		conds = storage.Conditions{GenerationMatch: generation}
	}

	obj, err := b.upload(ctx, key, body, conds)
	if err == nil {
		return obj, true, nil
	}
	if !isPreconditionError(err) {
		return blobstore.Object{}, false, err
	}
	current, getErr := b.Get(ctx, key)
	if errors.Is(getErr, blobstore.ErrObjectNotFound) {
		return blobstore.Object{}, false, nil
	}
	if getErr != nil {
		return blobstore.Object{}, false, getErr
	}
	return current, false, nil
}

func (b *Backend) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	limit := opts.NormalizedLimit()
	it := b.client.Bucket(b.bucket).Objects(ctx, &storage.Query{
		Prefix:     opts.Prefix,
		Projection: storage.ProjectionNoACL,
	})
	pager := iterator.NewPager(it, limit, opts.Cursor)

	var attrs []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&attrs)
	if err != nil {
		return blobstore.ObjectPage{}, mapError(err)
	}

	objects := make([]blobstore.ObjectInfo, 0, len(attrs))
	for _, attr := range attrs {
		if attr == nil || attr.Name == "" {
			continue
		}
		if attr.Size > int64(math.MaxInt) {
			return blobstore.ObjectPage{}, fmt.Errorf("%w: object %s size=%d exceeds int", blobstore.ErrInvalidRequest, attr.Name, attr.Size)
		}
		size := 0
		if attr.Size > 0 {
			size = int(attr.Size)
		}
		objects = append(objects, blobstore.ObjectInfo{
			Key:       attr.Name,
			Token:     generationToken(attr.Generation),
			SizeBytes: size,
			CreatedAt: attr.Created,
		})
	}
	return blobstore.ObjectPage{
		Objects:    objects,
		NextCursor: nextToken,
		HasMore:    nextToken != "",
	}, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", blobstore.ErrInvalidRequest)
	}
	err := b.client.Bucket(b.bucket).Object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return mapError(err)
}

func (b *Backend) upload(ctx context.Context, key string, body []byte, conds storage.Conditions) (blobstore.Object, error) {
	w := b.client.Bucket(b.bucket).Object(key).If(conds).NewWriter(ctx)
	w.ContentType = blobstore.JSONContentType
	if _, err := bytes.NewReader(body).WriteTo(w); err != nil {
		_ = w.Close()
		return blobstore.Object{}, mapError(err)
	}
	if err := w.Close(); err != nil {
		return blobstore.Object{}, mapError(err)
	}
	attrs := w.Attrs()
	if attrs == nil {
		return blobstore.Object{
			Key:  key,
			Body: bytes.Clone(body),
		}, nil
	}
	return blobstore.Object{
		Key:       key,
		Body:      bytes.Clone(body),
		Token:     generationToken(attrs.Generation),
		CreatedAt: attrs.Created,
	}, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("%w: %w", blobstore.ErrObjectNotFound, err)
	}
	if isPreconditionAPIError(err) {
		return fmt.Errorf("%w: %w", blobstore.ErrImmutableConflict, err)
	}
	return err
}

func isPreconditionError(err error) bool {
	return errors.Is(err, blobstore.ErrImmutableConflict) || isPreconditionAPIError(err)
}

func isPreconditionAPIError(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 412
}

func parseGeneration(token string) (int64, error) {
	generation, err := strconv.ParseInt(token, 10, 64)
	if err != nil || generation <= 0 {
		return 0, fmt.Errorf("%w: invalid gcs generation token %q", blobstore.ErrInvalidRequest, token)
	}
	return generation, nil
}

func generationToken(generation int64) string {
	if generation <= 0 {
		return ""
	}
	return strconv.FormatInt(generation, 10)
}
