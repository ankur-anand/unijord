package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blobsink/multipart"
	"google.golang.org/api/googleapi"
)

const composeSourceLimit = 32

type Store struct {
	client *storage.Client
	bucket string
}

var _ multipart.Store = (*Store)(nil)

func NewStore(client *storage.Client, bucket string) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil gcs client", multipart.ErrInvalidStore)
	}
	if bucket == "" {
		return nil, fmt.Errorf("%w: empty gcs bucket", multipart.ErrInvalidStore)
	}
	return &Store{client: client, bucket: bucket}, nil
}

func (s *Store) BeginMultipart(ctx context.Context, key string, opts multipart.Options) (multipart.Upload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := multipart.NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	return &upload{
		bucket: s.client.Bucket(s.bucket),
		key:    key,
		opts:   opts,
		parts:  make(map[int]gcsObject),
	}, nil
}

type upload struct {
	mu        sync.Mutex
	bucket    *storage.BucketHandle
	key       string
	opts      multipart.Options
	parts     map[int]gcsObject
	temps     []gcsObject
	aborted   bool
	completed bool
}

type gcsObject struct {
	key        string
	generation int64
	size       uint64
}

func (u *upload) UploadPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	if err := multipart.ValidatePart(part); err != nil {
		return multipart.Receipt{}, err
	}

	partKey := multipart.StagingPartKey(u.opts.StagingPrefix, part.Number)
	if err := u.reservePart(part.Number, partKey); err != nil {
		return multipart.Receipt{}, err
	}

	obj := u.bucket.Object(partKey).If(storage.Conditions{DoesNotExist: true})
	w := obj.NewWriter(ctx)
	w.ContentType = u.opts.ContentType
	if _, err := bytes.NewReader(part.Bytes).WriteTo(w); err != nil {
		u.dropPart(part.Number)
		_ = w.Close()
		return multipart.Receipt{}, mapError(err)
	}
	if err := w.Close(); err != nil {
		u.dropPart(part.Number)
		return multipart.Receipt{}, mapError(err)
	}
	attrs := w.Attrs()
	staged := gcsObject{
		key:        partKey,
		generation: attrs.Generation,
		size:       uint64(len(part.Bytes)),
	}

	u.mu.Lock()
	if u.aborted {
		delete(u.parts, part.Number)
		u.mu.Unlock()
		_ = deleteGCSObject(ctx, u.bucket, staged)
		return multipart.Receipt{}, multipart.ErrAborted
	}
	u.parts[part.Number] = staged
	u.mu.Unlock()

	return multipart.Receipt{
		Number:    part.Number,
		Token:     strconv.FormatInt(staged.generation, 10),
		SizeBytes: staged.size,
	}, nil
}

func (u *upload) Complete(ctx context.Context, receipts []multipart.Receipt) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateReceipts(receipts); err != nil {
		return multipart.ObjectAttrs{}, err
	}

	sources, size, err := u.sourcesFor(receipts)
	if err != nil {
		return multipart.ObjectAttrs{}, err
	}
	final, err := u.compose(ctx, sources)
	if err != nil {
		return multipart.ObjectAttrs{}, mapError(err)
	}

	u.mu.Lock()
	u.completed = true
	cleanup := append([]gcsObject(nil), u.temps...)
	cleanup = append(cleanup, sources...)
	u.mu.Unlock()
	_ = deleteGCSObjects(context.Background(), u.bucket, cleanup)

	return multipart.ObjectAttrs{
		Key:       final.key,
		SizeBytes: maxSize(final.size, size),
		Token:     strconv.FormatInt(final.generation, 10),
	}, nil
}

func (u *upload) Abort(ctx context.Context) error {
	u.mu.Lock()
	if u.completed || u.aborted {
		u.mu.Unlock()
		return nil
	}
	u.aborted = true
	cleanup := make([]gcsObject, 0, len(u.parts)+len(u.temps))
	for _, part := range u.parts {
		cleanup = append(cleanup, part)
	}
	cleanup = append(cleanup, u.temps...)
	u.parts = nil
	u.temps = nil
	u.mu.Unlock()

	return deleteGCSObjects(ctx, u.bucket, cleanup)
}

func (u *upload) reservePart(number int, key string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		return multipart.ErrAborted
	}
	if u.completed {
		return multipart.ErrCompleted
	}
	if _, exists := u.parts[number]; exists {
		return fmt.Errorf("%w: duplicate part %d", multipart.ErrInvalidStore, number)
	}
	u.parts[number] = gcsObject{key: key}
	return nil
}

func (u *upload) dropPart(number int) {
	u.mu.Lock()
	delete(u.parts, number)
	u.mu.Unlock()
}

func (u *upload) sourcesFor(receipts []multipart.Receipt) ([]gcsObject, uint64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		return nil, 0, multipart.ErrAborted
	}
	if u.completed {
		return nil, 0, multipart.ErrCompleted
	}

	sources := make([]gcsObject, 0, len(receipts))
	var size uint64
	for _, receipt := range receipts {
		part, ok := u.parts[receipt.Number]
		if !ok || part.generation == 0 {
			return nil, 0, fmt.Errorf("%w: missing gcs part %d", multipart.ErrInvalidStore, receipt.Number)
		}
		if receipt.Token != "" && receipt.Token != strconv.FormatInt(part.generation, 10) {
			return nil, 0, fmt.Errorf("%w: token mismatch for gcs part %d", multipart.ErrInvalidStore, receipt.Number)
		}
		sources = append(sources, part)
		size += part.size
	}
	return sources, size, nil
}

func (u *upload) compose(ctx context.Context, sources []gcsObject) (gcsObject, error) {
	level := 0
	for len(sources) > composeSourceLimit {
		next := make([]gcsObject, 0, (len(sources)+composeSourceLimit-1)/composeSourceLimit)
		for group := 0; len(sources) > 0; group++ {
			n := min(len(sources), composeSourceLimit)
			dst := path.Join(u.opts.StagingPrefix, fmt.Sprintf("compose-%02d-%06d", level, group))
			obj, err := u.composeTo(ctx, dst, sources[:n], false)
			if err != nil {
				return gcsObject{}, err
			}
			u.mu.Lock()
			u.temps = append(u.temps, obj)
			u.mu.Unlock()
			next = append(next, obj)
			sources = sources[n:]
		}
		sources = next
		level++
	}
	return u.composeTo(ctx, u.key, sources, true)
}

func (u *upload) composeTo(ctx context.Context, dstKey string, sources []gcsObject, final bool) (gcsObject, error) {
	handles := make([]*storage.ObjectHandle, 0, len(sources))
	var size uint64
	for _, source := range sources {
		handles = append(handles, u.bucket.Object(source.key).Generation(source.generation))
		size += source.size
	}
	dst := u.bucket.Object(dstKey).If(storage.Conditions{DoesNotExist: true})
	composer := dst.ComposerFrom(handles...)
	composer.ContentType = u.opts.ContentType
	attrs, err := composer.Run(ctx)
	if err != nil {
		return gcsObject{}, err
	}
	obj := gcsObject{key: attrs.Name, generation: attrs.Generation, size: size}
	if attrs.Size >= 0 {
		obj.size = uint64(attrs.Size)
	}
	if final && obj.key == "" {
		obj.key = u.key
	}
	return obj, nil
}

func deleteGCSObjects(ctx context.Context, bucket *storage.BucketHandle, objects []gcsObject) error {
	var firstErr error
	for _, obj := range objects {
		if err := deleteGCSObject(ctx, bucket, obj); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func deleteGCSObject(ctx context.Context, bucket *storage.BucketHandle, obj gcsObject) error {
	if obj.key == "" {
		return nil
	}
	handle := bucket.Object(obj.key)
	if obj.generation != 0 {
		handle = handle.Generation(obj.generation)
	}
	err := handle.Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return mapError(err)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 404:
			return fmt.Errorf("%w: %w", multipart.ErrAborted, err)
		case 412:
			return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
		}
	}
	return err
}

func maxSize(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
