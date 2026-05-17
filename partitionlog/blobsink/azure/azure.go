package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blobsink/multipart"
)

const maxBlockNumber = 50_000

type Store struct {
	container *container.Client
}

var _ multipart.Store = (*Store)(nil)

func NewStore(container *container.Client) (*Store, error) {
	if container == nil {
		return nil, fmt.Errorf("%w: nil azure container client", multipart.ErrInvalidStore)
	}
	return &Store{container: container}, nil
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
		blob:  s.container.NewBlockBlobClient(key),
		key:   key,
		opts:  opts,
		parts: make(map[int]azurePart),
	}, nil
}

type upload struct {
	mu        sync.Mutex
	blob      *blockblob.Client
	key       string
	opts      multipart.Options
	parts     map[int]azurePart
	aborted   bool
	completed bool
}

type azurePart struct {
	blockID string
	size    uint64
	done    bool
}

func (u *upload) UploadPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	if err := multipart.ValidatePart(part); err != nil {
		return multipart.Receipt{}, err
	}
	if part.Number > maxBlockNumber {
		return multipart.Receipt{}, fmt.Errorf("%w: azure block number=%d max=%d", multipart.ErrInvalidStore, part.Number, maxBlockNumber)
	}
	blockID := blockID(part.Number)
	if err := u.reservePart(part.Number, blockID); err != nil {
		return multipart.Receipt{}, err
	}

	body := readSeekCloser{Reader: bytes.NewReader(part.Bytes)}
	if _, err := u.blob.StageBlock(ctx, blockID, body, nil); err != nil {
		u.dropPart(part.Number)
		return multipart.Receipt{}, mapError(err)
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		delete(u.parts, part.Number)
		return multipart.Receipt{}, multipart.ErrAborted
	}
	u.parts[part.Number] = azurePart{blockID: blockID, size: uint64(len(part.Bytes)), done: true}
	return multipart.Receipt{
		Number:    part.Number,
		Token:     blockID,
		SizeBytes: uint64(len(part.Bytes)),
	}, nil
}

func (u *upload) Complete(ctx context.Context, receipts []multipart.Receipt) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateReceipts(receipts); err != nil {
		return multipart.ObjectAttrs{}, err
	}
	blockIDs, size, err := u.blocksFor(receipts)
	if err != nil {
		return multipart.ObjectAttrs{}, err
	}

	none := azcore.ETag("*")
	out, err := u.blob.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{
		HTTPHeaders: &azblobblob.HTTPHeaders{
			BlobContentType: &u.opts.ContentType,
		},
		AccessConditions: &azblobblob.AccessConditions{
			ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{
				IfNoneMatch: &none,
			},
		},
	})
	if err != nil {
		return multipart.ObjectAttrs{}, mapError(err)
	}
	token := etagString(out.ETag)
	if props, err := u.blob.BlobClient().GetProperties(ctx, nil); err == nil {
		if props.ContentLength != nil && *props.ContentLength >= 0 {
			size = uint64(*props.ContentLength)
		}
		if props.ETag != nil {
			token = etagString(props.ETag)
		}
	}

	u.mu.Lock()
	u.completed = true
	u.mu.Unlock()

	return multipart.ObjectAttrs{Key: u.key, SizeBytes: size, Token: token}, nil
}

func (u *upload) Abort(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.completed || u.aborted {
		return nil
	}
	u.aborted = true
	u.parts = nil
	return nil
}

func (u *upload) reservePart(number int, blockID string) error {
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
	u.parts[number] = azurePart{blockID: blockID}
	return nil
}

func (u *upload) dropPart(number int) {
	u.mu.Lock()
	delete(u.parts, number)
	u.mu.Unlock()
}

func (u *upload) blocksFor(receipts []multipart.Receipt) ([]string, uint64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.aborted {
		return nil, 0, multipart.ErrAborted
	}
	if u.completed {
		return nil, 0, multipart.ErrCompleted
	}
	if len(receipts) == 0 {
		return nil, 0, fmt.Errorf("%w: no receipts", multipart.ErrInvalidStore)
	}

	blockIDs := make([]string, 0, len(receipts))
	var size uint64
	for _, receipt := range receipts {
		part, ok := u.parts[receipt.Number]
		if !ok || !part.done {
			return nil, 0, fmt.Errorf("%w: missing azure block %d", multipart.ErrInvalidStore, receipt.Number)
		}
		if receipt.Token != "" && receipt.Token != part.blockID {
			return nil, 0, fmt.Errorf("%w: token mismatch for azure block %d", multipart.ErrInvalidStore, receipt.Number)
		}
		blockIDs = append(blockIDs, part.blockID)
		size += part.size
	}
	return blockIDs, size, nil
}

type readSeekCloser struct {
	*bytes.Reader
}

var _ io.ReadSeekCloser = readSeekCloser{}

func (r readSeekCloser) Close() error {
	return nil
}

func blockID(number int) string {
	raw := fmt.Sprintf("partitionlog-block-%06d", number)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func etagString(etag *azcore.ETag) string {
	if etag == nil {
		return ""
	}
	return string(*etag)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) {
		switch responseErr.StatusCode {
		case 404:
			return fmt.Errorf("%w: %w", multipart.ErrAborted, err)
		case 409, 412:
			return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
		}
	}
	return err
}
