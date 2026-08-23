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
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
)

var limits = multipart.Limits{
	MaxPartSize:   4_000 << 20,
	MaxPartCount:  50_000,
	MaxObjectSize: (4_000 << 20) * 50_000,
}

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

func (s *Store) Limits() multipart.Limits { return limits }

func (s *Store) Begin(ctx context.Context, key string, opts multipart.Options) (multipart.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := multipart.NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	return &session{
		blob:  s.container.NewBlockBlobClient(key),
		key:   key,
		opts:  opts,
		parts: make(map[int]*azurePart),
	}, nil
}

type session struct {
	mu sync.Mutex

	blob  *blockblob.Client
	key   string
	opts  multipart.Options
	parts map[int]*azurePart

	committing bool
	committed  *multipart.ObjectAttrs
	cleaned    bool
}

type azurePart struct {
	blockID  string
	checksum [32]byte
	done     chan struct{}
	doneOnce sync.Once
	receipt  multipart.Receipt
	complete bool
}

func (u *session) Limits() multipart.Limits { return limits }

func (u *session) PutPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	if err := multipart.ValidatePartLimits(part, limits); err != nil {
		return multipart.Receipt{}, err
	}
	for {
		entry, owner, cached, err := u.reservePart(part)
		if err != nil {
			return multipart.Receipt{}, err
		}
		if cached != nil {
			return *cached, nil
		}
		if !owner {
			select {
			case <-entry.done:
				continue
			case <-ctx.Done():
				return multipart.Receipt{}, ctx.Err()
			}
		}

		body := readSeekCloser{Reader: bytes.NewReader(part.Bytes)}
		if _, err := u.blob.StageBlock(ctx, entry.blockID, body, nil); err != nil {
			u.finishPart(part.Number, entry, multipart.Receipt{}, false)
			return multipart.Receipt{}, mapError(err)
		}
		receipt := multipart.Receipt{
			Number:         part.Number,
			Token:          entry.blockID,
			SizeBytes:      uint64(len(part.Bytes)),
			ChecksumSHA256: part.ChecksumSHA256,
		}
		if err := u.finishPart(part.Number, entry, receipt, true); err != nil {
			return multipart.Receipt{}, err
		}
		return receipt, nil
	}
}

func (u *session) Commit(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateCommitRequest(request, limits); err != nil {
		return multipart.ObjectAttrs{}, err
	}
	blockIDs, attrs, done, err := u.beginCommit(request)
	if done || err != nil {
		return attrs, err
	}

	none := azcore.ETag("*")
	out, commitErr := u.blob.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{
		HTTPHeaders: &azblobblob.HTTPHeaders{BlobContentType: &u.opts.ContentType},
		Metadata:    pointerMetadata(multipart.CommitMetadata(u.opts, request)),
		AccessConditions: &azblobblob.AccessConditions{
			ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{IfNoneMatch: &none},
		},
	})
	if commitErr != nil {
		return u.finishCommitError(ctx, request, mapError(commitErr))
	}

	attrs = multipart.ObjectAttrs{
		Key: u.key, SizeBytes: request.SizeBytes, Token: etagString(out.ETag), SessionID: u.opts.SessionID, ObjectSHA256: request.ObjectSHA256,
	}
	if props, err := u.blob.BlobClient().GetProperties(ctx, nil); err == nil {
		if props.ContentLength != nil && *props.ContentLength >= 0 {
			attrs.SizeBytes = uint64(*props.ContentLength)
		}
		if props.ETag != nil {
			attrs.Token = etagString(props.ETag)
		}
	}
	if attrs.SizeBytes != request.SizeBytes {
		u.endCommit(nil)
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: azure committed size=%d want=%d", multipart.ErrCommitIndeterminate, attrs.SizeBytes, request.SizeBytes)
	}
	u.endCommit(&attrs)
	return attrs, nil
}

// Cleanup is intentionally local-only for Azure. Uncommitted blocks are not a
// final object and Azure expires them. The method refuses while CommitBlockList
// is running, so it can never report successful cleanup while publication may
// be landing.
func (u *session) Cleanup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.committed != nil || u.cleaned {
		return nil
	}
	if u.committing {
		return multipart.ErrCommitInProgress
	}
	u.cleaned = true
	for _, part := range u.parts {
		part.signal()
	}
	u.parts = nil
	return nil
}

func (u *session) reservePart(part multipart.Part) (*azurePart, bool, *multipart.Receipt, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned {
		return nil, false, nil, multipart.ErrCleaned
	}
	if u.committed != nil {
		return nil, false, nil, multipart.ErrCommitted
	}
	if u.committing {
		return nil, false, nil, multipart.ErrCommitInProgress
	}
	if existing := u.parts[part.Number]; existing != nil {
		if existing.checksum != part.ChecksumSHA256 {
			return nil, false, nil, fmt.Errorf("%w: azure part %d", multipart.ErrPartConflict, part.Number)
		}
		if existing.complete {
			receipt := existing.receipt
			return existing, false, &receipt, nil
		}
		return existing, false, nil, nil
	}
	entry := &azurePart{
		blockID: blockID(u.opts.SessionID, part.Number), checksum: part.ChecksumSHA256, done: make(chan struct{}),
	}
	u.parts[part.Number] = entry
	return entry, true, nil, nil
}

func (u *session) finishPart(number int, entry *azurePart, receipt multipart.Receipt, success bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.parts[number] != entry {
		entry.signal()
		return multipart.ErrCleaned
	}
	if !success {
		delete(u.parts, number)
		entry.signal()
		return nil
	}
	if u.cleaned {
		delete(u.parts, number)
		entry.signal()
		return multipart.ErrCleaned
	}
	entry.receipt = receipt
	entry.complete = true
	entry.signal()
	return nil
}

func (p *azurePart) signal() { p.doneOnce.Do(func() { close(p.done) }) }

func (u *session) beginCommit(request multipart.CommitRequest) ([]string, multipart.ObjectAttrs, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned {
		return nil, multipart.ObjectAttrs{}, true, multipart.ErrCleaned
	}
	if u.committed != nil {
		attrs := *u.committed
		if attrs.SizeBytes != request.SizeBytes || request.ObjectSHA256 != ([32]byte{}) && attrs.ObjectSHA256 != request.ObjectSHA256 {
			return nil, multipart.ObjectAttrs{}, true, multipart.ErrPreconditionFailed
		}
		return nil, attrs, true, nil
	}
	if u.committing {
		return nil, multipart.ObjectAttrs{}, true, multipart.ErrCommitInProgress
	}
	if len(u.parts) != len(request.Receipts) {
		return nil, multipart.ObjectAttrs{}, true, fmt.Errorf("%w: session has %d parts but commit has %d receipts", multipart.ErrPartConflict, len(u.parts), len(request.Receipts))
	}
	blockIDs := make([]string, 0, len(request.Receipts))
	for _, receipt := range request.Receipts {
		part := u.parts[receipt.Number]
		if part == nil || !part.complete || part.receipt != receipt {
			return nil, multipart.ObjectAttrs{}, true, fmt.Errorf("%w: receipt mismatch for azure block %d", multipart.ErrPartConflict, receipt.Number)
		}
		blockIDs = append(blockIDs, part.blockID)
	}
	u.committing = true
	return blockIDs, multipart.ObjectAttrs{}, false, nil
}

func (u *session) finishCommitError(ctx context.Context, request multipart.CommitRequest, commitErr error) (multipart.ObjectAttrs, error) {
	attrs, found, reconcileErr := u.reconcile(ctx, request)
	if reconcileErr == nil && found {
		u.endCommit(&attrs)
		return attrs, nil
	}
	u.endCommit(nil)
	if errors.Is(reconcileErr, multipart.ErrPreconditionFailed) {
		return multipart.ObjectAttrs{}, reconcileErr
	}
	if errors.Is(commitErr, multipart.ErrPreconditionFailed) && reconcileErr == nil {
		return multipart.ObjectAttrs{}, commitErr
	}
	return multipart.ObjectAttrs{}, errors.Join(multipart.ErrCommitIndeterminate, commitErr, reconcileErr)
}

func (u *session) reconcile(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, bool, error) {
	props, err := u.blob.BlobClient().GetProperties(ctx, nil)
	if err != nil {
		if isStatus(err, 404) {
			return multipart.ObjectAttrs{}, false, nil
		}
		return multipart.ObjectAttrs{}, false, mapError(err)
	}
	size := uint64(0)
	if props.ContentLength != nil && *props.ContentLength >= 0 {
		size = uint64(*props.ContentLength)
	}
	if !multipart.MatchesCommittedObject(size, valueMetadata(props.Metadata), u.opts, request, true) {
		return multipart.ObjectAttrs{}, false, multipart.ErrPreconditionFailed
	}
	return multipart.ObjectAttrs{
		Key: u.key, SizeBytes: size, Token: etagString(props.ETag), SessionID: u.opts.SessionID, ObjectSHA256: request.ObjectSHA256,
	}, true, nil
}

func (u *session) endCommit(attrs *multipart.ObjectAttrs) {
	u.mu.Lock()
	u.committing = false
	if attrs != nil {
		copy := *attrs
		u.committed = &copy
		u.parts = nil
	}
	u.mu.Unlock()
}

type readSeekCloser struct{ *bytes.Reader }

var _ io.ReadSeekCloser = readSeekCloser{}

func (r readSeekCloser) Close() error { return nil }

func blockID(sessionID string, number int) string {
	raw := fmt.Sprintf("unijord-%s-%06d", sessionID, number)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func pointerMetadata(metadata map[string]string) map[string]*string {
	out := make(map[string]*string, len(metadata))
	for key, value := range metadata {
		value := value
		out[key] = &value
	}
	return out
}

func valueMetadata(metadata map[string]*string) map[string]string {
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if value != nil {
			out[key] = *value
		}
	}
	return out
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
	if isStatus(err, 404) {
		return fmt.Errorf("%w: %w", multipart.ErrCleaned, err)
	}
	if isStatus(err, 409) || isStatus(err, 412) {
		return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
	}
	return err
}

func isStatus(err error, status int) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == status
}
