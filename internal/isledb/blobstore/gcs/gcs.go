package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
)

const (
	provider = "gcs"
	// MaxCreateBytes is the GCS single-object ceiling (5 TiB).
	MaxCreateBytes = int64(5 << 40)
	// createChunkBytes is the storage.Writer ChunkSize used by Create. The SDK
	// allocates exactly one buffer of this size per Writer; it is the only
	// upload buffer of this package.
	createChunkBytes = 16 << 20
	// copyBufferBytes moves producer bytes into the Writer's pipe.
	copyBufferBytes = 256 << 10
	contentType     = "application/octet-stream"
)

// Store implements both blobstore contracts over one GCS bucket.
type Store struct {
	// objects is never given a retry configuration itself: BucketHandle.Object
	// clones the configuration, so each per-operation ObjectHandle.Retryer
	// call below mutates a private copy (see doc.go).
	objects *storage.BucketHandle
	// lister is the RetryNever handle used only for the Objects iterator.
	lister *storage.BucketHandle
	bucket string
	// chunkBytes is createChunkBytes except in tests that force the
	// multi-request resumable path with small bodies.
	chunkBytes int
}

var (
	_ blobstore.MetadataStore = (*Store)(nil)
	_ blobstore.RunStore      = (*Store)(nil)
)

// New binds a Store to an explicit client and bucket. It performs no I/O.
func New(client *storage.Client, bucket string) (*Store, error) {
	if client == nil || bucket == "" {
		return nil, errors.Join(blobstore.ErrInvalidRequest, errors.New("blobstore/gcs: nil client or empty bucket"))
	}
	return &Store{
		objects:    client.Bucket(bucket),
		lister:     client.Bucket(bucket).Retryer(storage.WithPolicy(storage.RetryNever)),
		bucket:     bucket,
		chunkBytes: createChunkBytes,
	}, nil
}

func neverRetry(error) bool { return false }

// object returns a fresh handle on which every request is issued exactly once.
func (s *Store) object(key string) *storage.ObjectHandle {
	return s.objects.Object(key).Retryer(storage.WithPolicy(storage.RetryNever))
}

// createObject returns the handle used by the chunked Create writer.
// RetryNever alone leaves the resumable per-chunk retry loop on its default
// predicate (doc.go, "Retry suppression"), so the writer instead installs an
// explicit never-retry predicate, which the SDK forwards to both the initial
// request and every chunk request.
func (s *Store) createObject(key string) *storage.ObjectHandle {
	return s.objects.Object(key).Retryer(
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(neverRetry),
		storage.WithMaxAttempts(1),
	)
}

// ---- error mapping (this leaf's SDK only) ----

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusNotFound
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.NotFound
	}
	return false
}

// isPrecondition reports only a definite conditional rejection: HTTP 412 or
// gRPC FailedPrecondition. Transport errors, 429, 5xx, context errors and
// decode errors are never conflicts.
func isPrecondition(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusPreconditionFailed
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.FailedPrecondition
	}
	return false
}

func begin(ctx context.Context, key string) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	return ctx.Err()
}

func cleanup(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(blobstore.ErrCleanup, err)
}

// ---- metadata contract ----

func metadataToken(generation int64) string { return strconv.FormatInt(generation, 10) }

func parseMetadataToken(token string) (int64, bool) {
	generation, err := strconv.ParseInt(token, 10, 64)
	if err != nil || generation <= 0 || metadataToken(generation) != token {
		return 0, false
	}
	return generation, true
}

func (s *Store) BoundedGet(ctx context.Context, key string, maxBytes int64) (blobstore.Object, error) {
	if maxBytes < 1 || maxBytes > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	reader, err := s.object(key).NewReader(ctx)
	if err != nil {
		if isNotFound(err) {
			return blobstore.Object{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.Object{}, err
	}
	size, generation := reader.Attrs.Size, reader.Attrs.Generation
	if size > maxBytes {
		// Rejected on the reported length, before any body allocation or read.
		return blobstore.Object{}, errors.Join(blobstore.ErrTooLarge, cleanup(reader.Close()))
	}
	if size < 0 {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/gcs: provider reported no object length"), cleanup(reader.Close()))
	}
	// size <= maxBytes <= MaxMetadataBytes, so the allocation is bounded.
	data := make([]byte, size)
	limited := io.LimitReader(reader, maxBytes+1)
	n, readErr := io.ReadFull(limited, data)
	var extra int
	if readErr == nil {
		var probe [1]byte
		extra, readErr = io.ReadFull(limited, probe[:])
		if readErr == io.EOF {
			readErr = nil
		}
	}
	closeErr := cleanup(reader.Close())
	switch {
	case readErr == io.EOF || readErr == io.ErrUnexpectedEOF || extra > 0:
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate,
			fmt.Errorf("blobstore/gcs: reported length %d disagrees with body (read %d)", size, n+extra), closeErr)
	case readErr != nil:
		return blobstore.Object{}, errors.Join(readErr, closeErr)
	case closeErr != nil:
		return blobstore.Object{}, closeErr
	case generation <= 0:
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/gcs: read response carried no generation"))
	}
	return blobstore.Object{Key: key, Body: data, Token: metadataToken(generation)}, nil
}

// writeSmall issues one non-chunked (ChunkSize 0) conditional insert. The
// body is already in memory, so the SDK streams it in a single multipart
// request without allocating an upload buffer.
func (s *Store) writeSmall(ctx context.Context, key string, conds storage.Conditions, body []byte) (*storage.ObjectAttrs, error) {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := s.object(key).If(conds).NewWriter(wctx)
	w.ChunkSize = 0
	w.ContentType = contentType
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			// Poison the pipe before Close so a truncated body can never be
			// presented to the service as complete.
			cancel()
			_ = w.CloseWithError(err)
			return nil, errors.Join(err, w.Close())
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	attrs := w.Attrs()
	if attrs == nil || attrs.Generation <= 0 || attrs.Size != int64(len(body)) {
		return nil, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/gcs: write acknowledged without a usable generation and size"))
	}
	return attrs, nil
}

func (s *Store) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.Object{}, err
	}
	if int64(len(body)) > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	attrs, err := s.writeSmall(ctx, key, storage.Conditions{DoesNotExist: true}, body)
	if err == nil {
		return blobstore.Object{Key: key, Body: bytes.Clone(body), Token: metadataToken(attrs.Generation)}, nil
	}
	if !isPrecondition(err) {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	// Definite "exists": compare against the winner with a bound of len(body).
	existing, getErr := s.BoundedGet(ctx, key, max(int64(len(body)), 1))
	switch {
	case getErr == nil && bytes.Equal(existing.Body, body):
		return existing, nil
	case getErr == nil || errors.Is(getErr, blobstore.ErrTooLarge):
		return blobstore.Object{}, errors.Join(blobstore.ErrImmutableConflict, err)
	default:
		// The create was rejected, but the winner could not be compared.
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, getErr, err)
	}
}

func (s *Store) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.CASResult, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.CASResult{}, err
	}
	if int64(len(body)) > blobstore.MaxMetadataBytes {
		return blobstore.CASResult{}, blobstore.ErrInvalidRequest
	}
	conds := storage.Conditions{DoesNotExist: true}
	if expectedToken != "" {
		generation, ok := parseMetadataToken(expectedToken)
		if !ok {
			return blobstore.CASResult{}, errors.Join(blobstore.ErrInvalidRequest,
				errors.New("blobstore/gcs: malformed metadata token"))
		}
		conds = storage.Conditions{GenerationMatch: generation}
	}
	attrs, err := s.writeSmall(ctx, key, conds, body)
	if err == nil {
		return blobstore.CASResult{Outcome: blobstore.CASApplied,
			Object: blobstore.Object{Key: key, Token: metadataToken(attrs.Generation)}}, nil
	}
	// 412 is the definite rejection. A 404 is definite too, but only for a
	// GenerationMatch swap: it means the object to swap does not exist.
	if !isPrecondition(err) && !(expectedToken != "" && isNotFound(err)) {
		return blobstore.CASResult{}, err
	}
	result := blobstore.CASResult{Outcome: blobstore.CASConflict}
	// Exactly one follow-up stat; its failure never downgrades the conflict.
	if current, statErr := s.object(key).Attrs(ctx); statErr == nil && current != nil && current.Generation > 0 {
		result.Current = blobstore.ObjectInfo{Key: key, Size: current.Size}
		result.CurrentToken, result.CurrentKnown = metadataToken(current.Generation), true
	}
	return result, nil
}

func (s *Store) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	if ctx == nil {
		return blobstore.ObjectPage{}, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return blobstore.ObjectPage{}, err
	}
	limit := opts.NormalizedLimit()
	// StartOffset is inclusive: one extra entry absorbs AfterKey itself and one
	// more proves HasMore. limit <= MaxListLimit, so this cannot overflow.
	want := limit + 2
	query := &storage.Query{Prefix: opts.Prefix, StartOffset: opts.AfterKey}
	if err := query.SetAttrSelection([]string{"Name", "Size"}); err != nil {
		return blobstore.ObjectPage{}, errors.Join(blobstore.ErrInvalidRequest, err)
	}
	// One pager page of `want` entries: the SDK issues list requests (each once,
	// RetryNever) only until `want` entries are buffered or the listing ends.
	var batch []*storage.ObjectAttrs
	pager := iterator.NewPager(s.lister.Objects(ctx, query), want, "")
	if _, err := pager.NextPage(&batch); err != nil {
		return blobstore.ObjectPage{}, err
	}
	page := blobstore.ObjectPage{Objects: make([]blobstore.ObjectInfo, 0, min(len(batch), limit))}
	for _, attrs := range batch {
		if attrs == nil || attrs.Name == "" || attrs.Name <= opts.AfterKey {
			continue // the inclusive StartOffset entry, or a synthetic prefix row
		}
		if len(page.Objects) == limit {
			page.HasMore = true
			break
		}
		page.Objects = append(page.Objects, blobstore.ObjectInfo{Key: attrs.Name, Size: attrs.Size})
	}
	if page.HasMore {
		page.NextAfterKey = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := begin(ctx, key); err != nil {
		return err
	}
	if err := s.object(key).Delete(ctx); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// ---- run-object contract ----

type runToken struct {
	Generation int64 `json:"generation"`
}

func runIdentity(key string, size, generation int64) (blobstore.RunIdentity, error) {
	if size <= 0 || generation <= 0 {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/gcs: provider response lacks size or generation"))
	}
	token, err := idtoken.Encode(provider, runToken{Generation: generation})
	if err != nil {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return blobstore.RunIdentity{Key: key, Size: size, Token: token}, nil
}

func decodeGeneration(identity blobstore.RunIdentity) (int64, error) {
	var token runToken
	if err := idtoken.Decode(provider, identity.Token, &token); err != nil {
		return 0, errors.Join(blobstore.ErrInvalidIdentity, err)
	}
	if token.Generation <= 0 {
		return 0, blobstore.ErrInvalidIdentity
	}
	return token.Generation, nil
}

func (s *Store) Create(ctx context.Context, key string, body io.Reader, exactSize int64) (blobstore.CreateResult, error) {
	absent := blobstore.CreateResult{Outcome: blobstore.DefinitelyAbsent}
	exists := blobstore.CreateResult{Outcome: blobstore.AlreadyExists}
	if ctx == nil || !blobstore.ValidKey(key) || body == nil || exactSize < 1 || exactSize > MaxCreateBytes {
		return absent, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return absent, err
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := s.createObject(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(wctx)
	w.ChunkSize = s.chunkBytes
	w.ContentType = contentType

	counted := blobstore.NewCountingBody(body, exactSize)
	var cause error
	buffer := make([]byte, copyBufferBytes)
	for cause == nil {
		if err := ctx.Err(); err != nil {
			cause = err
			break
		}
		n, readErr := counted.Read(buffer)
		if n > 0 {
			if _, err := w.Write(buffer[:n]); err != nil {
				cause = err
				break
			}
		}
		if readErr == io.EOF {
			break
		}
		cause = readErr
	}
	if cause == nil && !counted.Complete() {
		cause = io.ErrUnexpectedEOF
	}
	if cause != nil {
		// Abort order matters. Cancel first so no further request can be sent,
		// then poison the pipe so the SDK can never observe a clean EOF (which
		// is its finalization signal), and only then Close to join the upload
		// goroutine. The final producer byte was never released, so nothing
		// sent so far could have completed the object.
		cancel()
		_ = w.CloseWithError(cause)
		closeErr := w.Close()
		// With ChunkSize buffering the create-only rejection may be the error
		// that broke the copy (first chunk flush), or surface on Close.
		if isPrecondition(cause) || isPrecondition(closeErr) {
			return exists, errors.Join(blobstore.ErrAlreadyExists, cause, closeErr, counted.Err())
		}
		// closeErr is only the echo of our own abort; it is not retained so a
		// producer failure is not reported as a cancellation.
		return absent, errors.Join(cause, counted.Err())
	}
	// Producer EOF at exactly exactSize: Close is the commit.
	if err := w.Close(); err != nil {
		if isPrecondition(err) {
			return exists, errors.Join(blobstore.ErrAlreadyExists, err)
		}
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	attrs := w.Attrs()
	if attrs == nil || attrs.Size != exactSize {
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate,
			errors.New("blobstore/gcs: create acknowledged without the exact size"))
	}
	identity, err := runIdentity(key, attrs.Size, attrs.Generation)
	if err != nil {
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return blobstore.CreateResult{Outcome: blobstore.Created, Identity: identity}, nil
}

func (s *Store) Stat(ctx context.Context, key string) (blobstore.RunIdentity, error) {
	if err := begin(ctx, key); err != nil {
		return blobstore.RunIdentity{}, err
	}
	attrs, err := s.object(key).Attrs(ctx)
	if err != nil {
		if isNotFound(err) {
			return blobstore.RunIdentity{}, errors.Join(blobstore.ErrNotFound, err)
		}
		return blobstore.RunIdentity{}, err
	}
	if attrs == nil {
		return blobstore.RunIdentity{}, errors.Join(blobstore.ErrIndeterminate, errors.New("blobstore/gcs: empty attrs"))
	}
	return runIdentity(key, attrs.Size, attrs.Generation)
}

func (s *Store) OpenRange(ctx context.Context, key string, identity blobstore.RunIdentity, offset, length int64) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, blobstore.ErrInvalidRequest
	}
	if err := blobstore.CheckRange(identity, key, offset, length); err != nil {
		return nil, err
	}
	generation, err := decodeGeneration(identity)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := s.object(key).Generation(generation).NewRangeReader(ctx, offset, length)
	if err != nil {
		if !isNotFound(err) {
			return nil, err
		}
		// The pinned generation is gone. One stat of the live object tells
		// "replaced" from "missing"; it never changes the ErrRunChanged verdict.
		live, statErr := s.object(key).Attrs(ctx)
		switch {
		case statErr == nil && live != nil && live.Generation == generation:
			return nil, errors.Join(blobstore.ErrIndeterminate, err,
				errors.New("blobstore/gcs: pinned generation unreadable but still live"))
		case statErr == nil:
			return nil, errors.Join(blobstore.ErrRunChanged, err)
		case isNotFound(statErr):
			return nil, errors.Join(blobstore.ErrRunChanged, blobstore.ErrNotFound, err)
		default:
			return nil, errors.Join(blobstore.ErrRunChanged, err, statErr)
		}
	}
	attrs := reader.Attrs
	if attrs.Size != identity.Size || reader.Remain() != length ||
		(attrs.Generation != 0 && attrs.Generation != generation) {
		return nil, errors.Join(blobstore.ErrRunChanged,
			fmt.Errorf("blobstore/gcs: pinned read returned size %d remain %d generation %d", attrs.Size, reader.Remain(), attrs.Generation),
			cleanup(reader.Close()))
	}
	return &rangeBody{ctx: ctx, reader: reader, remain: length}, nil
}

// rangeBody enforces the exact range length and single close.
type rangeBody struct {
	ctx    context.Context
	reader *storage.Reader
	remain int64
	closed bool
}

func (b *rangeBody) Read(p []byte) (int, error) {
	if b.closed {
		return 0, errors.New("blobstore/gcs: read after close")
	}
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if b.remain == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > b.remain {
		p = p[:b.remain]
	}
	n, err := b.reader.Read(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrNoProgress
	}
	b.remain -= int64(n)
	switch {
	case err == io.EOF && b.remain > 0:
		return n, io.ErrUnexpectedEOF
	case err == io.EOF:
		return n, nil // the next Read reports EOF from remain == 0
	}
	return n, err
}

func (b *rangeBody) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	return cleanup(b.reader.Close())
}

func (s *Store) DeleteIfIdentity(ctx context.Context, key string, identity blobstore.RunIdentity) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	if !identity.Valid() || identity.Key != key {
		return blobstore.ErrInvalidIdentity
	}
	generation, err := decodeGeneration(identity)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Conditional delete of the LIVE object. Object.Generation(g).Delete would
	// instead destroy an archived version and can never be used here.
	err = s.object(key).If(storage.Conditions{GenerationMatch: generation}).Delete(ctx)
	switch {
	case err == nil:
		return nil
	case isPrecondition(err):
		return errors.Join(blobstore.ErrRunChanged, err)
	case isNotFound(err):
		return errors.Join(blobstore.ErrNotFound, err)
	default:
		return errors.Join(blobstore.ErrIndeterminate, err)
	}
}
