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
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
)

const (
	composeSourceLimit = 32
	libraryPartLimit   = 10_000
)

var limits = multipart.Limits{
	MaxPartSize:   5 << 40,
	MaxPartCount:  libraryPartLimit,
	MaxObjectSize: 5 << 40,
}

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

func (s *Store) Limits() multipart.Limits { return limits }

func (s *Store) Begin(ctx context.Context, key string, opts multipart.Options) (multipart.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts, err := multipart.NormalizeOptions(key, opts)
	if err != nil {
		return nil, err
	}
	u := &session{
		bucket:       s.client.Bucket(s.bucket),
		key:          key,
		opts:         opts,
		parts:        make(map[int]*gcsPart),
		staging:      make(map[string]gcsObject),
		newAttemptID: randomAttemptID,
	}
	u.composeObjects = u.compose
	return u, nil
}

type session struct {
	mu sync.Mutex

	bucket         *storage.BucketHandle
	key            string
	opts           multipart.Options
	parts          map[int]*gcsPart
	staging        map[string]gcsObject
	newAttemptID   func() (string, error)
	composeObjects func(context.Context, []gcsObject, string, multipart.CommitRequest) (gcsObject, error)
	partWG         sync.WaitGroup

	committing  bool
	committed   *multipart.ObjectAttrs
	cleaning    bool
	cleaned     bool
	cleanupDone chan struct{}
	cleanupErr  error
}

type gcsPart struct {
	checksum [32]byte
	done     chan struct{}
	doneOnce sync.Once
	object   gcsObject
	receipt  multipart.Receipt
	complete bool
}

type gcsObject struct {
	key        string
	generation int64
	size       uint64
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

		receipt, object, uploadErr := u.uploadPartAttempt(ctx, part)
		u.partWG.Done()
		if uploadErr != nil {
			u.finishPart(part.Number, entry, multipart.Receipt{}, gcsObject{}, false)
			return multipart.Receipt{}, uploadErr
		}
		if err := u.finishPart(part.Number, entry, receipt, object, true); err != nil {
			return multipart.Receipt{}, err
		}
		return receipt, nil
	}
}

func (u *session) uploadPartAttempt(ctx context.Context, part multipart.Part) (multipart.Receipt, gcsObject, error) {
	attemptID, err := u.newAttemptID()
	if err != nil {
		return multipart.Receipt{}, gcsObject{}, fmt.Errorf("%w: generate gcs part attempt: %w", multipart.ErrInvalidStore, err)
	}
	partKey := multipart.StagingPartKey(u.opts.StagingPrefix, part.Number, attemptID)
	u.trackStaging(gcsObject{key: partKey})

	obj := u.bucket.Object(partKey).If(storage.Conditions{DoesNotExist: true})
	w := obj.NewWriter(ctx)
	w.ContentType = u.opts.ContentType
	w.Metadata = multipart.SessionMetadata(u.opts)
	w.Metadata["unijord-part-number"] = strconv.Itoa(part.Number)
	w.Metadata["unijord-part-sha256"] = fmt.Sprintf("%x", part.ChecksumSHA256)
	if _, err := bytes.NewReader(part.Bytes).WriteTo(w); err != nil {
		_ = w.Close()
		return multipart.Receipt{}, gcsObject{}, mapError(err)
	}
	if err := w.Close(); err != nil {
		return multipart.Receipt{}, gcsObject{}, mapError(err)
	}
	attrs := w.Attrs()
	staged := gcsObject{key: partKey, generation: attrs.Generation, size: uint64(len(part.Bytes))}
	u.trackStaging(staged)
	return multipart.Receipt{
		Number:         part.Number,
		Token:          strconv.FormatInt(staged.generation, 10),
		SizeBytes:      staged.size,
		ChecksumSHA256: part.ChecksumSHA256,
	}, staged, nil
}

func (u *session) Commit(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, error) {
	if err := multipart.ValidateCommitRequest(request, limits); err != nil {
		return multipart.ObjectAttrs{}, err
	}
	sources, attrs, done, err := u.beginCommit(request)
	if done || err != nil {
		return attrs, err
	}

	attemptID, err := u.newAttemptID()
	if err != nil {
		u.endCommit(nil)
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: generate gcs commit attempt: %w", multipart.ErrInvalidStore, err)
	}
	final, commitErr := u.composeObjects(ctx, sources, attemptID, request)
	if commitErr != nil {
		return u.finishCommitError(ctx, request, mapError(commitErr))
	}
	attrs = multipart.ObjectAttrs{
		Key: final.key, SizeBytes: final.size, Token: strconv.FormatInt(final.generation, 10), SessionID: u.opts.SessionID, ObjectSHA256: request.ObjectSHA256,
	}
	if attrs.Key == "" {
		attrs.Key = u.key
	}
	if attrs.SizeBytes != request.SizeBytes {
		u.endCommit(nil)
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: gcs committed size=%d want=%d", multipart.ErrCommitIndeterminate, attrs.SizeBytes, request.SizeBytes)
	}
	u.endCommit(&attrs)
	u.cleanupStagingBestEffort(ctx)
	return attrs, nil
}

func (u *session) Cleanup(ctx context.Context) error {
	u.mu.Lock()
	if u.committed != nil {
		u.mu.Unlock()
		return u.cleanupStaging(ctx)
	}
	if u.committing {
		u.mu.Unlock()
		return multipart.ErrCommitInProgress
	}
	if u.cleaning {
		done := u.cleanupDone
		u.mu.Unlock()
		select {
		case <-done:
			u.mu.Lock()
			err := u.cleanupErr
			u.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if u.cleaned && u.cleanupErr == nil {
		u.mu.Unlock()
		return nil
	}
	u.cleaning = true
	u.cleaned = true
	u.cleanupDone = make(chan struct{})
	done := u.cleanupDone
	for _, part := range u.parts {
		part.signal()
	}
	u.mu.Unlock()

	if err := waitGroup(ctx, &u.partWG); err != nil {
		u.finishCleanup(err, done)
		return err
	}
	staging := u.snapshotStaging()
	err := deleteGCSObjects(ctx, u.bucket, staging)
	if err == nil {
		u.removeStaging(staging)
	}
	u.finishCleanup(err, done)
	return err
}

func (u *session) reservePart(part multipart.Part) (*gcsPart, bool, *multipart.Receipt, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned || u.cleaning {
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
			return nil, false, nil, fmt.Errorf("%w: gcs part %d", multipart.ErrPartConflict, part.Number)
		}
		if existing.complete {
			receipt := existing.receipt
			return existing, false, &receipt, nil
		}
		return existing, false, nil, nil
	}
	entry := &gcsPart{checksum: part.ChecksumSHA256, done: make(chan struct{})}
	u.parts[part.Number] = entry
	u.partWG.Add(1)
	return entry, true, nil, nil
}

func (u *session) finishPart(number int, entry *gcsPart, receipt multipart.Receipt, object gcsObject, success bool) error {
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
	if u.cleaned || u.cleaning {
		delete(u.parts, number)
		entry.signal()
		return multipart.ErrCleaned
	}
	entry.object = object
	entry.receipt = receipt
	entry.complete = true
	entry.signal()
	return nil
}

func (p *gcsPart) signal() { p.doneOnce.Do(func() { close(p.done) }) }

func (u *session) beginCommit(request multipart.CommitRequest) ([]gcsObject, multipart.ObjectAttrs, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.cleaned || u.cleaning {
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
	sources := make([]gcsObject, 0, len(request.Receipts))
	for _, receipt := range request.Receipts {
		part := u.parts[receipt.Number]
		if part == nil || !part.complete || part.receipt != receipt {
			return nil, multipart.ObjectAttrs{}, true, fmt.Errorf("%w: receipt mismatch for gcs part %d", multipart.ErrPartConflict, receipt.Number)
		}
		sources = append(sources, part.object)
	}
	u.committing = true
	return sources, multipart.ObjectAttrs{}, false, nil
}

func (u *session) compose(ctx context.Context, sources []gcsObject, attemptID string, request multipart.CommitRequest) (gcsObject, error) {
	prefix := multipart.StagingComposePrefix(u.opts.StagingPrefix, attemptID)
	level := 0
	for len(sources) > composeSourceLimit {
		next := make([]gcsObject, 0, (len(sources)+composeSourceLimit-1)/composeSourceLimit)
		for group := 0; len(sources) > 0; group++ {
			n := min(len(sources), composeSourceLimit)
			dst := path.Join(prefix, fmt.Sprintf("level-%02d", level), fmt.Sprintf("group-%06d", group))
			obj, err := u.composeTo(ctx, dst, sources[:n], multipart.SessionMetadata(u.opts))
			if err != nil {
				return gcsObject{}, err
			}
			next = append(next, obj)
			sources = sources[n:]
		}
		sources = next
		level++
	}
	return u.composeTo(ctx, u.key, sources, multipart.CommitMetadata(u.opts, request))
}

func (u *session) composeTo(ctx context.Context, dstKey string, sources []gcsObject, metadata map[string]string) (gcsObject, error) {
	handles := make([]*storage.ObjectHandle, 0, len(sources))
	var size uint64
	for _, source := range sources {
		handles = append(handles, u.bucket.Object(source.key).Generation(source.generation))
		size += source.size
	}
	staged := gcsObject{key: dstKey, size: size}
	if dstKey != u.key {
		u.trackStaging(staged)
	}
	dst := u.bucket.Object(dstKey).If(storage.Conditions{DoesNotExist: true})
	composer := dst.ComposerFrom(handles...)
	composer.ContentType = u.opts.ContentType
	composer.Metadata = metadata
	attrs, err := composer.Run(ctx)
	if err != nil {
		return gcsObject{}, err
	}
	object := gcsObject{key: attrs.Name, generation: attrs.Generation, size: size}
	if attrs.Size >= 0 {
		object.size = uint64(attrs.Size)
	}
	if object.key == "" {
		object.key = dstKey
	}
	if dstKey != u.key {
		u.trackStaging(object)
	}
	return object, nil
}

func (u *session) finishCommitError(ctx context.Context, request multipart.CommitRequest, commitErr error) (multipart.ObjectAttrs, error) {
	attrs, found, reconcileErr := u.reconcile(ctx, request)
	if reconcileErr == nil && found {
		u.endCommit(&attrs)
		u.cleanupStagingBestEffort(ctx)
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
	attrs, err := u.bucket.Object(u.key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return multipart.ObjectAttrs{}, false, nil
		}
		return multipart.ObjectAttrs{}, false, mapError(err)
	}
	size := uint64(0)
	if attrs.Size >= 0 {
		size = uint64(attrs.Size)
	}
	if !multipart.MatchesCommittedObject(size, attrs.Metadata, u.opts, request, true) {
		return multipart.ObjectAttrs{}, false, multipart.ErrPreconditionFailed
	}
	return multipart.ObjectAttrs{
		Key: u.key, SizeBytes: size, Token: strconv.FormatInt(attrs.Generation, 10), SessionID: u.opts.SessionID, ObjectSHA256: request.ObjectSHA256,
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

func (u *session) trackStaging(object gcsObject) {
	u.mu.Lock()
	u.staging[object.key] = object
	u.mu.Unlock()
}

func (u *session) snapshotStaging() []gcsObject {
	u.mu.Lock()
	defer u.mu.Unlock()
	objects := make([]gcsObject, 0, len(u.staging))
	for _, object := range u.staging {
		objects = append(objects, object)
	}
	return objects
}

func (u *session) removeStaging(objects []gcsObject) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, object := range objects {
		if current, ok := u.staging[object.key]; ok && current.generation == object.generation {
			delete(u.staging, object.key)
		}
	}
}

func (u *session) cleanupStagingBestEffort(ctx context.Context) {
	_ = u.cleanupStaging(ctx)
}

func (u *session) cleanupStaging(ctx context.Context) error {
	staging := u.snapshotStaging()
	if err := deleteGCSObjects(ctx, u.bucket, staging); err != nil {
		return err
	}
	if len(staging) > 0 {
		u.removeStaging(staging)
	}
	return nil
}

func (u *session) finishCleanup(err error, done chan struct{}) {
	u.mu.Lock()
	u.cleaning = false
	u.cleanupErr = err
	u.parts = nil
	u.mu.Unlock()
	close(done)
}

func randomAttemptID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func waitGroup(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func deleteGCSObjects(ctx context.Context, bucket *storage.BucketHandle, objects []gcsObject) error {
	var firstErr error
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			if firstErr != nil {
				return firstErr
			}
			return err
		}
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
			return fmt.Errorf("%w: %w", multipart.ErrCleaned, err)
		case 412:
			return fmt.Errorf("%w: %w", multipart.ErrPreconditionFailed, err)
		}
	}
	return err
}
