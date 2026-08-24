package stream

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"sort"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
)

const cleanupTimeout = 5 * time.Second

var (
	ErrInvalidOptions      = errors.New("blob/sink/stream: invalid options")
	ErrClosed              = errors.New("blob/sink/stream: upload closed")
	ErrAborted             = errors.New("blob/sink/stream: upload aborted")
	ErrEmptyUpload         = errors.New("blob/sink/stream: empty upload")
	ErrLimitExceeded       = errors.New("blob/sink/stream: provider limit exceeded")
	ErrCommitInProgress    = errors.New("blob/sink/stream: commit in progress")
	ErrCommitIndeterminate = errors.New("blob/sink/stream: commit outcome indeterminate")
	ErrBackendContract     = errors.New("blob/sink/stream: backend contract violation")
)

// Upload is an ordered byte stream backed by a multipart upload.
//
// Write calls must not be concurrent with other Write calls. Write copies all
// input bytes before returning and applies backpressure when the configured
// buffer bound is reached. Commit and Abort are safe to call concurrently.
// Once Commit starts the backend's final commit, Abort returns
// ErrCommitInProgress rather than claiming that the final object is absent.
// A caller that abandons an upload without a successful Commit must call Abort;
// this includes an empty upload whose Commit returned ErrEmptyUpload.
type Upload interface {
	Write(context.Context, []byte) error
	Commit(context.Context) (multipart.ObjectAttrs, error)
	Abort(context.Context) error
}

// UploadLimiter optionally coordinates provider request concurrency across
// multiple streams.
type UploadLimiter interface {
	Acquire(context.Context) error
	Release()
}

// MultipartOptions controls multipart splitting and bounded concurrency.
type MultipartOptions struct {
	PartSize          int
	UploadParallelism int
	UploadQueueSize   int
	UploadLimiter     UploadLimiter

	// BufferPool may be shared by many uploads to enforce a process-wide
	// memory bound. Its BufferSize must equal PartSize. When nil, the upload
	// creates a private pool large enough for one active buffer, every queued
	// part, and every in-flight part.
	BufferPool *BufferPool
}

type uploadState uint8

const (
	stateOpen uploadState = iota
	stateSealing
	stateSealed
	stateFailed
	stateAborting
	stateAborted
	stateCommitting
	stateCompleted
	stateIndeterminate
)

type partJob struct {
	number int
	buffer []byte
	used   int
}

// MultipartUpload adapts a multipart.Session into an ordered, bounded stream.
type MultipartUpload struct {
	backend  multipart.Session
	limits   multipart.Limits
	partSize int
	pool     *BufferPool
	limiter  UploadLimiter
	hasher   hash.Hash

	runCtx context.Context
	cancel context.CancelCauseFunc
	jobs   chan partJob

	writeMu  sync.Mutex
	commitMu sync.Mutex
	workers  sync.WaitGroup
	done     chan struct{}

	mu                    sync.Mutex
	state                 uploadState
	firstErr              error
	receipts              map[int]multipart.Receipt
	abortDone             chan struct{}
	abortErr              error
	cleanupErr            error
	abortWasIndeterminate bool
	completed             multipart.ObjectAttrs
	jobsClosed            bool
	active                []byte
	activeBytes           int
	nextPart              int
	offset                uint64
}

// BeginMultipartUpload validates provider limits before opening remote staging,
// begins a provider session, and wraps it in the ordered stream.
func BeginMultipartUpload(ctx context.Context, store multipart.Store, key string, sessionOpts multipart.Options, streamOpts MultipartOptions) (*MultipartUpload, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidOptions)
	}
	if store == nil {
		return nil, fmt.Errorf("%w: multipart store is nil", ErrInvalidOptions)
	}
	limits := store.Limits()
	if err := validateMultipartOptions(limits, streamOpts); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session, err := store.Begin(ctx, key, sessionOpts)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, fmt.Errorf("%w: store returned a nil session", ErrBackendContract)
	}
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		return session.Cleanup(cleanupCtx)
	}
	if session.Limits() != limits {
		contractErr := fmt.Errorf("%w: store limits=%+v session limits=%+v", ErrBackendContract, limits, session.Limits())
		return nil, errors.Join(contractErr, cleanup())
	}
	upload, err := NewMultipartUpload(ctx, session, streamOpts)
	if err != nil {
		return nil, errors.Join(err, cleanup())
	}
	return upload, nil
}

// NewMultipartUpload creates an ordered stream over backend. The constructor
// does not perform provider I/O; the caller is still responsible for beginning
// the provider's multipart transaction first.
func NewMultipartUpload(ctx context.Context, backend multipart.Session, opts MultipartOptions) (*MultipartUpload, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidOptions)
	}
	if backend == nil {
		return nil, fmt.Errorf("%w: multipart upload is nil", ErrInvalidOptions)
	}
	limits := backend.Limits()
	if err := validateMultipartOptions(limits, opts); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pool := opts.BufferPool
	if pool == nil {
		var err error
		pool, err = NewBufferPool(opts.PartSize, 1+opts.UploadQueueSize+opts.UploadParallelism)
		if err != nil {
			return nil, err
		}
	} else if pool.BufferSize() != opts.PartSize {
		return nil, fmt.Errorf("%w: pool buffer_size=%d part_size=%d", ErrInvalidOptions, pool.BufferSize(), opts.PartSize)
	}

	// The stream lifetime is not owned by the context of the call that creates
	// it. Context values still propagate to provider operations.
	runCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	u := &MultipartUpload{
		backend:  backend,
		limits:   limits,
		partSize: opts.PartSize,
		pool:     pool,
		limiter:  opts.UploadLimiter,
		hasher:   sha256.New(),
		runCtx:   runCtx,
		cancel:   cancel,
		jobs:     make(chan partJob, opts.UploadQueueSize),
		done:     make(chan struct{}),
		state:    stateOpen,
		receipts: make(map[int]multipart.Receipt),
		nextPart: 1,
	}
	for range opts.UploadParallelism {
		u.workers.Add(1)
		go u.uploadWorker()
	}
	go func() {
		u.workers.Wait()
		close(u.done)
	}()
	return u, nil
}

func validateMultipartOptions(limits multipart.Limits, opts MultipartOptions) error {
	if opts.PartSize <= 0 {
		return fmt.Errorf("%w: part_size must be positive", ErrInvalidOptions)
	}
	if opts.UploadParallelism <= 0 {
		return fmt.Errorf("%w: upload_parallelism must be positive", ErrInvalidOptions)
	}
	if opts.UploadQueueSize < 0 {
		return fmt.Errorf("%w: upload_queue_size must not be negative", ErrInvalidOptions)
	}
	if err := limits.Validate(); err != nil {
		return fmt.Errorf("%w: backend limits: %v", ErrInvalidOptions, err)
	}
	partSize := uint64(opts.PartSize)
	if partSize > limits.MaxPartSize {
		return fmt.Errorf("%w: part_size=%d exceeds backend maximum=%d", ErrInvalidOptions, partSize, limits.MaxPartSize)
	}
	if partSize < limits.MinPartSize {
		return fmt.Errorf("%w: part_size=%d is below backend non-final minimum=%d", ErrInvalidOptions, partSize, limits.MinPartSize)
	}
	if opts.BufferPool != nil && opts.BufferPool.BufferSize() != opts.PartSize {
		return fmt.Errorf("%w: pool buffer_size=%d part_size=%d", ErrInvalidOptions, opts.BufferPool.BufferSize(), opts.PartSize)
	}
	return nil
}

// Write appends bytes to the stream. It returns only after the bytes have been
// copied into buffers owned by the upload. If ctx is canceled after any bytes
// from this call were accepted, the upload becomes failed because Write cannot
// report a partial byte count safely.
func (u *MultipartUpload) Write(ctx context.Context, data []byte) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	u.writeMu.Lock()
	defer u.writeMu.Unlock()

	if err := u.writeStateError(); err != nil {
		return err
	}
	if err := u.validateGrowth(len(data)); err != nil {
		return err
	}
	accepted := false
	for len(data) > 0 {
		if u.active == nil {
			buffer, err := u.pool.acquire(ctx, u.runCtx)
			if err != nil {
				return u.failWriteLocked(err, accepted)
			}
			u.active = buffer
		}

		n := copy(u.active[u.activeBytes:], data)
		_, _ = u.hasher.Write(data[:n])
		u.activeBytes += n
		u.offset += uint64(n)
		data = data[n:]
		accepted = true

		if u.activeBytes == u.partSize {
			if err := u.enqueueActiveLocked(ctx); err != nil {
				return u.failWriteLocked(err, accepted)
			}
		}
	}
	return nil
}

// Commit seals the stream, waits for all parts, and asks the backend to create
// the final object. A failure returned by the backend's Commit operation is
// conservatively reported as ErrCommitIndeterminate because the final object
// may already be durable.
func (u *MultipartUpload) Commit(ctx context.Context) (multipart.ObjectAttrs, error) {
	if ctx == nil {
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: nil context", ErrInvalidOptions)
	}
	u.commitMu.Lock()
	defer u.commitMu.Unlock()

	u.writeMu.Lock()
	state, attrs, err := u.beginSealLocked(ctx)
	u.writeMu.Unlock()
	if err != nil {
		return multipart.ObjectAttrs{}, err
	}
	if state == stateCompleted {
		return attrs, nil
	}

	select {
	case <-u.done:
	case <-ctx.Done():
		u.mu.Lock()
		partErr := u.firstErr
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, errors.Join(partErr, ctx.Err())
	}

	u.mu.Lock()
	if u.state == stateAborting || u.state == stateAborted {
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, ErrAborted
	}
	if u.firstErr != nil {
		err := u.firstErr
		u.state = stateFailed
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, err
	}
	if err := ctx.Err(); err != nil {
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, err
	}
	if u.state != stateSealed {
		state := u.state
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, fmt.Errorf("%w: cannot commit state %d", ErrClosed, state)
	}
	receipts, err := u.orderedReceiptsLocked()
	if err != nil {
		u.firstErr = err
		u.state = stateFailed
		u.mu.Unlock()
		return multipart.ObjectAttrs{}, err
	}
	u.state = stateCommitting
	u.mu.Unlock()

	var objectSHA256 [sha256.Size]byte
	copy(objectSHA256[:], u.hasher.Sum(nil))
	attrs, err = u.backend.Commit(ctx, multipart.CommitRequest{
		Receipts:     receipts,
		SizeBytes:    u.offset,
		ObjectSHA256: objectSHA256,
	})
	if err == nil && attrs.Key == "" {
		err = fmt.Errorf("%w: complete returned an empty object key", ErrBackendContract)
	}
	if err == nil && attrs.SizeBytes != u.offset {
		err = fmt.Errorf("%w: complete size=%d accepted_bytes=%d", ErrBackendContract, attrs.SizeBytes, u.offset)
	}

	u.mu.Lock()
	if err != nil {
		if definiteCommitFailure(err) {
			u.firstErr = err
			u.state = stateFailed
			u.mu.Unlock()
			u.cancel(err)
			return multipart.ObjectAttrs{}, err
		}
		u.state = stateIndeterminate
		u.mu.Unlock()
		u.cancel(ErrCommitIndeterminate)
		return multipart.ObjectAttrs{}, errors.Join(ErrCommitIndeterminate, err)
	}
	u.completed = attrs
	u.state = stateCompleted
	u.mu.Unlock()
	u.cancel(ErrClosed)
	return attrs, nil
}

func definiteCommitFailure(err error) bool {
	return errors.Is(err, multipart.ErrPreconditionFailed) ||
		errors.Is(err, multipart.ErrPartConflict) ||
		errors.Is(err, multipart.ErrInvalidStore) ||
		errors.Is(err, multipart.ErrCleaned)
}

// Abort stops part uploads and asks the backend to discard staging work.
// Abort is idempotent. It deliberately refuses while backend Commit is in
// progress because a generic backend cannot prove that the final commit lost.
func (u *MultipartUpload) Abort(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidOptions)
	}

	u.mu.Lock()
	indeterminate := false
	switch u.state {
	case stateCompleted:
		u.mu.Unlock()
		return nil
	case stateCommitting:
		u.mu.Unlock()
		return ErrCommitInProgress
	case stateAborted:
		if u.cleanupErr == nil {
			err := u.abortErr
			u.mu.Unlock()
			return err
		}
		// The stream is already stopped, but staging cleanup may be retried
		// with a fresh caller context.
		indeterminate = u.abortWasIndeterminate
	case stateAborting:
		done := u.abortDone
		u.mu.Unlock()
		return waitAbort(ctx, done, u)
	default:
		indeterminate = u.state == stateIndeterminate
	}
	u.state = stateAborting
	u.abortDone = make(chan struct{})
	done := u.abortDone
	u.mu.Unlock()

	u.cancel(ErrAborted)
	u.writeMu.Lock()
	u.discardAndCloseLocked()
	u.writeMu.Unlock()

	backendErr := u.backend.Cleanup(ctx)
	go u.finishAbort(backendErr, indeterminate)
	return waitAbort(ctx, done, u)
}

func (u *MultipartUpload) beginSealLocked(ctx context.Context) (uploadState, multipart.ObjectAttrs, error) {
	u.mu.Lock()
	switch u.state {
	case stateCompleted:
		attrs := u.completed
		u.mu.Unlock()
		return stateCompleted, attrs, nil
	case stateOpen:
		if u.offset == 0 {
			u.mu.Unlock()
			return stateOpen, multipart.ObjectAttrs{}, ErrEmptyUpload
		}
		u.state = stateSealing
	case stateSealing, stateSealed:
		// A previous Commit may have returned when only its own context was
		// canceled. The stream remains sealed and can be committed again.
	case stateFailed:
		err := u.firstErr
		u.mu.Unlock()
		u.discardAndCloseLocked()
		return stateFailed, multipart.ObjectAttrs{}, err
	case stateAborting, stateAborted:
		state := u.state
		u.mu.Unlock()
		return state, multipart.ObjectAttrs{}, ErrAborted
	case stateCommitting:
		u.mu.Unlock()
		return stateCommitting, multipart.ObjectAttrs{}, ErrCommitInProgress
	case stateIndeterminate:
		// The provider session retains the commit identity and can reconcile or
		// safely retry the same request. No bytes or receipts are rebuilt.
		u.state = stateSealed
	}
	u.mu.Unlock()

	if u.jobsClosed {
		return stateSealed, multipart.ObjectAttrs{}, nil
	}
	if err := u.enqueueActiveLocked(ctx); err != nil {
		u.mu.Lock()
		state := u.state
		if state == stateSealing {
			u.state = stateOpen
		}
		u.mu.Unlock()
		if state == stateFailed || state == stateAborting || state == stateAborted {
			u.discardAndCloseLocked()
			if state == stateFailed {
				return state, multipart.ObjectAttrs{}, u.failure()
			}
			return state, multipart.ObjectAttrs{}, ErrAborted
		}
		return stateOpen, multipart.ObjectAttrs{}, err
	}
	u.closeJobsLocked()
	u.mu.Lock()
	if u.state == stateSealing {
		u.state = stateSealed
	}
	state := u.state
	err := u.firstErr
	u.mu.Unlock()
	if state == stateFailed {
		return state, multipart.ObjectAttrs{}, err
	}
	if state == stateAborting || state == stateAborted {
		return state, multipart.ObjectAttrs{}, ErrAborted
	}
	return state, multipart.ObjectAttrs{}, nil
}

func (u *MultipartUpload) enqueueActiveLocked(ctx context.Context) error {
	if u.active == nil || u.activeBytes == 0 {
		return nil
	}
	job := partJob{number: u.nextPart, buffer: u.active, used: u.activeBytes}
	select {
	case u.jobs <- job:
		u.active = nil
		u.activeBytes = 0
		u.nextPart++
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-u.runCtx.Done():
		return u.failure()
	}
}

// validateGrowth requires writeMu. It rejects the whole Write before accepting
// any bytes, so callers never have to reason about a partially accepted call.
func (u *MultipartUpload) validateGrowth(bytes int) error {
	if bytes == 0 {
		return nil
	}
	additional := uint64(bytes)
	if additional > u.limits.MaxObjectSize || u.offset > u.limits.MaxObjectSize-additional {
		return fmt.Errorf("%w: object size would exceed %d bytes", ErrLimitExceeded, u.limits.MaxObjectSize)
	}
	nextSize := u.offset + additional
	partCount := (nextSize-1)/uint64(u.partSize) + 1
	if partCount > uint64(u.limits.MaxPartCount) {
		return fmt.Errorf("%w: object would require %d parts, maximum is %d", ErrLimitExceeded, partCount, u.limits.MaxPartCount)
	}
	return nil
}

func (u *MultipartUpload) uploadWorker() {
	defer u.workers.Done()
	for job := range u.jobs {
		if context.Cause(u.runCtx) != nil {
			u.pool.release(job.buffer)
			continue
		}

		part := multipart.NewPart(job.number, job.buffer[:job.used])
		var receipt multipart.Receipt
		var err error
		if u.limiter != nil {
			err = u.limiter.Acquire(u.runCtx)
		}
		if err == nil {
			receipt, err = u.backend.PutPart(u.runCtx, part)
			if u.limiter != nil {
				u.limiter.Release()
			}
		}
		if err == nil && receipt.Number != job.number {
			err = fmt.Errorf("%w: part=%d returned receipt=%d", ErrBackendContract, job.number, receipt.Number)
		}
		if err == nil && receipt.SizeBytes != uint64(job.used) {
			err = fmt.Errorf("%w: part=%d receipt size=%d want=%d", ErrBackendContract, job.number, receipt.SizeBytes, job.used)
		}
		if err == nil && receipt.ChecksumSHA256 != part.ChecksumSHA256 {
			err = fmt.Errorf("%w: part=%d receipt checksum mismatch", ErrBackendContract, job.number)
		}
		if err == nil {
			u.mu.Lock()
			u.receipts[job.number] = receipt
			u.mu.Unlock()
		}
		u.pool.release(job.buffer)

		if err != nil {
			u.recordFailure(err)
			u.writeMu.Lock()
			u.discardAndCloseLocked()
			u.writeMu.Unlock()
		}
	}
}

func (u *MultipartUpload) recordFailure(err error) {
	if err == nil {
		return
	}
	u.mu.Lock()
	if u.state == stateAborting || u.state == stateAborted {
		u.mu.Unlock()
		return
	}
	if u.firstErr == nil {
		u.firstErr = err
	}
	if u.state == stateOpen || u.state == stateSealing || u.state == stateSealed {
		u.state = stateFailed
	}
	firstErr := u.firstErr
	u.mu.Unlock()
	u.cancel(firstErr)
}

func (u *MultipartUpload) failWriteLocked(err error, accepted bool) error {
	if !accepted {
		if failure := u.failure(); failure != nil && context.Cause(u.runCtx) != nil {
			return failure
		}
		return err
	}
	u.recordFailure(err)
	u.discardAndCloseLocked()
	return err
}

func (u *MultipartUpload) writeStateError() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch u.state {
	case stateOpen:
		return nil
	case stateFailed:
		return u.firstErr
	case stateAborting, stateAborted:
		return ErrAborted
	case stateCommitting:
		return ErrCommitInProgress
	case stateIndeterminate:
		return ErrCommitIndeterminate
	default:
		return ErrClosed
	}
}

func (u *MultipartUpload) failure() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.firstErr != nil {
		return u.firstErr
	}
	switch u.state {
	case stateAborting, stateAborted:
		return ErrAborted
	case stateIndeterminate:
		return ErrCommitIndeterminate
	default:
		if cause := context.Cause(u.runCtx); cause != nil {
			return cause
		}
		return ErrClosed
	}
}

func (u *MultipartUpload) orderedReceiptsLocked() ([]multipart.Receipt, error) {
	count := u.nextPart - 1
	if len(u.receipts) != count {
		return nil, fmt.Errorf("%w: received %d receipts for %d parts", ErrBackendContract, len(u.receipts), count)
	}
	receipts := make([]multipart.Receipt, 0, count)
	for _, receipt := range u.receipts {
		receipts = append(receipts, receipt)
	}
	sort.Slice(receipts, func(i, j int) bool {
		return receipts[i].Number < receipts[j].Number
	})
	if err := multipart.ValidateReceipts(receipts); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBackendContract, err)
	}
	return receipts, nil
}

// closeJobsLocked requires writeMu.
func (u *MultipartUpload) closeJobsLocked() {
	if u.jobsClosed {
		return
	}
	close(u.jobs)
	u.jobsClosed = true
}

// discardAndCloseLocked requires writeMu.
func (u *MultipartUpload) discardAndCloseLocked() {
	if u.active != nil {
		u.pool.release(u.active)
		u.active = nil
		u.activeBytes = 0
	}
	u.closeJobsLocked()
}

func (u *MultipartUpload) finishAbort(backendErr error, indeterminate bool) {
	<-u.done
	cleanupErr := backendErr
	if indeterminate {
		backendErr = errors.Join(ErrCommitIndeterminate, backendErr)
	}
	u.mu.Lock()
	u.abortErr = backendErr
	u.cleanupErr = cleanupErr
	u.abortWasIndeterminate = indeterminate
	u.state = stateAborted
	done := u.abortDone
	u.mu.Unlock()
	close(done)
}

func waitAbort(ctx context.Context, done <-chan struct{}, u *MultipartUpload) error {
	select {
	case <-done:
		u.mu.Lock()
		err := u.abortErr
		u.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
