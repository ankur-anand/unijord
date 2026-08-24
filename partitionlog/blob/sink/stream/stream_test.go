package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
)

func TestMultipartUploadPreservesByteAndReceiptOrder(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.partGates[1] = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 2,
		UploadQueueSize:   2,
	})

	if err := u.Write(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	if err := u.Write(context.Background(), []byte("defghijk")); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}

	result := make(chan commitResult, 1)
	go func() {
		attrs, err := u.Commit(context.Background())
		result <- commitResult{attrs: attrs, err: err}
	}()

	waitForPart(t, backend.partDone, 2)
	close(backend.partGates[1])
	got := receiveCommit(t, result)
	if got.err != nil {
		t.Fatalf("Commit() error = %v", got.err)
	}
	if got.attrs.SizeBytes != 11 {
		t.Fatalf("Commit() size = %d, want 11", got.attrs.SizeBytes)
	}
	if got := backend.objectBytes(); !bytes.Equal(got, []byte("abcdefghijk")) {
		t.Fatalf("committed bytes = %q, want %q", got, "abcdefghijk")
	}
	if got := backend.completedReceiptNumbers(); !equalInts(got, []int{1, 2, 3}) {
		t.Fatalf("Complete() receipt order = %v, want [1 2 3]", got)
	}
	if got := backend.uploadCompletionOrder(); len(got) < 2 || got[0] != 2 {
		t.Fatalf("part completion order = %v, want part 2 before blocked part 1", got)
	}
}

func TestMultipartUploadCopiesWriteInput(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          8,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})

	input := []byte("original")
	if err := u.Write(context.Background(), input); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	copy(input, "mutated!")
	if _, err := u.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got := backend.objectBytes(); !bytes.Equal(got, []byte("original")) {
		t.Fatalf("committed bytes = %q, want %q", got, "original")
	}
}

func TestMultipartUploadBackpressureHonorsPoolBound(t *testing.T) {
	t.Parallel()

	pool, err := NewBufferPool(4, 2)
	if err != nil {
		t.Fatalf("NewBufferPool() error = %v", err)
	}
	backend := newFakeUpload()
	backend.uploadGate = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
		BufferPool:        pool,
	})

	// Part 1 is held by the worker and part 2 is held by the queue, so the
	// pool is full before the write under test starts.
	if err := u.Write(context.Background(), []byte("abcdefgh")); err != nil {
		t.Fatalf("Write(initial) error = %v", err)
	}
	waitForPart(t, backend.partStarted, 1)

	writeCtx, acquireEntered := observeNthDone(context.Background(), 1)
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- u.Write(writeCtx, []byte("ijkl"))
	}()
	waitForSignal(t, acquireEntered, "Write to enter buffer acquisition")
	select {
	case err := <-writeResult:
		t.Fatalf("Write() returned before capacity was released: %v", err)
	default:
	}
	if got := pool.PeakInUseBytes(); got != pool.CapacityBytes() || got != 8 {
		t.Fatalf("peak bytes = %d, capacity = %d, want both 8", got, pool.CapacityBytes())
	}

	close(backend.uploadGate)
	select {
	case err := <-writeResult:
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write() remained blocked after upload capacity was released")
	}
	if _, err := u.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got := pool.InUseBytes(); got != 0 {
		t.Fatalf("in-use bytes after Commit = %d, want 0", got)
	}
}

func TestMultipartUploadCanceledBlockedWriteBecomesTerminal(t *testing.T) {
	t.Parallel()

	pool, err := NewBufferPool(4, 1)
	if err != nil {
		t.Fatalf("NewBufferPool() error = %v", err)
	}
	backend := newFakeUpload()
	backend.uploadGate = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   0,
		BufferPool:        pool,
	})

	// Leave one byte free in the only pool buffer. The cancellable write fills
	// and enqueues that buffer, then blocks acquiring its next buffer. Because
	// this all happens in one Write call, cancellation after partial acceptance
	// must make the stream terminal.
	if err := u.Write(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("Write(initial) error = %v", err)
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	// Done is evaluated once while enqueueing the completed first part and a
	// second time when acquiring the next buffer.
	ctx, acquireEntered := observeNthDone(baseCtx, 2)
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- u.Write(ctx, []byte("defgh"))
	}()
	waitForPart(t, backend.partStarted, 1)
	waitForSignal(t, acquireEntered, "Write to enter buffer acquisition")
	cancel()
	select {
	case err := <-writeResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Write() error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write() did not return after its context was canceled")
	}
	if err := u.Write(context.Background(), []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("later Write() error = %v, want original context.Canceled", err)
	}
	if err := u.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if got := pool.InUseBytes(); got != 0 {
		t.Fatalf("in-use bytes after Abort = %d, want 0", got)
	}
}

func TestMultipartUploadAbortInterruptsInFlightPart(t *testing.T) {
	t.Parallel()

	pool, err := NewBufferPool(4, 1)
	if err != nil {
		t.Fatalf("NewBufferPool() error = %v", err)
	}
	backend := newFakeUpload()
	backend.uploadGate = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   0,
		BufferPool:        pool,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	waitForPart(t, backend.partStarted, 1)

	if err := u.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if got := pool.InUseBytes(); got != 0 {
		t.Fatalf("in-use bytes after Abort = %d, want 0", got)
	}
	if backend.abortCount() != 1 {
		t.Fatalf("backend Abort calls = %d, want 1", backend.abortCount())
	}
	if err := u.Write(context.Background(), []byte("x")); !errors.Is(err, ErrAborted) {
		t.Fatalf("Write() after Abort error = %v, want ErrAborted", err)
	}
}

func TestMultipartUploadRetriesFailedCleanup(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.cleanupErrOnce = context.DeadlineExceeded
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := u.Abort(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Abort() error = %v, want context.DeadlineExceeded", err)
	}
	if err := u.Abort(context.Background()); err != nil {
		t.Fatalf("retried Abort() error = %v", err)
	}
	if backend.abortCount() != 2 {
		t.Fatalf("backend Cleanup calls = %d, want 2", backend.abortCount())
	}
}

func TestMultipartUploadCommitWaitUsesCallerContext(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.uploadGate = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	waitForPart(t, backend.partStarted, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := u.Commit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit() error = %v, want context.Canceled", err)
	}
	close(backend.uploadGate)
	if _, err := u.Commit(context.Background()); err != nil {
		t.Fatalf("retried Commit() error = %v", err)
	}
}

func TestMultipartUploadCommitPreservesRecordedPartFailureWhenCallerCancels(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("provider rejected part")
	backend := newFakeUpload()
	backend.uploadGate = make(chan struct{})
	backend.putPartErr = providerErr
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan commitResult, 1)
	go func() {
		attrs, err := u.Commit(ctx)
		result <- commitResult{attrs: attrs, err: err}
	}()
	waitForPart(t, backend.partStarted, 1)

	// Keep the failed worker from finishing after recordFailure. That leaves
	// Commit deterministically waiting on u.done when its caller cancels.
	u.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			u.writeMu.Unlock()
		}
	}()
	close(backend.uploadGate)
	waitForSignal(t, u.runCtx.Done(), "part failure to be recorded")
	cancel()

	got := receiveCommit(t, result)
	if !errors.Is(got.err, providerErr) {
		t.Fatalf("Commit() error = %v, want provider error", got.err)
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("Commit() error = %v, want context.Canceled", got.err)
	}
	u.writeMu.Unlock()
	locked = false
}

func TestMultipartUploadAbortRefusesDuringBackendCommit(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.completeGate = make(chan struct{})
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	result := make(chan commitResult, 1)
	go func() {
		attrs, err := u.Commit(context.Background())
		result <- commitResult{attrs: attrs, err: err}
	}()
	select {
	case <-backend.completeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Complete() did not start")
	}
	if err := u.Abort(context.Background()); !errors.Is(err, ErrCommitInProgress) {
		t.Fatalf("Abort() error = %v, want ErrCommitInProgress", err)
	}
	if backend.abortCount() != 0 {
		t.Fatal("backend Abort was called while Complete was in progress")
	}

	close(backend.completeGate)
	got := receiveCommit(t, result)
	if got.err != nil {
		t.Fatalf("Commit() error = %v", got.err)
	}
	attrs, err := u.Commit(context.Background())
	if err != nil {
		t.Fatalf("repeated Commit() error = %v", err)
	}
	if attrs != got.attrs {
		t.Fatalf("repeated Commit() attrs = %+v, want %+v", attrs, got.attrs)
	}
	if backend.completeCount() != 1 {
		t.Fatalf("backend Complete calls = %d, want 1", backend.completeCount())
	}
}

func TestMultipartUploadRejectsMismatchedReceipt(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.receiptNumberOffset = 1
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := u.Commit(context.Background()); !errors.Is(err, ErrBackendContract) {
		t.Fatalf("Commit() error = %v, want ErrBackendContract", err)
	}
	if err := u.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
}

func TestMultipartUploadMarksCompleteFailureIndeterminate(t *testing.T) {
	t.Parallel()

	completeErr := errors.New("response lost")
	backend := newFakeUpload()
	backend.completeErr = completeErr
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := u.Commit(context.Background()); !errors.Is(err, ErrCommitIndeterminate) || !errors.Is(err, completeErr) {
		t.Fatalf("Commit() error = %v, want ErrCommitIndeterminate joined with backend error", err)
	}
	if err := u.Abort(context.Background()); !errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("Abort() error = %v, want ErrCommitIndeterminate", err)
	}
	if backend.abortCount() != 1 {
		t.Fatalf("backend Abort calls = %d, want 1", backend.abortCount())
	}
}

func TestMultipartUploadRetriesIndeterminateCommit(t *testing.T) {
	t.Parallel()

	responseLost := errors.New("response lost")
	backend := newFakeUpload()
	backend.completeErrOnce = responseLost
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := u.Commit(context.Background()); !errors.Is(err, ErrCommitIndeterminate) || !errors.Is(err, responseLost) {
		t.Fatalf("first Commit() error = %v, want indeterminate response loss", err)
	}
	attrs, err := u.Commit(context.Background())
	if err != nil {
		t.Fatalf("retried Commit() error = %v", err)
	}
	if attrs.SizeBytes != 4 || backend.completeCount() != 2 {
		t.Fatalf("retried Commit() = (%+v, calls=%d), want size=4 and two calls", attrs, backend.completeCount())
	}
}

func TestMultipartUploadPreservesDefiniteCommitFailure(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.completeErr = multipart.ErrPreconditionFailed
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	_, err := u.Commit(context.Background())
	if !errors.Is(err, multipart.ErrPreconditionFailed) {
		t.Fatalf("Commit() error = %v, want ErrPreconditionFailed", err)
	}
	if errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("Commit() error = %v, definite conflict must not be indeterminate", err)
	}
}

func TestMultipartUploadValidatesOptions(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	for name, opts := range map[string]MultipartOptions{
		"part size":      {UploadParallelism: 1},
		"parallelism":    {PartSize: 4},
		"negative queue": {PartSize: 4, UploadParallelism: 1, UploadQueueSize: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewMultipartUpload(context.Background(), backend, opts); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("NewMultipartUpload() error = %v, want ErrInvalidOptions", err)
			}
		})
	}
	pool, err := NewBufferPool(8, 1)
	if err != nil {
		t.Fatalf("NewBufferPool() error = %v", err)
	}
	_, err = NewMultipartUpload(context.Background(), backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		BufferPool:        pool,
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("pool mismatch error = %v, want ErrInvalidOptions", err)
	}

	limited := newFakeUpload()
	limited.limits.MinPartSize = 5
	limited.limits.MaxPartSize = 8
	_, err = NewMultipartUpload(context.Background(), limited, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("part size below provider minimum error = %v, want ErrInvalidOptions", err)
	}
}

func TestBeginMultipartUploadValidatesBeforeOpeningProviderSession(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.limits.MaxPartSize = 4
	store := &fakeStore{limits: backend.limits, session: backend}
	_, err := BeginMultipartUpload(context.Background(), store, "segments/test", multipart.Options{}, MultipartOptions{
		PartSize:          8,
		UploadParallelism: 1,
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("BeginMultipartUpload() error = %v, want ErrInvalidOptions", err)
	}
	if store.beginCount != 0 {
		t.Fatalf("provider Begin calls = %d, want 0", store.beginCount)
	}
}

func TestBeginMultipartUploadCleansMismatchedProviderSession(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	cleanupErr := errors.New("cleanup failed")
	backend.cleanupErrOnce = cleanupErr
	storeLimits := backend.limits
	storeLimits.MaxObjectSize++
	store := &fakeStore{limits: storeLimits, session: backend}
	_, err := BeginMultipartUpload(context.Background(), store, "segments/test", multipart.Options{}, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
	})
	if !errors.Is(err, ErrBackendContract) {
		t.Fatalf("BeginMultipartUpload() error = %v, want ErrBackendContract", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("BeginMultipartUpload() error = %v, want cleanup error", err)
	}
	if store.beginCount != 1 || backend.abortCount() != 1 {
		t.Fatalf("provider calls = (begin:%d cleanup:%d), want (1, 1)", store.beginCount, backend.abortCount())
	}
}

func TestMultipartUploadRejectsProviderLimitBeforeAcceptingBytes(t *testing.T) {
	t.Parallel()

	backend := newFakeUpload()
	backend.limits = multipart.Limits{MaxPartSize: 4, MaxPartCount: 2, MaxObjectSize: 8}
	u := newTestUpload(t, backend, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
	})
	if err := u.Write(context.Background(), []byte("too-large")); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized Write() error = %v, want ErrLimitExceeded", err)
	}
	if err := u.Write(context.Background(), []byte("12345678")); err != nil {
		t.Fatalf("Write(after rejected input) error = %v", err)
	}
	attrs, err := u.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if attrs.SizeBytes != 8 {
		t.Fatalf("Commit() size = %d, want 8", attrs.SizeBytes)
	}
}

func TestMultipartUploadUsesSharedUploadLimiter(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	backendA := newFakeUpload()
	backendA.uploadGate = gate
	backendB := newFakeUpload()
	backendB.uploadGate = gate
	limiter := newCountingLimiter(1)
	uploadA := newTestUpload(t, backendA, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
		UploadLimiter:     limiter,
	})
	uploadB := newTestUpload(t, backendB, MultipartOptions{
		PartSize:          4,
		UploadParallelism: 1,
		UploadQueueSize:   1,
		UploadLimiter:     limiter,
	})
	if err := uploadA.Write(context.Background(), []byte("abcd")); err != nil {
		t.Fatalf("upload A Write() error = %v", err)
	}
	if err := uploadB.Write(context.Background(), []byte("efgh")); err != nil {
		t.Fatalf("upload B Write() error = %v", err)
	}

	// One provider call holds the only limiter slot. The contention signal
	// proves the other upload reached Acquire while that slot was still held.
	waitForEitherPart(t, backendA.partStarted, backendB.partStarted)
	waitForSignal(t, limiter.contended, "second upload to contend for the shared limiter")
	close(gate)

	if _, err := uploadA.Commit(context.Background()); err != nil {
		t.Fatalf("upload A Commit() error = %v", err)
	}
	if _, err := uploadB.Commit(context.Background()); err != nil {
		t.Fatalf("upload B Commit() error = %v", err)
	}
	acquires, releases, maxActive := limiter.snapshot()
	if acquires != 2 || releases != 2 || maxActive != 1 {
		t.Fatalf("limiter = (acquires=%d releases=%d max_active=%d), want (2, 2, 1)", acquires, releases, maxActive)
	}
}

type commitResult struct {
	attrs multipart.ObjectAttrs
	err   error
}

type fakeUpload struct {
	mu sync.Mutex

	parts               map[int][]byte
	partGates           map[int]chan struct{}
	uploadGate          chan struct{}
	partStarted         chan int
	partDone            chan int
	completionOrder     []int
	completedReceipts   []int
	object              []byte
	receiptNumberOffset int
	putPartErr          error
	limits              multipart.Limits

	completeStarted chan struct{}
	completeStart   sync.Once
	completeGate    chan struct{}
	completeErr     error
	completeErrOnce error
	completeCalls   int
	abortCalls      int
	cleanupErrOnce  error
}

type fakeStore struct {
	limits     multipart.Limits
	session    multipart.Session
	beginErr   error
	beginCount int
}

func (s *fakeStore) Limits() multipart.Limits { return s.limits }

func (s *fakeStore) Begin(context.Context, string, multipart.Options) (multipart.Session, error) {
	s.beginCount++
	return s.session, s.beginErr
}

func newFakeUpload() *fakeUpload {
	return &fakeUpload{
		parts:           make(map[int][]byte),
		partGates:       make(map[int]chan struct{}),
		partStarted:     make(chan int, 32),
		partDone:        make(chan int, 32),
		completeStarted: make(chan struct{}),
		limits: multipart.Limits{
			MaxPartSize:   1 << 20,
			MaxPartCount:  1_000,
			MaxObjectSize: 1 << 30,
		},
	}
}

func (u *fakeUpload) Limits() multipart.Limits { return u.limits }

func (u *fakeUpload) PutPart(ctx context.Context, part multipart.Part) (multipart.Receipt, error) {
	u.partStarted <- part.Number
	u.mu.Lock()
	gate := u.partGates[part.Number]
	allGate := u.uploadGate
	u.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return multipart.Receipt{}, ctx.Err()
		}
	}
	if allGate != nil {
		select {
		case <-allGate:
		case <-ctx.Done():
			return multipart.Receipt{}, ctx.Err()
		}
	}

	copyOfPart := append([]byte(nil), part.Bytes...)
	u.mu.Lock()
	u.parts[part.Number] = copyOfPart
	u.completionOrder = append(u.completionOrder, part.Number)
	offset := u.receiptNumberOffset
	err := u.putPartErr
	u.mu.Unlock()
	u.partDone <- part.Number
	if err != nil {
		return multipart.Receipt{}, err
	}
	return multipart.Receipt{
		Number:         part.Number + offset,
		Token:          fmt.Sprintf("part-%d", part.Number),
		SizeBytes:      uint64(len(part.Bytes)),
		ChecksumSHA256: part.ChecksumSHA256,
	}, nil
}

func (u *fakeUpload) Commit(ctx context.Context, request multipart.CommitRequest) (multipart.ObjectAttrs, error) {
	u.completeStart.Do(func() { close(u.completeStarted) })
	u.mu.Lock()
	u.completeCalls++
	gate := u.completeGate
	err := u.completeErr
	if u.completeErrOnce != nil {
		err = u.completeErrOnce
		u.completeErrOnce = nil
	}
	u.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return multipart.ObjectAttrs{}, ctx.Err()
		}
	}
	if err != nil {
		return multipart.ObjectAttrs{}, err
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	u.object = nil
	u.completedReceipts = nil
	for _, receipt := range request.Receipts {
		u.completedReceipts = append(u.completedReceipts, receipt.Number)
		u.object = append(u.object, u.parts[receipt.Number]...)
	}
	return multipart.ObjectAttrs{Key: "segments/test", SizeBytes: uint64(len(u.object)), Token: "object-1"}, nil
}

func (u *fakeUpload) Cleanup(context.Context) error {
	u.mu.Lock()
	err := u.cleanupErrOnce
	u.cleanupErrOnce = nil
	u.abortCalls++
	u.mu.Unlock()
	return err
}

func (u *fakeUpload) objectBytes() []byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]byte(nil), u.object...)
}

func (u *fakeUpload) completedReceiptNumbers() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int(nil), u.completedReceipts...)
}

func (u *fakeUpload) uploadCompletionOrder() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int(nil), u.completionOrder...)
}

func (u *fakeUpload) completeCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.completeCalls
}

func (u *fakeUpload) abortCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.abortCalls
}

func newTestUpload(t *testing.T, backend multipart.Session, opts MultipartOptions) *MultipartUpload {
	t.Helper()
	u, err := NewMultipartUpload(context.Background(), backend, opts)
	if err != nil {
		t.Fatalf("NewMultipartUpload() error = %v", err)
	}
	t.Cleanup(func() {
		_ = u.Abort(context.Background())
	})
	return u
}

func waitForPart(t *testing.T, parts <-chan int, want int) {
	t.Helper()
	for {
		select {
		case got := <-parts:
			if got == want {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("part %d did not reach expected state", want)
		}
	}
}

func waitForEitherPart(t *testing.T, a, b <-chan int) {
	t.Helper()
	select {
	case <-a:
	case <-b:
	case <-time.After(5 * time.Second):
		t.Fatal("neither part reached the provider")
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type doneObservedContext struct {
	context.Context
	remaining int
	once      sync.Once
	entered   chan struct{}
}

func observeNthDone(ctx context.Context, call int) (context.Context, <-chan struct{}) {
	observed := &doneObservedContext{
		Context:   ctx,
		remaining: call,
		entered:   make(chan struct{}),
	}
	return observed, observed.entered
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.remaining--
	if c.remaining == 0 {
		c.once.Do(func() { close(c.entered) })
	}
	return c.Context.Done()
}

func receiveCommit(t *testing.T, result <-chan commitResult) commitResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("Commit() did not return")
		return commitResult{}
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type countingLimiter struct {
	semaphore chan struct{}
	contended chan struct{}
	mu        sync.Mutex
	acquires  int
	releases  int
	active    int
	maxActive int
}

func newCountingLimiter(limit int) *countingLimiter {
	return &countingLimiter{
		semaphore: make(chan struct{}, limit),
		contended: make(chan struct{}, 1),
	}
}

func (l *countingLimiter) Acquire(ctx context.Context) error {
	select {
	case l.semaphore <- struct{}{}:
		l.recordAcquire()
		return nil
	default:
	}

	select {
	case l.contended <- struct{}{}:
	default:
	}
	select {
	case l.semaphore <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	l.recordAcquire()
	return nil
}

func (l *countingLimiter) recordAcquire() {
	l.mu.Lock()
	l.acquires++
	l.active++
	if l.active > l.maxActive {
		l.maxActive = l.active
	}
	l.mu.Unlock()
}

func (l *countingLimiter) Release() {
	l.mu.Lock()
	l.releases++
	l.active--
	l.mu.Unlock()
	<-l.semaphore
}

func (l *countingLimiter) snapshot() (acquires, releases, maxActive int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquires, l.releases, l.maxActive
}
