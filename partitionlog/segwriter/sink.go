package segwriter

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type Sink interface {
	// Begin must return promptly when ctx is canceled.
	Begin(ctx context.Context, plan Plan) (Txn, error)
}

type Txn interface {
	// Write appends one ordered byte slice. Calls are serialized by segwriter.
	// Implementations must consume or copy all bytes before returning and return
	// promptly when ctx is canceled or Abort interrupts the transaction.
	Write(ctx context.Context, bytes []byte) error

	// Commit publishes the complete byte stream. A successful result must have a
	// non-empty URI and report the exact committed byte size. When the sink cannot
	// establish whether the object became durable, it returns an error matching
	// ErrTxnCommitIndeterminate. Callers must not treat that outcome as a definite
	// failure.
	Commit(ctx context.Context) (CommittedObject, error)

	// Abort stops the transaction and cleans staging work. It must be idempotent.
	Abort(ctx context.Context) error
}

type Plan struct {
	Partition uint32
	Codec     segformat.Codec
	HashAlgo  segformat.HashAlgo

	// Multipart transport hints. Ordered byte sinks that do not use multipart
	// may ignore them.
	PartSize int

	UploadParallelism int
	UploadQueueSize   int
	UploadLimiter     UploadLimiter
}

type CommittedObject struct {
	URI       string
	SizeBytes uint64
	Token     string
}

type UploadLimiter interface {
	Acquire(ctx context.Context) error
	Release()
}

type SemaphoreUploadLimiter struct {
	ch chan struct{}
}

func NewSemaphoreUploadLimiter(n int) (*SemaphoreUploadLimiter, error) {
	if n <= 0 {
		return nil, fmt.Errorf("%w: upload limiter size must be positive", ErrInvalidOptions)
	}
	return &SemaphoreUploadLimiter{ch: make(chan struct{}, n)}, nil
}

func (l *SemaphoreUploadLimiter) Acquire(ctx context.Context) error {
	select {
	case l.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *SemaphoreUploadLimiter) Release() {
	<-l.ch
}
