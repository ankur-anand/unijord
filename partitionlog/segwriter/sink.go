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
	// UploadPart must be safe for concurrent calls. Implementations must fully
	// consume or copy part.Bytes before returning. It must return promptly when
	// ctx is canceled or Abort interrupts the transaction.
	UploadPart(ctx context.Context, part Part) (PartReceipt, error)

	// Complete receives exactly one receipt per uploaded part, sorted by part
	// number with a contiguous range starting at 1. It must return promptly when
	// ctx is canceled.
	Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error)

	// Abort discards staged parts. It must be idempotent, safe to call while
	// UploadPart is running, and must cause in-flight uploads to return.
	Abort(ctx context.Context) error
}

type Plan struct {
	Partition uint32
	Codec     segformat.Codec
	HashAlgo  segformat.HashAlgo
	PartSize  int
}

type Part struct {
	Number int
	Bytes  []byte
}

type PartReceipt struct {
	Number int
	Token  string
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
