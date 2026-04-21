package segwriter

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type Sink interface {
	Begin(ctx context.Context, plan Plan) (Txn, error)
}

type Txn interface {
	UploadPart(ctx context.Context, part Part) (PartReceipt, error)
	Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error)
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
