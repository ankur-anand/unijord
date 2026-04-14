package reader

import "context"

// Event is the internal representation returned by the reader backend.
type Event struct {
	Partition int32
	LSN       uint64
	Value     []byte
}

// ConsumeResult is the internal result for one partition read.
type ConsumeResult struct {
	Events            []Event
	NextStartAfterLSN uint64
	HighWatermarkLSN  uint64
}

// Backend is the storage-facing contract behind the reader service.
type Backend interface {
	ConsumePartition(ctx context.Context, partition int32, startAfterLSN uint64, limit uint32) (ConsumeResult, error)
	TailPartition(ctx context.Context, partition int32, startAfterLSN uint64, fromNow bool, handler func(Event) error) error
	Close() error
}
