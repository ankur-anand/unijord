package reader

import "context"

// Event is the internal representation returned by the reader backend.
type Event struct {
	Partition   int32
	LSN         uint64
	TimestampMS uint64
	Value       []byte
}

// PartitionHeadResult is the current visible head for one partition.
type PartitionHeadResult struct {
	Partition          int32
	HeadLSN            uint64
	OldestAvailableLSN uint64
}

// ConsumeResult is the internal result for one partition read.
type ConsumeResult struct {
	Events             []Event
	NextStartAfterLSN  uint64
	HeadLSN            uint64
	OldestAvailableLSN uint64
}

// Backend is the storage-facing contract behind the reader service.
type Backend interface {
	ListPartitionHeads(ctx context.Context) ([]PartitionHeadResult, error)
	GetPartitionHead(ctx context.Context, partition int32) (PartitionHeadResult, error)
	ConsumePartition(ctx context.Context, partition int32, startAfterLSN uint64, limit uint32) (ConsumeResult, error)
	ConsumePartitionFromTimestamp(ctx context.Context, partition int32, timestampMS uint64, limit uint32) (ConsumeResult, error)
	TailPartition(ctx context.Context, partition int32, startAfterLSN *uint64, handler func(Event) error) error
	Close() error
}
