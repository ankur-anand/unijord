package writer

import "context"

// Record is the internal append result returned by the writer backend.
type Record struct {
	LSN   uint64
	Value []byte
}

// Appender is the storage-facing contract behind the writer gRPC service.
type Appender interface {
	Append(ctx context.Context, value []byte) (Record, error)
	Close() error
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
