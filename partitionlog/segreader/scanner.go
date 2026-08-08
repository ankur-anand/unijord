package segreader

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

// Scanner streams records from a single Reader. Scanner is not safe for
// concurrent use.
type Scanner struct {
	r         *Reader
	nextBlock int
	fromLSN   uint64

	block    segformat.RawBlockScanner
	hasBlock bool
	closed   bool
}

func (s *Scanner) Next(ctx context.Context) (Record, bool, error) {
	if s.closed {
		return Record{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	for {
		if s.hasBlock {
			rawRecord, ok, err := s.block.Next()
			if err != nil {
				return Record{}, false, fmt.Errorf("%w: decode raw block: %w", ErrCorruptData, err)
			}
			if ok {
				if rawRecord.LSN < s.fromLSN {
					continue
				}
				return Record{
					Partition:   s.r.trailer.Partition,
					LSN:         rawRecord.LSN,
					TimestampMS: rawRecord.TimestampMS,
					Headers:     segformat.CloneHeaders(rawRecord.Headers),
					Value:       append([]byte(nil), rawRecord.Value...),
				}, true, nil
			}
			s.block = segformat.RawBlockScanner{}
			s.hasBlock = false
			s.fromLSN = 0
		}
		if s.nextBlock >= len(s.r.index) {
			s.closed = true
			return Record{}, false, nil
		}
		block, err := s.r.openBlockScanner(ctx, s.nextBlock)
		if err != nil {
			return Record{}, false, err
		}
		s.nextBlock++
		s.block = block
		s.hasBlock = true
	}
}

func (s *Scanner) Close() error {
	s.block = segformat.RawBlockScanner{}
	s.hasBlock = false
	s.closed = true
	return nil
}
