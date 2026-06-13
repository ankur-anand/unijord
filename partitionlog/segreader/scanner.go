package segreader

import (
	"context"
)

// Scanner streams records from a single Reader. Scanner is not safe for
// concurrent use.
type Scanner struct {
	r         *Reader
	nextBlock int
	fromLSN   uint64

	records []Record
	pos     int
	closed  bool
}

func (s *Scanner) Next(ctx context.Context) (Record, bool, error) {
	if s.closed {
		return Record{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	for {
		if s.pos < len(s.records) {
			record := s.records[s.pos]
			s.pos++
			return record, true, nil
		}
		if s.nextBlock >= len(s.r.index) {
			s.closed = true
			return Record{}, false, nil
		}
		records, err := s.r.readBlock(ctx, s.nextBlock, s.fromLSN)
		if err != nil {
			return Record{}, false, err
		}
		s.nextBlock++
		s.fromLSN = 0
		s.records = records
		s.pos = 0
	}
}

func (s *Scanner) Close() error {
	s.records = nil
	s.closed = true
	return nil
}
