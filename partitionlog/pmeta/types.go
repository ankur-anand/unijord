package pmeta

import (
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const (
	DefaultSegmentPageLimit = 128
	MaxSegmentPageLimit     = 1024
)

// PartitionHead is the bounded hot metadata for one partition. Full history is
// read through paged segment refs, not embedded here.
type PartitionHead struct {
	Partition      uint32
	NextLSN        uint64
	OldestLSN      uint64
	WriterEpoch    uint64
	SegmentCount   uint64
	LastSegment    SegmentRef
	HasLastSegment bool
}

func (h PartitionHead) Last() (SegmentRef, bool) {
	if !h.HasLastSegment {
		return SegmentRef{}, false
	}
	return h.LastSegment, true
}

// SegmentRef is the durable metadata for one committed segment object.
type SegmentRef struct {
	URI              string
	Partition        uint32
	WriterEpoch      uint64
	SegmentUUID      [16]byte
	WriterTag        [16]byte
	BaseLSN          uint64
	LastLSN          uint64
	MinTimestampMS   int64
	MaxTimestampMS   int64
	RecordCount      uint32
	BlockCount       uint32
	SizeBytes        uint64
	BlockIndexOffset uint64
	BlockIndexLength uint32
	Codec            segformat.Codec
	HashAlgo         segformat.HashAlgo
	SegmentHash      uint64
	TrailerHash      uint64
}

func (s SegmentRef) Validate() error {
	if s.URI == "" {
		return fmt.Errorf("pmeta: empty uri")
	}
	if s.WriterEpoch == 0 {
		return fmt.Errorf("pmeta: empty writer_epoch")
	}
	if s.SegmentUUID == ([16]byte{}) {
		return fmt.Errorf("pmeta: empty segment_uuid")
	}
	if s.BaseLSN > s.LastLSN {
		return fmt.Errorf("pmeta: base_lsn=%d last_lsn=%d", s.BaseLSN, s.LastLSN)
	}
	if s.LastLSN == math.MaxUint64 {
		return fmt.Errorf("pmeta: lsn exhausted: last_lsn=%d", s.LastLSN)
	}
	wantRecords := s.LastLSN - s.BaseLSN + 1
	if s.RecordCount == 0 || uint64(s.RecordCount) != wantRecords {
		return fmt.Errorf("pmeta: record_count=%d want=%d", s.RecordCount, wantRecords)
	}
	if s.BlockCount == 0 {
		return fmt.Errorf("pmeta: empty block_count")
	}
	if s.SizeBytes == 0 {
		return fmt.Errorf("pmeta: empty size_bytes")
	}
	if s.BlockIndexLength == 0 {
		return fmt.Errorf("pmeta: empty block_index_length")
	}
	if s.MinTimestampMS > s.MaxTimestampMS {
		return fmt.Errorf("pmeta: min_timestamp_ms=%d max_timestamp_ms=%d", s.MinTimestampMS, s.MaxTimestampMS)
	}
	if err := s.Codec.Validate(); err != nil {
		return fmt.Errorf("pmeta: %w", err)
	}
	if err := s.HashAlgo.Validate(); err != nil {
		return fmt.Errorf("pmeta: %w", err)
	}
	return nil
}

func (s SegmentRef) NextLSN() uint64 {
	return s.LastLSN + 1
}

type SegmentPage struct {
	Segments []SegmentRef
	NextLSN  uint64
	HasMore  bool
}

func (p SegmentPage) Clone() SegmentPage {
	p.Segments = append([]SegmentRef(nil), p.Segments...)
	return p
}
