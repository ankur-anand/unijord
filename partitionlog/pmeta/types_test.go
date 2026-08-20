package pmeta

import (
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestSegmentRefLSNBoundary(t *testing.T) {
	t.Run("max record lsn", func(t *testing.T) {
		segment := validBoundarySegment(segformat.MaxRecordLSN, segformat.MaxRecordLSN, 1)
		if err := segment.Validate(); err != nil {
			t.Fatalf("SegmentRef.Validate() error = %v", err)
		}
		if got := segment.NextLSN(); got != segformat.ReservedLSN {
			t.Fatalf("NextLSN() = %d, want %d", got, segformat.ReservedLSN)
		}
	})

	t.Run("reserved lsn", func(t *testing.T) {
		segment := validBoundarySegment(segformat.MaxRecordLSN, segformat.ReservedLSN, 2)
		if err := segment.Validate(); err == nil {
			t.Fatal("SegmentRef.Validate() error = nil, want reserved LSN rejection")
		}
	})
}

func validBoundarySegment(baseLSN, lastLSN uint64, recordCount uint32) SegmentRef {
	return SegmentRef{
		URI:              "memory://lsn-boundary",
		WriterEpoch:      1,
		SegmentUUID:      [16]byte{1},
		BaseLSN:          baseLSN,
		LastLSN:          lastLSN,
		MinTimestampMS:   100,
		MaxTimestampMS:   100,
		RecordCount:      recordCount,
		BlockCount:       1,
		SizeBytes:        1,
		BlockIndexLength: 1,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
	}
}
