package blob

import (
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func testSegmentRef(partition uint32, base, last, epoch uint64) pmeta.SegmentRef {
	return testSegmentRefWithTime(partition, base, last, epoch, int64(base), int64(last))
}

func testSegmentRefWithTime(partition uint32, base, last, epoch uint64, minTS, maxTS int64) pmeta.SegmentRef {
	segment := pmeta.SegmentRef{
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      [16]byte{byte(partition), byte(base + 1), byte(last + 1), byte(epoch + 1)},
		WriterTag:        [16]byte{byte(epoch)},
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   minTS,
		MaxTimestampMS:   maxTS,
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        128,
		BlockIndexOffset: 64,
		BlockIndexLength: 64,
		Codec:            segformat.CodecNone,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      base + 100,
		TrailerHash:      last + 100,
	}
	segment.URI = keylayout.SegmentObjectKey("partitionlog", "", partition, base, epoch, segment.SegmentUUID)
	return segment
}
