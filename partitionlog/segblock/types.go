package segblock

import "github.com/ankur-anand/unijord/partitionlog/segformat"

type Meta struct {
	BaseLSN        uint64
	RecordCount    uint32
	MinTimestampMS int64
	MaxTimestampMS int64
}

type Sealed struct {
	Preamble segformat.BlockPreamble
	Stored   []byte
}
