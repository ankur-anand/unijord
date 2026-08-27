package catformat

import "github.com/ankur-anand/unijord/partitionlog/segformat"

// LeafEntry is the fixed-width catalog representation of one SegmentRef.
// URI, StreamID, and Partition are supplied by the catalog reader.
type LeafEntry struct {
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
	WriterEpoch      uint64
	SegmentUUID      [16]byte
	WriterTag        [16]byte
}

// IndexEntry identifies one sealed child page. SegmentCount is the number of
// leaf entries in the entire child subtree, not the number of direct entries.
type IndexEntry struct {
	Level          uint8
	EntryCount     uint32
	SeqLo          uint64
	SeqHi          uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	Generation     uint64
	PageID         [16]byte
	SegmentCount   uint64
}

type PageHeader struct {
	Flags          uint32
	Partition      uint32
	Level          uint8
	HashAlgo       segformat.HashAlgo
	EntryCount     uint32
	EntrySize      uint32
	SeqLo          uint64
	SeqHi          uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	Generation     uint64
	WriterEpoch    uint64
	StreamKey      [32]byte
	SegmentCount   uint64
}

type LeafPage struct {
	Header  PageHeader
	Entries []LeafEntry
}

type IndexPage struct {
	Header  PageHeader
	Entries []IndexEntry
}

type HeadHeader struct {
	Flags                   uint32
	Partition               uint32
	HashAlgo                segformat.HashAlgo
	SegmentLayoutVersion    uint16
	LeafLimit               uint32
	IndexLimit              uint32
	ActiveCount             uint32
	LevelCount              uint32
	NextLSN                 uint64
	OldestLSN               uint64
	AppliedRetentionLSN     uint64
	AppliedRetentionVersion uint64
	WriterEpoch             uint64
	Generation              uint64
	SegmentCount            uint64
	ReachableSegmentCount   uint64
	WriterID                [16]byte
	StreamKey               [32]byte
	DataRootKey             [32]byte
}

type OpenIndexSection struct {
	Level   uint8
	Entries []IndexEntry
}

type Head struct {
	Header      HeadHeader
	LastSegment LeafEntry
	Active      []LeafEntry
	Sections    []OpenIndexSection
}

func (h Head) HasLastSegment() bool {
	return h.Header.Flags&FlagHasLastSegment != 0
}

type Trailer struct {
	BodyHash    uint64
	TrailerHash uint64
}
