package segformat

// FilePreamble is the fixed 64-byte region at offset 0. It only carries
// fields known before block bytes are emitted.
type FilePreamble struct {
	Flags        uint32
	Partition    uint32
	Codec        Codec
	HashAlgo     HashAlgo
	RecordFormat RecordFormat
	BaseLSN      uint64
	SegmentUUID  [16]byte
	WriterTag    [16]byte
}

// BlockPreamble is the fixed 64-byte prefix before every stored block payload.
type BlockPreamble struct {
	Flags          uint16
	StoredSize     uint32
	RawSize        uint32
	RecordCount    uint32
	BaseLSN        uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	BlockHash      uint64
}

// IndexPreamble is the fixed 64-byte prefix before block index entries.
type IndexPreamble struct {
	Flags      uint16
	EntrySize  uint32
	EntryCount uint32
	IndexHash  uint64
}

// BlockIndexEntry duplicates the block preamble fields needed for random
// access and for validating fetched block regions.
type BlockIndexEntry struct {
	BlockOffset    uint64
	StoredSize     uint32
	RawSize        uint32
	RecordCount    uint32
	BaseLSN        uint64
	MinTimestampMS int64
	MaxTimestampMS int64
	BlockHash      uint64
}

// Trailer is the final 192 bytes of the segment object.
type Trailer struct {
	Flags            uint32
	Partition        uint32
	Codec            Codec
	HashAlgo         HashAlgo
	RecordFormat     RecordFormat
	BaseLSN          uint64
	LastLSN          uint64
	MinTimestampMS   int64
	MaxTimestampMS   int64
	RecordCount      uint32
	BlockCount       uint32
	BlockIndexOffset uint64
	BlockIndexLength uint32
	TotalSize        uint64
	CreatedUnixMS    int64
	SegmentUUID      [16]byte
	WriterTag        [16]byte
	SegmentHash      uint64
	TrailerHash      uint64
}

// RawRecord is the record shape encoded inside one uncompressed block.
type RawRecord struct {
	TimestampMS int64
	Value       []byte
}

// Record is the logical record returned after raw-block decode. Value aliases
// the raw block buffer until the caller copies it.
type Record struct {
	LSN         uint64
	TimestampMS int64
	Value       []byte
}

func (r Record) Clone() Record {
	out := r
	if len(r.Value) > 0 {
		out.Value = append([]byte(nil), r.Value...)
	}
	return out
}
