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

// Header is optional record metadata encoded with the record envelope. Key and
// Value alias the raw block buffer until callers clone them.
type Header struct {
	Key   []byte
	Value []byte
}

// RawRecord is the record shape encoded inside one uncompressed block.
type RawRecord struct {
	TimestampMS int64
	Headers     []Header
	Value       []byte
}

// Record is the logical record returned after raw-block decode. Headers and
// Value alias the raw block buffer until the caller copies them.
type Record struct {
	LSN         uint64
	TimestampMS int64
	Headers     []Header
	Value       []byte
}

func (r Record) Clone() Record {
	out := r
	out.Headers = CloneHeaders(r.Headers)
	if len(r.Value) > 0 {
		out.Value = append([]byte(nil), r.Value...)
	}
	return out
}

func CloneHeaders(headers []Header) []Header {
	if len(headers) == 0 {
		return nil
	}
	out := make([]Header, len(headers))
	for i := range headers {
		if len(headers[i].Key) > 0 {
			out[i].Key = append([]byte(nil), headers[i].Key...)
		}
		if len(headers[i].Value) > 0 {
			out[i].Value = append([]byte(nil), headers[i].Value...)
		}
	}
	return out
}
