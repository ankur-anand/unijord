package ujpk

import (
	"errors"

	"github.com/ankur-anand/unijord/internal/objectsource"
	"github.com/ankur-anand/unijord/internal/record"
)

const (
	Version uint16 = 1

	PreambleSize           = 64
	PagePreambleSize       = 32
	ExtentPreambleSize     = 32
	IndexPagePreambleSize  = 32
	TimelineIndexEntrySize = 48
	IndexRootPreambleSize  = 32
	DataPageTableEntrySize = 32
	IndexRootEntrySize     = 64
	TrailerSize            = 128
	RecordHeaderSize       = 16

	MaxTimelineKeyBytes   = record.MaxTimelineKeyBytes
	MaxRawPageBytes       = 16 << 20
	MaxStoredPageBytes    = 17 << 20
	AutoIndexPageBytes    = 0
	MinAutoIndexPageBytes = 2 << 10
	MaxIndexPageBytes     = 1 << 20
	MaxIndexRootBytes     = 16 << 20
	MaxRecordValueBytes   = record.MaxRecordValueBytes
	MaxHeaderBytes        = record.MaxHeaderBytes
	MaxHeaders            = record.MaxHeaders
	MaxHeaderKeyBytes     = record.MaxHeaderKeyBytes
	MaxHeaderValueBytes   = record.MaxHeaderValueBytes
)

// RangeSource remains an alias at the UJPK boundary for existing internal
// callers. The provider-neutral contract is owned by objectsource.
type RangeSource = objectsource.RangeSource

var (
	ErrInvalidPack       = errors.New("ujpk: invalid pack")
	ErrUnsupported       = errors.New("ujpk: unsupported format")
	ErrInvalidOptions    = errors.New("ujpk: invalid options")
	ErrInvalidTimeline   = errors.New("ujpk: invalid timeline")
	ErrTimelineOrder     = errors.New("ujpk: timeline LSN order")
	ErrTimestampOrder    = errors.New("ujpk: timestamp order")
	ErrRecordTooLarge    = errors.New("ujpk: record too large")
	ErrBuilderClosed     = errors.New("ujpk: builder closed")
	ErrTimelineNotFound  = errors.New("ujpk: timeline not found")
	ErrIntegrityMismatch = errors.New("ujpk: integrity mismatch")
)

type Codec uint16

const (
	CodecNone Codec = iota
	CodecZstd
)

// Header and Record remain aliases for source compatibility while the common
// logical vocabulary lives independently of the UJPK physical format.
type Header = record.Header
type Record = record.Record

// Identity binds one pack to the namespace and shard whose committed chunks
// it materializes. NamespaceHash is the shared namespaceid digest.
type Identity struct {
	NamespaceHash [32]byte
	Shard         uint32
}

type Options struct {
	Codec        Codec
	RawPageBytes int
	// IndexPageBytes fixes the target index-page size when positive. Zero
	// selects a deterministic size at seal time from the number of extents.
	IndexPageBytes int
}

func DefaultOptions() Options {
	return Options{
		Codec:          CodecZstd,
		RawPageBytes:   64 << 10,
		IndexPageBytes: AutoIndexPageBytes,
	}
}

type ReadStats struct {
	IndexPagesRead int
	IndexBytes     int
	PagesDecoded   int
	StoredBytes    int
	RawBytes       int
}

// TimelineSpan describes one logical timeline's inclusive LSN range in a
// pack. It is pack metadata used to rebuild external derived indexes.
type TimelineSpan struct {
	TimelineKey []byte
	FirstLSN    uint64
	LastLSN     uint64
}

type timelineState struct {
	lastLSN uint64
	lastTS  int64
}

type extentBuild struct {
	key         []byte
	firstLSN    uint64
	recordCount uint32
	body        []byte
}

type indexEntry struct {
	key          []byte // builder-only; complete keys are not encoded in the index
	keyHash      timelineHash
	pageNo       uint32
	extentOffset uint32
	extentLength uint32
	firstLSN     uint64
	recordCount  uint32
}

type pageEntry struct {
	offset      uint64
	storedSize  uint32
	rawSize     uint32
	extentCount uint32
	hash        uint64
}

type timelineHash [16]byte

type indexPageRef struct {
	firstHash   timelineHash
	lastHash    timelineHash
	offset      uint64
	length      uint32
	entryCount  uint32
	recordCount uint32
	hash        uint64
}
