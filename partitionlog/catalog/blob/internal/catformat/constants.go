package catformat

import "errors"

const (
	Version uint16 = 1

	HeadHeaderSize    = 192
	PageHeaderSize    = 128
	SectionHeaderSize = 16
	TrailerSize       = 32
	LeafEntrySize     = 128
	IndexEntrySize    = 80

	MaxLeafEntries  = 1024
	MaxIndexEntries = 1024
	MaxIndexLevel   = 63
	// MaxOpenIndexLevel is one greater than the highest immutable index page.
	// The terminal open section can reference level-63 pages but cannot seal.
	MaxOpenIndexLevel = MaxIndexLevel + 1

	SegmentLayoutV1 uint16 = 1

	FlagHasLastSegment uint32 = 1 << 0
)

const (
	MaxRecordLSN uint64 = ^uint64(0) - 1
	ReservedLSN  uint64 = ^uint64(0)
)

var (
	ErrInvalidObject      = errors.New("catformat: invalid object")
	ErrUnsupportedVersion = errors.New("catformat: unsupported version")
	ErrUnsupportedLayout  = errors.New("catformat: unsupported segment layout")
	ErrIntegrityMismatch  = errors.New("catformat: integrity mismatch")
)

var (
	headMagic    = [4]byte{'P', 'L', 'C', 'H'}
	leafMagic    = [4]byte{'P', 'L', 'C', 'L'}
	indexMagic   = [4]byte{'P', 'L', 'C', 'X'}
	sectionMagic = [4]byte{'P', 'L', 'C', 'S'}
	trailerMagic = [4]byte{'P', 'L', 'C', 'T'}
)
