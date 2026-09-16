package runfile

const (
	// FormatVersion is the version-1 run-container wire version.
	FormatVersion uint16 = 1

	PreambleBytes               = 128
	DirectoryHeaderBytes        = 64
	RegionDescriptorBytes       = 128
	TrailerBytes                = 160
	TimelineFilterHeaderBytes   = 64
	TimelineFilterLineBytes     = 64
	TimelineFilterLinesPerPage  = 64
	TimelineFilterPageDataBytes = 4096
	RunIDBytes                  = 16
	SHA256Bytes                 = 32

	DefaultTimelineFilterBitsPerKey        = 10
	MaxTimelineFilterLines          uint64 = 1<<32 - 1
	MinRegionCount                  uint16 = 2
	MaxRegionCount                  uint16 = 8
	MaxTimelineBytes                uint64 = 512
	MaxTableKeyBytes                uint64 = 65527
	MaxDirectoryBytes               uint64 = 2 << 20
	MaxRunObjectBytes               uint64 = 5 << 40
	RegionAlignment                 uint64 = 8
	MinPublicationIDBytes                  = 1
	MaxPublicationIDBytes                  = 128
)

const (
	PreambleMagic       = "UJRN"
	DirectoryMagic      = "UJRD"
	TimelineFilterMagic = "UJTF"
	TrailerMagic        = "UJRT"
)

// HashAlgorithm identifies a run-container content-hash algorithm.
type HashAlgorithm uint8

const (
	// HashAlgorithmSHA256 identifies SHA-256.
	HashAlgorithmSHA256 HashAlgorithm = 1
)

// CreatorRole identifies the component that produced a run.
type CreatorRole uint8

const (
	// CreatorRoleWriterFlush identifies a foreground writer flush.
	CreatorRoleWriterFlush CreatorRole = 1
	// CreatorRoleCompactionOutput identifies compaction output.
	CreatorRoleCompactionOutput CreatorRole = 2
)
