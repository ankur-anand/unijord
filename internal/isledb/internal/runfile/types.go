package runfile

import "context"

// Preamble is the variable portion of a version-1 UJRN preamble. Fixed wire
// fields are supplied and checked by its codec.
type Preamble struct {
	CreatorRole     CreatorRole
	Shard           uint32
	CreatorEpoch    uint64
	SeqLo           uint64
	SeqHi           uint64
	NamespaceHash   [SHA256Bytes]byte
	RunID           [RunIDBytes]byte
	PublicationHash [SHA256Bytes]byte
}

// Trailer is the variable portion of a version-1 UJRT trailer. Its CRC is
// derived during encoding and checked during decoding.
type Trailer struct {
	DirectoryOffset uint64
	DirectoryLength uint64
	ObjectSize      uint64
	RegionCount     uint16
	DirectoryHash   [SHA256Bytes]byte
	PayloadHash     [SHA256Bytes]byte
	RunID           [RunIDBytes]byte
}

// Entry is one point entry supplied to an embedded SST builder. Timeline is
// the exact canonical timeline identity classified by the logical layer. The
// builder never attempts to infer a timeline from Key.
type Entry struct {
	Key      []byte
	Value    []byte
	Timeline []byte
	Seq      uint64
}

// EntryIterator supplies entries in Pebble internal-key order: user keys in
// ascending byte order and sequences for equal user keys in descending order.
// Key, Value, and Timeline remain owned by the iterator and need only remain
// valid until the next call to Next.
type EntryIterator interface {
	Next() bool
	Entry() Entry
	Err() error
}

// TimelineIterator supplies the exact timeline insertion set for the run-level
// filter. Duplicates are harmless and are removed by the builder.
type TimelineIterator interface {
	Next() bool
	Timeline() []byte
	Err() error
}

// RangeSource is the minimal object-store surface needed by recovery, bounded
// embedded-region reads, and verification. A successful ReadRange must return
// exactly length bytes; runfile treats a short successful result as corrupt
// persisted data.
type RangeSource interface {
	Size(ctx context.Context, objectKey string) (int64, error)
	ReadRange(ctx context.Context, objectKey string, offset, length int64) ([]byte, error)
}

// Ref is the owned run-local projection that a manifest converts into its
// durable RunMeta. It intentionally contains no object key or manifest level.
type Ref struct {
	FormatVersion uint16

	RunID           [RunIDBytes]byte
	NamespaceHash   [SHA256Bytes]byte
	Shard           uint32
	CreatorRole     CreatorRole
	CreatorEpoch    uint64
	SeqLo           uint64
	SeqHi           uint64
	PublicationHash [SHA256Bytes]byte

	MinTimeline []byte
	MaxTimeline []byte

	Events         RegionDescriptor
	Heads          RegionDescriptor
	TimelineFilter *FilterRef
	// OptionalRegions retains forward-compatible optional descriptors that this
	// version does not interpret.
	OptionalRegions []RegionDescriptor

	DirectoryOffset uint64
	DirectoryLength uint64
	ObjectSize      uint64
	DirectoryHash   [SHA256Bytes]byte
	PayloadHash     [SHA256Bytes]byte
}

// Validate checks a Ref supplied by a caller or manifest.
func (r Ref) Validate() error {
	return validateRef(r, false)
}

// Filter returns a detached filter reference suitable for a page lookup.
func (r Ref) Filter() (FilterRef, bool) {
	if r.TimelineFilter == nil {
		return FilterRef{}, false
	}
	filter := cloneFilterRef(*r.TimelineFilter)
	return filter, true
}

func (r Ref) preamble() Preamble {
	return Preamble{
		CreatorRole:     r.CreatorRole,
		Shard:           r.Shard,
		CreatorEpoch:    r.CreatorEpoch,
		SeqLo:           r.SeqLo,
		SeqHi:           r.SeqHi,
		NamespaceHash:   r.NamespaceHash,
		RunID:           r.RunID,
		PublicationHash: r.PublicationHash,
	}
}

func allZero(data []byte) bool {
	var combined byte
	for _, value := range data {
		combined |= value
	}
	return combined == 0
}
