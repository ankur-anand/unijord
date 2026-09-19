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

// TimelineID identifies one distinct exact timeline in a stable catalog.
// IDs are dense: [0, Len()). Their order need not be timeline byte order.
type TimelineID uint32

// TimelineCatalog borrows caller-owned exact timeline bytes. Len, the ID
// mapping, and all returned bytes must remain immutable until Prepare returns,
// including while it closes entry iterators. Each ID has one distinct nonempty
// value. Runfile validates exact identities and entry membership without taking
// ownership or interning. The caller must enforce the immutable lifetime;
// unsynchronized mutation is a data race, not a supported validation input.
// Implementations need only handle IDs in [0, Len()).
type TimelineCatalog interface {
	Len() int
	Timeline(TimelineID) []byte
}

// Entry is one point entry supplied to an embedded SST builder. Timeline is
// the exact canonical timeline identity classified by the logical layer. The
// builder never attempts to infer a timeline from Key.
type Entry struct {
	TimelineID TimelineID
	Key        []byte
	Value      []byte
	Timeline   []byte
	Seq        uint64
}

// EntryIterator supplies entries in Pebble internal-key order: user keys in
// ascending byte order and sequences for equal user keys in descending order.
// Key and Value remain owned by the iterator and need only remain valid until
// the next call to Next. Timeline must equal the stable catalog value for
// TimelineID; it may reference the catalog directly. Iterators must not mutate
// catalog storage, including in Close. Prepare never retains Entry slices.
type EntryIterator interface {
	Next() bool
	Entry() Entry
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
