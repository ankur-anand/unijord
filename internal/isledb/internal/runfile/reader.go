package runfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// Recover performs trailer-first recovery without listing object storage.
// It authenticates the directory, binds it to the preamble, and validates the
// repeated filter geometry. Complete region and payload hashes are the
// responsibility of Verify in complete mode.
func Recover(ctx context.Context, source RangeSource, objectKey string) (Ref, error) {
	if ctx == nil {
		return Ref{}, invalidRunf("nil context")
	}
	if source == nil {
		return Ref{}, invalidRunf("nil range source")
	}
	objectSize, err := source.Size(ctx, objectKey)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: read object size: %w", err)
	}
	if objectSize < TrailerBytes {
		return Ref{}, corruptRunf("object size %d is smaller than trailer %d", objectSize, TrailerBytes)
	}
	if uint64(objectSize) > MaxRunObjectBytes {
		return Ref{}, runTooLargef("object size %d exceeds %d", objectSize, MaxRunObjectBytes)
	}
	trailerBytes, err := readExact(ctx, source, objectKey, objectSize-TrailerBytes, TrailerBytes)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: read trailer: %w", err)
	}
	trailer, err := UnmarshalTrailerForObject(trailerBytes, uint64(objectSize))
	if err != nil {
		return Ref{}, err
	}
	directoryBytes, err := readExactU64(ctx, source, objectKey, trailer.DirectoryOffset, trailer.DirectoryLength)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: read directory: %w", err)
	}
	if digest := sha256.Sum256(directoryBytes); digest != trailer.DirectoryHash {
		return Ref{}, corruptRunf("directory SHA-256 mismatch")
	}
	directory, err := UnmarshalDirectory(directoryBytes, trailer.DirectoryOffset)
	if err != nil {
		return Ref{}, err
	}
	if uint16(len(directory.Regions)) != trailer.RegionCount {
		return Ref{}, corruptRunf("directory region count %d, trailer records %d", len(directory.Regions), trailer.RegionCount)
	}
	preambleBytes, err := readExact(ctx, source, objectKey, 0, PreambleBytes)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: read preamble: %w", err)
	}
	preamble, err := UnmarshalPreamble(preambleBytes)
	if err != nil {
		return Ref{}, err
	}
	if preamble.RunID != trailer.RunID {
		return Ref{}, corruptRunf("preamble and trailer run IDs differ")
	}

	var filterHeader *FilterHeader
	for i := range directory.Regions {
		region := directory.Regions[i]
		if region.Kind == RegionKindEventsSST || region.Kind == RegionKindHeadsSST {
			if region.SeqLo < preamble.SeqLo || region.SeqHi > preamble.SeqHi {
				return Ref{}, corruptRunf("region kind %d sequence range [%d,%d] exceeds preamble [%d,%d]", region.Kind, region.SeqLo, region.SeqHi, preamble.SeqLo, preamble.SeqHi)
			}
		}
		if region.Kind != RegionKindTimelineFilter || region.Encoding != RegionEncodingV1 {
			continue
		}
		if region.Length < TimelineFilterHeaderBytes {
			return Ref{}, corruptRunf(
				"timeline filter region length %d is smaller than header %d",
				region.Length,
				TimelineFilterHeaderBytes,
			)
		}
		headerBytes, err := readExactU64(ctx, source, objectKey, region.Offset, TimelineFilterHeaderBytes)
		if err != nil {
			return Ref{}, fmt.Errorf("runfile: read timeline filter header: %w", err)
		}
		header, err := UnmarshalFilterHeader(headerBytes)
		if err != nil {
			return Ref{}, err
		}
		filterRef := FilterRef{Header: header, Region: cloneRegion(region), ObjectSize: uint64(objectSize)}
		if err := validateFilterRef(filterRef, true); err != nil {
			return Ref{}, err
		}
		if header.RunID != preamble.RunID {
			return Ref{}, corruptRunf("timeline filter and preamble run IDs differ")
		}
		filterHeader = &header
	}
	ref := refFromParts(preamble, directory, trailer, filterHeader)
	if err := validateRef(ref, true); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// OpenTable opens either required SST through a bounded logical readable.
func OpenTable(ctx context.Context, source RangeSource, objectKey string, ref Ref, kind RegionKind) (*sstable.Reader, error) {
	return OpenTableWithReadOptions(ctx, source, objectKey, ref, kind, RegionReadOptions{})
}

// OpenTableWithReadOptions opens either required SST through a bounded logical
// readable with optional block caching and concurrent-load coalescing. The
// recovered run identity is authoritative and is always included in cache and
// coalescing keys.
func OpenTableWithReadOptions(
	ctx context.Context,
	source RangeSource,
	objectKey string,
	ref Ref,
	kind RegionKind,
	readOptions RegionReadOptions,
) (*sstable.Reader, error) {
	if ctx == nil {
		return nil, invalidRunf("nil context")
	}
	if err := validateRef(ref, true); err != nil {
		return nil, err
	}
	if !allZero(readOptions.RunID[:]) && readOptions.RunID != ref.RunID {
		return nil, invalidRunf("region read options run ID differs from recovered run")
	}
	readOptions.RunID = ref.RunID
	var region RegionDescriptor
	switch kind {
	case RegionKindEventsSST:
		region = ref.Events
	case RegionKindHeadsSST:
		region = ref.Heads
	default:
		return nil, invalidRunf("region kind %d is not an SST", kind)
	}
	if ref.ObjectSize > math.MaxInt64 {
		return nil, runTooLargef("object size %d exceeds signed range-reader limit", ref.ObjectSize)
	}
	readable, err := NewRegionReadable(source, objectKey, int64(ref.ObjectSize), region, readOptions)
	if err != nil {
		return nil, err
	}
	policy := bloom.FilterPolicy(canonicalNativeFilterBitsPerKey)
	reader, err := sstable.NewReader(ctx, readable, sstable.ReaderOptions{
		Filters: map[string]sstable.FilterPolicy{policy.Name(): policy},
	})
	if err != nil {
		_ = readable.Close()
		return nil, tableReadError(kind, "open SST", err)
	}
	format, err := reader.TableFormat()
	if err != nil {
		_ = reader.Close()
		return nil, tableReadError(kind, "read table format", err)
	}
	if format != sstable.TableFormatPebblev1 {
		_ = reader.Close()
		return nil, unsupportedRunf("region kind %d Pebble table format %s", kind, format)
	}
	if reader.BlockReader().ChecksumType() != block.ChecksumTypeCRC32c {
		_ = reader.Close()
		return nil, unsupportedRunf("region kind %d Pebble checksum %s", kind, reader.BlockReader().ChecksumType())
	}
	return reader, nil
}

func tableReadError(kind RegionKind, operation string, err error) error {
	var sourceErr *rangeSourceError
	if errors.As(err, &sourceErr) || errors.Is(err, ErrVerificationResource) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("runfile: region kind %d %s: %w", kind, operation, err)
	}
	return fmt.Errorf("%w: region kind %d %s: %w", ErrCorruptRun, kind, operation, err)
}

// OpenEvents opens the Events SST through its bounded region.
func OpenEvents(ctx context.Context, source RangeSource, objectKey string, ref Ref) (*sstable.Reader, error) {
	return OpenTable(ctx, source, objectKey, ref, RegionKindEventsSST)
}

// OpenHeads opens the Heads SST through its bounded region.
func OpenHeads(ctx context.Context, source RangeSource, objectKey string, ref Ref) (*sstable.Reader, error) {
	return OpenTable(ctx, source, objectKey, ref, RegionKindHeadsSST)
}

func readExact(ctx context.Context, source RangeSource, objectKey string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, corruptRunf("negative object range [%d,%d)", offset, offset+length)
	}
	data, err := source.ReadRange(ctx, objectKey, offset, length)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, corruptRunf("successful object range [%d,%d) returned %d bytes", offset, offset+length, len(data))
	}
	return data, nil
}

func readExactU64(ctx context.Context, source RangeSource, objectKey string, offset, length uint64) ([]byte, error) {
	if offset > math.MaxInt64 || length > math.MaxInt64 || length > uint64(math.MaxInt) {
		return nil, runTooLargef("object range offset=%d length=%d exceeds platform limits", offset, length)
	}
	if length > 0 && offset > math.MaxInt64-length {
		return nil, corruptRunf("object range overflows signed offsets")
	}
	return readExact(ctx, source, objectKey, int64(offset), int64(length))
}

func refFromParts(preamble Preamble, directory Directory, trailer Trailer, filterHeader *FilterHeader) Ref {
	ref := Ref{
		FormatVersion:   FormatVersion,
		RunID:           preamble.RunID,
		NamespaceHash:   preamble.NamespaceHash,
		Shard:           preamble.Shard,
		CreatorRole:     preamble.CreatorRole,
		CreatorEpoch:    preamble.CreatorEpoch,
		SeqLo:           preamble.SeqLo,
		SeqHi:           preamble.SeqHi,
		PublicationHash: preamble.PublicationHash,
		MinTimeline:     bytes.Clone(directory.MinTimeline),
		MaxTimeline:     bytes.Clone(directory.MaxTimeline),
		DirectoryOffset: trailer.DirectoryOffset,
		DirectoryLength: trailer.DirectoryLength,
		ObjectSize:      trailer.ObjectSize,
		DirectoryHash:   trailer.DirectoryHash,
		PayloadHash:     trailer.PayloadHash,
	}
	for i := range directory.Regions {
		region := cloneRegion(directory.Regions[i])
		switch region.Kind {
		case RegionKindEventsSST:
			ref.Events = region
		case RegionKindHeadsSST:
			ref.Heads = region
		case RegionKindTimelineFilter:
			if region.Encoding == RegionEncodingV1 && filterHeader != nil {
				ref.TimelineFilter = &FilterRef{Header: *filterHeader, Region: region, ObjectSize: trailer.ObjectSize}
			} else {
				ref.OptionalRegions = append(ref.OptionalRegions, region)
			}
		default:
			ref.OptionalRegions = append(ref.OptionalRegions, region)
		}
	}
	return ref
}
