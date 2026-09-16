package runfile

import (
	"bytes"
	"context"
	"slices"
)

// VerifyMode selects structural recovery or complete byte and SST validation.
type VerifyMode uint8

const (
	VerifyStructural VerifyMode = iota + 1
	VerifyComplete
)

// TimelineExtractor is supplied by the logical layer for complete semantic
// verification. In addition to returning the exact timeline identity, it must
// reject a key/value/sequence that does not belong to the supplied run's
// namespace, shard, table kind, or logical mutation contract. All byte slices
// are borrowed only for the duration of the call.
type TimelineExtractor func(run Ref, kind RegionKind, key, value []byte, sequence uint64) ([]byte, error)

// Verify validates an authoritative Ref against its object. Structural mode
// uses bounded RangeSource reads. Complete mode requires the version-bound
// StreamingRangeSource capability and exactly one TimelineExtractor.
func Verify(ctx context.Context, source RangeSource, objectKey string, expected Ref, mode VerifyMode, extractors ...TimelineExtractor) error {
	if mode != VerifyStructural && mode != VerifyComplete {
		return invalidRunf("unknown verify mode %d", mode)
	}
	if len(extractors) > 1 {
		return invalidRunf("multiple timeline extractors")
	}
	if mode == VerifyComplete {
		streaming, ok := source.(StreamingRangeSource)
		if !ok {
			return invalidRunf("complete verification requires a streaming range source")
		}
		_, err := VerifyCompleteStreaming(ctx, streaming, objectKey, expected, CompleteVerifyOptions{}, extractorsValue(extractors))
		return err
	}
	if err := validateRef(expected, true); err != nil {
		return err
	}
	recovered, err := Recover(ctx, source, objectKey)
	if err != nil {
		return err
	}
	if !sameRef(expected, recovered) {
		return corruptRunf("authoritative run reference disagrees with recovered object framing")
	}
	return nil
}

func extractorsValue(extractors []TimelineExtractor) TimelineExtractor {
	if len(extractors) == 0 {
		return nil
	}
	return extractors[0]
}

func validateRef(ref Ref, persisted bool) error {
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}
	if ref.FormatVersion != FormatVersion {
		return unsupportedRunf("run reference format version %d", ref.FormatVersion)
	}
	if err := validatePreamble(ref.preamble(), persisted); err != nil {
		return err
	}
	if ref.ObjectSize > MaxRunObjectBytes {
		return runTooLargef("object size %d exceeds %d", ref.ObjectSize, MaxRunObjectBytes)
	}
	trailer := Trailer{
		DirectoryOffset: ref.DirectoryOffset,
		DirectoryLength: ref.DirectoryLength,
		ObjectSize:      ref.ObjectSize,
		RegionCount:     uint16(2 + len(ref.OptionalRegions)),
		DirectoryHash:   ref.DirectoryHash,
		PayloadHash:     ref.PayloadHash,
		RunID:           ref.RunID,
	}
	if ref.TimelineFilter != nil {
		trailer.RegionCount++
	}
	if err := validateTrailer(trailer, persisted); err != nil {
		return err
	}
	regions := refRegions(ref)
	directory := Directory{
		DirectoryOffset: ref.DirectoryOffset,
		MinTimeline:     ref.MinTimeline,
		MaxTimeline:     ref.MaxTimeline,
		Regions:         regions,
	}
	layout, err := planDirectory(directory, persisted)
	if err != nil {
		return err
	}
	if layout.totalBytes != ref.DirectoryLength {
		return errorf("directory length %d, want canonical length %d", ref.DirectoryLength, layout.totalBytes)
	}
	if allZero(ref.DirectoryHash[:]) || allZero(ref.PayloadHash[:]) {
		return errorf("directory or payload hash is zero")
	}
	for i := range ref.OptionalRegions {
		if ref.OptionalRegions[i].Kind == RegionKindTimelineFilter && ref.OptionalRegions[i].Encoding == RegionEncodingV1 {
			return errorf("recognized timeline filter is not projected as a filter reference")
		}
		if i > 0 && ref.OptionalRegions[i-1].Kind >= ref.OptionalRegions[i].Kind {
			return errorf("optional region kinds are not strictly increasing")
		}
	}
	for _, region := range []RegionDescriptor{ref.Events, ref.Heads} {
		if region.SeqLo < ref.SeqLo || region.SeqHi > ref.SeqHi {
			return errorf("region kind %d sequence range [%d,%d] exceeds run range [%d,%d]", region.Kind, region.SeqLo, region.SeqHi, ref.SeqLo, ref.SeqHi)
		}
	}
	if ref.TimelineFilter != nil {
		filter := cloneFilterRef(*ref.TimelineFilter)
		if filter.ObjectSize != ref.ObjectSize {
			return errorf("timeline filter object size %d, want %d", filter.ObjectSize, ref.ObjectSize)
		}
		if filter.Header.RunID != ref.RunID {
			return errorf("timeline filter run ID differs from run reference")
		}
		if !sameRegion(filter.Region, ref.TimelineFilter.Region) {
			return errorf("timeline filter region projection is inconsistent")
		}
		if err := validateFilterRef(filter, persisted); err != nil {
			return err
		}
	}
	return nil
}

func refRegions(ref Ref) []RegionDescriptor {
	regions := make([]RegionDescriptor, 0, 2+len(ref.OptionalRegions)+1)
	regions = append(regions, cloneRegion(ref.Events), cloneRegion(ref.Heads))
	if ref.TimelineFilter != nil {
		regions = append(regions, cloneRegion(ref.TimelineFilter.Region))
	}
	for i := range ref.OptionalRegions {
		regions = append(regions, cloneRegion(ref.OptionalRegions[i]))
	}
	slices.SortFunc(regions, func(left, right RegionDescriptor) int {
		if left.Kind < right.Kind {
			return -1
		}
		if left.Kind > right.Kind {
			return 1
		}
		return 0
	})
	return regions
}

func cloneRegion(region RegionDescriptor) RegionDescriptor {
	region.MinKey = bytes.Clone(region.MinKey)
	region.MaxKey = bytes.Clone(region.MaxKey)
	return region
}

func cloneFilterRef(filter FilterRef) FilterRef {
	filter.Region = cloneRegion(filter.Region)
	return filter
}

func sameRegion(left, right RegionDescriptor) bool {
	return left.Kind == right.Kind && left.Required == right.Required && left.Encoding == right.Encoding &&
		left.Offset == right.Offset && left.Length == right.Length && left.EntryCount == right.EntryCount &&
		left.SeqLo == right.SeqLo && left.SeqHi == right.SeqHi && bytes.Equal(left.MinKey, right.MinKey) &&
		bytes.Equal(left.MaxKey, right.MaxKey) && left.ContentHash == right.ContentHash
}

func sameRef(left, right Ref) bool {
	if left.FormatVersion != right.FormatVersion || left.RunID != right.RunID || left.NamespaceHash != right.NamespaceHash ||
		left.Shard != right.Shard || left.CreatorRole != right.CreatorRole || left.CreatorEpoch != right.CreatorEpoch ||
		left.SeqLo != right.SeqLo || left.SeqHi != right.SeqHi || left.PublicationHash != right.PublicationHash ||
		!bytes.Equal(left.MinTimeline, right.MinTimeline) || !bytes.Equal(left.MaxTimeline, right.MaxTimeline) ||
		!sameRegion(left.Events, right.Events) || !sameRegion(left.Heads, right.Heads) ||
		left.DirectoryOffset != right.DirectoryOffset || left.DirectoryLength != right.DirectoryLength ||
		left.ObjectSize != right.ObjectSize || left.DirectoryHash != right.DirectoryHash || left.PayloadHash != right.PayloadHash ||
		len(left.OptionalRegions) != len(right.OptionalRegions) || (left.TimelineFilter == nil) != (right.TimelineFilter == nil) {
		return false
	}
	if left.TimelineFilter != nil && (left.TimelineFilter.Header != right.TimelineFilter.Header ||
		left.TimelineFilter.ObjectSize != right.TimelineFilter.ObjectSize || !sameRegion(left.TimelineFilter.Region, right.TimelineFilter.Region)) {
		return false
	}
	for i := range left.OptionalRegions {
		if !sameRegion(left.OptionalRegions[i], right.OptionalRegions[i]) {
			return false
		}
	}
	return true
}
