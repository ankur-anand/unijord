package runfile

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// ObjectIdentity is the immutable provider identity bound to every range in a
// complete verification attempt. A non-zero generation or ETag is required
// for multi-request remote verification.
type ObjectIdentity struct {
	Size       uint64
	ETag       string
	Generation int64
}

func (id ObjectIdentity) hasVersion() bool { return id.ETag != "" || id.Generation != 0 }

// StreamingRangeSource is the complete-verification capability. OpenRange
// must return a bounded streaming body and enforce the supplied immutable
// identity (If-Match, generation match, or an equivalent provider condition).
type StreamingRangeSource interface {
	Stat(ctx context.Context, objectKey string) (ObjectIdentity, error)
	OpenRange(ctx context.Context, objectKey string, identity ObjectIdentity, offset, length uint64) (io.ReadCloser, error)
}

// CompleteVerifyOptions bounds local memory, scratch, and stream buffers.
// Zero values select conservative defaults.
type CompleteVerifyOptions struct {
	ScratchDir       string
	MemoryBudget     uint64
	ScratchBudget    uint64
	SortMergeFanIn   int
	StreamBufferSize uint64
}

const (
	defaultCompleteMemoryBudget = 8 << 20
	defaultCompleteFanIn        = 16
	defaultCompleteBufferSize   = 128 << 10
)

// CompleteVerificationReport describes both successful and failed attempts.
// Counts exclude provider-internal retries that are not exposed by the
// StreamingRangeSource implementation.
type CompleteVerificationReport struct {
	RunID                     [RunIDBytes]byte
	ObjectSize                uint64
	RegionCount               uint16
	ProviderMetadataRequests  uint64
	ProviderGETAttempts       uint64
	ProviderGETs              uint64
	ProviderBytesRequested    uint64
	ProviderBytesConsumed     uint64
	RetryCount                uint64
	ResumedBytes              uint64
	ScratchBytesWritten       uint64
	ScratchBytesRead          uint64
	ScratchBytesDeleted       uint64
	ScratchHighWater          uint64
	TimelineSpillRuns         uint64
	TimelineMergePasses       uint64
	DistinctTimelines         uint64
	FilterContributionRecords uint64
	FilterContributionSpills  uint64
	FilterContributionMerges  uint64
	PhaseDurations            map[string]time.Duration
	FailurePhase              string
}

// VerifyCompleteStreaming performs the design's bounded remote complete
// verification path. It obtains one provider identity, fetches one bounded
// suffix, streams each physical region once into local scratch, verifies SSTs
// only from local files, compares exact timeline sets through external sorting,
// and compares a sequentially reconstructed filter.
func VerifyCompleteStreaming(
	ctx context.Context,
	source StreamingRangeSource,
	objectKey string,
	expected Ref,
	options CompleteVerifyOptions,
	extractor TimelineExtractor,
) (report CompleteVerificationReport, resultErr error) {
	report = CompleteVerificationReport{RunID: expected.RunID, PhaseDurations: make(map[string]time.Duration)}
	if ctx == nil {
		return report, invalidRunf("nil context")
	}
	if source == nil {
		return report, invalidRunf("nil streaming range source")
	}
	if extractor == nil {
		return report, invalidRunf("complete verification requires a timeline extractor")
	}
	if err := validateRef(expected, true); err != nil {
		return report, err
	}
	options = normalizeCompleteVerifyOptions(options)

	phase := func(name string) func() {
		started := time.Now()
		return func() { report.PhaseDurations[name] += time.Since(started) }
	}

	finishPhase := phase("stat")
	identity, err := source.Stat(ctx, objectKey)
	report.ProviderMetadataRequests++
	finishPhase()
	if err != nil {
		report.FailurePhase = "stat"
		return report, err
	}
	if !identity.hasVersion() {
		return report, invalidRunf("streaming source returned no immutable object version")
	}
	if identity.Size > MaxRunObjectBytes {
		return report, runTooLargef("object size %d exceeds %d", identity.Size, MaxRunObjectBytes)
	}
	if identity.Size > math.MaxInt64 {
		return report, runTooLargef("object size %d exceeds signed range limits", identity.Size)
	}
	if identity.Size != expected.ObjectSize {
		return report, corruptRunf("provider object size %d differs from authoritative reference %d", identity.Size, expected.ObjectSize)
	}
	report.ObjectSize = identity.Size

	finishPhase = phase("suffix")
	suffixLength := min(identity.Size, uint64(MaxDirectoryBytes)+TrailerBytes)
	suffixOffset := identity.Size - suffixLength
	suffix, err := readCompleteRemoteRange(ctx, source, objectKey, identity, suffixOffset, suffixLength, options.StreamBufferSize, &report)
	finishPhase()
	if err != nil {
		report.FailurePhase = "suffix"
		return report, err
	}
	plan, err := planStreamingRun(expected, identity, suffixOffset, suffix)
	if err != nil {
		report.FailurePhase = "planning"
		return report, err
	}
	report.RegionCount = uint16(len(plan.regions))

	workspace, err := newVerificationWorkspace(options)
	if err != nil {
		report.FailurePhase = "scratch"
		return report, err
	}
	workspace.report = &report
	workspace.ctx = ctx
	defer func() {
		if cleanupErr := workspace.close(&report); cleanupErr != nil {
			if resultErr == nil {
				resultErr = cleanupErr
				report.FailurePhase = "cleanup"
			} else {
				resultErr = errors.Join(resultErr, cleanupErr)
			}
		}
	}()

	verifier := streamingVerifier{
		ctx:       ctx,
		source:    source,
		objectKey: objectKey,
		identity:  identity,
		plan:      plan,
		options:   options,
		report:    &report,
		workspace: workspace,
		suffix:    suffix,
		suffixOff: suffixOffset,
	}
	resultErr = verifier.run(extractor)
	workspace.snapshot(&report)
	return report, resultErr
}

type streamingPlan struct {
	ref        Ref
	trailer    Trailer
	directory  Directory
	regions    []RegionDescriptor
	suffixOff  uint64
	suffixData []byte
}

func planStreamingRun(expected Ref, identity ObjectIdentity, suffixOffset uint64, suffix []byte) (streamingPlan, error) {
	plan := streamingPlan{suffixOff: suffixOffset, suffixData: suffix}
	if len(suffix) < TrailerBytes {
		return plan, corruptRunf("suffix length %d is smaller than trailer %d", len(suffix), TrailerBytes)
	}
	trailerBytes := suffix[len(suffix)-TrailerBytes:]
	trailer, err := UnmarshalTrailerForObject(trailerBytes, identity.Size)
	if err != nil {
		return plan, err
	}
	if trailer.ObjectSize != identity.Size || trailer.ObjectSize != expected.ObjectSize {
		return plan, corruptRunf("trailer object size %d disagrees with provider or reference", trailer.ObjectSize)
	}
	if trailer.DirectoryOffset < suffixOffset {
		return plan, corruptRunf("directory begins before fetched suffix")
	}
	directoryEnd, ok := checkedAdd(trailer.DirectoryOffset, trailer.DirectoryLength)
	if !ok || directoryEnd > identity.Size-TrailerBytes || directoryEnd > suffixOffset+uint64(len(suffix)-TrailerBytes) {
		return plan, corruptRunf("directory is not contained in fetched suffix")
	}
	directoryStart := trailer.DirectoryOffset - suffixOffset
	directoryBytes := suffix[directoryStart : directoryStart+trailer.DirectoryLength]
	if sha256.Sum256(directoryBytes) != trailer.DirectoryHash {
		return plan, corruptRunf("directory SHA-256 mismatch")
	}
	directory, err := UnmarshalDirectory(directoryBytes, trailer.DirectoryOffset)
	if err != nil {
		return plan, err
	}
	if uint16(len(directory.Regions)) != trailer.RegionCount {
		return plan, corruptRunf("directory region count %d, trailer records %d", len(directory.Regions), trailer.RegionCount)
	}
	if trailer.RunID != expected.RunID {
		return plan, corruptRunf("trailer run ID differs from authoritative reference")
	}
	var filterHeader *FilterHeader
	if expected.TimelineFilter != nil {
		header := expected.TimelineFilter.Header
		filterHeader = &header
	}
	plannedRef := refFromParts(expected.preamble(), directory, trailer, filterHeader)
	if err := validateRef(plannedRef, true); err != nil {
		return plan, err
	}
	if !sameRef(expected, plannedRef) {
		return plan, corruptRunf("authoritative run reference disagrees with suffix framing")
	}
	plan.ref = plannedRef
	plan.trailer = trailer
	plan.directory = directory
	plan.regions = slices.Clone(directory.Regions)
	return plan, nil
}

func readCompleteRemoteRange(
	ctx context.Context,
	source StreamingRangeSource,
	objectKey string,
	identity ObjectIdentity,
	offset, length, bufferSize uint64,
	report *CompleteVerificationReport,
) ([]byte, error) {
	if length > uint64(math.MaxInt) {
		return nil, runTooLargef("suffix length %d exceeds platform allocation limits", length)
	}
	data := make([]byte, int(length))
	if length == 0 {
		return data, nil
	}
	var position uint64
	err := streamCompleteRange(ctx, source, objectKey, identity, offset, length, bufferSize, report, func(chunk []byte) error {
		copy(data[position:], chunk)
		position += uint64(len(chunk))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func streamCompleteRange(
	ctx context.Context,
	source StreamingRangeSource,
	objectKey string,
	identity ObjectIdentity,
	offset, length, bufferSize uint64,
	report *CompleteVerificationReport,
	consume func([]byte) error,
) (result error) {
	if offset > identity.Size || length > identity.Size-offset {
		return corruptRunf("stream range [%d,%d) exceeds object size", offset, offset+length)
	}
	if length == 0 {
		return nil
	}
	if bufferSize == 0 || bufferSize > uint64(math.MaxInt) {
		bufferSize = defaultCompleteBufferSize
	}
	if bufferSize > uint64(math.MaxInt) {
		return runTooLargef("stream buffer size %d exceeds platform limits", bufferSize)
	}
	report.ProviderGETAttempts++
	report.ProviderBytesRequested += length
	body, err := source.OpenRange(ctx, objectKey, identity, offset, length)
	if err != nil {
		return classifyCompleteStreamError(ctx, source, objectKey, identity, err, report)
	}
	if body == nil {
		return fmt.Errorf("%w: provider returned a nil range body", ErrVerificationResource)
	}
	defer func() {
		if closeErr := body.Close(); result == nil && closeErr != nil {
			result = classifyCompleteStreamError(ctx, source, objectKey, identity, closeErr, report)
		}
	}()
	buffer := make([]byte, int(bufferSize))
	remaining := length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := min(remaining, uint64(len(buffer)))
		n, readErr := body.Read(buffer[:int(want)])
		if n < 0 || uint64(n) > want {
			return corruptRunf("stream returned invalid byte count %d", n)
		}
		if n > 0 {
			report.ProviderBytesConsumed += uint64(n)
			if err := consume(buffer[:n]); err != nil {
				return err
			}
			remaining -= uint64(n)
		}
		if readErr != nil {
			if readErr == io.EOF && remaining > 0 {
				return corruptRunf("successful stream ended %d bytes early", remaining)
			}
			if readErr != io.EOF {
				return classifyCompleteStreamError(ctx, source, objectKey, identity, readErr, report)
			}
		}
		if n == 0 && readErr == nil {
			return io.ErrNoProgress
		}
	}
	// A bounded provider response should be at EOF. Detect adapters that return
	// extra bytes instead of enforcing the requested range themselves.
	var extra [1]byte
	n, readErr := body.Read(extra[:])
	if n > 0 {
		return corruptRunf("stream returned bytes beyond requested range")
	}
	if readErr != nil && readErr != io.EOF {
		return classifyCompleteStreamError(ctx, source, objectKey, identity, readErr, report)
	}
	report.ProviderGETs++
	return nil
}

func classifyCompleteStreamError(ctx context.Context, source StreamingRangeSource, objectKey string, expected ObjectIdentity, err error, report *CompleteVerificationReport) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrObjectIdentityChanged) {
		identity, statErr := source.Stat(ctx, objectKey)
		report.ProviderMetadataRequests++
		if statErr != nil {
			return statErr
		}
		if identity == expected {
			return err
		}
		return fmt.Errorf("%w: immutable object generation changed: %w", ErrCorruptRun, err)
	}
	return err
}

type streamingVerifier struct {
	ctx       context.Context
	source    StreamingRangeSource
	objectKey string
	identity  ObjectIdentity
	plan      streamingPlan
	options   CompleteVerifyOptions
	report    *CompleteVerificationReport
	workspace *verificationWorkspace
	suffix    []byte
	suffixOff uint64
	phase     string
}

func (v *streamingVerifier) run(extractor TimelineExtractor) (result error) {
	started := time.Now()
	defer func() {
		v.report.PhaseDurations["complete"] += time.Since(started)
		if result != nil {
			v.report.FailurePhase = v.phase
		}
	}()
	payloadHash := sha256.New()
	cursor := uint64(0)
	var preambleBytes [PreambleBytes]byte
	var preambleCaptured uint64
	var eventsSet, headsSet *timelineStream
	var setsCompared bool
	var canonicalFilter string

	for index, region := range v.plan.regions {
		v.phase = "acquisition"
		if err := v.ctx.Err(); err != nil {
			return err
		}
		regionEnd, ok := checkedAdd(region.Offset, region.Length)
		if !ok {
			return corruptRunf("region kind %d end overflows", region.Kind)
		}
		segmentEnd := regionEnd
		if index == len(v.plan.regions)-1 {
			segmentEnd = v.plan.trailer.DirectoryOffset
		}
		if segmentEnd < cursor {
			return corruptRunf("region kind %d precedes streaming cursor", region.Kind)
		}

		recognizedFilter := region.Kind == RegionKindTimelineFilter && region.Encoding == RegionEncodingV1
		var regionFile *os.File
		var regionPath string
		if region.Kind == RegionKindEventsSST || region.Kind == RegionKindHeadsSST {
			if err := v.workspace.ensureAvailable(region.Length); err != nil {
				return err
			}
			var createErr error
			regionFile, regionPath, createErr = v.workspace.createFile("sst")
			if createErr != nil {
				return createErr
			}
		}
		regionHash := sha256.New()
		processor := segmentProcessor{
			verifier:       v,
			payloadHash:    payloadHash,
			regionHash:     regionHash,
			segmentStart:   cursor,
			segmentEnd:     segmentEnd,
			position:       cursor,
			region:         region,
			regionFile:     regionFile,
			preambleBytes:  &preambleBytes,
			preambleSeen:   &preambleCaptured,
			filterConsumer: nil,
		}
		if recognizedFilter {
			if canonicalFilter == "" {
				if !setsCompared || eventsSet == nil {
					return corruptRunf("timeline filter arrived before exact required-table comparison")
				}
				v.phase = "filter-build"
				filterStarted := time.Now()
				path, err := v.buildCanonicalFilter(eventsSet)
				v.report.PhaseDurations["filter-build"] += time.Since(filterStarted)
				if err != nil {
					return err
				}
				canonicalFilter = path
				if err := v.workspace.remove(eventsSet.path); err != nil {
					return err
				}
				eventsSet = nil
			}
			consumer, err := newFilterComparator(canonicalFilter, v.workspace)
			if err != nil {
				return err
			}
			processor.filterConsumer = consumer
		}
		v.phase = "acquisition"
		streamStarted := time.Now()
		streamErr := v.streamSegment(cursor, segmentEnd, &processor)
		v.report.PhaseDurations["acquisition"] += time.Since(streamStarted)
		if err := streamErr; err != nil {
			cleanupErrors := []error{err}
			if processor.filterConsumer != nil {
				cleanupErrors = append(cleanupErrors, processor.filterConsumer.close())
			}
			if regionFile != nil {
				if closeErr := regionFile.Close(); closeErr != nil {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("%w: close SST scratch: %w", ErrVerificationResource, closeErr))
				}
				cleanupErrors = append(cleanupErrors, v.workspace.remove(regionPath))
			}
			return errors.Join(cleanupErrors...)
		}
		if regionFile != nil {
			if err := regionFile.Sync(); err != nil {
				return errors.Join(
					fmt.Errorf("%w: sync SST scratch: %w", ErrVerificationResource, err),
					closeScratchReadFile(regionFile, "close SST scratch"),
					v.workspace.remove(regionPath),
				)
			}
			if err := regionFile.Close(); err != nil {
				return errors.Join(
					fmt.Errorf("%w: close SST scratch: %w", ErrVerificationResource, err),
					v.workspace.remove(regionPath),
				)
			}
		}
		var digest [SHA256Bytes]byte
		copy(digest[:], regionHash.Sum(nil))
		if digest != region.ContentHash {
			return corruptRunf("region kind %d SHA-256 mismatch", region.Kind)
		}
		if index == 0 {
			if err := v.validateStreamedPreamble(preambleBytes, preambleCaptured); err != nil {
				return err
			}
		}
		if processor.filterConsumer != nil {
			finishErr := processor.filterConsumer.finish()
			closeErr := processor.filterConsumer.close()
			if finishErr != nil {
				if closeErr != nil {
					return errors.Join(finishErr, closeErr)
				}
				return finishErr
			}
			if closeErr != nil {
				return closeErr
			}
			if err := v.workspace.remove(canonicalFilter); err != nil {
				return err
			}
			canonicalFilter = ""
		}

		switch {
		case region.Kind == RegionKindEventsSST || region.Kind == RegionKindHeadsSST:
			v.phase = "local-sst"
			sorter := newTimelineSorter(v.workspace, v.options.MemoryBudget, v.options.SortMergeFanIn, region.Kind, v.report)
			localStarted := time.Now()
			set, err := v.verifyLocalTable(regionPath, region, region.Kind, extractor, sorter)
			v.report.PhaseDurations["local-sst"] += time.Since(localStarted)
			if err != nil {
				return errors.Join(err, v.workspace.remove(regionPath))
			}
			if region.Kind == RegionKindEventsSST {
				eventsSet = set
				v.report.DistinctTimelines = set.count
			} else {
				headsSet = set
				v.phase = "timeline-compare"
				compareStarted := time.Now()
				compareErr := compareTimelineStreams(v.ctx, v.workspace, eventsSet, headsSet)
				v.report.PhaseDurations["timeline-compare"] += time.Since(compareStarted)
				if compareErr != nil {
					return compareErr
				}
				if eventsSet.count == 0 || !bytes.Equal(eventsSet.min, v.plan.ref.MinTimeline) || !bytes.Equal(eventsSet.max, v.plan.ref.MaxTimeline) {
					return corruptRunf("directory timeline bounds do not match required tables")
				}
				setsCompared = true
				if err := v.workspace.remove(headsSet.path); err != nil {
					return err
				}
				headsSet = nil
				if v.plan.ref.TimelineFilter == nil {
					if err := v.workspace.remove(eventsSet.path); err != nil {
						return err
					}
					eventsSet = nil
				}
			}
			if err := v.workspace.remove(regionPath); err != nil {
				return err
			}
		case recognizedFilter:
			// The comparator already authenticated every stored filter byte.
		default:
			// Unknown optional regions are covered by payload and region hashes.
		}
		cursor = regionEnd
	}
	v.phase = "finalize"
	if cursor > v.plan.trailer.DirectoryOffset {
		return corruptRunf("regions extend beyond directory")
	}
	if v.plan.trailer.DirectoryOffset < v.suffixOff {
		return corruptRunf("directory lies outside suffix planning window")
	}
	directoryStart := v.plan.trailer.DirectoryOffset - v.suffixOff
	directoryBytes := v.suffix[directoryStart : directoryStart+v.plan.trailer.DirectoryLength]
	_, _ = payloadHash.Write(directoryBytes)
	var payloadDigest [SHA256Bytes]byte
	copy(payloadDigest[:], payloadHash.Sum(nil))
	if payloadDigest != v.plan.ref.PayloadHash {
		return corruptRunf("payload SHA-256 mismatch")
	}
	if !setsCompared {
		return corruptRunf("required SST regions were not verified")
	}
	return nil
}

func (v *streamingVerifier) validateStreamedPreamble(encoded [PreambleBytes]byte, captured uint64) error {
	if captured != PreambleBytes {
		return corruptRunf("stream did not contain a complete preamble")
	}
	preamble, err := UnmarshalPreamble(encoded[:])
	if err != nil {
		return err
	}
	if preamble.RunID != v.plan.ref.RunID || preamble.NamespaceHash != v.plan.ref.NamespaceHash || preamble.Shard != v.plan.ref.Shard ||
		preamble.SeqLo != v.plan.ref.SeqLo || preamble.SeqHi != v.plan.ref.SeqHi || preamble.CreatorRole != v.plan.ref.CreatorRole ||
		preamble.CreatorEpoch != v.plan.ref.CreatorEpoch || preamble.PublicationHash != v.plan.ref.PublicationHash {
		return corruptRunf("streamed preamble disagrees with authoritative reference")
	}
	return nil
}

func (v *streamingVerifier) streamSegment(start, end uint64, processor *segmentProcessor) error {
	length := end - start
	if v.suffixOff == 0 {
		if end > uint64(len(v.suffix)) {
			return corruptRunf("small-object segment exceeds cached suffix")
		}
		return processor.consume(v.suffix[start:end])
	}
	return streamCompleteRange(v.ctx, v.source, v.objectKey, v.identity, start, length, v.options.StreamBufferSize, v.report, processor.consume)
}

type segmentProcessor struct {
	verifier       *streamingVerifier
	payloadHash    hash.Hash
	regionHash     hash.Hash
	segmentStart   uint64
	segmentEnd     uint64
	position       uint64
	region         RegionDescriptor
	regionFile     *os.File
	preambleBytes  *[PreambleBytes]byte
	preambleSeen   *uint64
	filterConsumer *filterComparator
}

func (p *segmentProcessor) consume(data []byte) error {
	position := p.position
	for len(data) > 0 {
		if err := p.verifier.ctx.Err(); err != nil {
			return err
		}
		next := p.segmentEnd
		if position < p.region.Offset {
			next = p.region.Offset
		} else if position < p.region.Offset+p.region.Length {
			next = p.region.Offset + p.region.Length
		}
		if next <= position {
			return corruptRunf("invalid segment geometry at offset %d", position)
		}
		count := min(uint64(len(data)), next-position)
		chunk := data[:int(count)]
		if position < PreambleBytes {
			copyStart := position
			copyEnd := min(position+count, PreambleBytes)
			if copyEnd > copyStart {
				copy(p.preambleBytes[copyStart:copyEnd], chunk[:int(copyEnd-copyStart)])
				*p.preambleSeen = max(*p.preambleSeen, copyEnd)
			}
		}
		if position >= p.region.Offset && position < p.region.Offset+p.region.Length {
			_, _ = p.regionHash.Write(chunk)
			if p.regionFile != nil {
				if err := p.verifier.workspace.write(p.regionFile, chunk); err != nil {
					return err
				}
			}
			if p.filterConsumer != nil {
				if err := p.filterConsumer.consume(chunk); err != nil {
					return err
				}
			}
		} else if position >= PreambleBytes && !allZero(chunk) {
			return corruptRunf("non-zero alignment padding at offset %d", position)
		}
		_, _ = p.payloadHash.Write(chunk)
		position += count
		data = data[int(count):]
	}
	p.position = position
	return nil
}

func (v *streamingVerifier) verifyLocalTable(path string, region RegionDescriptor, kind RegionKind, extractor TimelineExtractor, sorter *timelineSorter) (stream *timelineStream, resultErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open SST scratch: %w", ErrVerificationResource, err)
	}
	source := &localRegionSource{file: file, base: region.Offset, length: region.Length, objectSize: v.identity.Size, workspace: v.workspace}
	defer func() {
		if closeErr := closeScratchReadFile(source.file, "close SST scratch reader"); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	reader, err := OpenTable(v.ctx, source, "local-scratch", v.plan.ref, kind)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("%w: close local SST reader for region kind %d: %w", ErrVerificationResource, kind, closeErr))
		}
	}()
	properties, err := reader.ReadPropertiesBlock(v.ctx, nil)
	if err != nil {
		return nil, tableReadError(kind, "read local properties", err)
	}
	if properties.ComparerName != sstable.DefaultComparer.Name || properties.MergerName != "pebble.concatenate" ||
		properties.NumDeletions != 0 || properties.NumRangeDeletions != 0 || properties.NumRangeKeys() != 0 ||
		properties.NumMergeOperands != 0 || properties.NumValueBlocks != 0 || properties.NumValuesInBlobFiles != 0 {
		return nil, corruptRunf("region kind %d contains unsupported table properties", kind)
	}
	if err := reader.ValidateBlockChecksums(); err != nil {
		return nil, tableReadError(kind, "validate local block checksums", err)
	}
	iterator, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return nil, tableReadError(kind, "create local iterator", err)
	}
	defer func() {
		if closeErr := iterator.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("%w: close local SST iterator for region kind %d: %w", ErrVerificationResource, kind, closeErr))
		}
	}()
	count := uint64(0)
	seqLo := ^uint64(0)
	var seqHi uint64
	var minKey, maxKey []byte
	for kv := iterator.First(); kv != nil; kv = iterator.Next() {
		if err := v.ctx.Err(); err != nil {
			return nil, err
		}
		if kv.Kind() != pebble.InternalKeyKindSet {
			return nil, corruptRunf("region kind %d contains non-SET point kind %d", kind, kv.Kind())
		}
		key := kv.K.UserKey
		value, _, err := kv.V.Value(nil)
		if err != nil {
			return nil, tableReadError(kind, "read local value", err)
		}
		sequence := uint64(kv.SeqNum())
		timeline, err := extractor(v.plan.ref, kind, key, value, sequence)
		if err != nil {
			return nil, corruptRunf("region kind %d semantic validation: %v", kind, err)
		}
		if len(timeline) == 0 {
			return nil, corruptRunf("region kind %d has empty timeline", kind)
		}
		if err := sorter.add(timeline); err != nil {
			return nil, err
		}
		if count == 0 {
			minKey = bytes.Clone(key)
		}
		maxKey = bytes.Clone(key)
		seqLo = min(seqLo, sequence)
		seqHi = max(seqHi, sequence)
		count++
	}
	if err := iterator.Error(); err != nil {
		return nil, tableReadError(kind, "scan local SST", err)
	}
	if count != properties.NumEntries || count != region.EntryCount || seqLo != region.SeqLo || seqHi != region.SeqHi ||
		!bytes.Equal(minKey, region.MinKey) || !bytes.Equal(maxKey, region.MaxKey) {
		return nil, corruptRunf("region kind %d descriptor metadata does not match local SST", kind)
	}
	return sorter.finalize()
}

// localRegionSource maps a scratch SST's zero-based file to the physical
// offsets expected by OpenTable without exposing the remote source.
type localRegionSource struct {
	file       *os.File
	base       uint64
	length     uint64
	objectSize uint64
	workspace  *verificationWorkspace
}

func (s *localRegionSource) Size(context.Context, string) (int64, error) {
	return int64(s.objectSize), nil
}

func (s *localRegionSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || uint64(offset) < s.base {
		return nil, io.ErrUnexpectedEOF
	}
	relative := uint64(offset) - s.base
	if relative > s.length || uint64(length) > s.length-relative || uint64(length) > uint64(math.MaxInt) {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, int(length))
	n, err := s.file.ReadAt(data, offset-int64(s.base))
	if s.workspace != nil {
		s.workspace.readBytes(uint64(max(n, 0)))
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read scratch SST: %w", ErrVerificationResource, err)
	}
	if n != len(data) {
		return nil, fmt.Errorf("%w: short scratch SST read", ErrVerificationResource)
	}
	return data, nil
}

type verificationWorkspace struct {
	dir        string
	budget     uint64
	current    uint64
	high       uint64
	report     *CompleteVerificationReport
	ctx        context.Context
	cleanupErr error
}

const scratchIOBufferBytes = 4 << 10

func newVerificationWorkspace(options CompleteVerifyOptions) (*verificationWorkspace, error) {
	base := options.ScratchDir
	if base == "" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "unijord-verify-*")
	if err != nil {
		return nil, fmt.Errorf("%w: create verification workspace: %w", ErrVerificationResource, err)
	}
	return &verificationWorkspace{dir: dir, budget: options.ScratchBudget}, nil
}

func (w *verificationWorkspace) createFile(prefix string) (*os.File, string, error) {
	if err := w.contextErr(); err != nil {
		return nil, "", err
	}
	file, err := os.CreateTemp(w.dir, prefix+"-*")
	if err != nil {
		return nil, "", fmt.Errorf("%w: create scratch file: %w", ErrVerificationResource, err)
	}
	return file, file.Name(), nil
}

func (w *verificationWorkspace) createBufferedFile(prefix string) (*scratchBufferedFile, string, error) {
	file, path, err := w.createFile(prefix)
	if err != nil {
		return nil, "", err
	}
	sink := &workspaceFileSink{workspace: w, file: file}
	return &scratchBufferedFile{file: file, writer: bufio.NewWriterSize(sink, scratchIOBufferBytes)}, path, nil
}

func (w *verificationWorkspace) ensureAvailable(n uint64) error {
	available := uint64(0)
	if w.current <= w.budget {
		available = w.budget - w.current
	}
	if n > available {
		return fmt.Errorf("%w: scratch budget %d has %d bytes available, need %d", ErrVerificationResource, w.budget, available, n)
	}
	return nil
}

func (w *verificationWorkspace) reserve(n uint64) error {
	if err := w.ensureAvailable(n); err != nil {
		return err
	}
	w.current += n
	if w.current > w.high {
		w.high = w.current
	}
	return nil
}

func (w *verificationWorkspace) write(file *os.File, data []byte) error {
	if err := w.contextErr(); err != nil {
		return err
	}
	if err := w.reserve(uint64(len(data))); err != nil {
		return err
	}
	n, err := file.Write(data)
	if n < len(data) {
		w.current -= uint64(len(data) - max(n, 0))
	}
	w.report.ScratchBytesWritten += uint64(max(n, 0))
	if err != nil {
		return fmt.Errorf("%w: write scratch: %w", ErrVerificationResource, err)
	}
	if n != len(data) {
		return fmt.Errorf("%w: short scratch write", ErrVerificationResource)
	}
	return nil
}

func (w *verificationWorkspace) readBytes(n uint64) {
	w.report.ScratchBytesRead += n
}

func (w *verificationWorkspace) remove(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return w.recordCleanupError(fmt.Errorf("%w: stat scratch file for removal: %w", ErrVerificationResource, err))
	}
	if err := os.Remove(path); err != nil {
		return w.recordCleanupError(fmt.Errorf("%w: remove scratch file: %w", ErrVerificationResource, err))
	}
	size := uint64(info.Size())
	if size > w.current {
		return w.recordCleanupError(fmt.Errorf("%w: scratch accounting underflow removing %d bytes from %d", ErrVerificationResource, size, w.current))
	}
	w.current -= size
	w.report.ScratchBytesDeleted += size
	return nil
}

func (w *verificationWorkspace) recordCleanupError(err error) error {
	if err != nil && w.cleanupErr == nil {
		w.cleanupErr = err
	}
	return err
}

func (w *verificationWorkspace) close(report *CompleteVerificationReport) error {
	if w == nil {
		return nil
	}
	w.report = report
	var remaining uint64
	walkErr := filepath.WalkDir(w.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}
		remaining += uint64(info.Size())
		return nil
	})
	if walkErr != nil {
		w.recordCleanupError(fmt.Errorf("%w: inspect verification workspace during cleanup: %w", ErrVerificationResource, walkErr))
	}
	if err := os.RemoveAll(w.dir); err != nil {
		w.recordCleanupError(fmt.Errorf("%w: remove verification workspace: %w", ErrVerificationResource, err))
	} else if walkErr == nil {
		report.ScratchBytesDeleted += remaining
		w.current = 0
	}
	w.snapshot(report)
	return w.cleanupErr
}

func (w *verificationWorkspace) snapshot(report *CompleteVerificationReport) {
	if w == nil || report == nil {
		return
	}
	report.ScratchHighWater = w.high
}

func (w *verificationWorkspace) contextErr() error {
	if w != nil && w.ctx != nil {
		return w.ctx.Err()
	}
	return nil
}

func (w *verificationWorkspace) newReader(reader io.Reader) *bufio.Reader {
	return bufio.NewReaderSize(&scratchTrackedReader{workspace: w, reader: reader}, scratchIOBufferBytes)
}

func scratchReadError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrVerificationResource) {
		return fmt.Errorf("runfile: %s: %w", operation, err)
	}
	return fmt.Errorf("%w: %s: %w", ErrVerificationResource, operation, err)
}

func closeScratchReadFile(file *os.File, operation string) error {
	if file == nil {
		return nil
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrVerificationResource, operation, err)
	}
	return nil
}

func closeScratchReadFiles(files []*os.File, operation string) error {
	var result error
	for _, file := range files {
		if file != nil {
			result = errors.Join(result, closeScratchReadFile(file, operation))
		}
	}
	return result
}

type scratchTrackedReader struct {
	workspace *verificationWorkspace
	reader    io.Reader
}

func (r *scratchTrackedReader) Read(data []byte) (int, error) {
	if err := r.workspace.contextErr(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(data)
	if n > 0 {
		r.workspace.readBytes(uint64(n))
	}
	return n, err
}

type workspaceFileSink struct {
	workspace *verificationWorkspace
	file      *os.File
}

func (s *workspaceFileSink) Write(data []byte) (int, error) {
	if err := s.workspace.write(s.file, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

type scratchBufferedFile struct {
	file   *os.File
	writer *bufio.Writer
}

func (f *scratchBufferedFile) write(data []byte) error {
	n, err := f.writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("%w: short buffered scratch write", ErrVerificationResource)
	}
	return nil
}

func (f *scratchBufferedFile) close() error {
	flushErr := f.writer.Flush()
	closeErr := f.file.Close()
	if (errors.Is(flushErr, context.Canceled) || errors.Is(flushErr, context.DeadlineExceeded)) && closeErr == nil {
		return flushErr
	}
	if flushErr != nil || closeErr != nil {
		return fmt.Errorf("%w: close buffered scratch file: %w", ErrVerificationResource, errors.Join(flushErr, closeErr))
	}
	return nil
}

func (f *scratchBufferedFile) abort() {
	_ = f.file.Close()
}

func normalizeCompleteVerifyOptions(options CompleteVerifyOptions) CompleteVerifyOptions {
	if options.MemoryBudget == 0 {
		options.MemoryBudget = defaultCompleteMemoryBudget
	}
	if options.ScratchBudget == 0 {
		options.ScratchBudget = MaxRunObjectBytes
	}
	if options.SortMergeFanIn < 2 {
		options.SortMergeFanIn = defaultCompleteFanIn
	}
	if options.SortMergeFanIn > 128 {
		options.SortMergeFanIn = 128
	}
	if options.StreamBufferSize == 0 {
		options.StreamBufferSize = defaultCompleteBufferSize
	}
	if options.StreamBufferSize > 16<<20 {
		options.StreamBufferSize = 16 << 20
	}
	return options
}

type timelineStream struct {
	path  string
	count uint64
	min   []byte
	max   []byte
	bytes uint64
}

type timelineSorter struct {
	workspace *verificationWorkspace
	budget    uint64
	fanIn     int
	kind      RegionKind
	report    *CompleteVerificationReport
	batch     [][]byte
	batchSize uint64
	runs      []string
}

func newTimelineSorter(workspace *verificationWorkspace, budget uint64, fanIn int, kind RegionKind, report *CompleteVerificationReport) *timelineSorter {
	return &timelineSorter{workspace: workspace, budget: budget, fanIn: fanIn, kind: kind, report: report}
}

func (s *timelineSorter) add(timeline []byte) error {
	if len(timeline) == 0 {
		return corruptRunf("region kind %d has an empty timeline", s.kind)
	}
	if uint64(len(timeline)) > MaxTimelineBytes {
		return runTooLargef("region kind %d timeline length %d exceeds %d", s.kind, len(timeline), MaxTimelineBytes)
	}
	// Charge the encoded bytes plus a conservative slice-header/allocation
	// allowance. The budget is a cap, not a precise heap profiler.
	recordBytes, ok := checkedAdd(uint64(len(timeline)), 2+32)
	if !ok {
		return runTooLargef("timeline record memory calculation overflows")
	}
	if recordBytes > s.budget {
		return fmt.Errorf("%w: timeline record requires %d bytes, budget is %d", ErrVerificationResource, recordBytes, s.budget)
	}
	if len(s.batch) > 0 && recordBytes > s.budget-s.batchSize {
		if err := s.spill(); err != nil {
			return err
		}
	}
	s.batch = append(s.batch, bytes.Clone(timeline))
	s.batchSize += recordBytes
	return nil
}

func (s *timelineSorter) spill() error {
	if len(s.batch) == 0 {
		return nil
	}
	if err := s.workspace.contextErr(); err != nil {
		return err
	}
	sort.Slice(s.batch, func(i, j int) bool { return bytes.Compare(s.batch[i], s.batch[j]) < 0 })
	if err := s.workspace.contextErr(); err != nil {
		return err
	}
	file, path, err := s.workspace.createBufferedFile("timeline-spill")
	if err != nil {
		return err
	}
	last := []byte(nil)
	for index, timeline := range s.batch {
		if index%4096 == 0 {
			if err := s.workspace.contextErr(); err != nil {
				file.abort()
				s.workspace.remove(path)
				return err
			}
		}
		if bytes.Equal(last, timeline) {
			continue
		}
		if err := writeTimelineRecord(file, timeline); err != nil {
			file.abort()
			s.workspace.remove(path)
			return err
		}
		last = timeline
	}
	if err := file.close(); err != nil {
		s.workspace.remove(path)
		return fmt.Errorf("%w: close timeline spill: %w", ErrVerificationResource, err)
	}
	s.runs = append(s.runs, path)
	s.report.TimelineSpillRuns++
	s.batch = nil
	s.batchSize = 0
	return nil
}

func (s *timelineSorter) finalize() (*timelineStream, error) {
	if err := s.spill(); err != nil {
		return nil, err
	}
	if len(s.runs) == 0 {
		return nil, fmt.Errorf("%w: no timelines were emitted for region kind %d", ErrVerificationResource, s.kind)
	}
	runs := slices.Clone(s.runs)
	if len(runs) == 1 {
		return describeTimelineStream(s.workspace, runs[0])
	}
	for len(runs) > s.fanIn {
		var next []string
		for start := 0; start < len(runs); start += s.fanIn {
			end := min(start+s.fanIn, len(runs))
			merged, err := mergeTimelineRuns(s.workspace, runs[start:end])
			if err != nil {
				return nil, err
			}
			for _, path := range runs[start:end] {
				if err := s.workspace.remove(path); err != nil {
					return nil, err
				}
			}
			next = append(next, merged)
		}
		runs = next
		s.report.TimelineMergePasses++
	}
	finalPath, err := mergeTimelineRuns(s.workspace, runs)
	if err != nil {
		return nil, err
	}
	for _, path := range runs {
		if err := s.workspace.remove(path); err != nil {
			return nil, err
		}
	}
	return describeTimelineStream(s.workspace, finalPath)
}

func writeTimelineRecord(file *scratchBufferedFile, timeline []byte) error {
	if len(timeline) == 0 || uint64(len(timeline)) > MaxTimelineBytes {
		return fmt.Errorf("%w: invalid timeline spill record length %d", ErrVerificationResource, len(timeline))
	}
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(timeline)))
	if err := file.write(length[:]); err != nil {
		return err
	}
	return file.write(timeline)
}

func describeTimelineStream(workspace *verificationWorkspace, path string) (stream *timelineStream, resultErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open canonical timeline file: %w", ErrVerificationResource, err)
	}
	defer func() {
		if closeErr := closeScratchReadFile(file, "close canonical timeline file"); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	reader := workspace.newReader(file)
	stream = &timelineStream{path: path}
	for {
		timeline, err := readTimelineRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, scratchReadError("read timeline stream", err)
		}
		if stream.count == 0 {
			stream.min = bytes.Clone(timeline)
		}
		stream.max = bytes.Clone(timeline)
		stream.count++
		stream.bytes += uint64(len(timeline) + 2)
	}
	return stream, nil
}

func readTimelineRecord(reader *bufio.Reader) ([]byte, error) {
	var length [2]byte
	nRead, err := io.ReadFull(reader, length[:])
	if err != nil {
		if err == io.EOF && nRead == 0 {
			return nil, io.EOF
		}
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(length[:]))
	if n == 0 || uint64(n) > MaxTimelineBytes {
		return nil, fmt.Errorf("invalid timeline record length %d", n)
	}
	timeline := make([]byte, n)
	if _, err := io.ReadFull(reader, timeline); err != nil {
		return nil, err
	}
	return timeline, nil
}

type timelineHeapItem struct {
	value []byte
	index int
}
type timelineHeap []timelineHeapItem

func (h timelineHeap) Len() int           { return len(h) }
func (h timelineHeap) Less(i, j int) bool { return bytes.Compare(h[i].value, h[j].value) < 0 }
func (h timelineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timelineHeap) Push(value any)    { *h = append(*h, value.(timelineHeapItem)) }
func (h *timelineHeap) Pop() any {
	old := *h
	n := len(old)
	value := old[n-1]
	*h = old[:n-1]
	return value
}

func mergeTimelineRuns(workspace *verificationWorkspace, paths []string) (string, error) {
	if len(paths) == 0 {
		return "", fmt.Errorf("%w: no timeline runs to merge", ErrVerificationResource)
	}
	file, outputPath, err := workspace.createBufferedFile("timeline-merge")
	if err != nil {
		return "", err
	}
	readers := make([]*bufio.Reader, len(paths))
	files := make([]*os.File, len(paths))
	queue := timelineHeap{}
	for index, path := range paths {
		input, err := os.Open(path)
		if err != nil {
			file.abort()
			workspace.remove(outputPath)
			return "", errors.Join(
				fmt.Errorf("%w: open timeline run: %w", ErrVerificationResource, err),
				closeScratchReadFiles(files, "close timeline merge input"),
			)
		}
		files[index] = input
		readers[index] = workspace.newReader(input)
		timeline, err := readTimelineRecord(readers[index])
		if err == io.EOF {
			continue
		}
		if err != nil {
			file.abort()
			workspace.remove(outputPath)
			return "", errors.Join(
				scratchReadError("read timeline run", err),
				closeScratchReadFiles(files, "close timeline merge input"),
			)
		}
		heap.Push(&queue, timelineHeapItem{value: timeline, index: index})
	}
	var last []byte
	for queue.Len() > 0 {
		item := heap.Pop(&queue).(timelineHeapItem)
		if !bytes.Equal(last, item.value) {
			if err := writeTimelineRecord(file, item.value); err != nil {
				file.abort()
				workspace.remove(outputPath)
				return "", errors.Join(err, closeScratchReadFiles(files, "close timeline merge input"))
			}
			last = item.value
		}
		next, err := readTimelineRecord(readers[item.index])
		if err == nil {
			heap.Push(&queue, timelineHeapItem{value: next, index: item.index})
		} else if err != io.EOF {
			file.abort()
			workspace.remove(outputPath)
			return "", errors.Join(
				scratchReadError("read timeline merge input", err),
				closeScratchReadFiles(files, "close timeline merge input"),
			)
		}
	}
	if err := closeScratchReadFiles(files, "close timeline merge input"); err != nil {
		file.abort()
		workspace.remove(outputPath)
		return "", err
	}
	if err := file.close(); err != nil {
		workspace.remove(outputPath)
		return "", fmt.Errorf("%w: close timeline merge output: %w", ErrVerificationResource, err)
	}
	return outputPath, nil
}

func compareTimelineStreams(ctx context.Context, workspace *verificationWorkspace, events, heads *timelineStream) (resultErr error) {
	eventFile, err := os.Open(events.path)
	if err != nil {
		return fmt.Errorf("%w: open Events timeline stream: %w", ErrVerificationResource, err)
	}
	defer func() {
		if closeErr := closeScratchReadFile(eventFile, "close Events timeline stream"); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	headFile, err := os.Open(heads.path)
	if err != nil {
		return fmt.Errorf("%w: open Heads timeline stream: %w", ErrVerificationResource, err)
	}
	defer func() {
		if closeErr := closeScratchReadFile(headFile, "close Heads timeline stream"); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	eventsReader := workspace.newReader(eventFile)
	headsReader := workspace.newReader(headFile)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		event, eventErr := readTimelineRecord(eventsReader)
		head, headErr := readTimelineRecord(headsReader)
		if eventErr != nil && eventErr != io.EOF {
			return scratchReadError("compare Events timeline stream", eventErr)
		}
		if headErr != nil && headErr != io.EOF {
			return scratchReadError("compare Heads timeline stream", headErr)
		}
		if eventErr == io.EOF || headErr == io.EOF {
			if eventErr != headErr {
				return corruptRunf("Events and Heads timeline sets differ")
			}
			return nil
		}
		if !bytes.Equal(event, head) {
			return corruptRunf("Events and Heads timeline sets differ")
		}
	}
}

type filterContribution struct {
	line uint32
	bit  uint16
}

type contributionSorter struct {
	workspace *verificationWorkspace
	budget    uint64
	fanIn     int
	report    *CompleteVerificationReport
	batch     []filterContribution
	runs      []string
}

func (v *streamingVerifier) buildCanonicalFilter(events *timelineStream) (string, error) {
	header := v.plan.ref.TimelineFilter.Header
	if events.count != header.KeyCount {
		return "", corruptRunf("timeline filter key count %d differs from required-table set %d", header.KeyCount, events.count)
	}
	headerBytes, err := MarshalFilterHeader(header)
	if err != nil {
		return "", err
	}
	sorter := &contributionSorter{workspace: v.workspace, budget: v.options.MemoryBudget, fanIn: v.options.SortMergeFanIn, report: v.report}
	file, err := os.Open(events.path)
	if err != nil {
		return "", fmt.Errorf("%w: open Events timeline stream: %w", ErrVerificationResource, err)
	}
	reader := v.workspace.newReader(file)
	for {
		timeline, readErr := readTimelineRecord(reader)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			closeErr := closeScratchReadFile(file, "close Events timeline stream")
			return "", errors.Join(scratchReadError("read Events timeline stream", readErr), closeErr)
		}
		location, err := locateTimeline(header, timeline)
		if err != nil {
			return "", errors.Join(err, closeScratchReadFile(file, "close Events timeline stream"))
		}
		for probe := uint8(0); probe < header.Probes; probe++ {
			if err := sorter.add(filterContribution{line: location.line, bit: location.probeBits[probe]}); err != nil {
				return "", errors.Join(err, closeScratchReadFile(file, "close Events timeline stream"))
			}
		}
	}
	if err := closeScratchReadFile(file, "close Events timeline stream"); err != nil {
		return "", err
	}
	canonical, err := sorter.finalize(header, headerBytes)
	if err != nil {
		return "", err
	}
	return canonical, nil
}

func (s *contributionSorter) add(value filterContribution) error {
	const recordSize = uint64(8)
	if recordSize > s.budget {
		return fmt.Errorf("%w: filter contribution exceeds memory budget", ErrVerificationResource)
	}
	if len(s.batch) > 0 && uint64(len(s.batch)+1)*recordSize > s.budget {
		if err := s.spill(); err != nil {
			return err
		}
	}
	s.batch = append(s.batch, value)
	s.report.FilterContributionRecords++
	return nil
}

func (s *contributionSorter) spill() error {
	if len(s.batch) == 0 {
		return nil
	}
	if err := s.workspace.contextErr(); err != nil {
		return err
	}
	sort.Slice(s.batch, func(i, j int) bool { return contributionLess(s.batch[i], s.batch[j]) })
	if err := s.workspace.contextErr(); err != nil {
		return err
	}
	file, path, err := s.workspace.createBufferedFile("filter-spill")
	if err != nil {
		return err
	}
	var previous filterContribution
	havePrevious := false
	for index, value := range s.batch {
		if index%4096 == 0 {
			if err := s.workspace.contextErr(); err != nil {
				file.abort()
				s.workspace.remove(path)
				return err
			}
		}
		if havePrevious && value == previous {
			continue
		}
		if err := writeContribution(file, value); err != nil {
			file.abort()
			s.workspace.remove(path)
			return err
		}
		previous, havePrevious = value, true
	}
	if err := file.close(); err != nil {
		s.workspace.remove(path)
		return fmt.Errorf("%w: close filter contribution spill: %w", ErrVerificationResource, err)
	}
	s.runs = append(s.runs, path)
	s.report.FilterContributionSpills++
	s.batch = nil
	return nil
}

func contributionLess(left, right filterContribution) bool {
	if left.line != right.line {
		return left.line < right.line
	}
	return left.bit < right.bit
}

func writeContribution(file *scratchBufferedFile, value filterContribution) error {
	var encoded [8]byte
	binary.BigEndian.PutUint32(encoded[0:4], value.line)
	binary.BigEndian.PutUint16(encoded[4:6], value.bit)
	return file.write(encoded[:])
}

func readContribution(reader *bufio.Reader) (filterContribution, error) {
	var encoded [8]byte
	nRead, err := io.ReadFull(reader, encoded[:])
	if err != nil {
		if err == io.EOF && nRead == 0 {
			return filterContribution{}, io.EOF
		}
		return filterContribution{}, err
	}
	return filterContribution{line: binary.BigEndian.Uint32(encoded[0:4]), bit: binary.BigEndian.Uint16(encoded[4:6])}, nil
}

type contributionHeapItem struct {
	value filterContribution
	index int
}
type contributionHeap []contributionHeapItem

func (h contributionHeap) Len() int           { return len(h) }
func (h contributionHeap) Less(i, j int) bool { return contributionLess(h[i].value, h[j].value) }
func (h contributionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *contributionHeap) Push(value any)    { *h = append(*h, value.(contributionHeapItem)) }
func (h *contributionHeap) Pop() any {
	old := *h
	n := len(old)
	value := old[n-1]
	*h = old[:n-1]
	return value
}

func (s *contributionSorter) finalize(header FilterHeader, headerBytes []byte) (string, error) {
	if err := s.spill(); err != nil {
		return "", err
	}
	runs := slices.Clone(s.runs)
	if len(runs) == 0 {
		return "", fmt.Errorf("%w: no filter contributions were emitted", ErrVerificationResource)
	}
	if len(runs) > s.fanIn {
		for len(runs) > s.fanIn {
			var next []string
			for start := 0; start < len(runs); start += s.fanIn {
				end := min(start+s.fanIn, len(runs))
				merged, err := mergeContributionRuns(s.workspace, runs[start:end])
				if err != nil {
					return "", err
				}
				for _, path := range runs[start:end] {
					if err := s.workspace.remove(path); err != nil {
						return "", err
					}
				}
				next = append(next, merged)
			}
			runs = next
			s.report.FilterContributionMerges++
		}
	}
	file, path, err := s.workspace.createBufferedFile("canonical-filter")
	if err != nil {
		return "", err
	}
	if err := file.write(headerBytes); err != nil {
		file.abort()
		s.workspace.remove(path)
		return "", err
	}
	iterator, iteratorErr := newContributionRunIterator(s.workspace, runs)
	if iteratorErr != nil {
		file.abort()
		s.workspace.remove(path)
		return "", iteratorErr
	}
	if err := writeFilterPages(s.workspace, file, header, headerBytes, iterator); err != nil {
		closeErr := iterator.close()
		file.abort()
		s.workspace.remove(path)
		return "", errors.Join(err, closeErr)
	}
	if err := iterator.close(); err != nil {
		file.abort()
		s.workspace.remove(path)
		return "", err
	}
	for _, run := range runs {
		if err := s.workspace.remove(run); err != nil {
			file.abort()
			s.workspace.remove(path)
			return "", err
		}
	}
	if err := file.close(); err != nil {
		s.workspace.remove(path)
		return "", fmt.Errorf("%w: close canonical filter: %w", ErrVerificationResource, err)
	}
	return path, nil
}

type contributionIterator interface {
	next() (filterContribution, error)
}

func newContributionRunIterator(workspace *verificationWorkspace, paths []string) (*liveContributionRunIterator, error) {
	result := &liveContributionRunIterator{workspace: workspace, paths: paths, files: make([]*os.File, len(paths)), readers: make([]*bufio.Reader, len(paths))}
	for index, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("%w: open filter run: %w", ErrVerificationResource, err),
				result.close(),
			)
		}
		result.files[index] = file
		result.readers[index] = workspace.newReader(file)
		value, err := readContribution(result.readers[index])
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, errors.Join(scratchReadError("read filter run", err), result.close())
		}
		heap.Push(&result.queue, contributionHeapItem{value: value, index: index})
	}
	return result, nil
}

type liveContributionRunIterator struct {
	workspace *verificationWorkspace
	paths     []string
	files     []*os.File
	readers   []*bufio.Reader
	queue     contributionHeap
	last      filterContribution
	haveLast  bool
}

func (it *liveContributionRunIterator) next() (filterContribution, error) {
	for it.queue.Len() > 0 {
		item := heap.Pop(&it.queue).(contributionHeapItem)
		next, err := readContribution(it.readers[item.index])
		if err == nil {
			heap.Push(&it.queue, contributionHeapItem{value: next, index: item.index})
		} else if err != io.EOF {
			return filterContribution{}, scratchReadError("read filter merge input", err)
		}
		if it.haveLast && item.value == it.last {
			continue
		}
		it.last, it.haveLast = item.value, true
		return item.value, nil
	}
	return filterContribution{}, io.EOF
}
func (it *liveContributionRunIterator) close() error {
	return closeScratchReadFiles(it.files, "close filter merge input")
}

func mergeContributionRuns(workspace *verificationWorkspace, paths []string) (string, error) {
	it, err := newContributionRunIterator(workspace, paths)
	if err != nil {
		return "", err
	}
	file, path, err := workspace.createBufferedFile("filter-merge")
	if err != nil {
		return "", errors.Join(err, it.close())
	}
	for {
		value, nextErr := it.next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			file.abort()
			workspace.remove(path)
			return "", errors.Join(nextErr, it.close())
		}
		if err := writeContribution(file, value); err != nil {
			file.abort()
			workspace.remove(path)
			return "", errors.Join(err, it.close())
		}
	}
	if err := it.close(); err != nil {
		file.abort()
		workspace.remove(path)
		return "", err
	}
	if err := file.close(); err != nil {
		workspace.remove(path)
		return "", fmt.Errorf("%w: close filter merge: %w", ErrVerificationResource, err)
	}
	return path, nil
}

func writeFilterPages(workspace *verificationWorkspace, file *scratchBufferedFile, header FilterHeader, headerBytes []byte, input contributionIterator) error {
	var pageData [TimelineFilterPageDataBytes]byte
	var pending filterContribution
	var havePending bool
	advance := func() error {
		value, err := input.next()
		if err == io.EOF {
			havePending = false
			return nil
		}
		if err != nil {
			return err
		}
		pending, havePending = value, true
		return nil
	}
	if err := advance(); err != nil {
		return err
	}
	for page := uint32(0); page < header.PageCount; page++ {
		if err := workspace.contextErr(); err != nil {
			return err
		}
		clear(pageData[:])
		pageLineStart := page * uint32(header.LinesPerPage)
		pageLineEnd := min(uint64(pageLineStart)+uint64(header.LinesPerPage), uint64(header.LineCount))
		for havePending && uint64(pending.line) < pageLineEnd {
			if pending.line < pageLineStart || pending.bit >= TimelineFilterLineBytes*8 {
				return corruptRunf("invalid filter contribution line=%d bit=%d", pending.line, pending.bit)
			}
			relativeLine := pending.line - pageLineStart
			byteOffset := uint64(relativeLine)*TimelineFilterLineBytes + uint64(pending.bit/8)
			pageData[byteOffset] |= byte(1 << (pending.bit % 8))
			if err := advance(); err != nil {
				return err
			}
		}
		dataLength, ok := filterPageDataLength(header, page)
		if !ok {
			return corruptRunf("filter page %d has invalid geometry", page)
		}
		if err := file.write(pageData[:dataLength]); err != nil {
			return err
		}
		var checksum [TimelineFilterPageChecksumBytes]byte
		binary.BigEndian.PutUint32(checksum[:], filterPageCRC32C(headerBytes, page, pageData[:dataLength]))
		if err := file.write(checksum[:]); err != nil {
			return err
		}
	}
	if havePending {
		return corruptRunf("filter contribution lies beyond declared geometry")
	}
	return nil
}

type filterComparator struct {
	file      *os.File
	workspace *verificationWorkspace
	offset    uint64
	length    uint64
	buffer    []byte
}

func newFilterComparator(path string, workspace *verificationWorkspace) (*filterComparator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open canonical filter: %w", ErrVerificationResource, err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("%w: stat canonical filter: %w", ErrVerificationResource, err),
			closeScratchReadFile(file, "close canonical filter"),
		)
	}
	if info.Size() < 0 {
		return nil, errors.Join(
			fmt.Errorf("%w: canonical filter has a negative size", ErrVerificationResource),
			closeScratchReadFile(file, "close canonical filter"),
		)
	}
	return &filterComparator{file: file, workspace: workspace, length: uint64(info.Size())}, nil
}
func (c *filterComparator) consume(data []byte) error {
	if c.offset > c.length || uint64(len(data)) > c.length-c.offset {
		return corruptRunf("stored filter is longer than canonical filter")
	}
	if cap(c.buffer) < len(data) {
		c.buffer = make([]byte, len(data))
	}
	expected := c.buffer[:len(data)]
	n, err := c.file.ReadAt(expected, int64(c.offset))
	c.workspace.readBytes(uint64(max(n, 0)))
	if err != nil {
		return fmt.Errorf("%w: read canonical filter: %w", ErrVerificationResource, err)
	}
	if n != len(data) {
		return fmt.Errorf("%w: short canonical filter read: got %d bytes, want %d", ErrVerificationResource, n, len(data))
	}
	if !bytes.Equal(expected, data) {
		return corruptRunf("stored filter differs from canonical filter")
	}
	c.offset += uint64(len(data))
	return nil
}
func (c *filterComparator) finish() error {
	if c.offset != c.length {
		return corruptRunf("stored filter length %d, want canonical length %d", c.offset, c.length)
	}
	return nil
}
func (c *filterComparator) close() error {
	if c != nil && c.file != nil {
		if err := c.file.Close(); err != nil {
			return fmt.Errorf("%w: close canonical filter: %w", ErrVerificationResource, err)
		}
		c.file = nil
	}
	return nil
}
