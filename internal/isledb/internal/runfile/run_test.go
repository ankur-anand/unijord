package runfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/sstable"
)

type sliceEntryIterator struct {
	entries []Entry
	index   int
	err     error
}

func (i *sliceEntryIterator) Next() bool {
	if i.index >= len(i.entries) {
		return false
	}
	i.index++
	return true
}

func (i *sliceEntryIterator) Entry() Entry { return i.entries[i.index-1] }
func (i *sliceEntryIterator) Err() error   { return i.err }

type sliceTimelineIterator struct {
	timelines [][]byte
	index     int
	err       error
}

func (i *sliceTimelineIterator) Next() bool {
	if i.index >= len(i.timelines) {
		return false
	}
	i.index++
	return true
}

func (i *sliceTimelineIterator) Timeline() []byte { return i.timelines[i.index-1] }
func (i *sliceTimelineIterator) Err() error       { return i.err }

type memoryRangeSource struct {
	data     []byte
	requests [][2]int64
	short    bool
}

func (s *memoryRangeSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *memoryRangeSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	s.requests = append(s.requests, [2]int64{offset, length})
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	data := bytes.Clone(s.data[offset : offset+length])
	if s.short && len(data) > 0 {
		data = data[:len(data)-1]
	}
	return data, nil
}

func (s *memoryRangeSource) Stat(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{Size: uint64(len(s.data)), ETag: "memory-generation"}, nil
}

func (s *memoryRangeSource) OpenRange(_ context.Context, _ string, _ ObjectIdentity, offset, length uint64) (io.ReadCloser, error) {
	s.requests = append(s.requests, [2]int64{int64(offset), int64(length)})
	if offset > uint64(len(s.data)) || length > uint64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	data := bytes.Clone(s.data[offset : offset+length])
	if s.short && len(data) > 0 {
		data = data[:len(data)-1]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestBuildRecoverOpenAndVerify(t *testing.T) {
	for _, compression := range []TableCompression{TableCompressionNone, TableCompressionSnappy, TableCompressionZstd} {
		t.Run(compressionName(compression), func(t *testing.T) {
			options, input := validBuildFixture(compression)
			var first bytes.Buffer
			ref, err := Build(context.Background(), &first, options, input)
			if err != nil {
				t.Fatal(err)
			}
			options2, input2 := validBuildFixture(compression)
			var second bytes.Buffer
			secondRef, err := Build(context.Background(), &second, options2, input2)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Bytes(), second.Bytes()) || !sameRef(ref, secondRef) {
				t.Fatal("fixed build inputs did not produce deterministic bytes and reference")
			}
			if uint64(first.Len()) != ref.ObjectSize || ref.Events.Offset != PreambleBytes || ref.Heads.Offset%RegionAlignment != 0 || ref.TimelineFilter == nil {
				t.Fatalf("unexpected run layout: size=%d ref=%+v", first.Len(), ref)
			}

			source := &memoryRangeSource{data: first.Bytes()}
			recovered, err := Recover(context.Background(), source, "run")
			if err != nil {
				t.Fatal(err)
			}
			if !sameRef(ref, recovered) {
				t.Fatal("recovered reference differs from built reference")
			}
			if len(source.requests) < 3 || source.requests[0] != [2]int64{int64(ref.ObjectSize) - TrailerBytes, TrailerBytes} || source.requests[1] != [2]int64{int64(ref.DirectoryOffset), int64(ref.DirectoryLength)} || source.requests[2] != [2]int64{0, PreambleBytes} {
				t.Fatalf("recovery was not trailer-first: requests=%v", source.requests)
			}
			if err := Verify(context.Background(), source, "run", ref, VerifyStructural); err != nil {
				t.Fatal(err)
			}
			if err := Verify(context.Background(), source, "run", ref, VerifyComplete, fixtureTimelineExtractor); err != nil {
				t.Fatal(err)
			}

			for _, kind := range []RegionKind{RegionKindEventsSST, RegionKindHeadsSST} {
				reader, err := OpenTable(context.Background(), source, "run", ref, kind)
				if err != nil {
					t.Fatal(err)
				}
				format, err := reader.TableFormat()
				if err != nil {
					t.Fatal(err)
				}
				if format != sstable.TableFormatPebblev1 {
					t.Fatalf("table format=%s", format)
				}
				iterator, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
				if err != nil {
					t.Fatal(err)
				}
				var count int
				for kv := iterator.First(); kv != nil; kv = iterator.Next() {
					count++
				}
				if err := iterator.Close(); err != nil {
					t.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				if count != 3 {
					t.Fatalf("region kind %d entry count=%d", kind, count)
				}
			}
		})
	}
}

func TestBuildRejectsTimelineMismatchBeforeWriting(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	input.Heads.(*sliceEntryIterator).entries[0].Timeline = []byte("timeline-x")
	var destination bytes.Buffer
	if _, err := Build(context.Background(), &destination, options, input); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("error=%v, want invalid run", err)
	}
	if destination.Len() != 0 {
		t.Fatalf("destination received %d bytes before semantic validation", destination.Len())
	}
}

func TestBuildRejectsOversizedTimelineIdentityBeforeWriting(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	input.Events.(*sliceEntryIterator).entries[0].Timeline = bytes.Repeat([]byte{'x'}, int(MaxTimelineBytes+1))
	var destination bytes.Buffer
	if _, err := Build(context.Background(), &destination, options, input); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("error=%v, want run too large", err)
	}
	if destination.Len() != 0 {
		t.Fatalf("destination received %d bytes before size validation", destination.Len())
	}
}

func TestBuildAcceptsLargeSingleTimelineRun(t *testing.T) {
	options, _ := validBuildFixture(TableCompressionNone)
	options.SeqLo = 10
	options.SeqHi = 10
	largeValue := bytes.Repeat([]byte("unijord-large-value-"), 8<<10)
	input := BuildInput{
		Events: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-large|event-1"), Value: largeValue,
			Timeline: []byte("timeline-large"), Seq: 10,
		}}},
		Heads: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-large|head-1"), Value: largeValue,
			Timeline: []byte("timeline-large"), Seq: 10,
		}}},
		Timelines: &sliceTimelineIterator{timelines: [][]byte{[]byte("timeline-large")}},
	}
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	const nominalTarget = 128 << 10
	if destination.Len() <= nominalTarget {
		t.Fatalf("single-timeline run size=%d, want greater than nominal target %d", destination.Len(), nominalTarget)
	}
	if !bytes.Equal(ref.MinTimeline, []byte("timeline-large")) || !bytes.Equal(ref.MaxTimeline, []byte("timeline-large")) {
		t.Fatalf("single-timeline bounds=%q..%q", ref.MinTimeline, ref.MaxTimeline)
	}
	if err := Verify(context.Background(), &memoryRangeSource{data: destination.Bytes()}, "run", ref, VerifyComplete, fixtureTimelineExtractor); err != nil {
		t.Fatal(err)
	}
}

func TestBuildFilterScratchMatchesCanonicalEncoding(t *testing.T) {
	timelines := make([][]byte, 3300)
	for i := range timelines {
		timelines[i] = []byte(fmt.Sprintf("timeline-%04d", i))
	}
	runID := testRunID()
	filter, err := buildFilterScratch(context.Background(), runID, timelines, FilterOptions{}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer filter.cleanup()
	actual, err := io.ReadAll(filter.file)
	if err != nil {
		t.Fatal(err)
	}
	expected, expectedHeader, err := BuildTimelineFilter(runID, timelines, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatal("scratch-built filter differs from canonical in-memory encoding")
	}
	if filter.header != expectedHeader || filter.length != uint64(len(expected)) || filter.hash != sha256.Sum256(expected) {
		t.Fatal("scratch-built filter metadata differs from canonical encoding")
	}
}

func TestVerifyCompleteDetectsRegionAndPaddingCorruption(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		region RegionDescriptor
	}{
		{name: "events", region: ref.Events},
		{name: "heads", region: ref.Heads},
		{name: "filter", region: ref.TimelineFilter.Region},
	} {
		t.Run(test.name, func(t *testing.T) {
			region := test.region
			corrupt := bytes.Clone(destination.Bytes())
			corrupt[region.Offset+region.Length/2] ^= 1
			err := Verify(context.Background(), &memoryRangeSource{data: corrupt}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
			if !errors.Is(err, ErrCorruptRun) {
				t.Fatalf("error=%v, want corrupt run", err)
			}
		})
	}

	regions := refRegions(ref)
	cursor := uint64(PreambleBytes)
	foundPadding := false
	for _, region := range regions {
		if cursor < region.Offset {
			foundPadding = true
			corrupt := bytes.Clone(destination.Bytes())
			corrupt[cursor] = 1
			err := Verify(context.Background(), &memoryRangeSource{data: corrupt}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
			if !errors.Is(err, ErrCorruptRun) {
				t.Fatalf("padding error=%v, want corrupt run", err)
			}
		}
		cursor = region.Offset + region.Length
	}
	if cursor < ref.DirectoryOffset {
		foundPadding = true
		corrupt := bytes.Clone(destination.Bytes())
		corrupt[cursor] = 1
		err := Verify(context.Background(), &memoryRangeSource{data: corrupt}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
		if !errors.Is(err, ErrCorruptRun) {
			t.Fatalf("directory padding error=%v, want corrupt run", err)
		}
	}
	if !foundPadding {
		t.Fatal("fixture unexpectedly contains no alignment padding")
	}
}

func TestVerifyCompleteDetectsSemanticTableMismatch(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	heads := input.Heads.(*sliceEntryIterator)
	// The logical layer lies to Build about the first head's classified
	// timeline. The persisted key still names timeline-z, so a complete scan
	// with the real classifier must reject the otherwise valid container.
	heads.entries[0].Key = []byte("timeline-z|head-1")
	slices.SortFunc(heads.entries, func(left, right Entry) int { return bytes.Compare(left.Key, right.Key) })
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	err = Verify(context.Background(), &memoryRangeSource{data: destination.Bytes()}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
	if !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corrupt run", err)
	}
}

func TestVerifyCompleteRejectsLogicalEntryMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*sliceEntryIterator)
	}{
		{
			name: "wrong table kind",
			mutate: func(heads *sliceEntryIterator) {
				heads.entries[0].Key = []byte("timeline-a|event-0")
				slices.SortFunc(heads.entries, func(left, right Entry) int { return bytes.Compare(left.Key, right.Key) })
			},
		},
		{
			name: "invalid empty head value",
			mutate: func(heads *sliceEntryIterator) {
				heads.entries[0].Value = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options, input := validBuildFixture(TableCompressionSnappy)
			test.mutate(input.Heads.(*sliceEntryIterator))
			var destination bytes.Buffer
			ref, err := Build(context.Background(), &destination, options, input)
			if err != nil {
				t.Fatal(err)
			}
			err = Verify(context.Background(), &memoryRangeSource{data: destination.Bytes()}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
			if !errors.Is(err, ErrCorruptRun) {
				t.Fatalf("error=%v, want corrupt run", err)
			}
		})
	}
}

func TestRecoverRejectsFilterHeaderOutsideDeclaredRegionWithoutReadingIt(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	object := bytes.Clone(destination.Bytes())
	directory := object[ref.DirectoryOffset : ref.DirectoryOffset+ref.DirectoryLength]
	filterDescriptor := int(DirectoryHeaderBytes + 2*RegionDescriptorBytes)
	binary.BigEndian.PutUint64(directory[filterDescriptor+16:filterDescriptor+24], 1)
	directoryHash := sha256.Sum256(directory)
	trailer := Trailer{
		DirectoryOffset: ref.DirectoryOffset,
		DirectoryLength: ref.DirectoryLength,
		ObjectSize:      ref.ObjectSize,
		RegionCount:     3,
		DirectoryHash:   directoryHash,
		PayloadHash:     ref.PayloadHash,
		RunID:           ref.RunID,
	}
	trailerBytes, err := MarshalTrailer(trailer)
	if err != nil {
		t.Fatal(err)
	}
	copy(object[len(object)-TrailerBytes:], trailerBytes)
	source := &memoryRangeSource{data: object}
	_, err = Recover(context.Background(), source, "run")
	if !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corrupt run", err)
	}
	if len(source.requests) != 3 {
		t.Fatalf("recovery requests=%v, want only trailer, directory, and preamble", source.requests)
	}
	for _, request := range source.requests {
		if request == [2]int64{int64(ref.TimelineFilter.Region.Offset), TimelineFilterHeaderBytes} {
			t.Fatal("recovery read a filter header beyond the declared filter region")
		}
	}
}

func TestVerifyCompletePreservesTransportError(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	transport := errors.New("temporary object-store failure")
	source := &failRepeatedRegionSource{
		data:      destination.Bytes(),
		region:    ref.Events,
		failAfter: 2,
		err:       transport,
	}
	err = Verify(context.Background(), source, "run", ref, VerifyComplete, fixtureTimelineExtractor)
	if !errors.Is(err, transport) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("complete-verifier transport error classification=%v", err)
	}
}

func TestRecoverRejectsShortSuccessfulRead(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	var destination bytes.Buffer
	if _, err := Build(context.Background(), &destination, options, input); err != nil {
		t.Fatal(err)
	}
	_, err := Recover(context.Background(), &memoryRangeSource{data: destination.Bytes(), short: true}, "run")
	if !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("error=%v, want corrupt run", err)
	}
}

func TestVerifyCompleteRequiresLogicalClassifier(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	var destination bytes.Buffer
	ref, err := Build(context.Background(), &destination, options, input)
	if err != nil {
		t.Fatal(err)
	}
	err = Verify(context.Background(), &memoryRangeSource{data: destination.Bytes()}, "run", ref, VerifyComplete)
	if !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("error=%v, want invalid run", err)
	}
}

func validBuildFixture(compression TableCompression) (BuildOptions, BuildInput) {
	var options BuildOptions
	fillBytes(options.RunID[:], 0x11)
	fillBytes(options.NamespaceHash[:], 0x22)
	fillBytes(options.PublicationHash[:], 0x33)
	options.CreatorRole = CreatorRoleWriterFlush
	options.CreatorEpoch = 7
	options.SeqLo = 10
	options.SeqHi = 30
	options.Shard = 4
	options.Table.Compression = compression
	events := []Entry{
		{Key: []byte("timeline-a|event-1"), Value: []byte("event-a1"), Timeline: []byte("timeline-a"), Seq: 10},
		{Key: []byte("timeline-a|event-2"), Value: []byte("event-a2"), Timeline: []byte("timeline-a"), Seq: 20},
		{Key: []byte("timeline-b|event-1"), Value: []byte("event-b1"), Timeline: []byte("timeline-b"), Seq: 30},
	}
	heads := []Entry{
		{Key: []byte("timeline-a|head-1"), Value: []byte("head-a1"), Timeline: []byte("timeline-a"), Seq: 10},
		{Key: []byte("timeline-a|head-2"), Value: []byte("head-a2"), Timeline: []byte("timeline-a"), Seq: 20},
		{Key: []byte("timeline-b|head-1"), Value: []byte("head-b1"), Timeline: []byte("timeline-b"), Seq: 30},
	}
	return options, BuildInput{
		Events:    &sliceEntryIterator{entries: events},
		Heads:     &sliceEntryIterator{entries: heads},
		Timelines: &sliceTimelineIterator{timelines: [][]byte{[]byte("timeline-b"), []byte("timeline-a"), []byte("timeline-a")}},
	}
}

func fixtureTimelineExtractor(run Ref, kind RegionKind, key, value []byte, sequence uint64) ([]byte, error) {
	if run.Shard != 4 {
		return nil, fmt.Errorf("unexpected shard %d", run.Shard)
	}
	for _, value := range run.NamespaceHash {
		if value != 0x22 {
			return nil, errors.New("unexpected namespace hash")
		}
	}
	marker := []byte("|event-")
	if kind == RegionKindHeadsSST {
		marker = []byte("|head-")
	}
	if !bytes.Contains(key, marker) {
		return nil, fmt.Errorf("key does not belong to region kind %d", kind)
	}
	if len(value) == 0 {
		return nil, errors.New("empty logical value")
	}
	if sequence < run.SeqLo || sequence > run.SeqHi {
		return nil, fmt.Errorf("sequence %d outside run bounds", sequence)
	}
	separator := bytes.IndexByte(key, '|')
	if separator <= 0 {
		return nil, errors.New("missing timeline separator")
	}
	return key[:separator], nil
}

func compressionName(compression TableCompression) string {
	switch compression {
	case TableCompressionNone:
		return "none"
	case TableCompressionSnappy:
		return "snappy"
	case TableCompressionZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

type failRepeatedRegionSource struct {
	data      []byte
	region    RegionDescriptor
	failAfter int
	reads     int
	err       error
}

func (s *failRepeatedRegionSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *failRepeatedRegionSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	regionStart := int64(s.region.Offset)
	regionEnd := regionStart + int64(s.region.Length)
	if offset >= regionStart && length >= 0 && offset <= regionEnd && length <= regionEnd-offset {
		s.reads++
		if s.reads >= s.failAfter {
			return nil, s.err
		}
	}
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func (s *failRepeatedRegionSource) Stat(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{Size: uint64(len(s.data)), ETag: "failing-generation"}, nil
}

func (s *failRepeatedRegionSource) OpenRange(context.Context, string, ObjectIdentity, uint64, uint64) (io.ReadCloser, error) {
	return nil, s.err
}
