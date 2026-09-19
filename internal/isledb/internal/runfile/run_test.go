package runfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
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

type sliceTimelineCatalog struct {
	timelines [][]byte
}

func (i *sliceTimelineCatalog) Len() int                      { return len(i.timelines) }
func (i *sliceTimelineCatalog) Timeline(id TimelineID) []byte { return i.timelines[id] }

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
				want := 3
				if kind == RegionKindHeadsSST {
					want = 2
				}
				if count != want {
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
		Timelines: &sliceTimelineCatalog{timelines: [][]byte{[]byte("timeline-large")}},
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
	options.MaxTimelines = 2
	events := []Entry{
		{Key: []byte("timeline-a|event-1"), Value: []byte("event-a1"), Timeline: []byte("timeline-a"), Seq: 10},
		{Key: []byte("timeline-a|event-2"), Value: []byte("event-a2"), Timeline: []byte("timeline-a"), Seq: 20},
		{Key: []byte("timeline-b|event-1"), Value: []byte("event-b1"), Timeline: []byte("timeline-b"), TimelineID: 1, Seq: 30},
	}
	heads := []Entry{
		{Key: []byte("timeline-a|head-2"), Value: []byte("head-a2"), Timeline: []byte("timeline-a"), Seq: 20},
		{Key: []byte("timeline-b|head-1"), Value: []byte("head-b1"), Timeline: []byte("timeline-b"), TimelineID: 1, Seq: 30},
	}
	return options, BuildInput{
		Events:    &sliceEntryIterator{entries: events},
		Heads:     &sliceEntryIterator{entries: heads},
		Timelines: &sliceTimelineCatalog{timelines: [][]byte{[]byte("timeline-a"), []byte("timeline-b")}},
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

type preparedTestWriter func([]byte) (int, error)

func (f preparedTestWriter) Write(b []byte) (int, error) { return f(b) }

func requireEmptyScratch(t testing.TB, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch not empty: %v, %v", entries, err)
	}
}

func requirePreparedClosed(t *testing.T, p PreparedRun, regions [3]preparedRegion, dir string) {
	t.Helper()
	for range 3 {
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.WriteTo(context.Background(), io.Discard); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	if !sameRef(p.Ref(), Ref{}) {
		t.Fatal("reference after close")
	}
	for _, r := range regions {
		if _, err := r.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("descriptor not closed: %v", err)
		}
	}
	requireEmptyScratch(t, dir)
}

func TestPreparedCompatibilityAndOwnership(t *testing.T) {
	for _, c := range corpusCases {
		t.Run(c.Name, func(t *testing.T) {
			opts, input, _ := corpusFixture(t, c)
			opts.ScratchDir = t.TempDir()
			p, err := Prepare(context.Background(), opts, input)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			ref := p.Ref()
			if err := ref.Validate(); err != nil {
				t.Fatal(err)
			}
			// All input storage can be reused immediately after Prepare.
			for _, iterator := range []*sliceEntryIterator{input.Events.(*sliceEntryIterator), input.Heads.(*sliceEntryIterator)} {
				for _, entry := range iterator.entries {
					clear(entry.Key)
					clear(entry.Value)
					clear(entry.Timeline)
				}
			}
			changed := p.Ref()
			clear(changed.MinTimeline)
			clear(changed.MaxTimeline)
			clear(changed.Events.MinKey)
			clear(changed.Events.MaxKey)
			clear(changed.Heads.MinKey)
			clear(changed.Heads.MaxKey)
			changed.TimelineFilter.Header.KeyCount = 0
			frozen, err := os.ReadFile(filepath.Join(compatDir, c.Name+".run"))
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				var dst bytes.Buffer
				if err := p.WriteTo(context.Background(), &dst); err != nil {
					t.Fatal(err)
				}
				if !sameRef(ref, p.Ref()) {
					t.Fatal("reference changed")
				}
				built := dst.Bytes()
				if c.Padding {
					// This E00 reader fixture deliberately has noncanonical extra
					// padding; apply its unchanged fixture transform, as in E00.
					built, _ = paddedCorpus(t, built, ref)
				}
				if !bytes.Equal(built, frozen) {
					t.Fatal("E00 complete-run byte drift")
				}
			}
			requirePreparedClosed(t, p, p.(*preparedRun).regions, opts.ScratchDir)
		})
	}
}

// Exercise valid padding before every region (including after the preamble).
// Canonical Prepare uses minimal padding; E00 also pins this larger layout.
func padPreparedForTest(t *testing.T, p *preparedRun) {
	t.Helper()
	var dst bytes.Buffer
	if err := p.WriteTo(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	object, ref := paddedCorpus(t, dst.Bytes(), p.Ref())
	p.ref = ref
	p.directoryOffset = ref.DirectoryOffset
	p.directory = bytes.Clone(object[ref.DirectoryOffset : ref.DirectoryOffset+ref.DirectoryLength])
	p.trailer = bytes.Clone(object[len(object)-TrailerBytes:])
	for i, r := range []RegionDescriptor{ref.Events, ref.Heads, ref.TimelineFilter.Region} {
		p.regions[i].offset = r.Offset
	}
}

func TestPreparedWriteFailuresAndCancellationAtEveryBoundary(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionNone)
	// Include multiple reads inside Events, as well as every region boundary.
	input.Events.(*sliceEntryIterator).entries[0].Value = bytes.Repeat([]byte{0xa5}, 300<<10)
	opts.ScratchDir = t.TempDir()
	prepared, err := Prepare(context.Background(), opts, input)
	if err != nil {
		t.Fatal(err)
	}
	p := prepared.(*preparedRun)
	t.Cleanup(func() { _ = p.Close() })
	padPreparedForTest(t, p)
	var want bytes.Buffer
	var chunks []int
	if err := p.WriteTo(context.Background(), preparedTestWriter(func(b []byte) (int, error) {
		chunks = append(chunks, len(b))
		return want.Write(b)
	})); err != nil {
		t.Fatal(err)
	}
	// Preamble, >=4 padding writes, three regions, directory, trailer.
	if len(chunks) < 10 {
		t.Fatalf("incomplete boundary coverage: %v", chunks)
	}
	ref := p.Ref()
	failure := errors.New("injected destination failure")
	for boundary := range chunks {
		for _, mode := range []string{"cancel", "error", "short", "zero", "negative", "overcount"} {
			t.Run(fmt.Sprintf("chunk=%d/%s", boundary, mode), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				call := 0
				wantErr := io.ErrShortWrite
				if mode == "cancel" {
					wantErr = context.Canceled
				} else if mode == "error" {
					wantErr = failure
				}
				err := p.WriteTo(ctx, preparedTestWriter(func(b []byte) (int, error) {
					current := call
					call++
					if current != boundary {
						return len(b), nil
					}
					switch mode {
					case "cancel":
						cancel()
						return len(b), nil
					case "error":
						return len(b) / 2, failure
					case "short":
						return len(b) - 1, nil
					case "zero":
						return 0, nil
					case "negative":
						return -1, nil
					default:
						return len(b) + 1, nil
					}
				}))
				if !errors.Is(err, wantErr) || call != boundary+1 {
					t.Fatalf("failure=%v calls=%d, want %v/%d", err, call, wantErr, boundary+1)
				}
				var retry bytes.Buffer
				if err := p.WriteTo(context.Background(), &retry); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(retry.Bytes(), want.Bytes()) || !sameRef(ref, p.Ref()) {
					t.Fatal("retry drift")
				}
			})
		}
	}
	if err := p.WriteTo(nil, io.Discard); !errors.Is(err, ErrInvalidRun) {
		t.Fatal(err)
	}
	if err := p.WriteTo(context.Background(), nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatal(err)
	}
	requirePreparedClosed(t, p, p.regions, opts.ScratchDir)
}

func TestPreparedCloseLifecycle(t *testing.T) {
	for _, state := range []string{"before-write", "after-write", "after-failure", "scratch-read-failure"} {
		t.Run(state, func(t *testing.T) {
			opts, input := validBuildFixture(TableCompressionSnappy)
			opts.ScratchDir = t.TempDir()
			p, err := Prepare(context.Background(), opts, input)
			if err != nil {
				t.Fatal(err)
			}
			regions := p.(*preparedRun).regions
			switch state {
			case "after-write":
				if err := p.WriteTo(context.Background(), io.Discard); err != nil {
					t.Fatal(err)
				}
			case "after-failure":
				if err := p.WriteTo(context.Background(), preparedTestWriter(func([]byte) (int, error) { return 0, io.ErrClosedPipe })); !errors.Is(err, io.ErrClosedPipe) {
					t.Fatal(err)
				}
			case "scratch-read-failure":
				if err := regions[1].file.Truncate(0); err != nil {
					t.Fatal(err)
				}
				if err := p.WriteTo(context.Background(), io.Discard); !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			}
			requirePreparedClosed(t, p, regions, opts.ScratchDir)
		})
	}
}

func TestPreparedConcurrentWritesAndClose(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionSnappy)
	opts.ScratchDir = t.TempDir()
	p, err := Prepare(context.Background(), opts, input)
	if err != nil {
		t.Fatal(err)
	}
	regions := p.(*preparedRun).regions
	var want bytes.Buffer
	if err := p.WriteTo(context.Background(), &want); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst bytes.Buffer
			if err := p.WriteTo(context.Background(), &dst); err != nil || !bytes.Equal(dst.Bytes(), want.Bytes()) {
				t.Errorf("concurrent write: %v", err)
			}
		}()
	}
	wg.Wait()
	entered, release := make(chan struct{}), make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		first := true
		writeDone <- p.WriteTo(context.Background(), preparedTestWriter(func(b []byte) (int, error) {
			if first {
				first = false
				close(entered)
				<-release
			}
			return len(b), nil
		}))
	}()
	<-entered
	started := make(chan struct{}, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			if err := p.Close(); err != nil {
				t.Errorf("concurrent close: %v", err)
			}
		}()
	}
	for range 8 {
		<-started
	}
	// No sleeps: the writer barrier holds the lifecycle lock while Close races.
	for _, r := range regions {
		if _, err := r.file.Stat(); err != nil {
			t.Fatal("closed during write", err)
		}
	}
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	requirePreparedClosed(t, p, regions, opts.ScratchDir)
}

type preparedClosingEntries struct {
	*sliceEntryIterator
	closes   int
	closeErr error
	onNext   func()
}

func (i *preparedClosingEntries) Next() bool {
	if i.onNext != nil {
		i.onNext()
	}
	return i.sliceEntryIterator.Next()
}
func (i *preparedClosingEntries) Close() error { i.closes++; return i.closeErr }

type preparedCatalogProbe struct {
	*sliceTimelineCatalog
	closes   int
	closeErr error
	onNext   func()
}

func (i *preparedCatalogProbe) Len() int {
	if i.onNext != nil {
		i.onNext()
	}
	return i.sliceTimelineCatalog.Len()
}
func (i *preparedCatalogProbe) Close() error { i.closes++; return i.closeErr }

func TestPrepareFailureCleanup(t *testing.T) {
	failure := errors.New("injected iterator error")
	for _, mode := range []string{"success", "nil-context", "nil-events", "preamble", "compression", "filter", "scratch-create", "empty-events", "empty-heads", "empty-timelines", "empty-key", "oversized-key", "oversized-timeline", "order", "sequence", "mismatch", "events-error", "heads-error", "catalog-hole", "events-close", "heads-close", "canceled", "cancel-events", "cancel-heads", "cancel-timelines"} {
		t.Run(mode, func(t *testing.T) {
			opts, input := validBuildFixture(TableCompressionSnappy)
			dir := t.TempDir()
			opts.ScratchDir = dir
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := &preparedClosingEntries{sliceEntryIterator: input.Events.(*sliceEntryIterator)}
			h := &preparedClosingEntries{sliceEntryIterator: input.Heads.(*sliceEntryIterator)}
			tl := &preparedCatalogProbe{sliceTimelineCatalog: input.Timelines.(*sliceTimelineCatalog)}
			input = BuildInput{e, h, tl}
			switch mode {
			case "nil-context":
				ctx = nil
			case "nil-events":
				input.Events = nil
			case "preamble":
				opts.CreatorEpoch = 0
			case "compression":
				opts.Table.Compression = 255
			case "filter":
				opts.Filter.BitsPerKey = 33
			case "scratch-create":
				opts.ScratchDir = filepath.Join(dir, "missing")
			case "empty-events":
				e.entries = nil
			case "empty-heads":
				h.entries = nil
			case "empty-timelines":
				tl.timelines = nil
			case "empty-key":
				e.entries[0].Key = nil
			case "oversized-key":
				e.entries[0].Key = make([]byte, MaxTableKeyBytes+1)
			case "oversized-timeline":
				e.entries[0].Timeline = make([]byte, MaxTimelineBytes+1)
			case "order":
				e.entries[0], e.entries[2] = e.entries[2], e.entries[0]
			case "sequence":
				e.entries[0].Seq = opts.SeqHi + 1
			case "mismatch":
				h.entries[0].Timeline = []byte("different")
			case "events-error":
				e.err = failure
			case "heads-error":
				h.err = failure
			case "catalog-hole":
				tl.timelines[0] = nil
			case "events-close":
				e.closeErr = failure
			case "heads-close":
				h.closeErr = failure
			case "canceled":
				cancel()
			case "cancel-events":
				e.onNext = cancel
			case "cancel-heads":
				h.onNext = cancel
			case "cancel-timelines":
				tl.onNext = cancel
			}
			p, err := Prepare(ctx, opts, input)
			if mode == "success" {
				if err != nil {
					t.Fatal(err)
				}
				requirePreparedClosed(t, p, p.(*preparedRun).regions, dir)
			} else if err == nil || p != nil {
				t.Fatalf("failed preparation returned %v, %v", p, err)
			}
			if (mode != "nil-events" && e.closes != 1) || h.closes != 1 || tl.closes != 0 {
				t.Fatalf("iterator close counts: %d/%d/%d", e.closes, h.closes, tl.closes)
			}
			requireEmptyScratch(t, dir)
		})
	}
}

func TestBuildFailureReturnsNoRef(t *testing.T) {
	for _, mode := range []string{"short", "trailer-cancel", "nil-destination"} {
		t.Run(mode, func(t *testing.T) {
			opts, input := validBuildFixture(TableCompressionSnappy)
			opts.ScratchDir = t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var dst io.Writer = preparedTestWriter(func(b []byte) (int, error) {
				if mode == "short" {
					return len(b) - 1, nil
				}
				if len(b) == TrailerBytes && string(b[:4]) == "UJRT" {
					cancel()
				}
				return len(b), nil
			})
			if mode == "nil-destination" {
				dst = nil
			}
			ref, err := Build(ctx, dst, opts, input)
			if err == nil || !sameRef(ref, Ref{}) {
				t.Fatalf("ref=%v err=%v", ref, err)
			}
			requireEmptyScratch(t, opts.ScratchDir)
		})
	}
}

// Cancel at each synchronous context observation, including UJTF construction,
// the payload-hash pass, and the final check after closing the inputs.
type preparedStepContext struct {
	context.Context
	calls, cancelAt int
}

func (c *preparedStepContext) Err() error {
	c.calls++
	if c.cancelAt > 0 && c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestPrepareCancellationAtEveryCheck(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionNone)
	opts.ScratchDir = t.TempDir()
	ctx := &preparedStepContext{Context: context.Background()}
	p, err := Prepare(ctx, opts, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for check := 1; check <= ctx.calls; check++ {
		_, input := validBuildFixture(TableCompressionNone)
		canceled := &preparedStepContext{Context: context.Background(), cancelAt: check}
		p, err := Prepare(canceled, opts, input)
		if !errors.Is(err, context.Canceled) || p != nil {
			t.Fatalf("context check %d: prepared=%v err=%v", check, p, err)
		}
		requireEmptyScratch(t, opts.ScratchDir)
	}
}

func TestPreparedCleanupClosesDescriptorsOnFailure(t *testing.T) {
	// Descriptor-count verification complements direct file.Stat checks on
	// successful preparation. It catches unlinked-but-open failed scratch files.
	fdDir := "/dev/fd"
	if _, err := os.ReadDir(fdDir); err != nil {
		fdDir = "/proc/self/fd"
	}
	count := func() int {
		entries, err := os.ReadDir(fdDir)
		if err != nil {
			t.Skipf("descriptor inventory unavailable: %v", err)
		}
		return len(entries)
	}
	opts, _ := validBuildFixture(TableCompressionSnappy)
	opts.ScratchDir = t.TempDir()
	run := func() {
		for _, stage := range []string{"events", "heads", "timelines", "close"} {
			_, input := validBuildFixture(TableCompressionSnappy)
			switch stage {
			case "events":
				input.Events.(*sliceEntryIterator).err = io.ErrUnexpectedEOF
			case "heads":
				input.Heads.(*sliceEntryIterator).err = io.ErrUnexpectedEOF
			case "timelines":
				input.Timelines.(*sliceTimelineCatalog).timelines[0] = nil
			case "close":
				input.Events = &preparedClosingEntries{sliceEntryIterator: input.Events.(*sliceEntryIterator), closeErr: io.ErrUnexpectedEOF}
			}
			if p, err := Prepare(context.Background(), opts, input); err == nil || p != nil {
				t.Fatalf("expected failed preparation: %v, %v", p, err)
			}
		}
	}
	run() // Warm runtime/Pebble descriptors before taking the baseline.
	before := count()
	for range 16 {
		run()
	}
	if after := count(); after != before {
		t.Fatalf("descriptor leak: before=%d after=%d", before, after)
	}
	requireEmptyScratch(t, opts.ScratchDir)
}

func TestPreparedCleanupErrorStillClosesOtherRegions(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionSnappy)
	opts.ScratchDir = t.TempDir()
	p, err := Prepare(context.Background(), opts, input)
	if err != nil {
		t.Fatal(err)
	}
	regions := p.(*preparedRun).regions
	// Simulate an already-broken descriptor: Close must report it, still remove
	// its file, and close/remove every other region exactly once.
	if err := regions[0].file.Close(); err != nil {
		t.Fatal(err)
	}
	first := p.Close()
	if !errors.Is(first, os.ErrClosed) || p.Close() != first {
		t.Fatalf("cleanup error not stable: %v", first)
	}
	for _, r := range regions {
		if _, err := r.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal(err)
		}
	}
	requireEmptyScratch(t, opts.ScratchDir)
}

func TestPrepareScratchBoundsBeforeAdmission(t *testing.T) {
	opts, input := validBuildFixture(TableCompressionNone)
	opts.ScratchDir = t.TempDir()
	options, err := tableWriterOptions(opts.Table)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := validateCatalog(context.Background(), input.Timelines, opts.MaxTimelines, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildTableWithin(context.Background(), input.Events, options, opts, "bounded", 1, catalog, observedEvents); !errors.Is(err, ErrRunTooLarge) {
		t.Fatal("SST limit", err)
	}
	requireEmptyScratch(t, opts.ScratchDir)
	if _, err := buildFilterScratchWithin(context.Background(), opts.RunID, catalog, opts.Filter, opts.ScratchDir, 1); !errors.Is(err, ErrRunTooLarge) {
		t.Fatal("filter limit", err)
	}
	requireEmptyScratch(t, opts.ScratchDir)
	file, err := os.CreateTemp(opts.ScratchDir, "bounded")
	if err != nil {
		t.Fatal(err)
	}
	w := newScratchWritable(file)
	w.limit = 3
	if err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write([]byte("d")); !errors.Is(err, ErrRunTooLarge) {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != 3 {
		t.Fatalf("write exceeded budget: %v, %v", info, err)
	}
	if err := closeScratch(file, file.Name()); err != nil {
		t.Fatal(err)
	}
	requireEmptyScratch(t, opts.ScratchDir)
}

// Low-level fixture helpers also use the catalog validation path. They do not
// change any encoded SST/filter input bytes.
func buildFilterScratch(ctx context.Context, runID [RunIDBytes]byte, timelines [][]byte, options FilterOptions, scratchDir string) (*builtFilter, error) {
	catalog, err := validateCatalog(ctx, &sliceTimelineCatalog{timelines: timelines}, uint32(len(timelines)), nil)
	if err != nil {
		return nil, err
	}
	return buildFilterScratchWithin(ctx, runID, catalog, options, scratchDir, MaxRunObjectBytes)
}
