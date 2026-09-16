package runfile

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCompleteVerificationSpillRecordsRejectTruncation(t *testing.T) {
	if _, err := readTimelineRecord(bufio.NewReader(bytes.NewReader([]byte{0, 3, 'a'}))); err == nil {
		t.Fatal("truncated timeline spill record accepted")
	}
	if _, err := readContribution(bufio.NewReader(bytes.NewReader(make([]byte, 7)))); err == nil {
		t.Fatal("truncated filter contribution accepted")
	}
}

func TestVerifyCompleteStreamingUsesOneSuffixAndLocalPebble(t *testing.T) {
	options, _ := validBuildFixture(TableCompressionNone)
	largeValue := bytes.Repeat([]byte("streaming-value-"), 256<<10)
	input := BuildInput{
		Events: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-large|event-1"), Value: largeValue,
			Timeline: []byte("timeline-large"), Seq: options.SeqLo,
		}}},
		Heads: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-large|head-1"), Value: largeValue,
			Timeline: []byte("timeline-large"), Seq: options.SeqLo,
		}}},
		Timelines: &sliceTimelineIterator{timelines: [][]byte{[]byte("timeline-large")}},
	}
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "generation-a"}
	report, err := VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if err != nil {
		t.Fatal(err)
	}
	if report.ProviderGETs != 4 || report.ProviderGETAttempts != 4 {
		t.Fatalf("GET report=%+v, want suffix plus three region streams", report)
	}
	if len(source.ranges()) != 4 {
		t.Fatalf("provider ranges=%v, want four requests", source.ranges())
	}
	ranges := source.ranges()
	if ranges[0][1] != int64(min(ref.ObjectSize, uint64(MaxDirectoryBytes)+TrailerBytes)) {
		t.Fatalf("suffix range=%v", ranges[0])
	}
	if ranges[1] != [2]int64{0, int64(ref.Events.Offset + ref.Events.Length)} {
		t.Fatalf("Events range=%v", ranges[1])
	}
	if ranges[2] != [2]int64{int64(ref.Events.Offset + ref.Events.Length), int64(ref.Heads.Offset + ref.Heads.Length - ref.Events.Offset - ref.Events.Length)} {
		t.Fatalf("Heads range=%v", ranges[2])
	}
	if ranges[3] != [2]int64{int64(ref.Heads.Offset + ref.Heads.Length), int64(ref.DirectoryOffset - ref.Heads.Offset - ref.Heads.Length)} {
		t.Fatalf("filter range=%v", ranges[3])
	}
	if source.statCalls != 1 {
		t.Fatalf("stat calls=%d, want one", source.statCalls)
	}
	if report.ScratchHighWater == 0 || report.ScratchBytesDeleted == 0 {
		t.Fatalf("scratch report=%+v, want non-zero and cleaned", report)
	}
	if report.ScratchBytesDeleted != report.ScratchBytesWritten {
		t.Fatalf("scratch written=%d deleted=%d, want exact cleanup accounting", report.ScratchBytesWritten, report.ScratchBytesDeleted)
	}
	if report.ScratchBytesRead == 0 {
		t.Fatalf("scratch report=%+v, want tracked reads", report)
	}
}

func TestVerifyCompleteStreamingSmallObjectReusesSuffix(t *testing.T) {
	options, input := validBuildFixture(TableCompressionSnappy)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "small-a"}
	report, err := VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if err != nil {
		t.Fatal(err)
	}
	if report.ProviderGETs != 1 || len(source.ranges()) != 1 {
		t.Fatalf("small-object report=%+v ranges=%v, want one suffix GET", report, source.ranges())
	}
	routed := &testStreamingSource{data: object.Bytes(), etag: "routed-a"}
	if err := Verify(context.Background(), routed, "run", ref, VerifyComplete, fixtureTimelineExtractor); err != nil {
		t.Fatalf("Verify did not route streaming source: %v", err)
	}
	if routed.statCalls != 1 || len(routed.ranges()) != 1 {
		t.Fatalf("routed source stat=%d ranges=%v, want streaming path", routed.statCalls, routed.ranges())
	}
}

func TestVerifyCompleteRequiresStreamingSource(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	err = Verify(context.Background(), &rangeOnlyTestSource{data: object.Bytes()}, "run", ref, VerifyComplete, fixtureTimelineExtractor)
	if !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("Verify error=%v, want streaming-capability rejection", err)
	}
}

func TestVerifyCompleteStreamingPreservesTransportAndShortErrors(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	transport := errors.New("stream transport failure")
	source := &testStreamingSource{data: object.Bytes(), etag: "transport-a", failOpen: transport}
	_, err = VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, transport) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("transport classification=%v", err)
	}
	short := &testStreamingSource{data: object.Bytes(), etag: "short-a", short: true}
	_, err = VerifyCompleteStreaming(context.Background(), short, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("short stream classification=%v, want corruption", err)
	}
	nilBody := &testStreamingSource{data: object.Bytes(), etag: "nil-body-a", nilBody: true}
	_, err = VerifyCompleteStreaming(context.Background(), nilBody, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrVerificationResource) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("nil body classification=%v, want resource failure", err)
	}
}

func TestVerifyCompleteStreamingRejectsGenerationChange(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "generation-a", changeOnStat: true}
	_, err = VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrObjectIdentityChanged) || !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("generation-change classification=%v", err)
	}
}

func TestVerifyCompleteStreamingDoesNotInventGenerationChange(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{
		data:     object.Bytes(),
		etag:     "generation-a",
		failOpen: fmt.Errorf("%w: injected stale precondition response", ErrObjectIdentityChanged),
	}
	_, err = VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrObjectIdentityChanged) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("unchanged generation classification=%v", err)
	}
}

func TestVerifyCompleteStreamingEnforcesScratchBudget(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "budget-a"}
	_, err = VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{ScratchBudget: 1}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrVerificationResource) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("scratch budget classification=%v", err)
	}
}

func TestVerifyCompleteStreamingExternalSortsExactTimelineSets(t *testing.T) {
	options, events, heads, timelines := benchmarkRunFixture(100, 32)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, BuildInput{
		Events:    &sliceEntryIterator{entries: events},
		Heads:     &sliceEntryIterator{entries: heads},
		Timelines: &sliceTimelineIterator{timelines: timelines},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "sort-a"}
	report, err := VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{
		MemoryBudget:   64,
		ScratchBudget:  32 << 20,
		SortMergeFanIn: 2,
	}, fixtureTimelineExtractor)
	if err != nil {
		t.Fatal(err)
	}
	if report.DistinctTimelines != 100 || report.TimelineSpillRuns == 0 || report.TimelineMergePasses == 0 || report.FilterContributionSpills == 0 {
		t.Fatalf("external-sort report=%+v, want exact sets with spills and merge passes", report)
	}
}

func TestVerifyCompleteStreamingRejectsSetMismatchBeforeFilterWork(t *testing.T) {
	options, _ := validBuildFixture(TableCompressionNone)
	value := bytes.Repeat([]byte{'v'}, 2<<20)
	input := BuildInput{
		Events: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-a|event-1"), Value: value, Timeline: []byte("timeline-a"), Seq: options.SeqLo,
		}}},
		Heads: &sliceEntryIterator{entries: []Entry{{
			Key: []byte("timeline-z|head-1"), Value: value, Timeline: []byte("timeline-a"), Seq: options.SeqLo,
		}}},
		Timelines: &sliceTimelineIterator{timelines: [][]byte{[]byte("timeline-a")}},
	}
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, input)
	if err != nil {
		t.Fatal(err)
	}
	source := &testStreamingSource{data: object.Bytes(), etag: "set-mismatch-a"}
	_, err = VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor)
	if !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("set mismatch error=%v, want corruption", err)
	}
	if got := len(source.ranges()); got != 3 {
		t.Fatalf("provider requests=%v, want suffix, Events, and Heads only", source.ranges())
	}
}

func TestVerifyCompleteStreamingAcceptsUnknownOptionalFilterEncoding(t *testing.T) {
	options, input := validBuildFixture(TableCompressionNone)
	var built bytes.Buffer
	ref, err := Build(context.Background(), &built, options, input)
	if err != nil {
		t.Fatal(err)
	}

	object := bytes.Clone(built.Bytes())
	descriptor := int(ref.DirectoryOffset) + DirectoryHeaderBytes + 2*RegionDescriptorBytes
	binary.BigEndian.PutUint16(object[descriptor+4:descriptor+6], 2)
	directoryEnd := ref.DirectoryOffset + ref.DirectoryLength
	directoryHash := sha256.Sum256(object[ref.DirectoryOffset:directoryEnd])
	payloadHash := sha256.Sum256(object[:directoryEnd])
	trailer := Trailer{
		DirectoryOffset: ref.DirectoryOffset,
		DirectoryLength: ref.DirectoryLength,
		ObjectSize:      ref.ObjectSize,
		RegionCount:     3,
		DirectoryHash:   directoryHash,
		PayloadHash:     payloadHash,
		RunID:           ref.RunID,
	}
	trailerBytes, err := MarshalTrailer(trailer)
	if err != nil {
		t.Fatal(err)
	}
	copy(object[len(object)-TrailerBytes:], trailerBytes)

	unknown := cloneRegion(ref.TimelineFilter.Region)
	unknown.Encoding = 2
	ref.TimelineFilter = nil
	ref.OptionalRegions = []RegionDescriptor{unknown}
	ref.DirectoryHash = directoryHash
	ref.PayloadHash = payloadHash

	source := &testStreamingSource{data: object, etag: "unknown-optional-a"}
	if _, err := VerifyCompleteStreaming(context.Background(), source, "run", ref, CompleteVerifyOptions{}, fixtureTimelineExtractor); err != nil {
		t.Fatalf("unknown optional filter encoding rejected: %v", err)
	}
}

func TestTimelineSorterHonorsCancellationBeforeSpill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	report := CompleteVerificationReport{}
	workspace, err := newVerificationWorkspace(CompleteVerifyOptions{ScratchDir: t.TempDir(), ScratchBudget: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	workspace.ctx = ctx
	workspace.report = &report
	defer func() {
		if err := workspace.close(&report); err != nil {
			t.Error(err)
		}
	}()
	sorter := newTimelineSorter(workspace, 1024, 2, RegionKindEventsSST, &report)
	if err := sorter.add([]byte("timeline")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := sorter.finalize(); !errors.Is(err, context.Canceled) {
		t.Fatalf("finalize error=%v, want context cancellation", err)
	}
}

func TestCompareTimelineStreamsPreservesScratchReadFailure(t *testing.T) {
	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events")
	headsPath := filepath.Join(dir, "heads")
	if err := os.WriteFile(eventsPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(headsPath, []byte{0}, 0o600); err != nil {
		t.Fatal(err)
	}
	report := CompleteVerificationReport{}
	workspace := &verificationWorkspace{ctx: context.Background(), report: &report}
	err := compareTimelineStreams(
		context.Background(),
		workspace,
		&timelineStream{path: eventsPath},
		&timelineStream{path: headsPath},
	)
	if !errors.Is(err, ErrVerificationResource) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("comparison error=%v, want resource failure", err)
	}
}

func TestFilterComparatorPreservesScratchReadFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canonical-filter")
	if err := os.WriteFile(path, []byte("canonical"), 0o600); err != nil {
		t.Fatal(err)
	}
	report := CompleteVerificationReport{}
	workspace := &verificationWorkspace{ctx: context.Background(), report: &report}
	comparator, err := newFilterComparator(path, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	err = comparator.consume([]byte("canonical"))
	closeErr := comparator.close()
	if !errors.Is(err, ErrVerificationResource) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("comparison error=%v, want resource failure", err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

type testStreamingSource struct {
	mu           sync.Mutex
	data         []byte
	etag         string
	statCalls    int
	requests     [][2]int64
	failOpen     error
	short        bool
	nilBody      bool
	changeAfter  int
	changeOnStat bool
}

type rangeOnlyTestSource struct{ data []byte }

func (s *rangeOnlyTestSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *rangeOnlyTestSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func (s *testStreamingSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *testStreamingSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func (s *testStreamingSource) Stat(context.Context, string) (ObjectIdentity, error) {
	s.mu.Lock()
	s.statCalls++
	identity := ObjectIdentity{Size: uint64(len(s.data)), ETag: s.etag}
	if s.changeOnStat {
		s.etag = "generation-b"
	}
	s.mu.Unlock()
	return identity, nil
}

func (s *testStreamingSource) OpenRange(_ context.Context, _ string, identity ObjectIdentity, offset, length uint64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if identity.ETag != s.etag {
		return nil, fmt.Errorf("%w: expected %q got %q", ErrObjectIdentityChanged, identity.ETag, s.etag)
	}
	if s.failOpen != nil {
		err := s.failOpen
		s.failOpen = nil
		return nil, err
	}
	if s.nilBody {
		return nil, nil
	}
	if offset > uint64(len(s.data)) || length > uint64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	s.requests = append(s.requests, [2]int64{int64(offset), int64(length)})
	data := bytes.Clone(s.data[offset : offset+length])
	if s.short && len(data) > 0 {
		data = data[:len(data)-1]
	}
	if s.changeAfter > 0 {
		s.changeAfter--
		if s.changeAfter == 0 {
			s.etag = "generation-b"
		}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *testStreamingSource) ranges() [][2]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]int64(nil), s.requests...)
}
