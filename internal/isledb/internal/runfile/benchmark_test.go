package runfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

func BenchmarkTimelineFilterBuild(b *testing.B) {
	for _, timelineCount := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("timelines=%d", timelineCount), func(b *testing.B) {
			timelines := generatedFilterTimelines(timelineCount)
			var inputBytes int64
			for _, timeline := range timelines {
				inputBytes += int64(len(timeline))
			}
			for _, bitsPerKey := range []uint16{8, 10, 12} {
				b.Run(fmt.Sprintf("bits-per-key=%d", bitsPerKey), func(b *testing.B) {
					var encoded []byte
					b.ReportAllocs()
					b.SetBytes(inputBytes)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var err error
						encoded, _, err = BuildTimelineFilter(testRunID(), timelines, FilterOptions{BitsPerKey: bitsPerKey})
						if err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(len(encoded)), "filter_bytes/op")
				})
			}
		})
	}
}

func BenchmarkTimelineFilterColdAbsentLookup(b *testing.B) {
	const queryCount = 20_000
	queries := make([][]byte, queryCount)
	for index := range queries {
		queries[index] = []byte(fmt.Sprintf("benchmark-absent-%08d", index))
	}

	for _, timelineCount := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("timelines=%d", timelineCount), func(b *testing.B) {
			timelines := generatedFilterTimelines(timelineCount)
			for _, bitsPerKey := range []uint16{8, 10, 12} {
				b.Run(fmt.Sprintf("bits-per-key=%d", bitsPerKey), func(b *testing.B) {
					encoded, header, err := BuildTimelineFilter(testRunID(), timelines, FilterOptions{BitsPerKey: bitsPerKey})
					if err != nil {
						b.Fatal(err)
					}
					ref := testFilterRef(header, PreambleBytes, uint64(len(encoded)), PreambleBytes+uint64(len(encoded)))
					var rangeGets, rangeBytes, falsePositives uint64
					read := func(request FilterPageRequest) ([]byte, error) {
						start := uint64(request.Offset) - ref.Region.Offset
						end := start + uint64(request.Length)
						rangeGets++
						rangeBytes += uint64(request.Length)
						return encoded[int(start):int(end)], nil
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						mayContain, err := ReadFilterMembership(ref, queries[i%len(queries)], read)
						if err != nil {
							b.Fatal(err)
						}
						if mayContain {
							falsePositives++
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(falsePositives)*100/float64(b.N), "false_positive_pct")
					b.ReportMetric(float64(rangeBytes)/float64(b.N), "range_bytes/op")
					b.ReportMetric(float64(rangeGets)/float64(b.N), "range_gets/op")
				})
			}
		})
	}
}

func BenchmarkRegionReadable(b *testing.B) {
	object := bytes.Repeat([]byte{0x5a}, 1<<20)
	region := RegionDescriptor{
		Kind:   RegionKindEventsSST,
		Offset: 128,
		Length: uint64(len(object) - 256),
	}
	for _, benchmark := range []struct {
		name  string
		cache bool
	}{
		{name: "cold"},
		{name: "warm-cache", cache: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			source := &benchmarkRangeSource{data: object}
			options := RegionReadOptions{RunID: testRunID()}
			if benchmark.cache {
				options.Cache = newRegionTestCache()
			}
			readable, err := NewRegionReadable(source, "benchmark-run", int64(len(object)), region, options)
			if err != nil {
				b.Fatal(err)
			}
			buffer := make([]byte, 4<<10)
			if benchmark.cache {
				if err := readable.ReadAt(context.Background(), buffer, 32<<10); err != nil {
					b.Fatal(err)
				}
			}
			source.resetMetrics()
			b.ReportAllocs()
			b.SetBytes(int64(len(buffer)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := readable.ReadAt(context.Background(), buffer, 32<<10); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(source.requests.Load())/float64(b.N), "range_gets/op")
			b.ReportMetric(float64(source.bytes.Load())/float64(b.N), "range_bytes/op")
		})
	}
}

func BenchmarkRunBuild(b *testing.B) {
	for _, timelineCount := range []int{1, 100, 2048} {
		b.Run(fmt.Sprintf("timelines=%d", timelineCount), func(b *testing.B) {
			options, events, heads, timelines := benchmarkRunFixture(timelineCount, 256)
			var sizing benchmarkCountingWriter
			if _, err := Build(context.Background(), &sizing, options, BuildInput{
				Events:    &sliceEntryIterator{entries: events},
				Heads:     &sliceEntryIterator{entries: heads},
				Timelines: &sliceTimelineCatalog{timelines: timelines},
			}); err != nil {
				b.Fatal(err)
			}
			runBytes := sizing.bytes
			b.ReportAllocs()
			b.SetBytes(int64(timelineCount * 2 * 256))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Build(context.Background(), io.Discard, options, BuildInput{
					Events:    &sliceEntryIterator{entries: events},
					Heads:     &sliceEntryIterator{entries: heads},
					Timelines: &sliceTimelineCatalog{timelines: timelines},
				}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(runBytes), "run_bytes/op")
		})
	}
}

// BenchmarkRunBuild1MTimelineSets retains its historical name for comparison.
// Since E02, the production writer validates all three observations using a
// borrowed catalog, integer ordering index, and one source-mask array.
// Fixture construction is intentionally outside the timed allocation window.
func BenchmarkRunBuild1MTimelineSets(b *testing.B) {
	const timelineCount = 1_000_000
	options, events, heads, timelines := benchmarkRunFixture(timelineCount, 32)
	b.ReportAllocs()
	b.SetBytes(int64(timelineCount * 2 * 32))
	var destination benchmarkCountingWriter
	b.ResetTimer()
	for range b.N {
		destination.bytes = 0
		if _, err := Build(context.Background(), &destination, options, BuildInput{
			Events:    &sliceEntryIterator{entries: events},
			Heads:     &sliceEntryIterator{entries: heads},
			Timelines: &sliceTimelineCatalog{timelines: timelines},
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(destination.bytes), "run_bytes/op")
	b.ReportMetric(timelineCount, "timelines/op")
}

func BenchmarkRunRecover(b *testing.B) {
	ref, object := benchmarkBuiltRun(b, 100, 256)
	source := &benchmarkRangeSource{data: object}
	b.ReportAllocs()
	b.SetBytes(int64(len(object)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recovered, err := Recover(context.Background(), source, "benchmark-run")
		if err != nil {
			b.Fatal(err)
		}
		if !sameRef(ref, recovered) {
			b.Fatal("recovered reference differs from built reference")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(source.requests.Load())/float64(b.N), "range_gets/op")
	b.ReportMetric(float64(source.bytes.Load())/float64(b.N), "range_bytes/op")
}

func BenchmarkRunVerifyComplete(b *testing.B) {
	ref, object := benchmarkBuiltRun(b, 100, 256)
	source := &benchmarkRangeSource{data: object}
	b.ReportAllocs()
	b.SetBytes(int64(len(object)))
	var report CompleteVerificationReport
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		report, err = VerifyCompleteStreaming(
			context.Background(),
			source,
			"benchmark-run",
			ref,
			CompleteVerifyOptions{},
			fixtureTimelineExtractor,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(report.ProviderGETs), "range_gets/op")
	b.ReportMetric(float64(report.ProviderBytesConsumed), "range_bytes/op")
}

func BenchmarkRunVerifyComplete100KSpill(b *testing.B) {
	benchmarkRunVerifyCompleteSpill(b, 100_000)
}

func BenchmarkRunVerifyComplete1KSpill(b *testing.B) {
	benchmarkRunVerifyCompleteSpill(b, 1_000)
}

func BenchmarkRunVerifyComplete1MSpill(b *testing.B) {
	benchmarkRunVerifyCompleteSpill(b, 1_000_000)
}

func benchmarkRunVerifyCompleteSpill(b *testing.B, timelineCount int) {
	ref, object := benchmarkBuiltRun(b, timelineCount, 32)
	source := &benchmarkRangeSource{data: object}
	options := CompleteVerifyOptions{
		MemoryBudget:     1 << 20,
		ScratchBudget:    512 << 20,
		SortMergeFanIn:   4,
		StreamBufferSize: 128 << 10,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(object)))
	var report CompleteVerificationReport
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		report, err = VerifyCompleteStreaming(
			context.Background(),
			source,
			"benchmark-run-spill",
			ref,
			options,
			fixtureTimelineExtractor,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(report.ProviderGETs), "range_gets/op")
	b.ReportMetric(float64(report.ProviderBytesConsumed), "range_bytes/op")
	b.ReportMetric(float64(report.ScratchHighWater), "scratch_highwater_B/op")
	b.ReportMetric(float64(report.TimelineSpillRuns), "timeline_spills/op")
	b.ReportMetric(float64(report.TimelineBatchLogicalHighWater), "timeline_batch_logical_B/op")
	b.ReportMetric(float64(report.TimelineBatchReservedBytes), "timeline_batch_reserved_B/op")
	b.ReportMetric(float64(report.TimelineBatchSlabs), "timeline_batch_slabs/op")
	b.ReportMetric(float64(report.TimelineBatchResets), "timeline_batch_resets/op")
	b.ReportMetric(float64(report.FilterContributionSpills), "filter_spills/op")
}

func benchmarkRunFixture(timelineCount, valueBytes int) (BuildOptions, []Entry, []Entry, [][]byte) {
	var options BuildOptions
	fillBytes(options.RunID[:], 0x11)
	fillBytes(options.NamespaceHash[:], 0x22)
	fillBytes(options.PublicationHash[:], 0x33)
	options.CreatorRole = CreatorRoleWriterFlush
	options.CreatorEpoch = 7
	options.SeqLo = 1
	options.SeqHi = uint64(timelineCount)
	options.Shard = 4
	options.Table.Compression = TableCompressionSnappy
	options.MaxTimelines = uint32(timelineCount)

	value := bytes.Repeat([]byte{'v'}, valueBytes)
	events := make([]Entry, timelineCount)
	heads := make([]Entry, timelineCount)
	timelines := make([][]byte, timelineCount)
	for index := range timelineCount {
		timeline := []byte(fmt.Sprintf("timeline-%08d", index))
		sequence := uint64(index + 1)
		timelines[index] = timeline
		events[index] = Entry{
			TimelineID: TimelineID(index),
			Key:        []byte(fmt.Sprintf("%s|event-0", timeline)),
			Value:      value,
			Timeline:   timeline,
			Seq:        sequence,
		}
		heads[index] = Entry{
			TimelineID: TimelineID(index),
			Key:        []byte(fmt.Sprintf("%s|head-0", timeline)),
			Value:      value,
			Timeline:   timeline,
			Seq:        sequence,
		}
	}
	return options, events, heads, timelines
}

func benchmarkBuiltRun(b *testing.B, timelineCount, valueBytes int) (Ref, []byte) {
	b.Helper()
	options, events, heads, timelines := benchmarkRunFixture(timelineCount, valueBytes)
	var object bytes.Buffer
	ref, err := Build(context.Background(), &object, options, BuildInput{
		Events:    &sliceEntryIterator{entries: events},
		Heads:     &sliceEntryIterator{entries: heads},
		Timelines: &sliceTimelineCatalog{timelines: timelines},
	})
	if err != nil {
		b.Fatal(err)
	}
	return ref, object.Bytes()
}

type benchmarkRangeSource struct {
	data     []byte
	requests atomic.Uint64
	bytes    atomic.Uint64
}

func (s *benchmarkRangeSource) Size(context.Context, string) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *benchmarkRangeSource) Stat(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{Size: uint64(len(s.data)), ETag: "benchmark-generation"}, nil
}

func (s *benchmarkRangeSource) OpenRange(_ context.Context, _ string, _ ObjectIdentity, offset, length uint64) (io.ReadCloser, error) {
	if offset > uint64(len(s.data)) || length > uint64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return io.NopCloser(bytes.NewReader(s.data[offset : offset+length])), nil
}

func (s *benchmarkRangeSource) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset > int64(len(s.data)) || length > int64(len(s.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	s.requests.Add(1)
	s.bytes.Add(uint64(length))
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func (s *benchmarkRangeSource) resetMetrics() {
	s.requests.Store(0)
	s.bytes.Store(0)
}

type benchmarkCountingWriter struct {
	bytes uint64
}

func (w *benchmarkCountingWriter) Write(data []byte) (int, error) {
	w.bytes += uint64(len(data))
	return len(data), nil
}
