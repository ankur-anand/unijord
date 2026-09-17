package runfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"testing"
)

// E00 fixtures use one event per distinct timeline, 1 KiB seeded incompressible
// event values and 32-byte head values. MiB names count event payload only.
// Fixture generation, sizing, and telemetry are outside the measured window.
func e00Fixture(mib int) (BuildOptions, []Entry, []Entry, [][]byte) {
	opts, events, heads, timelines := benchmarkRunFixture(mib*1024, 32)
	rng := rand.New(rand.NewSource(1700))
	for i := range events {
		events[i].Value = make([]byte, 1024)
		_, _ = rng.Read(events[i].Value)
	}
	return opts, events, heads, timelines
}

type e00ObservedEntries struct {
	*sliceEntryIterator
	observe  func()
	observed bool
}

func (i *e00ObservedEntries) Next() bool {
	if !i.observed {
		i.observe()
		i.observed = true
	}
	return i.sliceEntryIterator.Next()
}

type e00ObservedTimelines struct {
	*sliceTimelineIterator
	observe  func()
	observed bool
}

func (i *e00ObservedTimelines) Next() bool {
	if !i.observed {
		i.observe()
		i.observed = true
	}
	return i.sliceTimelineIterator.Next()
}

type e00ObservedWriter struct {
	observe  func()
	observed bool
}

func (w *e00ObservedWriter) Write(b []byte) (int, error) {
	if !w.observed {
		w.observe()
		w.observed = true
	}
	return len(b), nil
}

func BenchmarkE00Build(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		b.Run(fmt.Sprintf("%dMiB", mib), func(b *testing.B) {
			opts, events, heads, timelines := e00Fixture(mib)
			opts.ScratchDir = b.TempDir()
			input := func() BuildInput {
				return BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineIterator{timelines: timelines}}
			}
			ref, err := Build(context.Background(), io.Discard, opts, input())
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(mib << 20))
			b.ResetTimer()
			for range b.N {
				if _, err := Build(context.Background(), io.Discard, opts, input()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			// Telemetry pass: samples at table starts, timeline collection, first
			// output, and completion. This is an observed lower bound, NOT peak RSS.
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			base, high := ms.HeapAlloc, ms.HeapAlloc
			observe := func() { runtime.ReadMemStats(&ms); high = max(high, ms.HeapAlloc) }
			var scratch uint64
			var files int
			dst := &e00ObservedWriter{observe: func() {
				observe()
				entries, err := os.ReadDir(opts.ScratchDir)
				if err != nil {
					b.Fatal(err)
				}
				for _, entry := range entries {
					info, err := entry.Info()
					if err != nil {
						b.Fatal(err)
					}
					scratch += uint64(info.Size())
					files++
				}
			}}
			_, err = Build(context.Background(), dst, opts, BuildInput{
				&e00ObservedEntries{sliceEntryIterator: &sliceEntryIterator{entries: events}, observe: observe},
				&e00ObservedEntries{sliceEntryIterator: &sliceEntryIterator{entries: heads}, observe: observe},
				&e00ObservedTimelines{sliceTimelineIterator: &sliceTimelineIterator{timelines: timelines}, observe: observe},
			})
			observe()
			if err != nil {
				b.Fatal(err)
			}
			if scratch != ref.Events.Length+ref.Heads.Length+ref.TimelineFilter.Region.Length || files != 3 {
				b.Fatal("unexpected scratch shape", scratch, files)
			}
			entries, err := os.ReadDir(opts.ScratchDir)
			if err != nil || len(entries) != 0 {
				b.Fatal("scratch cleanup", err)
			}
			b.ReportMetric(float64(high-base), "sampled_heap_delta_B/op")
			b.ReportMetric(float64(scratch), "scratch_highwater_B/op")
			b.ReportMetric(float64(ref.ObjectSize), "run_bytes/op")
			b.ReportMetric(float64(len(events)), "timelines/op")
			b.ReportMetric(0, "provider_gets/op")
			b.ReportMetric(0, "sort_spills/op")
		})
	}
}

func BenchmarkE00Verify(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		for _, mode := range []string{"no-pressure-spill", "forced-spill"} {
			b.Run(fmt.Sprintf("%dMiB/%s", mib, mode), func(b *testing.B) {
				opts, events, heads, timelines := e00Fixture(mib)
				opts.ScratchDir = b.TempDir()
				var object bytes.Buffer
				ref, err := Build(context.Background(), &object, opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineIterator{timelines: timelines}})
				if err != nil {
					b.Fatal(err)
				}
				// Large enough for every sorter in the no-pressure case. Final stream
				// materialization still reports two timeline runs and one filter run.
				memory := uint64(64 << 20)
				if mode == "forced-spill" {
					memory = 32 << 10
				}
				verifyOpts := CompleteVerifyOptions{ScratchDir: opts.ScratchDir, MemoryBudget: memory, ScratchBudget: 1 << 30, SortMergeFanIn: 16, StreamBufferSize: 128 << 10}
				source := &benchmarkRangeSource{data: object.Bytes()}
				var report CompleteVerificationReport
				b.ReportAllocs()
				b.SetBytes(int64(ref.ObjectSize))
				b.ResetTimer()
				for range b.N {
					source.resetMetrics()
					report, err = VerifyCompleteStreaming(context.Background(), source, "e00", ref, verifyOpts, fixtureTimelineExtractor)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if mode == "no-pressure-spill" && (report.TimelineSpillRuns != 2 || report.FilterContributionSpills != 1) {
					b.Fatal("unexpected pressure spill", report.TimelineSpillRuns, report.FilterContributionSpills)
				}
				if mode == "forced-spill" && report.TimelineSpillRuns <= 2 {
					b.Fatal("forced spill did not exercise external sort")
				}
				b.ReportMetric(float64(report.ProviderGETs), "range_gets/op")
				b.ReportMetric(float64(report.ProviderMetadataRequests), "metadata_requests/op")
				b.ReportMetric(float64(report.ProviderBytesConsumed), "range_bytes/op")
				b.ReportMetric(float64(report.ScratchHighWater), "scratch_highwater_B/op")
				b.ReportMetric(float64(report.ScratchBytesWritten), "scratch_written_B/op")
				b.ReportMetric(float64(report.TimelineBatchLogicalHighWater), "timeline_logical_B/op")
				b.ReportMetric(float64(report.TimelineBatchReservedBytes), "timeline_reserved_B/op")
				b.ReportMetric(float64(report.TimelineSpillRuns), "timeline_spills/op")
				b.ReportMetric(float64(report.FilterContributionSpills), "filter_spills/op")
				entries, err := os.ReadDir(opts.ScratchDir)
				if err != nil || len(entries) != 0 {
					b.Fatal("scratch cleanup", err)
				}
			})
		}
	}
}
