package runfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
)

// Same dataset, compression, environment and output sink as E00. Each stage
// excludes fixture generation and telemetry. Prepare excludes Close; FirstWrite
// excludes Prepare/Close; RepeatedWrite reuses a run after one untimed write.
func BenchmarkE01(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		for _, stage := range []string{"Prepare", "FirstWrite", "RepeatedWrite", "Build"} {
			b.Run(fmt.Sprintf("%dMiB/%s", mib, stage), func(b *testing.B) {
				opts, events, heads, timelines := e00Fixture(mib)
				opts.ScratchDir = b.TempDir()
				input := func() BuildInput {
					return BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineIterator{timelines: timelines}}
				}
				prepare := func() PreparedRun {
					p, err := Prepare(context.Background(), opts, input())
					if err != nil {
						b.Fatal(err)
					}
					return p
				}
				closeRun := func(p PreparedRun) {
					if err := p.Close(); err != nil {
						b.Fatal(err)
					}
				}
				write := func(p PreparedRun) {
					if err := p.WriteTo(context.Background(), io.Discard); err != nil {
						b.Fatal(err)
					}
				}
				p := prepare()
				ref := p.Ref()
				write(p)
				if stage != "RepeatedWrite" {
					closeRun(p)
				}
				b.ReportAllocs()
				b.SetBytes(int64(mib << 20))
				b.ResetTimer()
				for range b.N {
					switch stage {
					case "Prepare":
						p := prepare()
						b.StopTimer()
						closeRun(p)
						b.StartTimer()
					case "FirstWrite":
						b.StopTimer()
						p := prepare()
						b.StartTimer()
						write(p)
						b.StopTimer()
						closeRun(p)
						b.StartTimer()
					case "RepeatedWrite":
						write(p)
					case "Build":
						if _, err := Build(context.Background(), io.Discard, opts, input()); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				if stage == "RepeatedWrite" {
					closeRun(p)
				}
				requireEmptyScratch(b, opts.ScratchDir)

				// Separate telemetry pass. Phase-sampled high-water is a lower
				// bound, matching E00's method, not a claim of exact peak heap.
				runtime.GC()
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				base, high := ms.HeapAlloc, ms.HeapAlloc
				observe := func() { runtime.ReadMemStats(&ms); high = max(high, ms.HeapAlloc) }
				p, err := Prepare(context.Background(), opts, BuildInput{
					&e00ObservedEntries{sliceEntryIterator: &sliceEntryIterator{entries: events}, observe: observe},
					&e00ObservedEntries{sliceEntryIterator: &sliceEntryIterator{entries: heads}, observe: observe},
					&e00ObservedTimelines{sliceTimelineIterator: &sliceTimelineIterator{timelines: timelines}, observe: observe},
				})
				if err != nil {
					b.Fatal(err)
				}
				observe()
				prepareHigh := high - base
				var scratch uint64
				files, err := os.ReadDir(opts.ScratchDir)
				if err != nil {
					b.Fatal(err)
				}
				for _, f := range files {
					info, err := f.Info()
					if err != nil {
						b.Fatal(err)
					}
					scratch += uint64(info.Size())
				}
				if len(files) != 3 || scratch != ref.Events.Length+ref.Heads.Length+ref.TimelineFilter.Region.Length {
					b.Fatal("scratch shape", scratch)
				}
				if stage == "RepeatedWrite" {
					write(p)
				}
				if stage == "FirstWrite" || stage == "RepeatedWrite" {
					runtime.GC()
					runtime.ReadMemStats(&ms)
					base, high = ms.HeapAlloc, ms.HeapAlloc
				}
				if stage != "Prepare" {
					if err := p.WriteTo(context.Background(), &e00ObservedWriter{observe: observe}); err != nil {
						b.Fatal(err)
					}
					observe()
				}
				closeRun(p)
				observe()
				requireEmptyScratch(b, opts.ScratchDir)
				b.ReportMetric(float64(high-base), "sampled_heap_delta_B/op")
				b.ReportMetric(float64(prepareHigh), "prepare_sampled_heap_B/op")
				b.ReportMetric(float64(scratch), "scratch_highwater_B/op")
				b.ReportMetric(3, "scratch_files/op")
				b.ReportMetric(float64(ref.ObjectSize), "run_bytes/op")
				b.ReportMetric(0, "provider_gets/op")
				b.ReportMetric(0, "sort_spills/op")
				runtime.KeepAlive(events)
				runtime.KeepAlive(heads)
				runtime.KeepAlive(timelines)
			})
		}
	}
}

// A separate audit bounds heap high-water without a sampling gap. With GC
// disabled, HeapAlloc grows monotonically across the synchronous operation.
// It deliberately includes unreclaimed temporary objects, so this is a
// conservative bound, not normal-GC live heap or a throughput measurement.
func BenchmarkPreparedHeapBound(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		b.Run(fmt.Sprintf("%dMiB", mib), func(b *testing.B) {
			opts, events, heads, timelines := e00Fixture(mib)
			opts.ScratchDir = b.TempDir()
			var prepareHigh, firstHigh, repeatHigh uint64
			measure := func(operation func()) uint64 {
				runtime.GC()
				old := debug.SetGCPercent(-1)
				defer debug.SetGCPercent(old)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				operation()
				runtime.ReadMemStats(&after)
				if after.NumGC != before.NumGC {
					b.Fatal("collection invalidated heap-bound audit (check memory limit)")
				}
				return after.HeapAlloc - before.HeapAlloc
			}
			for range b.N {
				var p PreparedRun
				prepareHigh = max(prepareHigh, measure(func() {
					var err error
					p, err = Prepare(context.Background(), opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineIterator{timelines: timelines}})
					if err != nil {
						b.Fatal(err)
					}
				}))
				write := func() {
					if err := p.WriteTo(context.Background(), io.Discard); err != nil {
						b.Fatal(err)
					}
				}
				firstHigh = max(firstHigh, measure(write))
				repeatHigh = max(repeatHigh, measure(write))
				if err := p.Close(); err != nil {
					b.Fatal(err)
				}
			}
			requireEmptyScratch(b, opts.ScratchDir)
			b.ReportMetric(float64(prepareHigh), "prepare_heap_bound_B")
			b.ReportMetric(float64(firstHigh), "first_write_heap_bound_B")
			b.ReportMetric(float64(repeatHigh), "repeat_write_heap_bound_B")
			runtime.KeepAlive(events)
			runtime.KeepAlive(heads)
			runtime.KeepAlive(timelines)
		})
	}
}
