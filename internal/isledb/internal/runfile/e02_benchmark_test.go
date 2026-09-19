package runfile

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
)

// Unique is exactly the E00/E01 seed-1700 fixture, including keys and values.
// Hot keeps those same event values and sequences, with 16 events per timeline
// and one final Head. All fixture work stays outside measured operations.
func e02Fixture(mib int, hot bool) (BuildOptions, []Entry, []Entry, [][]byte) {
	opts, events, heads, timelines := e00Fixture(mib)
	if !hot {
		return opts, events, heads, timelines
	}
	const repeats = 16
	for i := range events {
		id := i / repeats
		timeline := timelines[id*repeats]
		events[i].Timeline, events[i].TimelineID = timeline, TimelineID(id)
		events[i].Key = []byte(fmt.Sprintf("%s|event-%08d", timeline, i%repeats))
	}
	for i := 0; i < len(timelines)/repeats; i++ {
		timelines[i] = timelines[i*repeats]
		heads[i] = heads[i*repeats+repeats-1]
		heads[i].Timeline, heads[i].TimelineID = timelines[i], TimelineID(i)
		heads[i].Key = []byte(fmt.Sprintf("%s|head-0", timelines[i]))
	}
	timelines = timelines[:len(timelines)/repeats]
	heads = heads[:len(timelines)]
	opts.MaxTimelines = uint32(len(timelines))
	return opts, events, heads, timelines
}

func e02Scratch(t testing.TB, dir string, ref Ref) uint64 {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var size uint64
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatal(err)
		}
		size += uint64(info.Size())
	}
	if len(files) != 3 || size != ref.Events.Length+ref.Heads.Length+ref.TimelineFilter.Region.Length {
		t.Fatal("scratch shape", size)
	}
	return size
}

func BenchmarkE02(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		for _, distribution := range []string{"unique", "hot16"} {
			for _, stage := range []string{"Prepare", "FirstWrite", "RepeatedWrite"} {
				b.Run(fmt.Sprintf("%dMiB/%s/%s", mib, distribution, stage), func(b *testing.B) {
					opts, events, heads, timelines := e02Fixture(mib, distribution == "hot16")
					opts.ScratchDir = b.TempDir()
					catalog := &sliceTimelineCatalog{timelines: timelines}
					input := func() BuildInput {
						return BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, catalog}
					}
					prepareRun := func() PreparedRun {
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
					p := prepareRun()
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
							p := prepareRun()
							b.StopTimer()
							closeRun(p)
							b.StartTimer()
						case "FirstWrite":
							b.StopTimer()
							p := prepareRun()
							b.StartTimer()
							write(p)
							b.StopTimer()
							closeRun(p)
							b.StartTimer()
						case "RepeatedWrite":
							write(p)
						}
					}
					b.StopTimer()
					if stage == "RepeatedWrite" {
						closeRun(p)
					}
					requireEmptyScratch(b, opts.ScratchDir)

					// Copy sensors are observed in a separate preparation so their
					// callbacks do not affect the time/allocation comparison.
					counts := &e02CopyCounts{}
					p, err := prepare(context.Background(), opts, input(), counts.instrument(catalog))
					if err != nil {
						b.Fatal(err)
					}
					counts.check(b, len(events)+len(heads), len(timelines))
					if counts.grows != 2 {
						b.Fatal("per-entry previous-key allocation", counts.grows)
					}
					scratch := e02Scratch(b, opts.ScratchDir, ref)
					closeRun(p)
					requireEmptyScratch(b, opts.ScratchDir)
					b.ReportMetric(float64(counts.payload), "unexpected_retained_payload_copies/op")
					b.ReportMetric(float64(counts.timeline), "unexpected_retained_timeline_copies/op")
					b.ReportMetric(float64(counts.key), "per_entry_key_clones/op")
					b.ReportMetric(float64(counts.grows), "previous_key_growths/op")
					b.ReportMetric(float64(counts.metadata), "metadata_key_copies/op")
					b.ReportMetric(float64(5*len(timelines)), "catalog_validation_B/op")
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
}

// Match E01's GC-disabled conservative upper-bound audit, independently of the
// normal-GC timing/allocation samples. No asynchronous sampler can miss a peak.
func BenchmarkE02HeapBound(b *testing.B) {
	for _, mib := range []int{1, 4, 128} {
		for _, distribution := range []string{"unique", "hot16"} {
			b.Run(fmt.Sprintf("%dMiB/%s", mib, distribution), func(b *testing.B) {
				opts, events, heads, timelines := e02Fixture(mib, distribution == "hot16")
				opts.ScratchDir = b.TempDir()
				catalog := &sliceTimelineCatalog{timelines: timelines}
				var prepareHigh, firstHigh, repeatHigh, scratch uint64
				measure := func(operation func()) uint64 {
					runtime.GC()
					old := debug.SetGCPercent(-1)
					defer debug.SetGCPercent(old)
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					operation()
					runtime.ReadMemStats(&after)
					if after.NumGC != before.NumGC {
						b.Fatal("GC invalidated upper bound")
					}
					return after.HeapAlloc - before.HeapAlloc
				}
				for range b.N {
					var p PreparedRun
					prepareHigh = max(prepareHigh, measure(func() {
						var err error
						p, err = Prepare(context.Background(), opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, catalog})
						if err != nil {
							b.Fatal(err)
						}
					}))
					scratch = e02Scratch(b, opts.ScratchDir, p.Ref())
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
				b.ReportMetric(float64(scratch), "scratch_highwater_B")
				runtime.KeepAlive(events)
				runtime.KeepAlive(heads)
				runtime.KeepAlive(timelines)
			})
		}
	}
}

func TestE02DatasetHashes(t *testing.T) {
	for _, mib := range []int{1, 4, 128} {
		for _, hot := range []bool{false, true} {
			opts, events, heads, timelines := e02Fixture(mib, hot)
			opts.ScratchDir = t.TempDir()
			h := sha256.New()
			ref, err := Build(context.Background(), h, opts, BuildInput{&sliceEntryIterator{entries: events}, &sliceEntryIterator{entries: heads}, &sliceTimelineCatalog{timelines: timelines}})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%dMiB hot=%t bytes=%d sha256=%x", mib, hot, ref.ObjectSize, h.Sum(nil))
			requireEmptyScratch(t, opts.ScratchDir)
		}
	}
}
