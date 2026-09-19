package manifest

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
)

var (
	runBenchmarkManifest   *RunManifest
	runBenchmarkBytes      []byte
	runBenchmarkRun        *RunMeta
	runBenchmarkCandidates []*RunMeta
	runBenchmarkPage       *RunPage
)

func benchmarkRunManifest(n int, lower bool) *RunManifest {
	m := testRunManifest()
	runs := make([]RunMeta, n)
	for i := range runs {
		r := testRun(uint64(i+1), 0)
		r.ObjectKey = fmt.Sprintf("runs/%016x.ujrn", i+1)
		if lower {
			r.Level = uint32(i%32 + 1)
			r.MinTimeline = make([]byte, 5+i%31)
			r.MinTimeline[0] = 0
			binary.BigEndian.PutUint32(r.MinTimeline[1:5], uint32(i))
			for j := 5; j < len(r.MinTimeline); j++ {
				r.MinTimeline[j] = byte(j * 255)
			}
			r.MaxTimeline = r.MinTimeline
		} else {
			r.MinTimeline = make([]byte, 1+i%31)
			r.MaxTimeline = make([]byte, 1+i%31)
			r.MaxTimeline[0] = 255
		}
		fixRunDirectory(&r)
		// Choose variable bounds in multiples compatible with exact 1/5 TiB
		// geometry. Add table-key padding to make directory length aligned.
		pad := (8 - r.DirectoryLength%8) % 8
		r.Events.MaxKey = append(r.Events.MaxKey, make([]byte, int(pad))...)
		fixRunDirectory(&r)
		r.ObjectSize = 1 << 40
		if i%2 != 0 {
			r.ObjectSize = 5 << 40
		}
		r.DirectoryOffset = r.ObjectSize - r.DirectoryLength - 160
		runs[i] = r
	}
	if !lower {
		m.L0Runs = runs
	} else {
		for l := uint32(1); l <= 32; l++ {
			level := RunLevel{Number: l}
			for i := int(l - 1); i < n; i += 32 {
				level.Runs = append(level.Runs, runs[i])
			}
			if len(level.Runs) > 0 {
				m.Levels = append(m.Levels, level)
			}
		}
	}
	if err := m.BuildIndexes(); err != nil {
		panic(err)
	}
	return m
}

func runBenchmarkMeasure(b *testing.B, n int, fn func()) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn()
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	denom := float64(b.N) * float64(n)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/denom, "ns/run")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/denom, "allocs/run")
}

func BenchmarkE07(b *testing.B) {
	for _, n := range []int{1, 1000, 100000, MaxManifestRuns} {
		for _, lower := range []bool{false, true} {
			layout := "l0"
			if lower {
				layout = "levels"
			}
			b.Run(fmt.Sprintf("%d/%s", n, layout), func(b *testing.B) {
				m := benchmarkRunManifest(n, lower)
				clone, err := m.Clone()
				if err != nil {
					b.Fatal(err)
				}
				encoded, err := EncodeRunCheckpoint(m)
				if err != nil {
					b.Fatal(err)
				}
				phases := []struct {
					name string
					fn   func()
				}{
					{"Validate", func() {
						if err := m.Validate(); err != nil {
							b.Fatal(err)
						}
					}},
					{"Clone", func() {
						c, err := m.Clone()
						if err != nil {
							b.Fatal(err)
						}
						runBenchmarkManifest = c
					}},
					{"Equal", func() {
						if !m.Equal(clone) {
							b.Fatal("unequal")
						}
					}},
					{"CheckpointEncode", func() {
						v, err := EncodeRunCheckpoint(m)
						if err != nil {
							b.Fatal(err)
						}
						runBenchmarkBytes = v
					}},
					{"CheckpointDecode", func() {
						v, err := DecodeRunCheckpoint(encoded)
						if err != nil {
							b.Fatal(err)
						}
						runBenchmarkManifest = v
					}},
					{"IndexBuild", func() { m.buildIndexes() }},
				}
				for _, p := range phases {
					b.Run(p.name, func(b *testing.B) {
						runBenchmarkMeasure(b, n, p.fn)
						b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
						b.ReportMetric(float64(m.IndexBytes()), "index-bytes")
					})
				}
				b.Run("Lookup", func(b *testing.B) {
					var stats RunLookupStats
					var err error
					if lower {
						q := m.Levels[len(m.Levels)/2].Runs[len(m.Levels[len(m.Levels)/2].Runs)/2].MinTimeline
						runBenchmarkMeasure(b, 1, func() {
							var r *RunMeta
							r, stats, err = m.LevelCandidate(m.Levels[len(m.Levels)/2].Number, q)
							if err != nil || r == nil {
								b.Fatal(err)
							}
							runBenchmarkRun = r
						})
					} else {
						dst := make([]*RunMeta, 0, n)
						runBenchmarkMeasure(b, 1, func() {
							dst, stats, err = m.L0Candidates([]byte{128}, dst[:0])
							if err != nil || len(dst) != n {
								b.Fatal(err)
							}
							runBenchmarkCandidates = dst
						})
					}
					b.ReportMetric(float64(stats.Comparisons), "comparisons/op")
					b.ReportMetric(float64(stats.Candidates), "candidates/op")
				})
				// Page phases measure a bounded leaf independently of checkpoints.
				pn := min(n, MaxRunPageEntries)
				p := &RunPage{SeqLo: 1, SeqHi: uint64(pn), Count: uint32(pn), Entries: make([]RunLogEntry, pn)}
				for i := range p.Entries {
					r := testRun(uint64(i+1), 0)
					p.Entries[i] = RunLogEntry{Op: RunLogAdd, Revision: uint64(i + 1), NextSequence: uint64(i + 2), AddRuns: []RunMeta{r}}
				}
				page, err := EncodeRunPage(p)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("PageEncode", func(b *testing.B) {
					runBenchmarkMeasure(b, pn, func() {
						v, err := EncodeRunPage(p)
						if err != nil {
							b.Fatal(err)
						}
						runBenchmarkBytes = v
					})
					b.ReportMetric(float64(len(page)), "page-bytes")
				})
				b.Run("PageDecode", func(b *testing.B) {
					runBenchmarkMeasure(b, pn, func() {
						v, err := DecodeRunPage(page)
						if err != nil {
							b.Fatal(err)
						}
						runBenchmarkPage = v
					})
					b.ReportMetric(float64(len(page)), "page-bytes")
				})
			})
		}
	}
}
