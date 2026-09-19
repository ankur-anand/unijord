package runingest

import (
	"fmt"
	"testing"
)

var arenaBenchmarkRef arenaRef

// One operation is one batch, including Reset in reuse mode. Fixture generation
// and reuse warmup are outside the timer. Sizes model synthetic workloads, not
// measured production traffic. Seed 0xe03 is stable across slab hypotheses.
func arenaBenchmarkSizes(name string, slab int) []int {
	var sizes []int
	var total int
	seed := uint64(0xe03)
	for total < 4<<20 {
		n := 64
		switch name {
		case "fixed-256":
			n = 256
		case "fixed-1KiB":
			n = 1 << 10
		case "fixed-16KiB":
			n = 16 << 10
		case "fixed-64KiB":
			n = 64 << 10
		case "variable":
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			switch p := seed % 100; {
			case p < 15:
				n = 8
			case p < 30:
				n = 17
			case p < 50:
				n = 64
			case p < 70:
				n = 256
			case p < 85:
				n = 1 << 10
			case p < 93:
				n = 4 << 10
			case p < 98:
				n = 16 << 10
			case p < 99:
				n = 64 << 10
			default:
				n = 128 << 10
			}
		case "mixed-tail":
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			switch p := seed % 100; {
			case p < 80:
				n = 256
			case p < 95:
				n = 16 << 10
			default:
				n = 256 << 10
			}
		case "boundary":
			n = slab
		case "boundary-plus-one":
			n = slab + 1
		case "million-64":
			sizes = make([]int, 1_000_000)
			for i := range sizes {
				sizes[i] = 64
			}
			return sizes
		}
		sizes = append(sizes, n)
		total += n
	}
	return sizes
}

func reportArenaBenchmark(b *testing.B, s arenaStats, count int) {
	b.ReportMetric(float64(count), "fields/op")
	b.ReportMetric(float64(s.LogicalBytes), "logical-B")
	b.ReportMetric(float64(s.CopyBytes), "copy-B")
	b.ReportMetric(float64(s.CopyCount), "copy-count")
	b.ReportMetric(float64(s.NormalBytes), "normal-res-B")
	b.ReportMetric(float64(s.LargeBytes), "large-res-B")
	b.ReportMetric(float64(s.DescriptorBytes), "descriptor-B")
	b.ReportMetric(float64(s.ChargedBytes), "charged-B")
	b.ReportMetric(float64(s.HighWater), "highwater-B")
	reserved := s.NormalBytes + s.LargeBytes
	if reserved != 0 {
		b.ReportMetric(float64(reserved-s.LogicalBytes)/float64(reserved), "waste-ratio")
	}
	if count != 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(count), "ns/field")
	}
}

func BenchmarkArena(b *testing.B) {
	for _, slab := range []int{32 << 10, 64 << 10, 128 << 10} {
		b.Run(fmt.Sprintf("slab-%dKiB", slab>>10), func(b *testing.B) {
			for _, name := range []string{"fixed-64", "fixed-256", "fixed-1KiB", "fixed-16KiB", "fixed-64KiB", "variable", "mixed-tail", "boundary", "boundary-plus-one", "million-64"} {
				b.Run(name, func(b *testing.B) {
					sizes := arenaBenchmarkSizes(name, slab)
					maxSize, total := 0, 0
					for _, n := range sizes {
						if n > maxSize {
							maxSize = n
						}
						total += n
					}
					source := make([]byte, maxSize)
					for i := range source {
						source[i] = byte(i*31 + 17)
					}
					for _, reuse := range []bool{false, true} {
						mode := "fresh"
						if reuse {
							mode = "reuse"
						}
						b.Run(mode, func(b *testing.B) {
							a := testArena(b, uint64(slab), uint64(slab), 256<<20)
							appendBatch := func() {
								for _, n := range sizes {
									r, err := a.Append(source[:n])
									if err != nil {
										b.Fatal(err)
									}
									arenaBenchmarkRef = r
								}
							}
							if reuse {
								appendBatch()
							}
							b.ReportAllocs()
							b.SetBytes(int64(total))
							b.ResetTimer()
							for range b.N {
								if reuse {
									if err := a.Reset(); err != nil {
										b.Fatal(err)
									}
								} else {
									a = testArena(b, uint64(slab), uint64(slab), 256<<20)
								}
								appendBatch()
							}
							b.StopTimer()
							checkArenaAccounting(b, a)
							if s := a.Stats(); s.CopyBytes != uint64(total) || s.CopyCount != uint64(len(sizes)) {
								b.Fatal(s)
							}
							reportArenaBenchmark(b, a.Stats(), len(sizes))
						})
					}
				})
			}
		})
	}
}

func BenchmarkArenaReject(b *testing.B) {
	for _, slab := range []uint64{32 << 10, 64 << 10, 128 << 10} {
		b.Run(fmt.Sprintf("slab-%dKiB", slab>>10), func(b *testing.B) {
			a := testArena(b, slab, slab, slab+arenaBlockBytes)
			source := make([]byte, int(slab))
			appendArena(b, a, source)
			before := a.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if r, err := a.Append(source); r != (arenaRef{}) || err != errArenaLimit {
					b.Fatal(r, err)
				}
			}
			b.StopTimer()
			if a.Stats() != before {
				b.Fatal("rejection changed state")
			}
			reportArenaBenchmark(b, a.Stats(), 0)
		})
	}
}
