package runingest

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// Fixtures are built outside the timer. Serial bytes guarantee exact uniqueness;
// little-endian order requires real sorting rather than an already sorted scan.
func catalogBenchmarkKeys(n int, lengths []int) [][]byte {
	keys := make([][]byte, n)
	total := 0
	for i := range keys {
		total += lengths[i%len(lengths)]
	}
	backing := make([]byte, total)
	offset := 0
	for i := range keys {
		size := lengths[i%len(lengths)]
		keys[i] = backing[offset : offset+size : offset+size]
		offset += size
		binary.LittleEndian.PutUint64(keys[i], uint64(i))
		for j := 8; j < size; j++ {
			keys[i][j] = byte((i*31 + j*17) ^ 0xe04)
		}
	}
	return keys
}

func newBenchmarkCatalog(b *testing.B, n int) *timelineCatalog {
	b.Helper()
	c, err := newTimelineCatalog(timelineCatalogConfig{512 << 20, uint32(n), 64 << 10, 64 << 10})
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func reportCatalogBenchmark(b *testing.B, c *timelineCatalog, operations int) {
	s := c.Stats()
	checkCatalogAccounting(b, c)
	b.ReportMetric(float64(s.DistinctTimelines), "distinct/batch")
	b.ReportMetric(float64(s.Insertions), "inserts/batch")
	b.ReportMetric(float64(s.CopiedTimelineBytes), "copied-B/batch")
	b.ReportMetric(float64(s.ArenaReservedBytes), "arena-B")
	b.ReportMetric(float64(s.ArenaMetadataBytes), "arena-meta-B")
	b.ReportMetric(float64(s.TableReservedBytes), "table-B")
	b.ReportMetric(float64(s.StateIDMetadataBytes), "state-ID-B")
	b.ReportMetric(float64(s.ProjectionBytes), "projection-B")
	b.ReportMetric(float64(s.FixedBytes), "fixed-B")
	b.ReportMetric(float64(s.ChargedBytes), "charged-B")
	b.ReportMetric(float64(s.HighWater), "highwater-B")
	b.ReportMetric(float64(s.Probes), "probes/batch")
	b.ReportMetric(float64(s.MaxProbe), "max-probe")
	b.ReportMetric(float64(s.RehashProbes), "rehash-probes/batch")
	b.ReportMetric(float64(s.TableGrowths), "table-growths/batch")
	if s.ProbeOperations > 0 {
		b.ReportMetric(float64(s.Probes)/float64(s.ProbeOperations), "probes/call")
	}
	if operations > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(operations), "ns/insert")
	}
}

func BenchmarkTimelineCatalog(b *testing.B) {
	for _, tc := range []struct {
		name            string
		distinct, total int
		lengths         []int
		collision       bool
	}{
		{"unique-1K", 1000, 1000, []int{64}, false},
		{"unique-100K", 100000, 100000, []int{64}, false},
		{"unique-1M", 1000000, 1000000, []int{64}, false},
		{"hot-16", 16, 100000, []int{64}, false},
		{"mixed-10K", 10000, 100000, []int{64}, false},
		{"variable-100K", 100000, 100000, []int{8, 17, 64, 512}, false},
		{"binary-1K", 1000, 1000, []int{17}, false},
		{"maximum-1K", 1000, 1000, []int{512}, false},
		{"forced-collision-1K-diagnostic", 1000, 1000, []int{8}, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			keys := catalogBenchmarkKeys(tc.distinct, tc.lengths)
			var expected uint64
			for _, key := range keys {
				expected += uint64(len(key))
			}
			for _, reuse := range []bool{false, true} {
				name := "fresh"
				if reuse {
					name = "reuse"
				}
				b.Run(name, func(b *testing.B) {
					var c *timelineCatalog
					create := func() {
						c = newBenchmarkCatalog(b, tc.distinct)
						if tc.collision {
							c.hooks.hash = func([]byte) uint64 { return 7 }
						}
					}
					insert := func() {
						v := c.View()
						for i := 0; i < tc.total; i++ {
							if _, err := v.Intern(keys[i%len(keys)]); err != nil {
								b.Fatal(err)
							}
						}
					}
					if reuse {
						create()
						insert()
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if reuse {
							if err := c.Reset(); err != nil {
								b.Fatal(err)
							}
						} else {
							create()
						}
						insert()
					}
					b.StopTimer()
					if s := c.Stats(); s.CopiedTimelineBytes != expected || s.DistinctTimelines != uint64(tc.distinct) || s.Insertions != uint64(tc.total) {
						b.Fatal(s)
					}
					reportCatalogBenchmark(b, c, tc.total)
				})
			}
		})
	}
}

func BenchmarkTimelineCatalogLookup(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			keys := catalogBenchmarkKeys(2*n, []int{64})
			c := newBenchmarkCatalog(b, n)
			v := c.View()
			for _, key := range keys[:n] {
				if _, err := v.Intern(key); err != nil {
					b.Fatal(err)
				}
			}
			for _, hit := range []bool{true, false} {
				name := "miss"
				if hit {
					name = "hit"
				}
				b.Run(name, func(b *testing.B) {
					// Remove setup probes from the measured lookup statistics.
					c.stats.Probes, c.stats.ProbeOperations, c.stats.MaxProbe = 0, 0, 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						key := keys[n+i%n]
						if hit {
							key = keys[i%n]
						}
						if _, found, err := v.Lookup(key); err != nil || found != hit {
							b.Fatal(found, err)
						}
					}
					b.StopTimer()
					reportCatalogBenchmark(b, c, 0)
				})
			}
		})
	}
}

func BenchmarkTimelineCatalogProjection(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			keys := catalogBenchmarkKeys(n, []int{64})
			c := newBenchmarkCatalog(b, n)
			v := c.View()
			for _, key := range keys {
				if _, err := v.Intern(key); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := v.SortedIDs(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				// Test-only invalidation forces a complete rebuild over retained ID
				// capacity on every iteration; a cache hit is not a sort benchmark.
				c.order = c.order[:0]
				if _, err := v.SortedIDs(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportCatalogBenchmark(b, c, 0)
		})
	}
}

func BenchmarkTimelineCatalogGrowth(b *testing.B) {
	keys := catalogBenchmarkKeys(769, []int{64})
	var c *timelineCatalog
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		c = newBenchmarkCatalog(b, 1024)
		v := c.View()
		for _, key := range keys[:768] {
			if _, err := v.Intern(key); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if _, err := v.Intern(keys[768]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportCatalogBenchmark(b, c, 1)
}
