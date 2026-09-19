package runingest

import (
	"bytes"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

type batchBenchmarkFixture struct {
	name          string
	records, keys int
	valueSizes    []int
	timelineSizes []int
	headers       int
}

func reportBatchBenchmark(b *testing.B, a BatchAccounting, records int, reason SealReason, rejected, rolledback int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(records), "ns/record")
	b.ReportMetric(float64(a.CanonicalBytes), "canonical-B/batch")
	b.ReportMetric(float64(a.EstimatedRunBytes), "estimated-B/batch")
	b.ReportMetric(float64(a.ChargedBytes), "charged-B")
	b.ReportMetric(float64(a.HighWater), "highwater-B")
	b.ReportMetric(float64(a.Copies.ValueBytes+a.Copies.HeaderBytes+a.Copies.AnnotationBytes+a.Copies.TimelineBytes), "source-copy-B/batch")
	b.ReportMetric(float64(a.Copies.ValueCopies+a.Copies.HeaderCopies+a.Copies.AnnotationCopies+a.Copies.TimelineCopies), "copies/batch")
	b.ReportMetric(float64(a.Copies.ValueBytes), "value-copy-B/batch")
	b.ReportMetric(float64(a.Copies.HeaderBytes), "header-copy-B/batch")
	b.ReportMetric(float64(a.Copies.AnnotationBytes), "annotation-copy-B/batch")
	b.ReportMetric(float64(a.Copies.TimelineBytes), "timeline-copy-B/batch")
	b.ReportMetric(float64(a.TimelineCount), "timelines/batch")
	b.ReportMetric(float64(a.PayloadCapacity), "payload-cap-B")
	b.ReportMetric(float64(a.PayloadMetadata), "payload-meta-B")
	b.ReportMetric(float64(a.TimelineCapacity), "timeline-cap-B")
	b.ReportMetric(float64(a.CatalogTable), "catalog-table-B")
	b.ReportMetric(float64(a.CatalogMetadata), "catalog-meta-B")
	b.ReportMetric(float64(a.DescriptorCapacity), "record-cap")
	b.ReportMetric(float64(a.DescriptorBytes), "record-cap-B")
	b.ReportMetric(float64(a.HeadroomBytes), "headroom-B")
	b.ReportMetric(float64(rejected), "rejected/batch")
	b.ReportMetric(float64(rolledback), "rollback/batch")
	for _, r := range []SealReason{SealTargetBytes, SealChargedBytes, SealControl} {
		n := 0.0
		if reason == r {
			n = 1
		}
		name := "control-seals/batch"
		if r == SealTargetBytes {
			name = "target-seals/batch"
		}
		if r == SealChargedBytes {
			name = "hard-seals/batch"
		}
		b.ReportMetric(n, name)
	}
	b.ReportMetric(0, "requests/batch")
	b.ReportMetric(0, "scratch-B")
	b.ReportMetric(0, "spills/batch")
}

// One op is a fresh complete slot: construction, append, one Seal, accounting
// snapshot and Close. Fixtures/source buffers are outside timing. The 1M cases
// intentionally raise operational limits to measure scaling, not default policy.
// There is no reuse benchmark because E05 deliberately has no Reset lifecycle.
func BenchmarkBatchAppendSeal(b *testing.B) {
	for _, tc := range []batchBenchmarkFixture{
		{"small-1K", 1000, 1000, []int{64}, []int{64}, 0},
		{"unique-100K", 100000, 100000, []int{64}, []int{64}, 0},
		{"unique-1M", 1000000, 1000000, []int{64}, []int{64}, 0},
		{"hot-100K", 100000, 16, []int{256}, []int{17}, 0},
		{"mixed-100K", 100000, 10000, []int{256}, []int{8, 17, 64, 512}, 2},
		{"variable-100K", 100000, 10000, []int{64, 256, 256, 1024, 1024, 256, 16384, 65536}, []int{8, 17, 64, 512}, 3},
		{"headers-1K", 1000, 100, []int{64}, []int{64}, 128},
		{"large-1K", 1000, 1000, []int{65536}, []int{512}, 0},
		{"singleton", 1, 1, []int{8 << 20}, []int{512}, 3},
	} {
		b.Run(tc.name, func(b *testing.B) {
			keys := catalogBenchmarkKeys(tc.keys, tc.timelineSizes)
			values := make([][]byte, len(tc.valueSizes))
			for i, n := range tc.valueSizes {
				values[i] = bytes.Repeat([]byte{byte(i)}, n)
			}
			headers := make([]BorrowedHeader, tc.headers)
			for i := range headers {
				headers[i] = BorrowedHeader{Key: []byte("ordered-duplicate"), Value: bytes.Repeat([]byte{byte(i)}, i%31)}
			}
			annotation := []byte{0, 255, 4, 9}
			c := batchTestConfig()
			c.MaxRecords, c.MaxTimelines = uint32(tc.records+1), uint32(tc.keys+1)
			c.SlabBytes, c.LargeThreshold, c.MaxSlotChargedBytes = 64<<10, 64<<10, 3<<30
			c.TargetRunBytes, c.MaxResidence = 2<<30, time.Hour
			if tc.records == 1 {
				c.TargetRunBytes = 4 << 20
			}
			var final BatchAccounting
			var reason SealReason
			var expectedCanonical, expectedValue, expectedHeader, expectedTimeline uint64
			for _, key := range keys {
				expectedTimeline += uint64(len(key))
			}
			for _, h := range headers {
				expectedHeader += uint64(len(h.Key) + len(h.Value))
			}
			expectedHeader *= uint64(tc.records)
			for i := 0; i < tc.records; i++ {
				r := BorrowedRecord{Offset: int64(i), LeaderEpoch: -1, Timeline: keys[i%len(keys)], Value: values[i%len(values)], Headers: headers, Annotations: annotation}
				n, err := runcontract.EventSize(r.event())
				if err != nil {
					b.Fatal(err)
				}
				expectedCanonical += uint64(n)
				expectedValue += uint64(len(r.Value))
			}
			b.ReportAllocs()
			b.SetBytes(int64(expectedCanonical))
			b.ResetTimer()
			for range b.N {
				s, err := newBatchSlot(c)
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < tc.records; i++ {
					r := BorrowedRecord{Offset: int64(i), LeaderEpoch: -1, Timeline: keys[i%len(keys)], Value: values[i%len(values)], Headers: headers, Annotations: annotation}
					out, err := s.AppendBorrowed(r, batchTestTime)
					if err != nil || (out.Disposition != AppendMutable && out.Disposition != AppendSingleton) {
						b.Fatal(i, out, err)
					}
				}
				if _, err := s.Seal(SealControl); err != nil {
					b.Fatal(err)
				}
				final, reason = s.Accounting(), s.reason
				s.Close()
			}
			b.StopTimer()
			if final.CanonicalBytes != expectedCanonical || final.Copies.ValueBytes != expectedValue ||
				final.Copies.HeaderBytes != expectedHeader || final.Copies.AnnotationBytes != uint64(4*tc.records) ||
				final.Copies.TimelineBytes != expectedTimeline || final.Copies.TimelineCopies != uint64(tc.keys) ||
				final.Copies.ValueCopies != uint64(tc.records) || final.Copies.HeaderCopies != uint64(2*tc.headers*tc.records) ||
				final.Copies.AnnotationCopies != uint64(tc.records) || final.HighWater > c.MaxSlotChargedBytes {
				b.Fatal("copy or accounting mismatch", final)
			}
			reportBatchBenchmark(b, final, tc.records, reason, 0, 0)
		})
	}
}

// Both transitions exercise a rejected second append with no mutation followed
// by the explicit seal boundary. A valid large retry belongs in a new slot.
func BenchmarkBatchTransitions(b *testing.B) {
	for _, hard := range []bool{false, true} {
		name := "soft"
		if hard {
			name = "hard"
		}
		b.Run(name, func(b *testing.B) {
			c := batchTestConfig()
			r := batchRecord(0, "transition")
			r.Value = make([]byte, 512)
			probe, err := newBatchSlot(c)
			if err != nil {
				b.Fatal(err)
			}
			appendBatch(b, probe, r)
			r.Offset = 1
			n, _ := runcontract.EventSize(r.event())
			p, err := probe.plan(r, n)
			if err != nil {
				b.Fatal(err)
			}
			if hard {
				c.MaxSlotChargedBytes = p.peak - 1
			} else {
				c.TargetRunBytes = p.estimate - 1
			}
			probe.Close()
			var final BatchAccounting
			var reason SealReason
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s, err := newBatchSlot(c)
				if err != nil {
					b.Fatal(err)
				}
				r.Offset = 0
				if out, err := s.AppendBorrowed(r, batchTestTime); err != nil || out.Disposition != AppendMutable {
					b.Fatal(out, err)
				}
				r.Offset = 1
				out, err := s.AppendBorrowed(r, batchTestTime)
				if err != nil || out.Disposition != AppendSealFirst {
					b.Fatal(out, err)
				}
				if _, err := s.Seal(out.Reason); err != nil {
					b.Fatal(err)
				}
				final, reason = s.Accounting(), s.reason
				s.Close()
			}
			b.StopTimer()
			reportBatchBenchmark(b, final, 1, reason, 1, 0)
		})
	}
}

func BenchmarkBatchSealIdempotent(b *testing.B) {
	s := testBatch(b, batchTestConfig())
	appendBatch(b, s, batchRecord(0, "seal"))
	want := sealBatch(b, s)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		got, err := s.Seal(SealControl)
		if err != nil || got != want {
			b.Fatal(err)
		}
	}
}
