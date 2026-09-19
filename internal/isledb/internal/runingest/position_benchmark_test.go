package runingest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/runfile"
)

type positionFixture struct {
	name                           string
	records, timelines, valueBytes int
	variable, existing, seals      bool
}

var positionFixtures = []positionFixture{
	{"hot-1K", 1000, 8, 256, false, false, false},
	{"unique-1K", 1000, 1000, 64, false, false, false},
	{"binary-1K", 1000, 127, 1024, true, true, false},
	{"seals-1K", 1000, 1000, 64, true, true, true},
	{"hot-100K", 100000, 8, 256, false, true, false},
	{"unique-100K", 100000, 100000, 64, false, false, false},
	{"mixed-100K", 100000, 4096, 256, false, true, false},
	{"binary-100K", 100000, 4096, 64, true, true, true},
	{"unique-1M", 1000000, 1000000, 64, false, false, false},
	{"hot-1M", 1000000, 8, 64, false, true, true},
}

func positionBenchmarkFixture(b *testing.B, f positionFixture) (*batchSlot, *sealedBatch, []ResolvedHead) {
	b.Helper()
	c := batchTestConfig()
	c.MaxRecords = uint32(f.records + 1)
	c.MaxTimelines = uint32(f.timelines + 1)
	c.MaxSlotChargedBytes = 2 << 30
	c.TargetRunBytes = 2 << 30
	c.SlabBytes = 64 << 10
	c.LargeThreshold = 64 << 10
	c.HeadroomBytes = uint64(f.records)*12 + uint64(f.timelines)*(positionedHeadBytes+4) + positionFixedBytes
	c.MaxResidence = time.Hour
	s, err := newBatchSlot(c)
	if err != nil {
		b.Fatal(err)
	}
	// Fixtures are outside timers and reported as setup sites in profiles.
	timelines := make([][]byte, f.timelines)
	for i := range timelines {
		n := 64
		if f.variable {
			n = 8 + (i*37)%505
		}
		x := make([]byte, n)
		binary.BigEndian.PutUint64(x, uint64(i))
		for j := 8; j < n; j++ {
			x[j] = byte(i + j*31)
		}
		timelines[i] = x
	}
	value := make([]byte, f.valueBytes)
	for i := range value {
		value[i] = byte(i * 29)
	}
	for i := 0; i < f.records; i++ {
		// Reverse interleaving makes Kafka order differ from exact byte order.
		id := f.timelines - 1 - i%f.timelines
		r := BorrowedRecord{Offset: int64(i), LeaderEpoch: -1, Timeline: timelines[id], Value: value}
		if i%3 == 0 {
			r.Flags = RecordTimestampPresent
			r.TimestampMS = int64(f.records - i)
		}
		if f.seals && i+f.timelines >= f.records {
			r.Flags |= RecordSeal
		}
		appendBatch(b, s, r)
	}
	sealed := sealBatch(b, s)
	heads := missingHeads(s)
	for i := range heads {
		if f.existing {
			heads[i].Present = true
			heads[i].Head.NextLSN = 2 + uint64(i%100)
		}
	}
	return s, sealed, heads
}

// Each stage excludes fixture creation. Scan/sort/iteration reuse already
// charged scalar/scratch storage; sorts include initializing their uint32 order
// and iterations include resetting their scalar cursor. Timing those tiny
// operations avoids per-iteration timer/GC-stat overhead dominating calibration.
// Position measures fresh allocation and Close.
// Prepare excludes Position, Input and prepared.Close. WriteTo excludes Prepare.
// Resetting benchmark-private iterator cursors is not a public rewind API.
func BenchmarkPosition(b *testing.B) {
	for _, f := range positionFixtures {
		b.Run(f.name, func(b *testing.B) {
			for _, phase := range []string{"Position", "Scan", "EventsSort", "HeadsSort", "EventsIterate", "HeadsIterate", "Prepare", "WriteTo"} {
				b.Run(phase, func(b *testing.B) {
					s, sealed, heads := positionBenchmarkFixture(b, f)
					defer s.Close()
					before := s.accounting.Copies
					p, err := positionBatch(sealed, heads, positionTestOptions())
					if err != nil {
						b.Fatal(err)
					}
					defer p.Close()
					a := p.accounting
					dir := b.TempDir()
					opts := positionBuildOptions(p, dir)
					in, err := p.Input()
					if err != nil {
						b.Fatal(err)
					}
					defer in.Close()
					var prepared runfile.PreparedRun
					var scratch uint64
					// Obtain deterministic output evidence outside the measured region.
					if phase == "Prepare" || phase == "WriteTo" {
						prepared, err = runfile.Prepare(context.Background(), opts, in.BuildInput())
						if err != nil {
							b.Fatal(err)
						}
						defer prepared.Close()
						files, err := os.ReadDir(dir)
						if err != nil {
							b.Fatal(err)
						}
						for _, file := range files {
							info, err := file.Info()
							if err != nil {
								b.Fatal(err)
							}
							scratch += uint64(info.Size())
						}
						h := sha256.New()
						if err := prepared.WriteTo(context.Background(), h); err != nil {
							b.Fatal(err)
						}
						b.Logf("SHA256 %x scratch=%d bytes", h.Sum(nil), scratch)
					}
					if phase == "Position" || phase == "Prepare" {
						if prepared != nil {
							_ = prepared.Close()
						}
						_ = in.Close()
						_ = p.Close()
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						switch phase {
						case "Position":
							q, err := positionBatch(sealed, heads, positionTestOptions())
							if err != nil {
								b.Fatal(err)
							}
							_ = q.Close()
						case "Scan":
							if err := p.scan(heads); err != nil {
								b.Fatal(err)
							}
						case "EventsSort":
							for i := range p.eventOrder {
								p.eventOrder[i] = uint32(i)
							}
							p.sortEvents()
						case "HeadsSort":
							for i := range p.headOrder {
								p.headOrder[i] = uint32(i)
							}
							p.sortHeads()
						case "EventsIterate":
							in.events.index = 0
							in.events.closed = false
							for in.events.Next() {
								_ = in.events.Entry()
							}
							if err := in.events.Err(); err != nil {
								b.Fatal(err)
							}
						case "HeadsIterate":
							in.heads.index = 0
							in.heads.closed = false
							for in.heads.Next() {
								_ = in.heads.Entry()
							}
							if err := in.heads.Err(); err != nil {
								b.Fatal(err)
							}
						case "Prepare":
							b.StopTimer()
							q, err := positionBatch(sealed, heads, positionTestOptions())
							if err != nil {
								b.Fatal(err)
							}
							input, err := q.Input()
							if err != nil {
								_ = q.Close()
								b.Fatal(err)
							}
							b.StartTimer()
							pr, err := runfile.Prepare(context.Background(), opts, input.BuildInput())
							if err != nil {
								_ = input.Close()
								_ = q.Close()
								b.Fatal(err)
							}
							b.StopTimer()
							if err := pr.Close(); err != nil {
								_ = input.Close()
								_ = q.Close()
								b.Fatal(err)
							}
							_ = input.Close()
							_ = q.Close()
							b.StartTimer()
						case "WriteTo":
							if err := prepared.WriteTo(context.Background(), io.Discard); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					if prepared != nil {
						_ = prepared.Close()
					}
					_ = in.Close()
					_ = p.Close()
					if s.accounting.Copies != before {
						b.Fatal("source copy accounting changed")
					}
					after := s.accounting.Copies
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(f.records), "ns/record")
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(f.timelines), "ns/timeline")
					b.ReportMetric(float64(a.ChargedBytes), "position-B")
					b.ReportMetric(float64(a.SlotHighWater), "slot-high-B")
					b.ReportMetric(float64(a.NewCredits), "new-credit-B")
					b.ReportMetric(float64(a.EventsOrderBytes), "events-order-B")
					b.ReportMetric(float64(a.HeadsOrderBytes), "heads-order-B")
					b.ReportMetric(float64(a.IteratorScratchBytes), "iterator-scratch-B")
					b.ReportMetric(float64(scratch), "prepare-scratch-B")
					b.ReportMetric(float64(after.ValueBytes-before.ValueBytes+after.HeaderBytes-before.HeaderBytes+after.AnnotationBytes-before.AnnotationBytes), "payload-copy-B")
					b.ReportMetric(float64(after.ValueCopies-before.ValueCopies+after.HeaderCopies-before.HeaderCopies+after.AnnotationCopies-before.AnnotationCopies), "payload-copies")
					b.ReportMetric(float64(after.TimelineBytes-before.TimelineBytes), "timeline-copy-B")
					b.ReportMetric(float64(after.TimelineCopies-before.TimelineCopies), "timeline-copies")
					b.ReportMetric(1, "uint32-sort")
					b.ReportMetric(0, "object-requests")
					b.ReportMetric(0, "spills")
				})
			}
		})
	}
}

func TestPositionAllocationPhases(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	for i := 0; i < 100; i++ {
		appendBatch(t, s, batchRecord(int64(i), fmt.Sprint(i%17)))
	}
	b := sealBatch(t, s)
	heads := missingHeads(s)
	p := testPosition(t, b, heads, positionTestOptions())
	in := testPositionInput(t, p)
	if allocations := testing.AllocsPerRun(10, func() {
		if err := p.scan(heads); err != nil {
			panic(err)
		}
		p.sortEvents()
		p.sortHeads()
		in.events.index = 0
		in.heads.index = 0
		for in.events.Next() {
			_ = in.events.Entry()
		}
		for in.heads.Next() {
			_ = in.heads.Entry()
		}
	}); allocations != 0 {
		t.Fatalf("scan/sort/iteration allocations = %g", allocations)
	}
}
