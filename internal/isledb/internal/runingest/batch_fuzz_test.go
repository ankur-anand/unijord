package runingest

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

// The linear model owns test-only expected bytes. It never uses hashes as
// identity or slot planning as an oracle. Inputs cap operations and every
// allocation: at most 64 records, 16 timelines, 256 payload bytes, 64 KiB charge.
func modelBatch(t testing.TB, input []byte) {
	t.Helper()
	if len(input) > 256 {
		input = input[:256]
	}
	c := batchTestConfig()
	c.MaxRecords, c.MaxTimelines, c.MaxSlotChargedBytes = 64, 16, 64<<10
	c.TargetRunBytes, c.SlabBytes, c.LargeThreshold = 8192, 256, 256
	credits := &batchCredits{}
	c.Credits = credits
	s, err := newBatchSlot(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.catalog.hooks.hash = func([]byte) uint64 { return 0 }
	var timelines, values [][]byte
	var ids []timelineID
	var offsets []int64
	next, first := int64(0), int64(-1)
	var missing, gaps uint64
	sealed := false
	for i := 0; i+3 < len(input); i += 4 {
		code, key, length, gap := input[i], input[i+1], input[i+2], input[i+3]
		if code%11 == 0 {
			b, err := s.PollSeal(batchTestTime.Add(time.Second), 0)
			if err != nil || (b != nil) != (len(values) != 0) {
				t.Fatal(b, err)
			}
			sealed = len(values) != 0
			continue
		}
		r := batchRecord(next, string([]byte{0, key % 16, 255}))
		r.Value = bytes.Repeat([]byte{length}, int(length))
		r.Annotations = []byte{code}
		if code%3 == 0 {
			r.Offset += int64(gap%4 + 1)
			r.Gap = SourceGap{next, r.Offset, GapCompacted}
		}
		invalid := code%7 == 0
		if invalid {
			r.Flags = 255
		}
		fault := code%5 == 0
		if fault {
			s.hooks.encode = func(dst []byte, e runcontract.Event) ([]byte, error) {
				clear(dst)
				return nil, errBatchInjected
			}
		}
		before := snapshotBatch(t, s)
		out, err := s.AppendBorrowed(r, batchTestTime)
		s.hooks.encode = nil
		accepted := out.Disposition == AppendMutable || out.Disposition == AppendSealed || out.Disposition == AppendSingleton
		if accepted {
			if sealed || invalid || fault || err != nil {
				t.Fatal(out, err, sealed, invalid, fault)
			}
			id := -1
			for j, timeline := range timelines {
				if bytes.Equal(timeline, r.Timeline) {
					id = j
					break
				}
			}
			if id < 0 {
				id = len(timelines)
				timelines = append(timelines, bytes.Clone(r.Timeline))
			}
			ids = append(ids, timelineID(id))
			n, err := runcontract.EventSize(r.event())
			if err != nil {
				t.Fatal(err)
			}
			value, err := runcontract.EncodeEvent(make([]byte, n), r.event())
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, value)
			offsets = append(offsets, r.Offset)
			if first < 0 {
				first = r.Offset
			}
			if r.Offset > next {
				gaps++
				missing += uint64(r.Offset - next)
			}
			next = r.Offset + 1
			sealed = out.Disposition != AppendMutable
		} else {
			unchangedBatch(t, s, before)
			if sealed && out.Disposition != AppendUnavailable {
				t.Fatal(out)
			}
			if !sealed && invalid && out.Disposition != AppendInvalid {
				t.Fatal(out)
			}
			if out.Disposition == AppendSealFirst {
				b, err := s.Seal(out.Reason)
				if err != nil || b == nil {
					t.Fatal(b, err)
				}
				sealed = true
			}
		}
		clear(r.Value)
		clear(r.Timeline)
		clear(r.Annotations)
		if s.interval.Next != next || s.interval.FirstObserved != first || s.interval.GapCount != gaps || s.interval.MissingOffsets != missing || len(s.records) != len(values) || len(s.catalog.metadata) != len(timelines) {
			t.Fatal("source/catalog model divergence")
		}
		for j, expected := range values {
			if s.records[j].Timeline != ids[j] || s.records[j].Offset != offsets[j] {
				t.Fatal("descriptor model divergence")
			}
			checkArenaValue(t, s.payload, s.records[j].Value, expected)
		}
		for j, timeline := range timelines {
			if !bytes.Equal(s.catalog.bytes(timelineID(j)), timeline) {
				t.Fatal("timeline model divergence")
			}
		}
		checkBatchAccounting(t, s)
		if credits.current != s.accounting.ChargedBytes || credits.highWater > c.MaxSlotChargedBytes {
			t.Fatal(credits, s.accounting)
		}
	}
	s.Close()
	if credits.current != 0 {
		t.Fatal("leaked credit")
	}
}

func TestBatchRandomizedModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 20260919))
	var input [256]byte
	for range 128 {
		for i := range input {
			input[i] = byte(rng.Uint32())
		}
		modelBatch(t, input[:])
	}
}

func FuzzBatchAppend(f *testing.F) {
	f.Add([]byte{1, 0, 64, 0, 3, 0, 255, 3, 5, 1, 10, 0, 7, 2, 0, 0, 11, 0, 0, 0})
	f.Add(bytes.Repeat([]byte{1, 2, 3, 4}, 64))
	f.Fuzz(func(t *testing.T, input []byte) { modelBatch(t, input) })
}
