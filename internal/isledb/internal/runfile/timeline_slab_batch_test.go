package runfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestTimelineSlabBatchConstructionAndAccounting(t *testing.T) {
	tests := []struct {
		name                  string
		budget                uint64
		descriptorEntries     uint64
		wantReferenceCapacity int
		wantSlabCapacity      int
		wantSlabCapacityCount int
		wantInitialReserved   uint64
		wantReservedLimit     uint64
	}{
		{
			name:                  "minimum-budget-zero-descriptor-hint",
			budget:                35,
			wantReferenceCapacity: 1,
			wantSlabCapacity:      35,
			wantSlabCapacityCount: 2,
			wantInitialReserved:   96,
			wantReservedLimit:     166,
		},
		{
			name:                  "maximum-timeline-budget",
			budget:                546,
			descriptorEntries:     1_000,
			wantReferenceCapacity: 15,
			wantSlabCapacity:      546,
			wantSlabCapacityCount: 16,
			wantInitialReserved:   992,
			wantReservedLimit:     9_728,
		},
		{
			name:                  "normal-budget",
			budget:                1 << 20,
			descriptorEntries:     1_000,
			wantReferenceCapacity: 1_000,
			wantSlabCapacity:      64 << 10,
			wantSlabCapacityCount: 17,
			wantInitialReserved:   32_544,
			wantReservedLimit:     1_146_656,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch, err := newTimelineSlabBatch(test.budget, test.descriptorEntries)
			if err != nil {
				t.Fatal(err)
			}
			if cap(batch.batch) != test.wantReferenceCapacity {
				t.Fatalf("reference capacity=%d, want %d", cap(batch.batch), test.wantReferenceCapacity)
			}
			if batch.slabCapacity != test.wantSlabCapacity {
				t.Fatalf("slab capacity=%d, want %d", batch.slabCapacity, test.wantSlabCapacity)
			}
			if cap(batch.slabs) != test.wantSlabCapacityCount {
				t.Fatalf("slab descriptor capacity=%d, want %d", cap(batch.slabs), test.wantSlabCapacityCount)
			}
			if len(batch.slabs) != 0 {
				t.Fatalf("constructor allocated %d data slabs, want lazy allocation", len(batch.slabs))
			}
			metrics := batch.metrics()
			if metrics.ReservedBytes != test.wantInitialReserved {
				t.Fatalf("initial reserved bytes=%d, want %d", metrics.ReservedBytes, test.wantInitialReserved)
			}
			if metrics.ReservedLimit != test.wantReservedLimit {
				t.Fatalf("reserved limit=%d, want %d", metrics.ReservedLimit, test.wantReservedLimit)
			}
			if metrics.LogicalUsed != 0 || metrics.LogicalHighWater != 0 || metrics.Slabs != 0 || metrics.Resets != 0 {
				t.Fatalf("initial metrics=%+v", metrics)
			}
		})
	}
}

func TestTimelineSlabBatchConstructionRejectsInvalidAndOverflowingBudgets(t *testing.T) {
	for _, budget := range []uint64{0, minimumTimelineRecordBytes - 1} {
		if _, err := newTimelineSlabBatch(budget, 1); !errors.Is(err, ErrVerificationResource) {
			t.Fatalf("budget %d error=%v, want ErrVerificationResource", budget, err)
		}
	}

	if _, err := newTimelineSlabBatch(math.MaxUint64, 1); !errors.Is(err, ErrVerificationResource) {
		t.Fatalf("maximum budget error=%v, want checked resource failure", err)
	}
	if _, err := newTimelineSlabBatch(math.MaxUint64, math.MaxUint64); !errors.Is(err, ErrVerificationResource) {
		t.Fatalf("maximum budget and descriptor error=%v, want checked resource failure", err)
	}
}

func TestTimelineSlabBatchTimelineValidation(t *testing.T) {
	batch := mustNewTimelineSlabBatch(t, 1<<20, 10)

	if added, err := batch.tryAdd(nil); added || !errors.Is(err, ErrCorruptRun) {
		t.Fatalf("empty timeline=(%v,%v), want false ErrCorruptRun", added, err)
	}
	if added, err := batch.tryAdd(make([]byte, MaxTimelineBytes+1)); added || !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("513-byte timeline=(%v,%v), want false ErrRunTooLarge", added, err)
	}
	if len(batch.timelines()) != 0 || len(batch.slabs) != 0 {
		t.Fatalf("rejected timelines changed batch: timelines=%d slabs=%d", len(batch.timelines()), len(batch.slabs))
	}

	values := [][]byte{
		{0x00},
		bytes.Repeat([]byte{0xff}, int(MaxTimelineBytes)),
		{0x00, 0xff, 0xfe, 0x80, 0x00},
	}
	for _, value := range values {
		if added, err := batch.tryAdd(value); err != nil || !added {
			t.Fatalf("tryAdd(%x)=(%v,%v), want true nil", value, added, err)
		}
	}
	for index, value := range values {
		if !bytes.Equal(batch.timelines()[index], value) {
			t.Fatalf("timeline %d=%x, want %x", index, batch.timelines()[index], value)
		}
	}
}

func TestTimelineSlabBatchOwnsBorrowedAndReusedBuffers(t *testing.T) {
	batch := mustNewTimelineSlabBatch(t, 1<<20, 8)

	borrowed := []byte{1, 2, 3, 4}
	if added, err := batch.tryAdd(borrowed); err != nil || !added {
		t.Fatalf("first tryAdd=(%v,%v)", added, err)
	}
	copy(borrowed, []byte{9, 9, 9, 9})
	if got := batch.timelines()[0]; !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("owned timeline changed with caller buffer: %v", got)
	}

	scratch := make([]byte, 4)
	want := [][]byte{{10, 11, 12, 13}, {20, 21, 22, 23}, {30, 31, 32, 33}}
	for _, value := range want {
		copy(scratch, value)
		if added, err := batch.tryAdd(scratch); err != nil || !added {
			t.Fatalf("reused-buffer tryAdd=(%v,%v)", added, err)
		}
	}
	clear(scratch)
	for index, value := range want {
		if got := batch.timelines()[index+1]; !bytes.Equal(got, value) {
			t.Fatalf("reused timeline %d=%v, want %v", index, got, value)
		}
	}
}

func TestTimelineSlabBatchLogicalBudgetBoundaries(t *testing.T) {
	t.Run("minimum-record-exact-fit", func(t *testing.T) {
		batch := mustNewTimelineSlabBatch(t, minimumTimelineRecordBytes, 1)
		if added, err := batch.tryAdd([]byte{1}); err != nil || !added {
			t.Fatalf("tryAdd=(%v,%v), want exact fit", added, err)
		}
		if got := batch.metrics().LogicalUsed; got != minimumTimelineRecordBytes {
			t.Fatalf("logical used=%d, want %d", got, minimumTimelineRecordBytes)
		}
	})

	t.Run("maximum-record-exact-fit", func(t *testing.T) {
		const budget = MaxTimelineBytes + timelineSpillLengthBytes + timelineReferenceChargeBytes
		batch := mustNewTimelineSlabBatch(t, budget, 1)
		if added, err := batch.tryAdd(make([]byte, MaxTimelineBytes)); err != nil || !added {
			t.Fatalf("tryAdd=(%v,%v), want exact fit", added, err)
		}
		if got := batch.metrics().LogicalUsed; got != budget {
			t.Fatalf("logical used=%d, want %d", got, budget)
		}
	})

	t.Run("record-one-byte-over-budget", func(t *testing.T) {
		const budget = MaxTimelineBytes + timelineSpillLengthBytes + timelineReferenceChargeBytes - 1
		batch := mustNewTimelineSlabBatch(t, budget, 1)
		added, err := batch.tryAdd(make([]byte, MaxTimelineBytes))
		if added || !errors.Is(err, ErrVerificationResource) {
			t.Fatalf("tryAdd=(%v,%v), want false ErrVerificationResource", added, err)
		}
		if len(batch.timelines()) != 0 || len(batch.slabs) != 0 {
			t.Fatalf("rejected record changed batch: timelines=%d slabs=%d", len(batch.timelines()), len(batch.slabs))
		}
	})

	t.Run("batch-exact-fit-then-spill-needed", func(t *testing.T) {
		batch := mustNewTimelineSlabBatch(t, 2*minimumTimelineRecordBytes, 10)
		for _, value := range []byte{1, 2} {
			if added, err := batch.tryAdd([]byte{value}); err != nil || !added {
				t.Fatalf("tryAdd(%d)=(%v,%v)", value, added, err)
			}
		}
		before := batch.metrics()
		added, err := batch.tryAdd([]byte{3})
		if err != nil || added {
			t.Fatalf("third tryAdd=(%v,%v), want spill-needed false nil", added, err)
		}
		if got := batch.metrics(); got != before {
			t.Fatalf("spill-needed result mutated metrics: got %+v, want %+v", got, before)
		}
		if got := batch.timelines(); len(got) != 2 || got[0][0] != 1 || got[1][0] != 2 {
			t.Fatalf("spill-needed result mutated timelines: %v", got)
		}
	})
}

func TestTimelineSlabBatchReferenceCapacityAndDescriptorHints(t *testing.T) {
	for _, descriptorEntries := range []uint64{0, 1} {
		t.Run("descriptor-hint-"+string(rune('0'+descriptorEntries)), func(t *testing.T) {
			batch := mustNewTimelineSlabBatch(t, 1<<20, descriptorEntries)
			if cap(batch.batch) != 1 {
				t.Fatalf("reference capacity=%d, want 1", cap(batch.batch))
			}
			if added, err := batch.tryAdd([]byte("first")); err != nil || !added {
				t.Fatalf("first tryAdd=(%v,%v)", added, err)
			}
			before := batch.metrics()
			if added, err := batch.tryAdd([]byte("second")); err != nil || added {
				t.Fatalf("second tryAdd=(%v,%v), want spill-needed", added, err)
			}
			if got := batch.metrics(); got != before {
				t.Fatalf("reference-capacity rejection mutated metrics: got %+v, want %+v", got, before)
			}
			batch.reset()
			if added, err := batch.tryAdd([]byte("second")); err != nil || !added {
				t.Fatalf("retry tryAdd=(%v,%v)", added, err)
			}
			if got := batch.timelines(); len(got) != 1 || !bytes.Equal(got[0], []byte("second")) {
				t.Fatalf("retry timelines=%q", got)
			}
		})
	}
}

func TestTimelineSlabBatchSlabTransitions(t *testing.T) {
	const budget = uint64(200_000)

	t.Run("exact-slab-fit", func(t *testing.T) {
		batch := mustNewTimelineSlabBatch(t, budget, 129)
		initialReserved := batch.metrics().ReservedBytes
		for index := 0; index < 128; index++ {
			value := bytes.Repeat([]byte{byte(index)}, int(MaxTimelineBytes))
			if added, err := batch.tryAdd(value); err != nil || !added {
				t.Fatalf("record %d tryAdd=(%v,%v)", index, added, err)
			}
		}
		if len(batch.slabs) != 1 || batch.slabIndex != 0 || batch.slabOffset != 64<<10 {
			t.Fatalf("exact fit state: slabs=%d index=%d offset=%d", len(batch.slabs), batch.slabIndex, batch.slabOffset)
		}
		if got := batch.metrics().ReservedBytes; got != initialReserved+timelineSlabTargetBytes {
			t.Fatalf("one-slab reserved bytes=%d, want %d", got, initialReserved+timelineSlabTargetBytes)
		}
		if added, err := batch.tryAdd([]byte{0xaa}); err != nil || !added {
			t.Fatalf("post-fit tryAdd=(%v,%v)", added, err)
		}
		if len(batch.slabs) != 2 || batch.slabIndex != 1 || batch.slabOffset != 1 {
			t.Fatalf("post-fit state: slabs=%d index=%d offset=%d", len(batch.slabs), batch.slabIndex, batch.slabOffset)
		}
		if got := batch.metrics().ReservedBytes; got != initialReserved+2*timelineSlabTargetBytes {
			t.Fatalf("two-slab reserved bytes=%d, want %d", got, initialReserved+2*timelineSlabTargetBytes)
		}
	})

	t.Run("one-byte-tail-miss", func(t *testing.T) {
		batch := mustNewTimelineSlabBatch(t, budget, 129)
		value := make([]byte, MaxTimelineBytes)
		for index := 0; index < 127; index++ {
			value[0] = byte(index)
			if added, err := batch.tryAdd(value); err != nil || !added {
				t.Fatalf("record %d tryAdd=(%v,%v)", index, added, err)
			}
		}
		if added, err := batch.tryAdd([]byte{0xbb}); err != nil || !added {
			t.Fatalf("tail byte tryAdd=(%v,%v)", added, err)
		}
		if batch.slabOffset != (127*int(MaxTimelineBytes))+1 {
			t.Fatalf("tail offset=%d, want 65025", batch.slabOffset)
		}
		if added, err := batch.tryAdd(value); err != nil || !added {
			t.Fatalf("tail-miss tryAdd=(%v,%v)", added, err)
		}
		if len(batch.slabs) != 2 || batch.slabIndex != 1 || batch.slabOffset != int(MaxTimelineBytes) {
			t.Fatalf("tail-miss state: slabs=%d index=%d offset=%d", len(batch.slabs), batch.slabIndex, batch.slabOffset)
		}
	})

	t.Run("multiple-slabs", func(t *testing.T) {
		batch := mustNewTimelineSlabBatch(t, budget, 300)
		initialReserved := batch.metrics().ReservedBytes
		for index := 0; index < 300; index++ {
			value := bytes.Repeat([]byte{byte(index)}, int(MaxTimelineBytes))
			if added, err := batch.tryAdd(value); err != nil || !added {
				t.Fatalf("record %d tryAdd=(%v,%v)", index, added, err)
			}
		}
		if len(batch.slabs) != 3 || batch.slabIndex != 2 || batch.slabOffset != 44*int(MaxTimelineBytes) {
			t.Fatalf("multi-slab state: slabs=%d index=%d offset=%d", len(batch.slabs), batch.slabIndex, batch.slabOffset)
		}
		metrics := batch.metrics()
		if metrics.ReservedBytes > metrics.ReservedLimit {
			t.Fatalf("reserved bytes=%d exceeds limit=%d", metrics.ReservedBytes, metrics.ReservedLimit)
		}
		if metrics.ReservedBytes != initialReserved+3*timelineSlabTargetBytes {
			t.Fatalf("three-slab reserved bytes=%d, want %d", metrics.ReservedBytes, initialReserved+3*timelineSlabTargetBytes)
		}
		if metrics.Slabs != 3 {
			t.Fatalf("metric slabs=%d, want 3", metrics.Slabs)
		}
	})
}

func TestTimelineSlabBatchWorstCaseTailWasteStaysWithinEnvelope(t *testing.T) {
	const budget = uint64(1 << 20)
	batch := mustNewTimelineSlabBatch(t, budget, budget/minimumTimelineRecordBytes)
	maximum := make([]byte, MaxTimelineBytes)
	addedRecords := 0
	full := false
	for !full {
		// 127 maximum-size timelines plus one byte consume 65,025 bytes.
		// The next maximum-size timeline cannot use the 511-byte tail and
		// therefore advances to the next slab.
		for index := 0; index < 127; index++ {
			added, err := batch.tryAdd(maximum)
			if err != nil {
				t.Fatal(err)
			}
			if !added {
				full = true
				break
			}
			addedRecords++
		}
		if full {
			break
		}
		added, err := batch.tryAdd([]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		if !added {
			full = true
			break
		}
		addedRecords++
	}
	if addedRecords == 0 || len(batch.timelines()) != addedRecords {
		t.Fatalf("added records=%d timelines=%d", addedRecords, len(batch.timelines()))
	}
	metrics := batch.metrics()
	if metrics.LogicalUsed > budget {
		t.Fatalf("logical bytes=%d exceeds budget=%d", metrics.LogicalUsed, budget)
	}
	if len(batch.slabs) > cap(batch.slabs) {
		t.Fatalf("allocated slabs=%d exceeds descriptor capacity=%d", len(batch.slabs), cap(batch.slabs))
	}
	if metrics.ReservedBytes > metrics.ReservedLimit {
		t.Fatalf("reserved bytes=%d exceeds limit=%d", metrics.ReservedBytes, metrics.ReservedLimit)
	}
}

func TestTimelineSlabBatchResetReusesBackingAndHidesStaleValues(t *testing.T) {
	batch := mustNewTimelineSlabBatch(t, 200_000, 300)
	value := bytes.Repeat([]byte{0x7a}, int(MaxTimelineBytes))
	for index := 0; index < 130; index++ {
		value[0] = byte(index)
		if added, err := batch.tryAdd(value); err != nil || !added {
			t.Fatalf("initial record %d tryAdd=(%v,%v)", index, added, err)
		}
	}
	if len(batch.slabs) != 2 {
		t.Fatalf("initial slab count=%d, want 2", len(batch.slabs))
	}
	batchBacking := &batch.batch[0]
	slab0Backing := &batch.slabs[0][0]
	slab1Backing := &batch.slabs[1][0]
	reserved := batch.metrics().ReservedBytes
	highWater := batch.metrics().LogicalHighWater

	batch.reset()
	if len(batch.timelines()) != 0 || batch.logicalUsed != 0 || batch.slabIndex != 0 || batch.slabOffset != 0 {
		t.Fatalf("reset state: timelines=%d logical=%d index=%d offset=%d", len(batch.timelines()), batch.logicalUsed, batch.slabIndex, batch.slabOffset)
	}
	if batch.metrics().Resets != 1 {
		t.Fatalf("reset count=%d, want 1", batch.metrics().Resets)
	}
	batch.reset()
	if batch.metrics().Resets != 1 {
		t.Fatalf("empty reset count=%d, want unchanged", batch.metrics().Resets)
	}

	if added, err := batch.tryAdd([]byte{1, 2, 3}); err != nil || !added {
		t.Fatalf("short post-reset tryAdd=(%v,%v)", added, err)
	}
	if len(batch.timelines()) != 1 || !bytes.Equal(batch.timelines()[0], []byte{1, 2, 3}) {
		t.Fatalf("post-reset timelines=%v", batch.timelines())
	}
	if &batch.batch[0] != batchBacking || &batch.slabs[0][0] != slab0Backing {
		t.Fatal("reset replaced batch or first-slab backing allocation")
	}

	for index := 0; index < 127; index++ {
		if added, err := batch.tryAdd(value); err != nil || !added {
			t.Fatalf("reuse record %d tryAdd=(%v,%v)", index, added, err)
		}
	}
	if added, err := batch.tryAdd(value); err != nil || !added {
		t.Fatalf("second-slab reuse tryAdd=(%v,%v)", added, err)
	}
	if len(batch.slabs) != 2 || &batch.slabs[1][0] != slab1Backing {
		t.Fatal("reset failed to reuse retained second slab")
	}
	metrics := batch.metrics()
	if metrics.ReservedBytes != reserved {
		t.Fatalf("reserved bytes after reuse=%d, want %d", metrics.ReservedBytes, reserved)
	}
	if metrics.LogicalHighWater < highWater {
		t.Fatalf("logical high water decreased from %d to %d", highWater, metrics.LogicalHighWater)
	}
}

func TestTimelineSlabBatchBytewiseSortAndDedupEquivalence(t *testing.T) {
	random := rand.New(rand.NewSource(20260916))
	randomValues := make([][]byte, 1_000)
	for index := range randomValues {
		value := make([]byte, 1+random.Intn(int(MaxTimelineBytes)))
		if _, err := random.Read(value); err != nil {
			t.Fatal(err)
		}
		if index > 0 && index%5 == 0 {
			value = bytes.Clone(randomValues[random.Intn(index)])
		}
		randomValues[index] = value
	}

	distinct := make([][]byte, 256)
	for index := range distinct {
		distinct[index] = []byte{byte(index), byte(255 - index)}
	}
	reversed := cloneTimelineSlabTestValues(distinct)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}

	tests := []struct {
		name   string
		values [][]byte
	}{
		{name: "all-equal", values: [][]byte{{0, 1}, {0, 1}, {0, 1}}},
		{name: "all-distinct-sorted", values: distinct},
		{name: "reverse-sorted", values: reversed},
		{name: "common-prefix", values: [][]byte{{1}, {1, 0}, {1, 0, 0}, {1, 0, 1}, {1, 1}, {1, 0}}},
		{name: "random-binary", values: randomValues},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := uint64(len(test.values)) * (MaxTimelineBytes + timelineSpillLengthBytes + timelineReferenceChargeBytes)
			budget = max(budget, minimumTimelineRecordBytes)
			batch := mustNewTimelineSlabBatch(t, budget, uint64(len(test.values)))
			for index, value := range test.values {
				if added, err := batch.tryAdd(value); err != nil || !added {
					t.Fatalf("record %d tryAdd=(%v,%v)", index, added, err)
				}
			}

			got := batch.timelines()
			sort.Slice(got, func(i, j int) bool { return bytes.Compare(got[i], got[j]) < 0 })
			got = deduplicateTimelineSlabTestValues(got)
			want := canonicalTimelineSlabTestValues(test.values)
			assertTimelineSlabTestValuesEqual(t, got, want)
			if len(want) > 0 {
				if !bytes.Equal(got[0], want[0]) || !bytes.Equal(got[len(got)-1], want[len(want)-1]) {
					t.Fatalf("min/max=(%x,%x), want (%x,%x)", got[0], got[len(got)-1], want[0], want[len(want)-1])
				}
			}
		})
	}
}

func TestTimelineSlabBatchRandomizedSpillEquivalence(t *testing.T) {
	random := rand.New(rand.NewSource(73))
	values := make([][]byte, 5_000)
	for index := range values {
		if index > 0 && index%4 == 0 {
			values[index] = bytes.Clone(values[random.Intn(index)])
			continue
		}
		value := make([]byte, 1+random.Intn(int(MaxTimelineBytes)))
		if _, err := random.Read(value); err != nil {
			t.Fatal(err)
		}
		values[index] = value
	}

	for _, test := range []struct {
		name              string
		budget            uint64
		descriptorEntries uint64
	}{
		{name: "logical-budget-spills", budget: 4 << 10, descriptorEntries: 5_000},
		{name: "reference-capacity-spills", budget: 64 << 10, descriptorEntries: 17},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, gotSpills := consumeTimelineSlabTestBatches(t, test.budget, test.descriptorEntries, values)
			want, wantSpills := consumeTimelineSlabReferenceBatches(test.budget, test.descriptorEntries, values)
			if gotSpills != wantSpills {
				t.Fatalf("spill count=%d, want %d", gotSpills, wantSpills)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("canonical spill bytes differ: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

func TestTimelineSlabBatchSteadyStateReuseDoesNotAllocate(t *testing.T) {
	batch := mustNewTimelineSlabBatch(t, 1<<20, 100)
	timeline := bytes.Repeat([]byte{0x5a}, 17)
	for index := 0; index < 100; index++ {
		if added, err := batch.tryAdd(timeline); err != nil || !added {
			t.Fatalf("warmup record %d tryAdd=(%v,%v)", index, added, err)
		}
	}
	batch.reset()

	allocations := testing.AllocsPerRun(1_000, func() {
		for index := 0; index < 100; index++ {
			added, err := batch.tryAdd(timeline)
			if err != nil || !added {
				panic("steady-state slab add failed")
			}
		}
		batch.reset()
	})
	if allocations != 0 {
		t.Fatalf("steady-state batch allocated %g times per run, want 0", allocations)
	}
	if len(batch.slabs) != 1 {
		t.Fatalf("steady-state slab count=%d, want 1", len(batch.slabs))
	}
}

func mustNewTimelineSlabBatch(t *testing.T, budget, descriptorEntries uint64) *timelineSlabBatch {
	t.Helper()
	batch, err := newTimelineSlabBatch(budget, descriptorEntries)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func cloneTimelineSlabTestValues(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for index, value := range values {
		result[index] = bytes.Clone(value)
	}
	return result
}

func canonicalTimelineSlabTestValues(values [][]byte) [][]byte {
	result := cloneTimelineSlabTestValues(values)
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return deduplicateTimelineSlabTestValues(result)
}

func deduplicateTimelineSlabTestValues(sortedValues [][]byte) [][]byte {
	result := sortedValues[:0]
	for _, value := range sortedValues {
		if len(result) == 0 || !bytes.Equal(result[len(result)-1], value) {
			result = append(result, value)
		}
	}
	return result
}

func assertTimelineSlabTestValuesEqual(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("timeline count=%d, want %d", len(got), len(want))
	}
	for index := range want {
		if !bytes.Equal(got[index], want[index]) {
			t.Fatalf("timeline %d=%x, want %x", index, got[index], want[index])
		}
	}
}

func consumeTimelineSlabTestBatches(t *testing.T, budget, descriptorEntries uint64, values [][]byte) ([]byte, int) {
	t.Helper()
	batch := mustNewTimelineSlabBatch(t, budget, descriptorEntries)
	var encoded []byte
	spills := 0
	consume := func() {
		encoded = appendTimelineSlabTestBatch(encoded, batch.timelines())
		spills++
		batch.reset()
	}
	for index, value := range values {
		added, err := batch.tryAdd(value)
		if err != nil {
			t.Fatalf("record %d first tryAdd: %v", index, err)
		}
		if added {
			continue
		}
		consume()
		added, err = batch.tryAdd(value)
		if err != nil || !added {
			t.Fatalf("record %d retry tryAdd=(%v,%v)", index, added, err)
		}
	}
	if len(batch.timelines()) > 0 {
		consume()
	}
	return encoded, spills
}

func consumeTimelineSlabReferenceBatches(budget, descriptorEntries uint64, values [][]byte) ([]byte, int) {
	maxInt := uint64(^uint(0) >> 1)
	referenceCapacity := min(max(descriptorEntries, uint64(1)), budget/minimumTimelineRecordBytes, maxInt)
	batch := make([][]byte, 0, int(referenceCapacity))
	var logicalUsed uint64
	var encoded []byte
	spills := 0
	consume := func() {
		encoded = appendTimelineSlabTestBatch(encoded, batch)
		spills++
		batch = batch[:0]
		logicalUsed = 0
	}
	for _, value := range values {
		recordBytes := uint64(len(value)) + timelineSpillLengthBytes + timelineReferenceChargeBytes
		if len(batch) == cap(batch) || recordBytes > budget-logicalUsed {
			consume()
		}
		batch = append(batch, bytes.Clone(value))
		logicalUsed += recordBytes
	}
	if len(batch) > 0 {
		consume()
	}
	return encoded, spills
}

func appendTimelineSlabTestBatch(encoded []byte, values [][]byte) []byte {
	values = canonicalTimelineSlabTestValues(values)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(values)))
	encoded = append(encoded, count[:]...)
	for _, value := range values {
		encoded = append(encoded, byte(len(value)>>8), byte(len(value)))
		encoded = append(encoded, value...)
	}
	return encoded
}
