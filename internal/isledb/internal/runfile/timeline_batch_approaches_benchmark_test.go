package runfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
)

// This file intentionally contains benchmark-only implementations. It lets us
// compare timeline batch ownership strategies without adding three production
// branches to complete verification. All approaches use the current logical
// record charge, so they see identical batch boundaries and perform identical
// sort/dedup work. Scratch filesystem and merge I/O are deliberately excluded.

const (
	benchmarkTimelineBatchBudget = uint64(1 << 20)
	benchmarkTimelineRefBytes    = uint64(8)
)

var errBenchmarkTimelineDoesNotFit = errors.New("benchmark timeline does not fit batch budget")

type benchmarkTimelineBatchResult struct {
	spills       uint64
	unique       uint64
	encodedBytes uint64
	checksum     uint64
}

func (r *benchmarkTimelineBatchResult) consume(timeline []byte) {
	r.unique++
	r.encodedBytes += uint64(len(timeline) + 2)
	value := uint64(len(timeline)) + 0x9e3779b97f4a7c15
	for _, b := range timeline {
		value ^= uint64(b)
		value *= 0x100000001b3
	}
	r.checksum ^= value + (r.checksum << 6) + (r.checksum >> 2)
}

func (r benchmarkTimelineBatchResult) equal(other benchmarkTimelineBatchResult) bool {
	return r == other
}

type benchmarkTimelineBatch interface {
	add([]byte) error
	finish() (benchmarkTimelineBatchResult, error)
}

type benchmarkTimelineBatchFactory func() benchmarkTimelineBatch

func benchmarkTimelineRecordCharge(timeline []byte) (uint64, bool) {
	return checkedAdd(uint64(len(timeline)), 2+32)
}

func benchmarkCheckedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

// benchmarkCurrentTimelineBatch mirrors the production ownership model:
// every timeline gets an independent clone and the [][]byte backing array is
// discarded after each spill.
type benchmarkCurrentTimelineBatch struct {
	budget uint64
	used   uint64
	batch  [][]byte
	result benchmarkTimelineBatchResult
}

func (b *benchmarkCurrentTimelineBatch) add(timeline []byte) error {
	charge, ok := benchmarkTimelineRecordCharge(timeline)
	if !ok || charge > b.budget {
		return errBenchmarkTimelineDoesNotFit
	}
	if len(b.batch) > 0 && charge > b.budget-b.used {
		b.spill()
	}
	b.batch = append(b.batch, bytes.Clone(timeline))
	b.used += charge
	return nil
}

func (b *benchmarkCurrentTimelineBatch) spill() {
	if len(b.batch) == 0 {
		return
	}
	sort.Slice(b.batch, func(i, j int) bool { return bytes.Compare(b.batch[i], b.batch[j]) < 0 })
	var last []byte
	for _, timeline := range b.batch {
		if bytes.Equal(last, timeline) {
			continue
		}
		b.result.consume(timeline)
		last = timeline
	}
	b.result.spills++
	b.batch = nil
	b.used = 0
}

func (b *benchmarkCurrentTimelineBatch) finish() (benchmarkTimelineBatchResult, error) {
	b.spill()
	return b.result, nil
}

// benchmarkPackedTimelineBatch models the proposed two-ended arena. Timeline
// bytes grow from the front and packed offset/length references grow from the
// back. Sorting swaps references rather than timeline bytes.
type benchmarkPackedTimelineBatch struct {
	budget      uint64
	logicalUsed uint64
	storage     []byte
	dataEnd     uint32
	recordCount uint32
	result      benchmarkTimelineBatchResult
}

func newBenchmarkPackedTimelineBatch(budget uint64, entryCount int) *benchmarkPackedTimelineBatch {
	worstCase, ok := benchmarkCheckedMul(uint64(entryCount), MaxTimelineBytes+benchmarkTimelineRefBytes)
	if !ok {
		worstCase = budget
	}
	capacity := min(budget, worstCase, uint64(^uint32(0)), uint64(^uint(0)>>1))
	return &benchmarkPackedTimelineBatch{budget: budget, storage: make([]byte, int(capacity))}
}

func (b *benchmarkPackedTimelineBatch) add(timeline []byte) error {
	charge, ok := benchmarkTimelineRecordCharge(timeline)
	if !ok || charge > b.budget {
		return errBenchmarkTimelineDoesNotFit
	}
	if b.recordCount > 0 && charge > b.budget-b.logicalUsed {
		b.spill()
	}
	if !b.fits(len(timeline)) {
		return errBenchmarkTimelineDoesNotFit
	}
	offset := b.dataEnd
	copy(b.storage[int(offset):], timeline)
	b.storeRef(b.recordCount, offset, uint16(len(timeline)))
	b.dataEnd += uint32(len(timeline))
	b.recordCount++
	b.logicalUsed += charge
	return nil
}

func (b *benchmarkPackedTimelineBatch) fits(length int) bool {
	if length <= 0 || uint64(length) > MaxTimelineBytes {
		return false
	}
	nextDataEnd, ok := checkedAdd(uint64(b.dataEnd), uint64(length))
	if !ok {
		return false
	}
	nextRefBytes, ok := benchmarkCheckedMul(uint64(b.recordCount)+1, benchmarkTimelineRefBytes)
	if !ok || nextRefBytes > uint64(len(b.storage)) {
		return false
	}
	return nextDataEnd <= uint64(len(b.storage))-nextRefBytes
}

func (b *benchmarkPackedTimelineBatch) refPosition(index int) int {
	return len(b.storage) - (index+1)*int(benchmarkTimelineRefBytes)
}

func (b *benchmarkPackedTimelineBatch) loadPackedRef(index int) uint64 {
	position := b.refPosition(index)
	return binary.BigEndian.Uint64(b.storage[position : position+int(benchmarkTimelineRefBytes)])
}

func (b *benchmarkPackedTimelineBatch) storePackedRef(index int, packed uint64) {
	position := b.refPosition(index)
	binary.BigEndian.PutUint64(b.storage[position:position+int(benchmarkTimelineRefBytes)], packed)
}

func (b *benchmarkPackedTimelineBatch) storeRef(index uint32, offset uint32, length uint16) {
	b.storePackedRef(int(index), uint64(offset)<<16|uint64(length))
}

func (b *benchmarkPackedTimelineBatch) value(index int) []byte {
	packed := b.loadPackedRef(index)
	offset := uint32(packed >> 16)
	length := uint16(packed)
	return b.storage[int(offset) : int(offset)+int(length)]
}

func (b *benchmarkPackedTimelineBatch) Len() int { return int(b.recordCount) }

func (b *benchmarkPackedTimelineBatch) Less(i, j int) bool {
	return bytes.Compare(b.value(i), b.value(j)) < 0
}

func (b *benchmarkPackedTimelineBatch) Swap(i, j int) {
	left := b.loadPackedRef(i)
	right := b.loadPackedRef(j)
	b.storePackedRef(i, right)
	b.storePackedRef(j, left)
}

func (b *benchmarkPackedTimelineBatch) spill() {
	if b.recordCount == 0 {
		return
	}
	sort.Sort(b)
	var last []byte
	for index := 0; index < int(b.recordCount); index++ {
		timeline := b.value(index)
		if bytes.Equal(last, timeline) {
			continue
		}
		b.result.consume(timeline)
		last = timeline
	}
	b.result.spills++
	b.dataEnd = 0
	b.recordCount = 0
	b.logicalUsed = 0
}

func (b *benchmarkPackedTimelineBatch) finish() (benchmarkTimelineBatchResult, error) {
	b.spill()
	return b.result, nil
}

// benchmarkSlabTimelineBatch exercises the actual standalone slab component.
// Only the sort/dedup consumer remains benchmark-specific.
type benchmarkSlabTimelineBatch struct {
	batch  *timelineSlabBatch
	err    error
	result benchmarkTimelineBatchResult
}

func newBenchmarkSlabTimelineBatch(budget uint64, entryCount int) *benchmarkSlabTimelineBatch {
	batch, err := newTimelineSlabBatch(budget, uint64(entryCount))
	return &benchmarkSlabTimelineBatch{batch: batch, err: err}
}

func (b *benchmarkSlabTimelineBatch) add(timeline []byte) error {
	if b.err != nil {
		return b.err
	}
	added, err := b.batch.tryAdd(timeline)
	if err != nil {
		return err
	}
	if !added {
		b.spill()
		added, err = b.batch.tryAdd(timeline)
		if err != nil {
			return err
		}
		if !added {
			return errBenchmarkTimelineDoesNotFit
		}
	}
	return nil
}

func (b *benchmarkSlabTimelineBatch) spill() {
	batch := b.batch.timelines()
	if len(batch) == 0 {
		return
	}
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i], batch[j]) < 0 })
	var last []byte
	for _, timeline := range batch {
		if bytes.Equal(last, timeline) {
			continue
		}
		b.result.consume(timeline)
		last = timeline
	}
	b.result.spills++
	b.batch.reset()
}

func (b *benchmarkSlabTimelineBatch) finish() (benchmarkTimelineBatchResult, error) {
	b.spill()
	return b.result, nil
}

func TestTimelineBatchApproachEquivalence(t *testing.T) {
	testCases := []struct {
		name      string
		budget    uint64
		timelines [][]byte
	}{
		{name: "binary-and-duplicates", budget: 128, timelines: [][]byte{{0xff}, {0}, {0xff}, {0, 1}, {0, 0}, {0, 1}}},
		{name: "mixed-lengths", budget: 600, timelines: benchmarkVariableTimelines(257)},
		{name: "many-spills", budget: 64, timelines: benchmarkFixedTimelines(1_000, 17)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			factories := benchmarkTimelineBatchFactories(testCase.budget, len(testCase.timelines))
			var expected benchmarkTimelineBatchResult
			for index, factory := range factories {
				result, err := runBenchmarkTimelineBatch(factory, testCase.timelines)
				if err != nil {
					t.Fatal(err)
				}
				if index == 0 {
					expected = result
					continue
				}
				if !result.equal(expected) {
					t.Fatalf("approach %d result=%+v, want %+v", index, result, expected)
				}
			}
		})
	}
}

func BenchmarkTimelineBatchApproaches(b *testing.B) {
	for _, timelineCount := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("timelines=%d", timelineCount), func(b *testing.B) {
			benchmarkTimelineBatchDataset(b, benchmarkFixedTimelines(timelineCount, 17))
		})
	}
}

func BenchmarkTimelineBatchApproachesVariable100K(b *testing.B) {
	benchmarkTimelineBatchDataset(b, benchmarkVariableTimelines(100_000))
}

func benchmarkTimelineBatchDataset(b *testing.B, timelines [][]byte) {
	var inputBytes int64
	for _, timeline := range timelines {
		inputBytes += int64(len(timeline))
	}
	factories := benchmarkTimelineBatchFactories(benchmarkTimelineBatchBudget, len(timelines))
	names := []string{"current-clone", "packed-arena", "slab-component"}
	expected, err := runBenchmarkTimelineBatch(factories[0], timelines)
	if err != nil {
		b.Fatal(err)
	}
	for index, factory := range factories {
		factory, name := factory, names[index]
		b.Run(name, func(b *testing.B) {
			result, err := runBenchmarkTimelineBatch(factory, timelines)
			if err != nil {
				b.Fatal(err)
			}
			if !result.equal(expected) {
				b.Fatalf("preflight result=%+v, want %+v", result, expected)
			}
			b.ReportAllocs()
			b.SetBytes(inputBytes)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err = runBenchmarkTimelineBatch(factory, timelines)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if !result.equal(expected) {
				b.Fatalf("result=%+v, want %+v", result, expected)
			}
			b.ReportMetric(float64(result.spills), "spills/op")
			b.ReportMetric(float64(result.unique), "unique/op")
		})
	}
}

func benchmarkTimelineBatchFactories(budget uint64, entryCount int) []benchmarkTimelineBatchFactory {
	return []benchmarkTimelineBatchFactory{
		func() benchmarkTimelineBatch { return &benchmarkCurrentTimelineBatch{budget: budget} },
		func() benchmarkTimelineBatch { return newBenchmarkPackedTimelineBatch(budget, entryCount) },
		func() benchmarkTimelineBatch { return newBenchmarkSlabTimelineBatch(budget, entryCount) },
	}
}

func runBenchmarkTimelineBatch(factory benchmarkTimelineBatchFactory, timelines [][]byte) (benchmarkTimelineBatchResult, error) {
	batch := factory()
	for _, timeline := range timelines {
		if err := batch.add(timeline); err != nil {
			return benchmarkTimelineBatchResult{}, err
		}
	}
	return batch.finish()
}

func benchmarkFixedTimelines(count, length int) [][]byte {
	data := make([]byte, count*length)
	result := make([][]byte, count)
	for index := range count {
		timeline := data[index*length : (index+1)*length]
		for byteIndex := range timeline {
			timeline[byteIndex] = byte(index*131 + byteIndex*17)
		}
		if length >= 8 {
			binary.BigEndian.PutUint64(timeline[length-8:], uint64(index))
		}
		result[index] = timeline
	}
	return result
}

func benchmarkVariableTimelines(count int) [][]byte {
	var total int
	for index := range count {
		total += 1 + index%int(MaxTimelineBytes)
	}
	data := make([]byte, total)
	result := make([][]byte, count)
	offset := 0
	for index := range count {
		length := 1 + index%int(MaxTimelineBytes)
		timeline := data[offset : offset+length]
		offset += length
		for byteIndex := range timeline {
			timeline[byteIndex] = byte(index*31 + byteIndex*7)
		}
		result[index] = timeline
	}
	return result
}
