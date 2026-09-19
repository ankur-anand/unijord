package runfile

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"
	"sort"
	"testing"
)

const (
	benchmarkWriterSourceEvents uint8 = 1 << iota
	benchmarkWriterSourceHeads
	benchmarkWriterSourceFilter
	benchmarkWriterSourceAll = benchmarkWriterSourceEvents | benchmarkWriterSourceHeads | benchmarkWriterSourceFilter

	benchmarkWriterTimelineMemoryBudget = uint64(8 << 20)
	benchmarkWriterTimelineMergeFanIn   = 16
	benchmarkWriterTimelineBufferBytes  = 64 << 10
)

type benchmarkWriterTimelineSetResult struct {
	count            uint64
	digest           [sha256.Size]byte
	spills           uint64
	mergePasses      uint64
	scratchBytes     uint64
	logicalHighWater uint64
	reservedBytes    uint64
}

func (r benchmarkWriterTimelineSetResult) sameCanonical(other benchmarkWriterTimelineSetResult) bool {
	return r.count == other.count && r.digest == other.digest
}

type benchmarkWriterTimelineSet interface {
	add(source uint8, timeline []byte) error
	finish() (benchmarkWriterTimelineSetResult, error)
	close() error
}

type benchmarkCanonicalTimelineHasher struct {
	h      hash.Hash
	length [2]byte
	count  uint64
}

func newBenchmarkCanonicalTimelineHasher() *benchmarkCanonicalTimelineHasher {
	return &benchmarkCanonicalTimelineHasher{h: sha256.New()}
}

func (h *benchmarkCanonicalTimelineHasher) add(timeline []byte) {
	binary.BigEndian.PutUint16(h.length[:], uint16(len(timeline)))
	_, _ = h.h.Write(h.length[:])
	_, _ = h.h.Write(timeline)
	h.count++
}

func (h *benchmarkCanonicalTimelineHasher) result() benchmarkWriterTimelineSetResult {
	var result benchmarkWriterTimelineSetResult
	result.count = h.count
	copy(result.digest[:], h.h.Sum(nil))
	return result
}

type benchmarkCurrentWriterTimelineSet struct {
	events          map[string]struct{}
	heads           map[string]struct{}
	filter          map[string]struct{}
	filterTimelines [][]byte
}

func newBenchmarkCurrentWriterTimelineSet() *benchmarkCurrentWriterTimelineSet {
	return &benchmarkCurrentWriterTimelineSet{
		events: make(map[string]struct{}),
		heads:  make(map[string]struct{}),
		filter: make(map[string]struct{}),
	}
}

func (s *benchmarkCurrentWriterTimelineSet) add(source uint8, timeline []byte) error {
	switch source {
	case benchmarkWriterSourceEvents:
		s.events[string(timeline)] = struct{}{}
	case benchmarkWriterSourceHeads:
		s.heads[string(timeline)] = struct{}{}
	case benchmarkWriterSourceFilter:
		key := string(timeline)
		if _, exists := s.filter[key]; !exists {
			s.filter[key] = struct{}{}
			s.filterTimelines = append(s.filterTimelines, bytes.Clone(timeline))
		}
	default:
		return fmt.Errorf("unknown writer timeline source mask %03b", source)
	}
	return nil
}

func (s *benchmarkCurrentWriterTimelineSet) finish() (benchmarkWriterTimelineSetResult, error) {
	if !sameStringSet(s.events, s.heads) || !sameStringSet(s.events, s.filter) {
		return benchmarkWriterTimelineSetResult{}, fmt.Errorf("writer timeline sets differ")
	}
	sort.Slice(s.filterTimelines, func(i, j int) bool {
		return bytes.Compare(s.filterTimelines[i], s.filterTimelines[j]) < 0
	})
	hasher := newBenchmarkCanonicalTimelineHasher()
	for _, timeline := range s.filterTimelines {
		hasher.add(timeline)
	}
	return hasher.result(), nil
}

func (*benchmarkCurrentWriterTimelineSet) close() error { return nil }

type benchmarkSharedMapWriterTimelineSet struct {
	set map[string]uint8
}

func newBenchmarkSharedMapWriterTimelineSet() *benchmarkSharedMapWriterTimelineSet {
	return &benchmarkSharedMapWriterTimelineSet{set: make(map[string]uint8)}
}

func (s *benchmarkSharedMapWriterTimelineSet) add(source uint8, timeline []byte) error {
	if source != benchmarkWriterSourceEvents && source != benchmarkWriterSourceHeads && source != benchmarkWriterSourceFilter {
		return fmt.Errorf("unknown writer timeline source mask %03b", source)
	}
	key := string(timeline)
	s.set[key] |= source
	return nil
}

func (s *benchmarkSharedMapWriterTimelineSet) finish() (benchmarkWriterTimelineSetResult, error) {
	timelines := make([]string, 0, len(s.set))
	for timeline, mask := range s.set {
		if mask != benchmarkWriterSourceAll {
			return benchmarkWriterTimelineSetResult{}, fmt.Errorf("timeline %x has source mask %03b", timeline, mask)
		}
		timelines = append(timelines, timeline)
	}
	sort.Strings(timelines)
	hasher := newBenchmarkCanonicalTimelineHasher()
	for _, timeline := range timelines {
		hasher.add([]byte(timeline))
	}
	return hasher.result(), nil
}

func (*benchmarkSharedMapWriterTimelineSet) close() error { return nil }

type benchmarkTaggedTimelineBatch struct {
	timelines [][]byte
	masks     []uint8
}

func (b benchmarkTaggedTimelineBatch) Len() int { return len(b.timelines) }

func (b benchmarkTaggedTimelineBatch) Less(i, j int) bool {
	return bytes.Compare(b.timelines[i], b.timelines[j]) < 0
}

func (b benchmarkTaggedTimelineBatch) Swap(i, j int) {
	b.timelines[i], b.timelines[j] = b.timelines[j], b.timelines[i]
	b.masks[i], b.masks[j] = b.masks[j], b.masks[i]
}

type benchmarkExternalWriterTimelineSet struct {
	dir              string
	budget           uint64
	fanIn            int
	batch            *timelineSlabBatch
	masks            []uint8
	paths            []string
	spills           uint64
	mergePasses      uint64
	scratchBytes     uint64
	logicalHighWater uint64
	reservedBytes    uint64
}

func newBenchmarkExternalWriterTimelineSet(dir string, timelineCount int) (*benchmarkExternalWriterTimelineSet, error) {
	entryHint, ok := checkedMultiply(uint64(timelineCount), 3)
	if !ok {
		return nil, fmt.Errorf("writer timeline entry hint overflows")
	}
	batch, err := newTimelineSlabBatch(benchmarkWriterTimelineMemoryBudget, entryHint)
	if err != nil {
		return nil, err
	}
	result := &benchmarkExternalWriterTimelineSet{
		dir:    dir,
		budget: benchmarkWriterTimelineMemoryBudget,
		fanIn:  benchmarkWriterTimelineMergeFanIn,
		batch:  batch,
		masks:  make([]uint8, 0, cap(batch.batch)),
	}
	result.snapshotMetrics()
	return result, nil
}

func (s *benchmarkExternalWriterTimelineSet) add(source uint8, timeline []byte) error {
	if source != benchmarkWriterSourceEvents && source != benchmarkWriterSourceHeads && source != benchmarkWriterSourceFilter {
		return fmt.Errorf("unknown writer timeline source mask %03b", source)
	}
	charge, ok := checkedAdd(uint64(len(timeline)), timelineSpillLengthBytes+timelineReferenceChargeBytes+1)
	if !ok || charge > s.budget {
		return fmt.Errorf("writer timeline record exceeds memory budget")
	}
	logicalUsed := s.batch.logicalUsed + uint64(len(s.masks))
	if len(s.masks) > 0 && charge > s.budget-logicalUsed {
		if err := s.spill(); err != nil {
			return err
		}
	}
	added, err := s.batch.tryAdd(timeline)
	if err != nil {
		return err
	}
	if !added {
		if err := s.spill(); err != nil {
			return err
		}
		added, err = s.batch.tryAdd(timeline)
		if err != nil {
			return err
		}
		if !added {
			return fmt.Errorf("writer timeline did not fit empty slab batch")
		}
	}
	s.masks = append(s.masks, source)
	s.snapshotMetrics()
	return nil
}

func (s *benchmarkExternalWriterTimelineSet) spill() error {
	timelines := s.batch.timelines()
	if len(timelines) == 0 {
		return nil
	}
	sort.Sort(benchmarkTaggedTimelineBatch{timelines: timelines, masks: s.masks})
	file, err := os.CreateTemp(s.dir, "writer-timeline-spill-*")
	if err != nil {
		return err
	}
	path := file.Name()
	writer := newBenchmarkTaggedTimelineWriter(file)
	for start := 0; start < len(timelines); {
		mask := s.masks[start]
		end := start + 1
		for end < len(timelines) && bytes.Equal(timelines[start], timelines[end]) {
			mask |= s.masks[end]
			end++
		}
		if err := writer.write(timelines[start], mask); err != nil {
			_ = writer.abort()
			_ = os.Remove(path)
			return err
		}
		start = end
	}
	if err := writer.close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	s.scratchBytes += writer.bytes
	s.paths = append(s.paths, path)
	s.spills++
	s.batch.reset()
	s.masks = s.masks[:0]
	s.snapshotMetrics()
	return nil
}

func (s *benchmarkExternalWriterTimelineSet) snapshotMetrics() {
	metrics := s.batch.metrics()
	logical := metrics.LogicalUsed + uint64(len(s.masks))
	s.logicalHighWater = max(s.logicalHighWater, logical)
	s.reservedBytes = max(s.reservedBytes, metrics.ReservedBytes+uint64(cap(s.masks)))
}

func (s *benchmarkExternalWriterTimelineSet) finish() (result benchmarkWriterTimelineSetResult, resultErr error) {
	defer func() {
		resultErr = errors.Join(resultErr, s.close())
	}()
	if err := s.spill(); err != nil {
		return result, err
	}
	if len(s.paths) == 0 {
		return result, fmt.Errorf("no writer timelines")
	}
	for len(s.paths) > s.fanIn {
		var next []string
		for start := 0; start < len(s.paths); start += s.fanIn {
			end := min(start+s.fanIn, len(s.paths))
			path, written, err := benchmarkMergeTaggedTimelineRuns(s.dir, s.paths[start:end])
			if err != nil {
				return result, errors.Join(err, benchmarkRemoveTaggedTimelinePaths(next))
			}
			s.scratchBytes += written
			for _, input := range s.paths[start:end] {
				if err := os.Remove(input); err != nil {
					_ = os.Remove(path)
					return result, errors.Join(err, benchmarkRemoveTaggedTimelinePaths(next))
				}
			}
			next = append(next, path)
		}
		s.paths = next
		s.mergePasses++
	}
	hasher := newBenchmarkCanonicalTimelineHasher()
	if err := benchmarkMergeTaggedTimelineRecords(s.paths, func(timeline []byte, mask uint8) error {
		if mask != benchmarkWriterSourceAll {
			return fmt.Errorf("timeline %x has source mask %03b", timeline, mask)
		}
		hasher.add(timeline)
		return nil
	}); err != nil {
		return result, err
	}
	s.mergePasses++
	result = hasher.result()
	result.spills = s.spills
	result.mergePasses = s.mergePasses
	result.scratchBytes = s.scratchBytes
	result.logicalHighWater = s.logicalHighWater
	result.reservedBytes = s.reservedBytes
	return result, nil
}

func (s *benchmarkExternalWriterTimelineSet) close() error {
	result := benchmarkRemoveTaggedTimelinePaths(s.paths)
	s.paths = nil
	return result
}

func benchmarkRemoveTaggedTimelinePaths(paths []string) error {
	var result error
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}

type benchmarkTaggedTimelineWriter struct {
	file   *os.File
	writer *bufio.Writer
	header [3]byte
	bytes  uint64
}

func newBenchmarkTaggedTimelineWriter(file *os.File) *benchmarkTaggedTimelineWriter {
	return &benchmarkTaggedTimelineWriter{
		file:   file,
		writer: bufio.NewWriterSize(file, benchmarkWriterTimelineBufferBytes),
	}
}

func (w *benchmarkTaggedTimelineWriter) write(timeline []byte, mask uint8) error {
	if len(timeline) == 0 || uint64(len(timeline)) > MaxTimelineBytes {
		return fmt.Errorf("invalid tagged timeline length %d", len(timeline))
	}
	binary.BigEndian.PutUint16(w.header[:2], uint16(len(timeline)))
	w.header[2] = mask
	if _, err := w.writer.Write(w.header[:]); err != nil {
		return err
	}
	if _, err := w.writer.Write(timeline); err != nil {
		return err
	}
	w.bytes += uint64(len(timeline) + len(w.header))
	return nil
}

func (w *benchmarkTaggedTimelineWriter) close() error {
	return errors.Join(w.writer.Flush(), w.file.Close())
}

func (w *benchmarkTaggedTimelineWriter) abort() error {
	return w.file.Close()
}

type benchmarkTaggedTimelineReader struct {
	file     *os.File
	reader   *bufio.Reader
	header   [3]byte
	timeline []byte
}

func newBenchmarkTaggedTimelineReader(file *os.File) *benchmarkTaggedTimelineReader {
	return &benchmarkTaggedTimelineReader{
		file:   file,
		reader: bufio.NewReaderSize(file, benchmarkWriterTimelineBufferBytes),
	}
}

func (r *benchmarkTaggedTimelineReader) next() ([]byte, uint8, error) {
	n, err := io.ReadFull(r.reader, r.header[:])
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, 0, io.EOF
		}
		return nil, 0, err
	}
	length := int(binary.BigEndian.Uint16(r.header[:2]))
	if length == 0 || uint64(length) > MaxTimelineBytes {
		return nil, 0, fmt.Errorf("invalid tagged timeline length %d", length)
	}
	if cap(r.timeline) < length {
		r.timeline = make([]byte, length)
	} else {
		r.timeline = r.timeline[:length]
	}
	if _, err := io.ReadFull(r.reader, r.timeline); err != nil {
		return nil, 0, err
	}
	return r.timeline, r.header[2], nil
}

type benchmarkTaggedTimelineHeapItem struct {
	timeline []byte
	mask     uint8
	reader   int
}

type benchmarkTaggedTimelineHeap []benchmarkTaggedTimelineHeapItem

func benchmarkPushTaggedTimelineHeap(h *benchmarkTaggedTimelineHeap, value benchmarkTaggedTimelineHeapItem) {
	*h = append(*h, value)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if bytes.Compare((*h)[parent].timeline, (*h)[child].timeline) <= 0 {
			break
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		child = parent
	}
}

func benchmarkPopTaggedTimelineHeap(h *benchmarkTaggedTimelineHeap) benchmarkTaggedTimelineHeapItem {
	old := *h
	last := len(old) - 1
	result := old[0]
	if last == 0 {
		old[0] = benchmarkTaggedTimelineHeapItem{}
		*h = old[:0]
		return result
	}
	old[0] = old[last]
	old[last] = benchmarkTaggedTimelineHeapItem{}
	*h = old[:last]
	for parent := 0; ; {
		left := parent*2 + 1
		if left >= len(*h) {
			break
		}
		child := left
		right := left + 1
		if right < len(*h) && bytes.Compare((*h)[right].timeline, (*h)[left].timeline) < 0 {
			child = right
		}
		if bytes.Compare((*h)[parent].timeline, (*h)[child].timeline) <= 0 {
			break
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
	return result
}

func benchmarkMergeTaggedTimelineRuns(dir string, paths []string) (string, uint64, error) {
	file, err := os.CreateTemp(dir, "writer-timeline-merge-*")
	if err != nil {
		return "", 0, err
	}
	path := file.Name()
	writer := newBenchmarkTaggedTimelineWriter(file)
	if err := benchmarkMergeTaggedTimelineRecords(paths, writer.write); err != nil {
		_ = writer.abort()
		_ = os.Remove(path)
		return "", 0, err
	}
	if err := writer.close(); err != nil {
		_ = os.Remove(path)
		return "", 0, err
	}
	return path, writer.bytes, nil
}

func benchmarkMergeTaggedTimelineRecords(paths []string, consume func([]byte, uint8) error) (resultErr error) {
	readers := make([]*benchmarkTaggedTimelineReader, len(paths))
	defer func() {
		for _, reader := range readers {
			if reader != nil {
				resultErr = errors.Join(resultErr, reader.file.Close())
			}
		}
	}()
	queue := benchmarkTaggedTimelineHeap{}
	for index, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		reader := newBenchmarkTaggedTimelineReader(file)
		readers[index] = reader
		timeline, mask, err := reader.next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		benchmarkPushTaggedTimelineHeap(&queue, benchmarkTaggedTimelineHeapItem{timeline: timeline, mask: mask, reader: index})
	}
	var pending []byte
	var pendingMask uint8
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		return consume(pending, pendingMask)
	}
	for len(queue) > 0 {
		item := benchmarkPopTaggedTimelineHeap(&queue)
		if len(pending) == 0 || !bytes.Equal(pending, item.timeline) {
			if err := flush(); err != nil {
				return err
			}
			pending = append(pending[:0], item.timeline...)
			pendingMask = item.mask
		} else {
			pendingMask |= item.mask
		}
		timeline, mask, err := readers[item.reader].next()
		if err == nil {
			item.timeline = timeline
			item.mask = mask
			benchmarkPushTaggedTimelineHeap(&queue, item)
		} else if err != io.EOF {
			return err
		}
	}
	return flush()
}

func TestBenchmarkWriterTimelineSetApproachesEquivalent(t *testing.T) {
	timelines := [][]byte{{0xff}, {0x00}, {0x00, 0x01}, {0xff}, []byte("timeline"), {0x00}}
	expected, err := benchmarkRunWriterTimelineSetApproach(newBenchmarkCurrentWriterTimelineSet(), timelines)
	if err != nil {
		t.Fatal(err)
	}
	approaches := []benchmarkWriterTimelineSet{
		newBenchmarkSharedMapWriterTimelineSet(),
		mustNewBenchmarkExternalWriterTimelineSet(t, t.TempDir(), len(timelines)),
	}
	for index, approach := range approaches {
		result, err := benchmarkRunWriterTimelineSetApproach(approach, timelines)
		if err != nil {
			t.Fatalf("approach %d: %v", index, err)
		}
		if !result.sameCanonical(expected) {
			t.Fatalf("approach %d result=%+v, want %+v", index, result, expected)
		}
	}
}

func TestBenchmarkWriterTimelineSetApproachesRejectMismatch(t *testing.T) {
	factories := []func() benchmarkWriterTimelineSet{
		func() benchmarkWriterTimelineSet { return newBenchmarkCurrentWriterTimelineSet() },
		func() benchmarkWriterTimelineSet { return newBenchmarkSharedMapWriterTimelineSet() },
		func() benchmarkWriterTimelineSet {
			return mustNewBenchmarkExternalWriterTimelineSet(t, t.TempDir(), 2)
		},
	}
	for index, factory := range factories {
		approach := factory()
		for _, timeline := range [][]byte{[]byte("a"), []byte("b")} {
			if err := approach.add(benchmarkWriterSourceEvents, timeline); err != nil {
				t.Fatal(err)
			}
		}
		if err := approach.add(benchmarkWriterSourceHeads, []byte("a")); err != nil {
			t.Fatal(err)
		}
		for _, timeline := range [][]byte{[]byte("a"), []byte("b")} {
			if err := approach.add(benchmarkWriterSourceFilter, timeline); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := approach.finish(); err == nil {
			t.Fatalf("approach %d accepted mismatched sets", index)
		}
	}
}

func BenchmarkWriterTimelineSetApproaches(b *testing.B) {
	benchmarks := []struct {
		name      string
		timelines [][]byte
	}{
		{name: "fixed-1m", timelines: benchmarkFixedTimelines(1_000_000, 17)},
		{name: "variable-100k", timelines: benchmarkVariableTimelines(100_000)},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkWriterTimelineSetDataset(b, benchmark.timelines)
		})
	}
}

func benchmarkWriterTimelineSetDataset(b *testing.B, timelines [][]byte) {
	expected, err := benchmarkRunWriterTimelineSetApproach(newBenchmarkCurrentWriterTimelineSet(), timelines)
	if err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	scratchDir := b.TempDir()
	approaches := []struct {
		name    string
		factory func() (benchmarkWriterTimelineSet, error)
	}{
		{name: "current-three-maps", factory: func() (benchmarkWriterTimelineSet, error) {
			return newBenchmarkCurrentWriterTimelineSet(), nil
		}},
		{name: "shared-mask-map", factory: func() (benchmarkWriterTimelineSet, error) {
			return newBenchmarkSharedMapWriterTimelineSet(), nil
		}},
		{name: "slab-external-mask", factory: func() (benchmarkWriterTimelineSet, error) {
			return newBenchmarkExternalWriterTimelineSet(scratchDir, len(timelines))
		}},
	}
	var inputBytes int64
	for _, timeline := range timelines {
		inputBytes += int64(len(timeline)) * 3
	}
	for _, candidate := range approaches {
		candidate := candidate
		b.Run(candidate.name, func(b *testing.B) {
			preflight, err := candidate.factory()
			if err != nil {
				b.Fatal(err)
			}
			result, err := benchmarkRunWriterTimelineSetApproach(preflight, timelines)
			if err != nil {
				b.Fatal(err)
			}
			if !result.sameCanonical(expected) {
				b.Fatalf("preflight result=%+v, want %+v", result, expected)
			}
			runtime.GC()
			b.ReportAllocs()
			b.SetBytes(inputBytes)
			b.ResetTimer()
			for range b.N {
				approach, err := candidate.factory()
				if err != nil {
					b.Fatal(err)
				}
				result, err = benchmarkRunWriterTimelineSetApproach(approach, timelines)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if !result.sameCanonical(expected) {
				b.Fatalf("result=%+v, want %+v", result, expected)
			}
			b.ReportMetric(float64(result.spills), "spills/op")
			b.ReportMetric(float64(result.mergePasses), "merge_passes/op")
			b.ReportMetric(float64(result.scratchBytes), "scratch_bytes/op")
			b.ReportMetric(float64(result.logicalHighWater), "logical_highwater_B/op")
			b.ReportMetric(float64(result.reservedBytes), "reserved_B/op")
		})
	}
}

func benchmarkRunWriterTimelineSetApproach(approach benchmarkWriterTimelineSet, timelines [][]byte) (benchmarkWriterTimelineSetResult, error) {
	for _, timeline := range timelines {
		if err := approach.add(benchmarkWriterSourceEvents, timeline); err != nil {
			_ = approach.close()
			return benchmarkWriterTimelineSetResult{}, err
		}
	}
	for index := range timelines {
		timeline := timelines[(index*8191)%len(timelines)]
		if err := approach.add(benchmarkWriterSourceHeads, timeline); err != nil {
			_ = approach.close()
			return benchmarkWriterTimelineSetResult{}, err
		}
	}
	for index := len(timelines) - 1; index >= 0; index-- {
		if err := approach.add(benchmarkWriterSourceFilter, timelines[index]); err != nil {
			_ = approach.close()
			return benchmarkWriterTimelineSetResult{}, err
		}
	}
	return approach.finish()
}

func mustNewBenchmarkExternalWriterTimelineSet(t *testing.T, dir string, timelineCount int) *benchmarkExternalWriterTimelineSet {
	t.Helper()
	result, err := newBenchmarkExternalWriterTimelineSet(dir, timelineCount)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
