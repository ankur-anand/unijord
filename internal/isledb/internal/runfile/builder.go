package runfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"slices"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

const (
	canonicalTableBlockSize          = 4096
	canonicalTableRestartInterval    = 16
	canonicalTableBlockThreshold     = 90
	canonicalTableSizeClassThreshold = 60
	canonicalNativeFilterBitsPerKey  = 10
	canonicalDeletionThreshold       = 100
	canonicalDeletionSizeRatio       = 0.5
	maxPebbleSequence                = uint64(1<<56 - 1)
)

// TableCompression selects the self-describing compression used by both
// embedded SSTs. The zero value is the canonical Snappy encoding.
type TableCompression uint8

const (
	TableCompressionSnappy TableCompression = iota
	TableCompressionNone
	TableCompressionZstd
)

// TableOptions controls the version-1 choices which the format permits a
// writer to vary. All other Pebble writer settings are pinned by the codec.
type TableOptions struct {
	Compression TableCompression
	// NativeFilterBitsPerKey selects the table-level Pebble Bloom filter. Zero
	// resolves to the canonical value of 10. Set DisableNativeFilter to omit it.
	NativeFilterBitsPerKey int
	DisableNativeFilter    bool
}

// BuildOptions binds the two tables and derived filter to one immutable run.
type BuildOptions struct {
	RunID           [RunIDBytes]byte
	NamespaceHash   [SHA256Bytes]byte
	Shard           uint32
	CreatorRole     CreatorRole
	CreatorEpoch    uint64
	SeqLo           uint64
	SeqHi           uint64
	PublicationHash [SHA256Bytes]byte

	Table  TableOptions
	Filter FilterOptions

	// ScratchDir optionally selects bounded scratch storage for the two SSTs.
	// The empty value uses the operating system's temporary directory.
	ScratchDir string
}

// BuildInput contains independently sorted Events and Heads entries plus the
// authoritative filter insertion set.
type BuildInput struct {
	Events    EntryIterator
	Heads     EntryIterator
	Timelines TimelineIterator
}

type builtTable struct {
	file      *os.File
	path      string
	length    uint64
	entries   uint64
	seqLo     uint64
	seqHi     uint64
	minKey    []byte
	maxKey    []byte
	hash      [SHA256Bytes]byte
	timelines map[string]struct{}
}

type builtFilter struct {
	file   *os.File
	path   string
	length uint64
	header FilterHeader
	hash   [SHA256Bytes]byte
}

func (t *builtTable) cleanup() {
	if t == nil {
		return
	}
	if t.file != nil {
		_ = t.file.Close()
	}
	if t.path != "" {
		_ = os.Remove(t.path)
	}
}

func (f *builtFilter) cleanup() {
	if f == nil {
		return
	}
	if f.file != nil {
		_ = f.file.Close()
	}
	if f.path != "" {
		_ = os.Remove(f.path)
	}
}

// Build writes a complete version-1 run to dst. It constructs each SST in
// bounded scratch storage first, so semantic mismatches are rejected before
// any run bytes are exposed to dst and no complete encoded region is retained
// in heap memory.
func Build(ctx context.Context, dst io.Writer, opts BuildOptions, input BuildInput) (Ref, error) {
	if ctx == nil {
		return Ref{}, invalidRunf("nil context")
	}
	if dst == nil {
		return Ref{}, invalidRunf("nil destination")
	}
	if input.Events == nil || input.Heads == nil || input.Timelines == nil {
		return Ref{}, invalidRunf("nil build iterator")
	}
	preamble := Preamble{
		CreatorRole:     opts.CreatorRole,
		Shard:           opts.Shard,
		CreatorEpoch:    opts.CreatorEpoch,
		SeqLo:           opts.SeqLo,
		SeqHi:           opts.SeqHi,
		NamespaceHash:   opts.NamespaceHash,
		RunID:           opts.RunID,
		PublicationHash: opts.PublicationHash,
	}
	if err := preamble.Validate(); err != nil {
		return Ref{}, err
	}
	writerOptions, err := tableWriterOptions(opts.Table)
	if err != nil {
		return Ref{}, err
	}

	events, err := buildTable(ctx, input.Events, writerOptions, opts, "events")
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: build Events SST: %w", err)
	}
	defer events.cleanup()
	heads, err := buildTable(ctx, input.Heads, writerOptions, opts, "heads")
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: build Heads SST: %w", err)
	}
	defer heads.cleanup()

	timelineSet, timelines, err := collectTimelines(ctx, input.Timelines)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: collect timelines: %w", err)
	}
	if !sameStringSet(events.timelines, heads.timelines) {
		return Ref{}, invalidRunf("Events and Heads timeline sets differ")
	}
	if !sameStringSet(events.timelines, timelineSet) {
		return Ref{}, invalidRunf("table and filter timeline sets differ")
	}
	slices.SortFunc(timelines, bytes.Compare)

	filter, err := buildFilterScratch(ctx, opts.RunID, timelines, opts.Filter, opts.ScratchDir)
	if err != nil {
		return Ref{}, fmt.Errorf("runfile: build timeline filter: %w", err)
	}
	defer filter.cleanup()

	preambleBytes, err := MarshalPreamble(preamble)
	if err != nil {
		return Ref{}, err
	}

	offset := uint64(PreambleBytes)
	eventsRegion := tableRegion(RegionKindEventsSST, offset, events)
	offset, err = alignedEnd(eventsRegion.Offset, eventsRegion.Length)
	if err != nil {
		return Ref{}, err
	}
	headsRegion := tableRegion(RegionKindHeadsSST, offset, heads)
	offset, err = alignedEnd(headsRegion.Offset, headsRegion.Length)
	if err != nil {
		return Ref{}, err
	}
	filterRegion := RegionDescriptor{
		Kind:        RegionKindTimelineFilter,
		Required:    false,
		Encoding:    RegionEncodingV1,
		Offset:      offset,
		Length:      filter.length,
		EntryCount:  filter.header.KeyCount,
		ContentHash: filter.hash,
	}
	directoryOffset, err := alignedEnd(filterRegion.Offset, filterRegion.Length)
	if err != nil {
		return Ref{}, err
	}

	directory := Directory{
		DirectoryOffset: directoryOffset,
		MinTimeline:     bytes.Clone(timelines[0]),
		MaxTimeline:     bytes.Clone(timelines[len(timelines)-1]),
		Regions:         []RegionDescriptor{eventsRegion, headsRegion, filterRegion},
	}
	directoryBytes, err := MarshalDirectory(directory)
	if err != nil {
		return Ref{}, err
	}
	directoryHash := sha256.Sum256(directoryBytes)
	directoryEnd, ok := checkedAdd(directoryOffset, uint64(len(directoryBytes)))
	if !ok {
		return Ref{}, runTooLargef("directory end overflows")
	}
	objectSize, ok := checkedAdd(directoryEnd, TrailerBytes)
	if !ok || objectSize > MaxRunObjectBytes {
		return Ref{}, runTooLargef("run object size exceeds %d", MaxRunObjectBytes)
	}

	payload := newPayloadWriter(dst)
	if err := payload.write(ctx, preambleBytes); err != nil {
		return Ref{}, err
	}
	if err := streamRegion(ctx, payload, events.file, eventsRegion); err != nil {
		return Ref{}, fmt.Errorf("runfile: write Events SST: %w", err)
	}
	if err := streamRegion(ctx, payload, heads.file, headsRegion); err != nil {
		return Ref{}, fmt.Errorf("runfile: write Heads SST: %w", err)
	}
	if err := streamRegion(ctx, payload, filter.file, filterRegion); err != nil {
		return Ref{}, fmt.Errorf("runfile: write timeline filter: %w", err)
	}
	if err := writePaddingTo(ctx, payload, directoryOffset); err != nil {
		return Ref{}, err
	}
	if err := payload.write(ctx, directoryBytes); err != nil {
		return Ref{}, fmt.Errorf("runfile: write directory: %w", err)
	}
	if payload.size != directoryEnd {
		return Ref{}, invalidRunf("payload size %d, want %d", payload.size, directoryEnd)
	}
	var payloadHash [SHA256Bytes]byte
	copy(payloadHash[:], payload.hash.Sum(nil))
	trailer := Trailer{
		DirectoryOffset: directoryOffset,
		DirectoryLength: uint64(len(directoryBytes)),
		ObjectSize:      objectSize,
		RegionCount:     uint16(len(directory.Regions)),
		DirectoryHash:   directoryHash,
		PayloadHash:     payloadHash,
		RunID:           opts.RunID,
	}
	trailerBytes, err := MarshalTrailer(trailer)
	if err != nil {
		return Ref{}, err
	}
	if err := writeContext(ctx, dst, trailerBytes); err != nil {
		return Ref{}, fmt.Errorf("runfile: write trailer: %w", err)
	}

	ref := refFromParts(preamble, directory, trailer, &filter.header)
	if err := ref.Validate(); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

func tableWriterOptions(options TableOptions) (sstable.WriterOptions, error) {
	var compression *sstable.CompressionProfile
	switch options.Compression {
	case TableCompressionSnappy:
		compression = sstable.SnappyCompression
	case TableCompressionNone:
		compression = sstable.NoCompression
	case TableCompressionZstd:
		compression = sstable.ZstdCompression
	default:
		return sstable.WriterOptions{}, invalidRunf("unknown table compression %d", options.Compression)
	}
	bitsPerKey := options.NativeFilterBitsPerKey
	if bitsPerKey == 0 {
		bitsPerKey = canonicalNativeFilterBitsPerKey
	}
	if bitsPerKey < 0 || bitsPerKey > 100 {
		return sstable.WriterOptions{}, invalidRunf("native filter bits per key %d outside [0,100]", bitsPerKey)
	}
	writerOptions := sstable.WriterOptions{
		BlockRestartInterval:       canonicalTableRestartInterval,
		BlockSize:                  canonicalTableBlockSize,
		BlockSizeThreshold:         canonicalTableBlockThreshold,
		SizeClassAwareThreshold:    canonicalTableSizeClassThreshold,
		Comparer:                   sstable.DefaultComparer,
		Compression:                compression,
		FilterType:                 sstable.TableFilter,
		IndexBlockSize:             canonicalTableBlockSize,
		MergerName:                 "pebble.concatenate",
		TableFormat:                sstable.TableFormatPebblev1,
		Checksum:                   block.ChecksumTypeCRC32c,
		AllocatorSizeClasses:       []int{},
		NumDeletionsThreshold:      canonicalDeletionThreshold,
		DeletionSizeRatioThreshold: canonicalDeletionSizeRatio,
	}
	if !options.DisableNativeFilter && bitsPerKey > 0 {
		writerOptions.FilterPolicy = bloom.FilterPolicy(bitsPerKey)
	}
	return writerOptions, nil
}

func buildTable(ctx context.Context, iterator EntryIterator, writerOptions sstable.WriterOptions, opts BuildOptions, name string) (_ *builtTable, err error) {
	file, err := os.CreateTemp(opts.ScratchDir, "unijord-run-"+name+"-*.sst")
	if err != nil {
		return nil, err
	}
	result := &builtTable{file: file, path: file.Name(), seqLo: ^uint64(0), timelines: make(map[string]struct{})}
	succeeded := false
	defer func() {
		if closer, ok := iterator.(io.Closer); ok {
			err = errors.Join(err, closer.Close())
		}
		if !succeeded || err != nil {
			result.cleanup()
		}
	}()

	writable := newScratchWritable(file)
	writer := sstable.NewWriter(writable, writerOptions)
	abort := func(buildErr error) (*builtTable, error) {
		writable.Abort()
		_ = writer.Close()
		return nil, buildErr
	}
	var previousKey []byte
	var previousSeq uint64
	for iterator.Next() {
		if err := ctx.Err(); err != nil {
			return abort(err)
		}
		entry := iterator.Entry()
		if len(entry.Key) == 0 {
			return abort(invalidRunf("%s entry has empty key", name))
		}
		if uint64(len(entry.Key)) > MaxTableKeyBytes {
			return abort(runTooLargef("%s key length %d exceeds %d", name, len(entry.Key), MaxTableKeyBytes))
		}
		if len(entry.Timeline) == 0 {
			return abort(invalidRunf("%s entry has empty timeline", name))
		}
		if uint64(len(entry.Timeline)) > MaxTimelineBytes {
			return abort(runTooLargef("%s timeline length %d exceeds %d", name, len(entry.Timeline), MaxTimelineBytes))
		}
		if entry.Seq < opts.SeqLo || entry.Seq > opts.SeqHi {
			return abort(invalidRunf("%s entry sequence %d outside run range [%d,%d]", name, entry.Seq, opts.SeqLo, opts.SeqHi))
		}
		if entry.Seq > maxPebbleSequence {
			return abort(runTooLargef("%s entry sequence %d exceeds Pebble maximum %d", name, entry.Seq, maxPebbleSequence))
		}
		if result.entries > 0 {
			switch comparison := bytes.Compare(previousKey, entry.Key); {
			case comparison > 0:
				return abort(invalidRunf("%s keys are out of order", name))
			case comparison == 0 && entry.Seq >= previousSeq:
				return abort(invalidRunf("%s duplicate-key sequences are not strictly descending", name))
			}
		}
		key := bytes.Clone(entry.Key)
		internalKey := pebble.MakeInternalKey(key, pebble.SeqNum(entry.Seq), pebble.InternalKeyKindSet)
		if err := writer.Raw().Add(internalKey, entry.Value, false); err != nil {
			return abort(err)
		}
		if result.entries == 0 {
			result.minKey = bytes.Clone(key)
		}
		result.maxKey = bytes.Clone(key)
		result.seqLo = min(result.seqLo, entry.Seq)
		result.seqHi = max(result.seqHi, entry.Seq)
		result.entries++
		previousKey = key
		previousSeq = entry.Seq
		result.timelines[string(entry.Timeline)] = struct{}{}
	}
	if err := iterator.Err(); err != nil {
		return abort(err)
	}
	if result.entries == 0 {
		return abort(invalidRunf("%s table is empty", name))
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	result.length = writable.size
	copy(result.hash[:], writable.hash.Sum(nil))
	if result.length == 0 {
		return nil, invalidRunf("%s table encoded to zero bytes", name)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	succeeded = true
	return result, nil
}

func collectTimelines(ctx context.Context, iterator TimelineIterator) (_ map[string]struct{}, timelines [][]byte, err error) {
	set := make(map[string]struct{})
	defer func() {
		if closer, ok := iterator.(io.Closer); ok {
			err = errors.Join(err, closer.Close())
		}
	}()
	for iterator.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		timeline := iterator.Timeline()
		if len(timeline) == 0 {
			return nil, nil, invalidRunf("filter timeline is empty")
		}
		if uint64(len(timeline)) > MaxTimelineBytes {
			return nil, nil, runTooLargef("filter timeline length %d exceeds %d", len(timeline), MaxTimelineBytes)
		}
		key := string(timeline)
		if _, exists := set[key]; !exists {
			set[key] = struct{}{}
			timelines = append(timelines, bytes.Clone(timeline))
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, nil, err
	}
	if len(set) == 0 {
		return nil, nil, invalidRunf("timeline set is empty")
	}
	return set, timelines, nil
}

// buildFilterScratch constructs the UJTF region in a sparse scratch file. It
// keeps only one 64-byte line or one 4 KiB page in memory while setting bits
// and finalizing page checksums, so the run builder does not retain the whole
// filter region in heap memory.
func buildFilterScratch(
	ctx context.Context,
	runID [RunIDBytes]byte,
	timelines [][]byte,
	options FilterOptions,
	scratchDir string,
) (_ *builtFilter, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bitsPerKey := options.BitsPerKey
	if bitsPerKey == 0 {
		bitsPerKey = DefaultTimelineFilterBitsPerKey
	}
	header, err := NewFilterHeader(runID, uint64(len(timelines)), bitsPerKey)
	if err != nil {
		return nil, err
	}
	length, err := header.EncodedLength()
	if err != nil {
		return nil, err
	}

	file, err := os.CreateTemp(scratchDir, "unijord-run-filter-*.ujtf")
	if err != nil {
		return nil, err
	}
	result := &builtFilter{file: file, path: file.Name(), length: length, header: header}
	succeeded := false
	defer func() {
		if !succeeded || err != nil {
			result.cleanup()
		}
	}()

	if err := file.Truncate(int64(length)); err != nil {
		return nil, err
	}
	headerBytes, err := MarshalFilterHeader(header)
	if err != nil {
		return nil, err
	}
	if err := writeFileAt(file, headerBytes, 0); err != nil {
		return nil, err
	}

	var line [TimelineFilterLineBytes]byte
	for i, timeline := range timelines {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Equal(timelines[i-1], timeline) {
			return nil, invalidRunf("timeline filter scratch input contains duplicate timeline %x", timeline)
		}
		location, err := locateTimeline(header, timeline)
		if err != nil {
			return nil, err
		}
		pageOffset, ok := filterPageRelativeOffset(location.page)
		if !ok {
			return nil, runTooLargef("timeline filter page %d offset overflows", location.page)
		}
		lineOffset, ok := checkedAdd(pageOffset, uint64(location.lineByteOffset))
		if !ok || lineOffset > length || TimelineFilterLineBytes > length-lineOffset {
			return nil, invalidRunf("timeline filter line range exceeds scratch region")
		}
		if err := readFileAt(file, line[:], lineOffset); err != nil {
			return nil, err
		}
		for probe := uint8(0); probe < header.Probes; probe++ {
			bit := location.probeBits[probe]
			line[bit/8] |= byte(1 << (bit % 8))
		}
		if err := writeFileAt(file, line[:], lineOffset); err != nil {
			return nil, err
		}
		clear(line[:])
	}

	pageData := make([]byte, TimelineFilterPageDataBytes)
	for page := uint32(0); page < header.PageCount; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pageOffset, ok := filterPageRelativeOffset(page)
		if !ok {
			return nil, runTooLargef("timeline filter page %d offset overflows", page)
		}
		dataLength, ok := filterPageDataLength(header, page)
		if !ok {
			return nil, invalidRunf("timeline filter page %d has invalid geometry", page)
		}
		data := pageData[:int(dataLength)]
		if err := readFileAt(file, data, pageOffset); err != nil {
			return nil, err
		}
		var checksum [TimelineFilterPageChecksumBytes]byte
		binary.BigEndian.PutUint32(checksum[:], filterPageCRC32C(headerBytes, page, data))
		if err := writeFileAt(file, checksum[:], pageOffset+dataLength); err != nil {
			return nil, err
		}
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	hasher := sha256.New()
	buffer := make([]byte, 128<<10)
	remaining := length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkLength := min(remaining, uint64(len(buffer)))
		n, err := io.ReadFull(file, buffer[:int(chunkLength)])
		if err != nil {
			return nil, err
		}
		_, _ = hasher.Write(buffer[:n])
		remaining -= uint64(n)
	}
	copy(result.hash[:], hasher.Sum(nil))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	succeeded = true
	return result, nil
}

func readFileAt(file *os.File, dst []byte, offset uint64) error {
	n, err := file.ReadAt(dst, int64(offset))
	if err != nil {
		return err
	}
	if n != len(dst) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func writeFileAt(file *os.File, data []byte, offset uint64) error {
	for len(data) > 0 {
		n, err := file.WriteAt(data, int64(offset))
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		if n > 0 {
			offset += uint64(n)
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}

func tableRegion(kind RegionKind, offset uint64, table *builtTable) RegionDescriptor {
	return RegionDescriptor{
		Kind:        kind,
		Required:    true,
		Encoding:    RegionEncodingV1,
		Offset:      offset,
		Length:      table.length,
		EntryCount:  table.entries,
		SeqLo:       table.seqLo,
		SeqHi:       table.seqHi,
		MinKey:      bytes.Clone(table.minKey),
		MaxKey:      bytes.Clone(table.maxKey),
		ContentHash: table.hash,
	}
}

func alignedEnd(offset, length uint64) (uint64, error) {
	end, ok := checkedAdd(offset, length)
	if !ok {
		return 0, runTooLargef("region end overflows")
	}
	aligned, ok := checkedAlign8(end)
	if !ok || aligned > MaxRunObjectBytes {
		return 0, runTooLargef("aligned region end exceeds %d", MaxRunObjectBytes)
	}
	return aligned, nil
}

type scratchWritable struct {
	file     *os.File
	hash     hash.Hash
	size     uint64
	finished bool
	aborted  bool
}

var _ objstorage.Writable = (*scratchWritable)(nil)

func newScratchWritable(file *os.File) *scratchWritable {
	return &scratchWritable{file: file, hash: sha256.New()}
}

func (w *scratchWritable) Write(data []byte) error {
	if w.finished || w.aborted {
		return errors.New("runfile: write to closed scratch SST")
	}
	if uint64(len(data)) > MaxRunObjectBytes-w.size {
		return runTooLargef("scratch SST exceeds maximum run object size %d", MaxRunObjectBytes)
	}
	n, err := w.file.Write(data)
	if n > 0 {
		_, _ = w.hash.Write(data[:n])
		w.size += uint64(n)
	}
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func (w *scratchWritable) Finish() error {
	if w.aborted {
		return errors.New("runfile: finish aborted scratch SST")
	}
	w.finished = true
	return nil
}

func (w *scratchWritable) Abort() {
	w.aborted = true
}

type payloadWriter struct {
	dst  io.Writer
	hash hash.Hash
	size uint64
}

func newPayloadWriter(dst io.Writer) *payloadWriter {
	return &payloadWriter{dst: dst, hash: sha256.New()}
}

func (w *payloadWriter) write(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeContext(ctx, io.MultiWriter(w.dst, w.hash), data); err != nil {
		return err
	}
	w.size += uint64(len(data))
	return nil
}

func writeContext(ctx context.Context, dst io.Writer, data []byte) error {
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := dst.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writePaddingTo(ctx context.Context, dst *payloadWriter, target uint64) error {
	if dst.size > target {
		return invalidRunf("output position %d exceeds target %d", dst.size, target)
	}
	var zeros [RegionAlignment]byte
	remaining := target - dst.size
	for remaining > 0 {
		length := min(remaining, uint64(len(zeros)))
		if err := dst.write(ctx, zeros[:length]); err != nil {
			return err
		}
		remaining -= length
	}
	return nil
}

func streamRegion(ctx context.Context, dst *payloadWriter, file *os.File, region RegionDescriptor) error {
	if err := writePaddingTo(ctx, dst, region.Offset); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buffer := make([]byte, 128<<10)
	remaining := region.Length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := min(remaining, uint64(len(buffer)))
		n, err := io.ReadFull(file, buffer[:length])
		if err != nil {
			return err
		}
		if err := dst.write(ctx, buffer[:n]); err != nil {
			return err
		}
		remaining -= uint64(n)
	}
	return nil
}
