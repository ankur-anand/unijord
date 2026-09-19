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

	// ScratchDir optionally selects bounded scratch storage for both SSTs and UJTF.
	// The empty value uses the operating system's temporary directory.
	ScratchDir string
	// MaxTimelines is a required admission limit for the borrowed catalog.
	// Validation uses five bytes per timeline (one mask and one uint32 index),
	// plus bounded key/metadata storage. There is no implicit sizing default.
	MaxTimelines uint32
}

// BuildInput contains independently sorted Events and Heads entries plus the
// stable exact catalog used by both tables and the filter. Every catalog ID
// must occur in Events and exactly once in Heads. The caller owns the catalog;
// Prepare does not close it. Its lifetime ends only after Prepare returns.
type BuildInput struct {
	Events    EntryIterator
	Heads     EntryIterator
	Timelines TimelineCatalog
}

type builtTable struct {
	file    *os.File
	path    string
	length  uint64
	entries uint64
	seqLo   uint64
	seqHi   uint64
	minKey  []byte
	maxKey  []byte
	hash    [SHA256Bytes]byte
}

type builtFilter struct {
	file   *os.File
	path   string
	length uint64
	header FilterHeader
	hash   [SHA256Bytes]byte
}

func (t *builtTable) cleanup() error {
	if t == nil {
		return nil
	}
	return closeScratch(t.file, t.path)
}

func (f *builtFilter) cleanup() error {
	if f == nil {
		return nil
	}
	return closeScratch(f.file, f.path)
}

// Build is the temporary one-shot wrapper around Prepare and WriteTo. On any
// failure, including cancellation or cleanup failure, it returns a zero Ref.
func Build(ctx context.Context, dst io.Writer, opts BuildOptions, input BuildInput) (ref Ref, err error) {
	prepared, err := Prepare(ctx, opts, input)
	if err != nil {
		return Ref{}, err
	}
	defer func() {
		err = errors.Join(err, prepared.Close())
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			ref = Ref{}
		}
	}()
	if err := prepared.WriteTo(ctx, dst); err != nil {
		return Ref{}, err
	}
	return prepared.Ref(), nil
}

// Prepare consumes and closes all supplied io.Closer iterators, including on
// validation failure. The iterators must be independently owned. It freezes
// version-1 framing and hashes over three bounded scratch regions, retaining
// neither the input nor the temporary timeline/key validation collections.
// A successful result owns its scratch until Close; failure returns nil.
func Prepare(ctx context.Context, opts BuildOptions, input BuildInput) (prepared PreparedRun, err error) {
	return prepare(ctx, opts, input, nil)
}

// buildInstrumentation is per-call test instrumentation, never global state.
// It observes the exact slices passed to SST materialization and filter hashing.
type buildInstrumentation struct {
	entry       func(Entry, []byte, []byte, []byte)
	filter      func(TimelineID, []byte)
	keyBuffer   func(oldCapacity, newCapacity int)
	metadataKey func()
}

func prepare(ctx context.Context, opts BuildOptions, input BuildInput, audit *buildInstrumentation) (prepared PreparedRun, err error) {
	defer func() {
		for _, iterator := range []any{input.Events, input.Heads} {
			if closer, ok := iterator.(io.Closer); ok {
				err = errors.Join(err, closer.Close())
			}
		}
		if err == nil && ctx != nil {
			err = ctx.Err()
		}
		if err != nil && prepared != nil {
			err = errors.Join(err, prepared.Close())
			prepared = nil
		}
	}()
	if ctx == nil {
		return nil, invalidRunf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if input.Events == nil || input.Heads == nil || input.Timelines == nil {
		return nil, invalidRunf("nil build iterator")
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
		return nil, err
	}
	writerOptions, err := tableWriterOptions(opts.Table)
	if err != nil {
		return nil, err
	}
	catalog, err := validateCatalog(ctx, input.Timelines, opts.MaxTimelines, audit)
	if err != nil {
		return nil, err
	}

	events, err := buildTableWithin(ctx, input.Events, writerOptions, opts, "events", MaxRunObjectBytes, catalog, observedEvents)
	if err != nil {
		return nil, fmt.Errorf("runfile: build Events SST: %w", err)
	}
	p := &preparedRun{}
	p.regions[0] = preparedRegion{file: events.file, path: events.path}
	defer func() {
		if prepared == nil {
			err = errors.Join(err, p.Close())
		}
	}()
	// Admit scratch before writing it: all three regions together stay within
	// the format object cap, even when a later validation rejects the run.
	heads, err := buildTableWithin(ctx, input.Heads, writerOptions, opts, "heads", MaxRunObjectBytes-events.length, catalog, observedHeads)
	if err != nil {
		return nil, fmt.Errorf("runfile: build Heads SST: %w", err)
	}
	p.regions[1] = preparedRegion{file: heads.file, path: heads.path}

	filter, err := buildFilterScratchWithin(ctx, opts.RunID, catalog, opts.Filter, opts.ScratchDir, MaxRunObjectBytes-events.length-heads.length)
	if err != nil {
		return nil, fmt.Errorf("runfile: build timeline filter: %w", err)
	}
	p.regions[2] = preparedRegion{file: filter.file, path: filter.path}
	if err := catalog.complete(); err != nil {
		return nil, err
	}

	preambleBytes, err := MarshalPreamble(preamble)
	if err != nil {
		return nil, err
	}

	offset := uint64(PreambleBytes)
	eventsRegion := tableRegion(RegionKindEventsSST, offset, events)
	offset, err = alignedEnd(eventsRegion.Offset, eventsRegion.Length)
	if err != nil {
		return nil, err
	}
	headsRegion := tableRegion(RegionKindHeadsSST, offset, heads)
	offset, err = alignedEnd(headsRegion.Offset, headsRegion.Length)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	directory := Directory{
		DirectoryOffset: directoryOffset,
		MinTimeline:     bytes.Clone(catalog.timeline(catalog.order[0])),
		MaxTimeline:     bytes.Clone(catalog.timeline(catalog.order[len(catalog.order)-1])),
		Regions:         []RegionDescriptor{eventsRegion, headsRegion, filterRegion},
	}
	directoryBytes, err := MarshalDirectory(directory)
	if err != nil {
		return nil, err
	}
	directoryHash := sha256.Sum256(directoryBytes)
	directoryEnd, ok := checkedAdd(directoryOffset, uint64(len(directoryBytes)))
	if !ok {
		return nil, runTooLargef("directory end overflows")
	}
	objectSize, ok := checkedAdd(directoryEnd, TrailerBytes)
	if !ok || objectSize > MaxRunObjectBytes {
		return nil, runTooLargef("run object size exceeds %d", MaxRunObjectBytes)
	}

	p.preamble, p.directory = preambleBytes, directoryBytes
	p.directoryOffset = directoryOffset
	for i, region := range directory.Regions {
		p.regions[i].offset, p.regions[i].length = region.Offset, region.Length
	}
	hasher := sha256.New()
	if err := p.writePayload(ctx, hasher); err != nil {
		return nil, fmt.Errorf("runfile: prepare payload hash: %w", err)
	}
	var payloadHash [SHA256Bytes]byte
	copy(payloadHash[:], hasher.Sum(nil))
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
		return nil, err
	}

	p.trailer = trailerBytes
	p.ref = refFromParts(preamble, directory, trailer, &filter.header)
	if err := p.ref.Validate(); err != nil {
		return nil, err
	}
	return p, nil
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

func buildTableWithin(ctx context.Context, iterator EntryIterator, writerOptions sstable.WriterOptions, opts BuildOptions, name string, scratchLimit uint64, catalog *catalogValidation, source uint8) (_ *builtTable, err error) {
	file, err := os.CreateTemp(opts.ScratchDir, "unijord-run-"+name+"-*.sst")
	if err != nil {
		return nil, err
	}
	result := &builtTable{file: file, path: file.Name(), seqLo: ^uint64(0)}
	succeeded := false
	defer func() {
		if !succeeded || err != nil {
			err = errors.Join(err, result.cleanup())
		}
	}()

	writable := newScratchWritable(file)
	writable.limit = min(scratchLimit, MaxRunObjectBytes)
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
		if result.entries == ^uint64(0) {
			return abort(runTooLargef("%s entry count overflows", name))
		}
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
		if err := catalog.observe(entry.TimelineID, entry.Timeline, source); err != nil {
			return abort(err)
		}
		// Pebble-v1 Add synchronously encodes these borrowed slices into its
		// bounded data block. They need not survive the next iterator call.
		internalKey := pebble.MakeInternalKey(entry.Key, pebble.SeqNum(entry.Seq), pebble.InternalKeyKindSet)
		if catalog.audit != nil && catalog.audit.entry != nil {
			catalog.audit.entry(entry, internalKey.UserKey, entry.Value, catalog.timeline(entry.TimelineID))
		}
		if err := writer.Raw().Add(internalKey, entry.Value, false); err != nil {
			return abort(err)
		}
		if result.entries == 0 {
			result.minKey = bytes.Clone(entry.Key)
			if catalog.audit != nil && catalog.audit.metadataKey != nil {
				catalog.audit.metadataKey()
			}
		}
		result.seqLo = min(result.seqLo, entry.Seq)
		result.seqHi = max(result.seqHi, entry.Seq)
		result.entries++
		if len(entry.Key) > cap(previousKey) {
			// Geometric growth stays within the format key limit. Exactly one
			// reusable validation buffer is live per table, never one per entry.
			capacity := min(int(MaxTableKeyBytes), max(len(entry.Key), 2*cap(previousKey)))
			if catalog.audit != nil && catalog.audit.keyBuffer != nil {
				catalog.audit.keyBuffer(cap(previousKey), capacity)
			}
			previousKey = make([]byte, capacity)
		}
		previousKey = previousKey[:len(entry.Key)]
		copy(previousKey, entry.Key)
		previousSeq = entry.Seq
	}
	if err := iterator.Err(); err != nil {
		return abort(err)
	}
	if result.entries == 0 {
		return abort(invalidRunf("%s table is empty", name))
	}
	result.maxKey = bytes.Clone(previousKey)
	if catalog.audit != nil && catalog.audit.metadataKey != nil {
		catalog.audit.metadataKey()
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

// buildFilterScratchWithin constructs the UJTF region in a sparse scratch file. It
// keeps only one 64-byte line or one 4 KiB page in memory while setting bits
// and finalizing page checksums, so the run builder does not retain the whole
// filter region in heap memory.
func buildFilterScratchWithin(ctx context.Context, runID [RunIDBytes]byte, catalog *catalogValidation, options FilterOptions, scratchDir string, scratchLimit uint64) (_ *builtFilter, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bitsPerKey := options.BitsPerKey
	if bitsPerKey == 0 {
		bitsPerKey = DefaultTimelineFilterBitsPerKey
	}
	header, err := NewFilterHeader(runID, uint64(len(catalog.masks)), bitsPerKey)
	if err != nil {
		return nil, err
	}
	length, err := header.EncodedLength()
	if err != nil {
		return nil, err
	}
	if length > scratchLimit {
		return nil, runTooLargef("filter exceeds remaining scratch budget %d", scratchLimit)
	}

	file, err := os.CreateTemp(scratchDir, "unijord-run-filter-*.ujtf")
	if err != nil {
		return nil, err
	}
	result := &builtFilter{file: file, path: file.Name(), length: length, header: header}
	succeeded := false
	defer func() {
		if !succeeded || err != nil {
			err = errors.Join(err, result.cleanup())
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
	for _, id := range catalog.order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		timeline := catalog.timeline(id)
		if catalog.audit != nil && catalog.audit.filter != nil {
			catalog.audit.filter(id, timeline)
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
		if err := catalog.observe(id, timeline, observedFilter); err != nil {
			return nil, err
		}
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
	limit    uint64
	finished bool
	aborted  bool
}

var _ objstorage.Writable = (*scratchWritable)(nil)

func newScratchWritable(file *os.File) *scratchWritable {
	return &scratchWritable{file: file, hash: sha256.New(), limit: MaxRunObjectBytes}
}

func (w *scratchWritable) Write(data []byte) error {
	if w.finished || w.aborted {
		return errors.New("runfile: write to closed scratch SST")
	}
	if w.size > w.limit || uint64(len(data)) > w.limit-w.size {
		return runTooLargef("scratch SST exceeds remaining budget %d", w.limit)
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
	size uint64
}

func newPayloadWriter(dst io.Writer) *payloadWriter {
	return &payloadWriter{dst: dst}
}

func (w *payloadWriter) write(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeContext(ctx, w.dst, data); err != nil {
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
		short := n != len(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if short {
			return io.ErrShortWrite
		}
	}
	return ctx.Err()
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

func streamRegion(ctx context.Context, dst *payloadWriter, region preparedRegion, buffer []byte) error {
	if err := writePaddingTo(ctx, dst, region.offset); err != nil {
		return err
	}
	remaining := region.length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := min(remaining, uint64(len(buffer)))
		n, err := region.file.ReadAt(buffer[:length], int64(region.length-remaining))
		if err != nil {
			return err
		}
		if uint64(n) != length {
			return io.ErrUnexpectedEOF
		}
		if err := dst.write(ctx, buffer[:n]); err != nil {
			return err
		}
		remaining -= uint64(n)
	}
	return nil
}
