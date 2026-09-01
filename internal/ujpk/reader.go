package ujpk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
)

type Reader struct {
	source     RangeSource
	identity   Identity
	rootOffset uint64
	packHash   uint64
	codec      Codec
	pages      uint32
	extents    uint32
	records    uint32
	dataPages  []pageEntry
	indexPages []indexPageRef
	indexCache map[uint64][]indexEntry
	pageCache  map[uint64][]byte
}

func Open(buf []byte) (*Reader, error) {
	reader, err := openRangeAtSize(context.Background(), bytesSource{buf: buf}, uint64(len(buf)))
	if err != nil {
		return nil, err
	}
	if err := validatePreamble(buf[:PreambleSize], reader.codec, reader.identity); err != nil {
		return nil, err
	}
	if got := xxhash.Sum64(buf[:len(buf)-TrailerSize]); got != reader.packHash {
		return nil, fmt.Errorf("%w: pack hash", ErrIntegrityMismatch)
	}
	if err := reader.validateAll(context.Background()); err != nil {
		return nil, err
	}
	reader.indexCache = make(map[uint64][]indexEntry)
	reader.pageCache = make(map[uint64][]byte)
	return reader, nil
}

// OpenRange opens a pack through its trailer and compact index root. Timeline
// index pages and data pages are fetched and validated only when selected.
func OpenRange(ctx context.Context, source RangeSource) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: nil range source", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	size, err := source.Size(ctx)
	if err != nil {
		return nil, fmt.Errorf("ujpk: read source size: %w", err)
	}
	return openRangeAtSize(ctx, source, size)
}

// OpenRangeAtSize opens an immutable pack when its exact size is already
// carried by an authenticated catalog reference, avoiding a separate size
// lookup. The source must remain bound to that immutable object generation.
func OpenRangeAtSize(ctx context.Context, source RangeSource, size uint64) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: nil range source", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return openRangeAtSize(ctx, source, size)
}

func openRangeAtSize(ctx context.Context, source RangeSource, size uint64) (*Reader, error) {
	if size < PreambleSize+TrailerSize {
		return nil, fmt.Errorf("%w: size=%d", ErrInvalidPack, size)
	}
	trailerOffset := size - TrailerSize
	trailer, err := readExactRange(ctx, source, trailerOffset, TrailerSize)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(trailer[0:4], trailerMagic[:]) {
		return nil, fmt.Errorf("%w: trailer magic=%q", ErrInvalidPack, trailer[0:4])
	}
	if binary.BigEndian.Uint16(trailer[4:6]) != Version ||
		binary.BigEndian.Uint16(trailer[6:8]) != TrailerSize {
		return nil, fmt.Errorf("%w: invalid trailer version or size", ErrInvalidPack)
	}
	codec := Codec(binary.BigEndian.Uint16(trailer[8:10]))
	if (codec != CodecNone && codec != CodecZstd) || !allZero(trailer[10:12]) {
		return nil, fmt.Errorf("%w: trailer codec=%d", ErrUnsupported, codec)
	}
	if total := binary.BigEndian.Uint64(trailer[56:64]); total != size {
		return nil, fmt.Errorf("%w: total size=%d actual=%d", ErrInvalidPack, total, size)
	}
	if !allZero(trailer[100:120]) {
		return nil, fmt.Errorf("%w: non-zero trailer reserved bytes", ErrInvalidPack)
	}
	if got, want := xxhash.Sum64(trailer[:120]), binary.BigEndian.Uint64(trailer[120:128]); got != want {
		return nil, fmt.Errorf("%w: trailer hash", ErrIntegrityMismatch)
	}
	var namespaceHash [32]byte
	copy(namespaceHash[:], trailer[64:96])
	if namespaceHash == ([32]byte{}) {
		return nil, fmt.Errorf("%w: zero namespace hash", ErrInvalidPack)
	}
	identity := Identity{NamespaceHash: namespaceHash, Shard: binary.BigEndian.Uint32(trailer[96:100])}

	pages := binary.BigEndian.Uint32(trailer[12:16])
	extents := binary.BigEndian.Uint32(trailer[16:20])
	records := binary.BigEndian.Uint32(trailer[20:24])
	if pages == 0 || extents == 0 || records == 0 {
		return nil, fmt.Errorf("%w: zero trailer count", ErrInvalidPack)
	}
	rootOffset := binary.BigEndian.Uint64(trailer[40:48])
	rootLength := binary.BigEndian.Uint64(trailer[48:56])
	if rootOffset < PreambleSize || rootOffset > trailerOffset ||
		rootLength < IndexRootPreambleSize || rootLength > MaxIndexRootBytes ||
		rootLength > trailerOffset-rootOffset || rootOffset+rootLength != trailerOffset {
		return nil, fmt.Errorf("%w: index root range off=%d len=%d", ErrInvalidPack, rootOffset, rootLength)
	}
	root, err := readExactRange(ctx, source, rootOffset, rootLength)
	if err != nil {
		return nil, err
	}
	if got, want := xxhash.Sum64(root), binary.BigEndian.Uint64(trailer[32:40]); got != want {
		return nil, fmt.Errorf("%w: index root hash", ErrIntegrityMismatch)
	}
	dataPages, indexPages, err := parseIndexRoot(root, rootOffset, pages, extents, records)
	if err != nil {
		return nil, err
	}
	return &Reader{
		source:     source,
		identity:   identity,
		rootOffset: rootOffset,
		packHash:   binary.BigEndian.Uint64(trailer[24:32]),
		codec:      codec,
		pages:      pages,
		extents:    extents,
		records:    records,
		dataPages:  dataPages,
		indexPages: indexPages,
		indexCache: make(map[uint64][]indexEntry),
		pageCache:  make(map[uint64][]byte),
	}, nil
}

func validatePreamble(preamble []byte, codec Codec, identity Identity) error {
	if len(preamble) != PreambleSize || !bytes.Equal(preamble[0:4], packMagic[:]) {
		return fmt.Errorf("%w: pack preamble", ErrInvalidPack)
	}
	if version := binary.BigEndian.Uint16(preamble[4:6]); version != Version {
		return fmt.Errorf("%w: version=%d", ErrUnsupported, version)
	}
	if binary.BigEndian.Uint16(preamble[6:8]) != PreambleSize ||
		Codec(binary.BigEndian.Uint16(preamble[8:10])) != codec ||
		!allZero(preamble[10:12]) ||
		binary.BigEndian.Uint32(preamble[12:16]) != identity.Shard ||
		!bytes.Equal(preamble[16:48], identity.NamespaceHash[:]) ||
		!allZero(preamble[48:64]) {
		return fmt.Errorf("%w: invalid preamble", ErrInvalidPack)
	}
	return nil
}

func (r *Reader) Pages() int         { return int(r.pages) }
func (r *Reader) Records() int       { return int(r.records) }
func (r *Reader) Identity() Identity { return r.identity }

func (r *Reader) ReadTimeline(key []byte, fromLSN uint64) ([]Record, ReadStats, error) {
	return r.ReadTimelineContext(context.Background(), key, fromLSN)
}

func (r *Reader) TimelineSpans() ([]TimelineSpan, error) {
	return r.TimelineSpansContext(context.Background())
}

// TimelineSpansContext enumerates the complete logical timeline ranges in the
// pack. It is a full-pack metadata/repair operation, not a point-read path.
func (r *Reader) TimelineSpansContext(ctx context.Context) ([]TimelineSpan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var stats ReadStats
	entries := make([]indexEntry, 0, r.extents)
	for _, ref := range r.indexPages {
		pageEntries, err := r.loadIndexPage(ctx, ref, &stats)
		if err != nil {
			return nil, err
		}
		entries = append(entries, pageEntries...)
	}
	type extentSpan struct {
		first uint64
		last  uint64
	}
	byKey := make(map[string][]extentSpan)
	keys := make(map[string][]byte)
	seenPages := make(map[uint32]struct{})
	for _, entry := range entries {
		raw, err := r.loadDataPage(ctx, entry.pageNo, &stats, seenPages)
		if err != nil {
			return nil, err
		}
		_, key, err := decodeExtent(raw, entry)
		if err != nil {
			return nil, err
		}
		name := string(key)
		keys[name] = append([]byte(nil), key...)
		byKey[name] = append(byKey[name], extentSpan{
			first: entry.firstLSN,
			last:  entry.firstLSN + uint64(entry.recordCount) - 1,
		})
	}
	names := make([]string, 0, len(byKey))
	for name := range byKey {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]TimelineSpan, 0, len(names))
	for _, name := range names {
		extents := byKey[name]
		sort.Slice(extents, func(i, j int) bool { return extents[i].first < extents[j].first })
		for i := 1; i < len(extents); i++ {
			if extents[i-1].last == math.MaxUint64 || extents[i].first != extents[i-1].last+1 {
				return nil, fmt.Errorf("%w: key=%q span discontinuity", ErrTimelineOrder, name)
			}
		}
		result = append(result, TimelineSpan{TimelineKey: keys[name], FirstLSN: extents[0].first, LastLSN: extents[len(extents)-1].last})
	}
	return result, nil
}

func (r *Reader) ReadTimelineContext(ctx context.Context, key []byte, fromLSN uint64) ([]Record, ReadStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, ReadStats{}, err
	}
	if len(key) == 0 || len(key) > MaxTimelineKeyBytes {
		return nil, ReadStats{}, fmt.Errorf("%w: key bytes=%d", ErrInvalidTimeline, len(key))
	}
	hash := hashTimelineKey(key)
	start := sort.Search(len(r.indexPages), func(i int) bool {
		return bytes.Compare(r.indexPages[i].lastHash[:], hash[:]) >= 0
	})
	if start == len(r.indexPages) {
		return nil, ReadStats{}, ErrTimelineNotFound
	}

	var stats ReadStats
	var candidates []indexEntry
	for i := start; i < len(r.indexPages); i++ {
		ref := r.indexPages[i]
		if bytes.Compare(ref.firstHash[:], hash[:]) > 0 {
			break
		}
		if bytes.Compare(ref.lastHash[:], hash[:]) < 0 {
			continue
		}
		entries, err := r.loadIndexPage(ctx, ref, &stats)
		if err != nil {
			return nil, stats, err
		}
		entryStart := sort.Search(len(entries), func(j int) bool {
			return bytes.Compare(entries[j].keyHash[:], hash[:]) >= 0
		})
		for j := entryStart; j < len(entries) && entries[j].keyHash == hash; j++ {
			candidates = append(candidates, entries[j])
		}
	}
	if len(candidates) == 0 {
		return nil, stats, ErrTimelineNotFound
	}

	type matchedExtent struct {
		entry   indexEntry
		records []Record
	}
	matches := make([]matchedExtent, 0, len(candidates))
	seenPages := make(map[uint32]struct{})
	for _, entry := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		raw, err := r.loadDataPage(ctx, entry.pageNo, &stats, seenPages)
		if err != nil {
			return nil, stats, err
		}
		extentRecords, actualKey, err := decodeExtent(raw, entry)
		if err != nil {
			return nil, stats, err
		}
		if bytes.Equal(actualKey, key) {
			matches = append(matches, matchedExtent{entry: entry, records: extentRecords})
		}
	}
	if len(matches) == 0 {
		return nil, stats, ErrTimelineNotFound
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].entry.firstLSN < matches[j].entry.firstLSN
	})

	var result []Record
	var wantLSN uint64
	var lastTimestamp int64
	for i, match := range matches {
		if i > 0 && match.entry.firstLSN != wantLSN {
			return nil, stats, fmt.Errorf("%w: key=%q lsn=%d want=%d", ErrTimelineOrder, key, match.entry.firstLSN, wantLSN)
		}
		if i > 0 && match.records[0].TimestampMS < lastTimestamp {
			return nil, stats, fmt.Errorf("%w: key=%q timestamp=%d previous=%d", ErrTimestampOrder, key, match.records[0].TimestampMS, lastTimestamp)
		}
		wantLSN = match.entry.firstLSN + uint64(match.entry.recordCount)
		lastTimestamp = match.records[len(match.records)-1].TimestampMS
		for _, record := range match.records {
			if record.TimelineLSN >= fromLSN {
				result = append(result, record)
			}
		}
	}
	return result, stats, nil
}

func (r *Reader) loadIndexPage(ctx context.Context, ref indexPageRef, stats *ReadStats) ([]indexEntry, error) {
	if entries, ok := r.indexCache[ref.offset]; ok {
		return entries, nil
	}
	page, err := readExactRange(ctx, r.source, ref.offset, uint64(ref.length))
	if err != nil {
		return nil, err
	}
	if xxhash.Sum64(page) != ref.hash {
		return nil, fmt.Errorf("%w: timeline index page hash", ErrIntegrityMismatch)
	}
	entries, err := parseIndexPage(page, ref, r.dataPages)
	if err != nil {
		return nil, err
	}
	r.indexCache[ref.offset] = entries
	stats.IndexPagesRead++
	stats.IndexBytes += len(page)
	return entries, nil
}

func (r *Reader) loadDataPage(ctx context.Context, pageNo uint32, stats *ReadStats, seen map[uint32]struct{}) ([]byte, error) {
	if pageNo >= uint32(len(r.dataPages)) {
		return nil, fmt.Errorf("%w: data page=%d", ErrInvalidPack, pageNo)
	}
	meta := r.dataPages[pageNo]
	if raw, ok := r.pageCache[meta.offset]; ok {
		return raw, nil
	}
	pageLength := uint64(PagePreambleSize) + uint64(meta.storedSize)
	if meta.offset+pageLength > r.rootOffset || meta.offset+pageLength < meta.offset {
		return nil, fmt.Errorf("%w: data page range", ErrInvalidPack)
	}
	page, err := readExactRange(ctx, r.source, meta.offset, pageLength)
	if err != nil {
		return nil, err
	}
	preamble := page[:PagePreambleSize]
	if !bytes.Equal(preamble[0:4], pageMagic[:]) ||
		binary.BigEndian.Uint16(preamble[4:6]) != PagePreambleSize ||
		!allZero(preamble[6:8]) || !allZero(preamble[20:24]) {
		return nil, fmt.Errorf("%w: data page preamble", ErrInvalidPack)
	}
	if binary.BigEndian.Uint32(preamble[8:12]) != meta.storedSize ||
		binary.BigEndian.Uint32(preamble[12:16]) != meta.rawSize ||
		binary.BigEndian.Uint32(preamble[16:20]) != meta.extentCount ||
		binary.BigEndian.Uint64(preamble[24:32]) != meta.hash {
		return nil, fmt.Errorf("%w: data page root mismatch", ErrInvalidPack)
	}
	stored := page[PagePreambleSize:]
	if xxhash.Sum64(stored) != meta.hash {
		return nil, fmt.Errorf("%w: data page hash", ErrIntegrityMismatch)
	}
	raw, err := decodePage(r.codec, stored, meta.rawSize)
	if err != nil {
		return nil, err
	}
	r.pageCache[meta.offset] = raw
	if _, counted := seen[pageNo]; !counted {
		seen[pageNo] = struct{}{}
		stats.PagesDecoded++
		stats.StoredBytes += len(stored)
		stats.RawBytes += len(raw)
	}
	return raw, nil
}

func (r *Reader) validateAll(ctx context.Context) error {
	allEntries := make([]indexEntry, 0, r.extents)
	var stats ReadStats
	for _, ref := range r.indexPages {
		entries, err := r.loadIndexPage(ctx, ref, &stats)
		if err != nil {
			return err
		}
		allEntries = append(allEntries, entries...)
	}
	if len(allEntries) != int(r.extents) {
		return fmt.Errorf("%w: index entries=%d want=%d", ErrInvalidPack, len(allEntries), r.extents)
	}

	byPage := make([][]indexEntry, len(r.dataPages))
	for _, entry := range allEntries {
		byPage[entry.pageNo] = append(byPage[entry.pageNo], entry)
	}
	type timelineExtent struct {
		firstLSN uint64
		count    uint32
		firstTS  int64
		lastTS   int64
	}
	byTimeline := make(map[string][]timelineExtent)
	seenPages := make(map[uint32]struct{})
	for pageNo, entries := range byPage {
		if len(entries) != int(r.dataPages[pageNo].extentCount) {
			return fmt.Errorf("%w: data page=%d extents=%d want=%d", ErrInvalidPack, pageNo, len(entries), r.dataPages[pageNo].extentCount)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].extentOffset < entries[j].extentOffset })
		var rawOffset uint64
		for _, entry := range entries {
			if uint64(entry.extentOffset) != rawOffset {
				return fmt.Errorf("%w: data page=%d extent offset=%d want=%d", ErrInvalidPack, pageNo, entry.extentOffset, rawOffset)
			}
			rawOffset += uint64(entry.extentLength)
		}
		if rawOffset != uint64(r.dataPages[pageNo].rawSize) {
			return fmt.Errorf("%w: data page=%d extent bytes=%d raw=%d", ErrInvalidPack, pageNo, rawOffset, r.dataPages[pageNo].rawSize)
		}
		raw, err := r.loadDataPage(ctx, uint32(pageNo), &stats, seenPages)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			records, key, err := decodeExtent(raw, entry)
			if err != nil {
				return err
			}
			byTimeline[string(key)] = append(byTimeline[string(key)], timelineExtent{
				firstLSN: entry.firstLSN,
				count:    entry.recordCount,
				firstTS:  records[0].TimestampMS,
				lastTS:   records[len(records)-1].TimestampMS,
			})
		}
	}
	for key, extents := range byTimeline {
		sort.Slice(extents, func(i, j int) bool { return extents[i].firstLSN < extents[j].firstLSN })
		for i := 1; i < len(extents); i++ {
			want := extents[i-1].firstLSN + uint64(extents[i-1].count)
			if extents[i].firstLSN != want {
				return fmt.Errorf("%w: key=%q lsn=%d want=%d", ErrTimelineOrder, key, extents[i].firstLSN, want)
			}
			if extents[i].firstTS < extents[i-1].lastTS {
				return fmt.Errorf("%w: key=%q timestamp=%d previous=%d", ErrTimestampOrder, key, extents[i].firstTS, extents[i-1].lastTS)
			}
		}
	}
	return nil
}

func decodeExtent(raw []byte, entry indexEntry) ([]Record, []byte, error) {
	start := uint64(entry.extentOffset)
	end := start + uint64(entry.extentLength)
	if end > uint64(len(raw)) || end < start {
		return nil, nil, fmt.Errorf("%w: extent range", ErrInvalidPack)
	}
	extent := raw[start:end]
	if len(extent) < ExtentPreambleSize || !bytes.Equal(extent[0:4], extentMagic[:]) ||
		binary.BigEndian.Uint16(extent[4:6]) != ExtentPreambleSize || !allZero(extent[24:32]) {
		return nil, nil, fmt.Errorf("%w: extent preamble", ErrInvalidPack)
	}
	keyLen := int(binary.BigEndian.Uint16(extent[6:8]))
	firstLSN := binary.BigEndian.Uint64(extent[8:16])
	recordCount := binary.BigEndian.Uint32(extent[16:20])
	bodyLen := int(binary.BigEndian.Uint32(extent[20:24]))
	if keyLen == 0 || keyLen > MaxTimelineKeyBytes || ExtentPreambleSize+keyLen > len(extent) ||
		firstLSN != entry.firstLSN || recordCount != entry.recordCount ||
		ExtentPreambleSize+keyLen+bodyLen != len(extent) {
		return nil, nil, fmt.Errorf("%w: extent index mismatch", ErrInvalidPack)
	}
	key := extent[ExtentPreambleSize : ExtentPreambleSize+keyLen]
	if hashTimelineKey(key) != entry.keyHash {
		return nil, nil, fmt.Errorf("%w: extent timeline hash mismatch", ErrInvalidPack)
	}
	off := ExtentPreambleSize + keyLen
	records := make([]Record, 0, recordCount)
	var lastTimestamp int64
	for i := uint32(0); i < recordCount; i++ {
		if len(extent)-off < RecordHeaderSize {
			return nil, nil, fmt.Errorf("%w: truncated record=%d", ErrInvalidPack, i)
		}
		timestamp := int64(binary.BigEndian.Uint64(extent[off : off+8]))
		if i > 0 && timestamp < lastTimestamp {
			return nil, nil, fmt.Errorf("%w: record=%d", ErrTimestampOrder, i)
		}
		headerLen := int(binary.BigEndian.Uint32(extent[off+8 : off+12]))
		valueLen := int(binary.BigEndian.Uint32(extent[off+12 : off+16]))
		if headerLen > MaxHeaderBytes || valueLen > MaxRecordValueBytes ||
			headerLen+valueLen > len(extent)-off-RecordHeaderSize {
			return nil, nil, fmt.Errorf("%w: record=%d lengths", ErrInvalidPack, i)
		}
		headers, err := decodeHeaders(extent[off+RecordHeaderSize : off+RecordHeaderSize+headerLen])
		if err != nil {
			return nil, nil, err
		}
		valueStart := off + RecordHeaderSize + headerLen
		records = append(records, Record{
			TimelineKey: append([]byte(nil), key...),
			TimelineLSN: firstLSN + uint64(i),
			TimestampMS: timestamp,
			Headers:     headers,
			Value:       append([]byte(nil), extent[valueStart:valueStart+valueLen]...),
		})
		off = valueStart + valueLen
		lastTimestamp = timestamp
	}
	if off != len(extent) {
		return nil, nil, fmt.Errorf("%w: trailing extent bytes=%d", ErrInvalidPack, len(extent)-off)
	}
	return records, append([]byte(nil), key...), nil
}

func decodeHeaders(buf []byte) ([]Header, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < 4 || !allZero(buf[2:4]) {
		return nil, fmt.Errorf("%w: header preamble", ErrInvalidPack)
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	if count == 0 || count > MaxHeaders {
		return nil, fmt.Errorf("%w: headers=%d", ErrInvalidPack, count)
	}
	off := 4
	headers := make([]Header, 0, count)
	for i := 0; i < count; i++ {
		if len(buf)-off < 8 || !allZero(buf[off+2:off+4]) {
			return nil, fmt.Errorf("%w: header=%d", ErrInvalidPack, i)
		}
		keyLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
		valueLen := int(binary.BigEndian.Uint32(buf[off+4 : off+8]))
		off += 8
		if keyLen > MaxHeaderKeyBytes || valueLen > MaxHeaderValueBytes || keyLen+valueLen > len(buf)-off {
			return nil, fmt.Errorf("%w: header=%d lengths", ErrInvalidPack, i)
		}
		headers = append(headers, Header{
			Key:   append([]byte(nil), buf[off:off+keyLen]...),
			Value: append([]byte(nil), buf[off+keyLen:off+keyLen+valueLen]...),
		})
		off += keyLen + valueLen
	}
	if off != len(buf) {
		return nil, fmt.Errorf("%w: trailing header bytes=%d", ErrInvalidPack, len(buf)-off)
	}
	return headers, nil
}

func allZero(buf []byte) bool {
	for _, value := range buf {
		if value != 0 {
			return false
		}
	}
	return true
}
