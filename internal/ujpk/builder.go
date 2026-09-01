package ujpk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
)

var (
	packMagic      = [4]byte{'U', 'J', 'P', 'K'}
	pageMagic      = [4]byte{'U', 'J', 'P', 'G'}
	extentMagic    = [4]byte{'U', 'J', 'E', 'X'}
	indexPageMagic = [4]byte{'U', 'J', 'I', 'P'}
	indexRootMagic = [4]byte{'U', 'J', 'I', 'R'}
	trailerMagic   = [4]byte{'U', 'J', 'F', 'T'}
)

type Builder struct {
	identity Identity
	opts     Options
	records  map[string][]Record
	state    map[string]timelineState
	count    uint32
	closed   bool
}

func NewBuilder(identity Identity, opts Options) (*Builder, error) {
	if identity.NamespaceHash == ([32]byte{}) {
		return nil, fmt.Errorf("%w: zero namespace hash", ErrInvalidOptions)
	}
	if opts.RawPageBytes == 0 {
		opts.RawPageBytes = DefaultOptions().RawPageBytes
	}
	if opts.RawPageBytes <= ExtentPreambleSize || opts.RawPageBytes > MaxRawPageBytes {
		return nil, fmt.Errorf("%w: raw page bytes=%d", ErrInvalidOptions, opts.RawPageBytes)
	}
	if opts.IndexPageBytes != AutoIndexPageBytes &&
		(opts.IndexPageBytes < IndexPagePreambleSize+TimelineIndexEntrySize ||
			opts.IndexPageBytes > MaxIndexPageBytes) {
		return nil, fmt.Errorf("%w: index page bytes=%d", ErrInvalidOptions, opts.IndexPageBytes)
	}
	if opts.Codec != CodecNone && opts.Codec != CodecZstd {
		return nil, fmt.Errorf("%w: codec=%d", ErrInvalidOptions, opts.Codec)
	}
	return &Builder{
		identity: identity,
		opts:     opts,
		records:  make(map[string][]Record),
		state:    make(map[string]timelineState),
	}, nil
}

func (b *Builder) Add(record Record) error {
	if b.closed {
		return ErrBuilderClosed
	}
	if len(record.TimelineKey) == 0 || len(record.TimelineKey) > MaxTimelineKeyBytes {
		return fmt.Errorf("%w: key bytes=%d", ErrInvalidTimeline, len(record.TimelineKey))
	}
	if record.TimelineLSN == math.MaxUint64 {
		return fmt.Errorf("%w: reserved timeline LSN", ErrTimelineOrder)
	}
	if b.count == math.MaxUint32 {
		return fmt.Errorf("%w: record count exhausted", ErrRecordTooLarge)
	}
	if _, err := encodeRecord(record); err != nil {
		return err
	}

	key := string(record.TimelineKey)
	state, exists := b.state[key]
	if exists {
		if state.lastLSN == math.MaxUint64 || record.TimelineLSN != state.lastLSN+1 {
			return fmt.Errorf("%w: key=%q lsn=%d want=%d", ErrTimelineOrder, record.TimelineKey, record.TimelineLSN, state.lastLSN+1)
		}
		if record.TimestampMS < state.lastTS {
			return fmt.Errorf("%w: key=%q timestamp=%d previous=%d", ErrTimestampOrder, record.TimelineKey, record.TimestampMS, state.lastTS)
		}
	}
	b.state[key] = timelineState{lastLSN: record.TimelineLSN, lastTS: record.TimestampMS}
	b.records[key] = append(b.records[key], cloneRecord(record))
	b.count++
	return nil
}

func (b *Builder) Records() int { return int(b.count) }

func (b *Builder) Seal() ([]byte, error) {
	if b.closed {
		return nil, ErrBuilderClosed
	}
	b.closed = true
	if b.count == 0 {
		return nil, fmt.Errorf("%w: empty pack", ErrInvalidPack)
	}

	keys := make([]string, 0, len(b.records))
	for key := range b.records {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	extents := make([]extentBuild, 0, len(keys))
	for _, key := range keys {
		built, err := b.buildTimelineExtents([]byte(key), b.records[key])
		if err != nil {
			return nil, err
		}
		extents = append(extents, built...)
	}

	out := make([]byte, PreambleSize)
	copy(out[0:4], packMagic[:])
	binary.BigEndian.PutUint16(out[4:6], Version)
	binary.BigEndian.PutUint16(out[6:8], PreambleSize)
	binary.BigEndian.PutUint16(out[8:10], uint16(b.opts.Codec))
	binary.BigEndian.PutUint32(out[12:16], b.identity.Shard)
	copy(out[16:48], b.identity.NamespaceHash[:])

	entries := make([]indexEntry, 0, len(extents))
	dataPages := make([]pageEntry, 0)
	pageCount := uint32(0)
	for start := 0; start < len(extents); {
		end := start
		rawSize := 0
		for end < len(extents) {
			next := ExtentPreambleSize + len(extents[end].key) + len(extents[end].body)
			if end > start && rawSize+next > b.opts.RawPageBytes {
				break
			}
			if next > MaxRawPageBytes {
				return nil, fmt.Errorf("%w: extent bytes=%d", ErrRecordTooLarge, next)
			}
			rawSize += next
			end++
			if rawSize >= b.opts.RawPageBytes {
				break
			}
		}

		raw := make([]byte, 0, rawSize)
		extentOffsets := make([]uint32, 0, end-start)
		for _, extent := range extents[start:end] {
			extentOffsets = append(extentOffsets, uint32(len(raw)))
			raw = appendExtent(raw, extent)
		}
		stored, err := encodePage(b.opts.Codec, raw)
		if err != nil {
			return nil, err
		}
		if len(stored) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: stored page bytes=%d", ErrRecordTooLarge, len(stored))
		}
		if len(stored) > MaxStoredPageBytes {
			return nil, fmt.Errorf("%w: stored page bytes=%d", ErrRecordTooLarge, len(stored))
		}
		pageOffset := uint64(len(out))
		pageHash := xxhash.Sum64(stored)
		preamble := make([]byte, PagePreambleSize)
		copy(preamble[0:4], pageMagic[:])
		binary.BigEndian.PutUint16(preamble[4:6], PagePreambleSize)
		binary.BigEndian.PutUint32(preamble[8:12], uint32(len(stored)))
		binary.BigEndian.PutUint32(preamble[12:16], uint32(len(raw)))
		binary.BigEndian.PutUint32(preamble[16:20], uint32(end-start))
		binary.BigEndian.PutUint64(preamble[24:32], pageHash)
		out = append(out, preamble...)
		out = append(out, stored...)
		dataPages = append(dataPages, pageEntry{
			offset:      pageOffset,
			storedSize:  uint32(len(stored)),
			rawSize:     uint32(len(raw)),
			extentCount: uint32(end - start),
			hash:        pageHash,
		})

		for i, extent := range extents[start:end] {
			length := ExtentPreambleSize + len(extent.key) + len(extent.body)
			entries = append(entries, indexEntry{
				key:          extent.key,
				keyHash:      hashTimelineKey(extent.key),
				pageNo:       pageCount,
				extentOffset: extentOffsets[i],
				extentLength: uint32(length),
				firstLSN:     extent.firstLSN,
				recordCount:  extent.recordCount,
			})
		}
		pageCount++
		start = end
	}

	indexPageBytes, err := selectIndexPageBytes(len(entries), len(dataPages), b.opts.IndexPageBytes)
	if err != nil {
		return nil, err
	}
	var indexPages []indexPageRef
	out, indexPages = appendIndexPages(out, entries, indexPageBytes)
	rootOffset := uint64(len(out))
	root := encodeIndexRoot(dataPages, indexPages)
	if len(root) > MaxIndexRootBytes {
		return nil, fmt.Errorf("%w: index root bytes=%d", ErrRecordTooLarge, len(root))
	}
	out = append(out, root...)

	trailer := make([]byte, TrailerSize)
	copy(trailer[0:4], trailerMagic[:])
	binary.BigEndian.PutUint16(trailer[4:6], Version)
	binary.BigEndian.PutUint16(trailer[6:8], TrailerSize)
	binary.BigEndian.PutUint16(trailer[8:10], uint16(b.opts.Codec))
	binary.BigEndian.PutUint32(trailer[12:16], uint32(len(dataPages)))
	binary.BigEndian.PutUint32(trailer[16:20], uint32(len(entries)))
	binary.BigEndian.PutUint32(trailer[20:24], b.count)
	binary.BigEndian.PutUint64(trailer[24:32], xxhash.Sum64(out))
	binary.BigEndian.PutUint64(trailer[32:40], xxhash.Sum64(root))
	binary.BigEndian.PutUint64(trailer[40:48], rootOffset)
	binary.BigEndian.PutUint64(trailer[48:56], uint64(len(root)))
	binary.BigEndian.PutUint64(trailer[56:64], uint64(len(out)+TrailerSize))
	copy(trailer[64:96], b.identity.NamespaceHash[:])
	binary.BigEndian.PutUint32(trailer[96:100], b.identity.Shard)
	binary.BigEndian.PutUint64(trailer[120:128], xxhash.Sum64(trailer[:120]))
	out = append(out, trailer...)
	return out, nil
}

func (b *Builder) buildTimelineExtents(key []byte, records []Record) ([]extentBuild, error) {
	extentBudget := b.opts.RawPageBytes - ExtentPreambleSize - len(key)
	if extentBudget <= 0 {
		return nil, fmt.Errorf("%w: key leaves no page space", ErrInvalidOptions)
	}
	extents := make([]extentBuild, 0, 1)
	var current extentBuild
	for _, record := range records {
		encoded, err := encodeRecord(record)
		if err != nil {
			return nil, err
		}
		if current.recordCount > 0 && len(current.body)+len(encoded) > extentBudget {
			extents = append(extents, current)
			current = extentBuild{}
		}
		if len(encoded)+ExtentPreambleSize+len(key) > MaxRawPageBytes {
			return nil, fmt.Errorf("%w: encoded record bytes=%d", ErrRecordTooLarge, len(encoded))
		}
		if current.recordCount == 0 {
			current.key = append([]byte(nil), key...)
			current.firstLSN = record.TimelineLSN
		}
		current.body = append(current.body, encoded...)
		current.recordCount++
	}
	if current.recordCount > 0 {
		extents = append(extents, current)
	}
	return extents, nil
}

func appendExtent(dst []byte, extent extentBuild) []byte {
	header := make([]byte, ExtentPreambleSize)
	copy(header[0:4], extentMagic[:])
	binary.BigEndian.PutUint16(header[4:6], ExtentPreambleSize)
	binary.BigEndian.PutUint16(header[6:8], uint16(len(extent.key)))
	binary.BigEndian.PutUint64(header[8:16], extent.firstLSN)
	binary.BigEndian.PutUint32(header[16:20], extent.recordCount)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(extent.body)))
	dst = append(dst, header...)
	dst = append(dst, extent.key...)
	dst = append(dst, extent.body...)
	return dst
}

func encodeRecord(record Record) ([]byte, error) {
	if len(record.Value) > MaxRecordValueBytes {
		return nil, fmt.Errorf("%w: value bytes=%d", ErrRecordTooLarge, len(record.Value))
	}
	headers, err := encodeHeaders(record.Headers)
	if err != nil {
		return nil, err
	}
	out := make([]byte, RecordHeaderSize, RecordHeaderSize+len(headers)+len(record.Value))
	binary.BigEndian.PutUint64(out[0:8], uint64(record.TimestampMS))
	binary.BigEndian.PutUint32(out[8:12], uint32(len(headers)))
	binary.BigEndian.PutUint32(out[12:16], uint32(len(record.Value)))
	out = append(out, headers...)
	out = append(out, record.Value...)
	return out, nil
}

func encodeHeaders(headers []Header) ([]byte, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	if len(headers) > MaxHeaders {
		return nil, fmt.Errorf("%w: headers=%d", ErrRecordTooLarge, len(headers))
	}
	var out bytes.Buffer
	var scratch [8]byte
	binary.BigEndian.PutUint16(scratch[0:2], uint16(len(headers)))
	out.Write(scratch[:4])
	for _, header := range headers {
		if len(header.Key) > MaxHeaderKeyBytes || len(header.Value) > MaxHeaderValueBytes {
			return nil, fmt.Errorf("%w: header key=%d value=%d", ErrRecordTooLarge, len(header.Key), len(header.Value))
		}
		binary.BigEndian.PutUint16(scratch[0:2], uint16(len(header.Key)))
		binary.BigEndian.PutUint32(scratch[4:8], uint32(len(header.Value)))
		out.Write(scratch[:])
		out.Write(header.Key)
		out.Write(header.Value)
		if out.Len() > MaxHeaderBytes {
			return nil, fmt.Errorf("%w: header bytes=%d", ErrRecordTooLarge, out.Len())
		}
	}
	return out.Bytes(), nil
}

func cloneRecord(record Record) Record {
	cloned := Record{
		TimelineKey: append([]byte(nil), record.TimelineKey...),
		TimelineLSN: record.TimelineLSN,
		TimestampMS: record.TimestampMS,
		Value:       append([]byte(nil), record.Value...),
		Headers:     make([]Header, len(record.Headers)),
	}
	for i, header := range record.Headers {
		cloned.Headers[i] = Header{
			Key:   append([]byte(nil), header.Key...),
			Value: append([]byte(nil), header.Value...),
		}
	}
	return cloned
}
