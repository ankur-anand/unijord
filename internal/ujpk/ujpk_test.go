package ujpk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/ankur-anand/unijord/internal/namespaceid"
	"github.com/cespare/xxhash/v2"
)

type requestedRange struct {
	offset uint64
	length uint64
}

type recordingRangeSource struct {
	data      []byte
	sizeCalls int
	reads     []requestedRange
}

func testPackIdentity() Identity {
	return Identity{NamespaceHash: namespaceid.Sum([]byte("tenant-a")), Shard: 7}
}

func (s *recordingRangeSource) Size(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.sizeCalls++
	return uint64(len(s.data)), nil
}

func (s *recordingRangeSource) ReadRange(ctx context.Context, offset, length uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset > uint64(len(s.data)) || length > uint64(len(s.data))-offset {
		return nil, fmt.Errorf("test source: range outside object")
	}
	s.reads = append(s.reads, requestedRange{offset: offset, length: length})
	return append([]byte(nil), s.data[offset:offset+length]...), nil
}

func (s *recordingRangeSource) bytesRead() uint64 {
	var total uint64
	for _, read := range s.reads {
		total += read.length
	}
	return total
}

func (s *recordingRangeSource) reset() {
	s.sizeCalls = 0
	s.reads = nil
}

func TestRoundTripReadsOnlySelectedTimelinePages(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecZstd, RawPageBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	const timelines = 200
	const recordsPerTimeline = 4
	for record := 0; record < recordsPerTimeline; record++ {
		for timeline := 0; timeline < timelines; timeline++ {
			key := []byte(fmt.Sprintf("timeline-%04d", timeline))
			if err := builder.Add(Record{
				TimelineKey: key,
				TimelineLSN: uint64(record),
				TimestampMS: int64(record),
				Headers:     []Header{{Key: []byte("kind"), Value: []byte("event")}},
				Value:       bytes.Repeat([]byte{byte(timeline)}, 1024),
			}); err != nil {
				t.Fatalf("Add(%q,%d): %v", key, record, err)
			}
		}
	}

	pack, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Open(pack)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Identity() != testPackIdentity() {
		t.Fatalf("Identity()=%+v want=%+v", reader.Identity(), testPackIdentity())
	}
	if reader.Pages() < 2 {
		t.Fatalf("Pages() = %d, need multiple pages to exercise selective decode", reader.Pages())
	}

	records, stats, err := reader.ReadTimeline([]byte("timeline-0100"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != recordsPerTimeline {
		t.Fatalf("records = %d, want %d", len(records), recordsPerTimeline)
	}
	if stats.PagesDecoded != 1 {
		t.Fatalf("pages decoded = %d, want 1 (pack has %d)", stats.PagesDecoded, reader.Pages())
	}
	for i, record := range records {
		if record.TimelineLSN != uint64(i) || record.TimestampMS != int64(i) {
			t.Fatalf("record[%d] position=(%d,%d)", i, record.TimelineLSN, record.TimestampMS)
		}
		if len(record.Value) != 1024 || record.Value[0] != 100 {
			t.Fatalf("record[%d] value mismatch", i)
		}
	}
}

func TestRangeReaderFetchesOnlyTrailerRootSelectedIndexAndDataPage(t *testing.T) {
	pack := buildTimelinePack(t, 200, 4, 1024, 16<<10)
	source := &recordingRangeSource{data: pack}

	reader, err := OpenRange(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if source.sizeCalls != 1 {
		t.Fatalf("Size() calls = %d, want 1", source.sizeCalls)
	}
	if len(source.reads) != 2 {
		t.Fatalf("metadata range reads = %d, want trailer + root", len(source.reads))
	}

	trailerOffset := uint64(len(pack) - TrailerSize)
	rootOffset := binary.BigEndian.Uint64(pack[len(pack)-TrailerSize+40 : len(pack)-TrailerSize+48])
	rootLength := binary.BigEndian.Uint64(pack[len(pack)-TrailerSize+48 : len(pack)-TrailerSize+56])
	if source.reads[0] != (requestedRange{offset: trailerOffset, length: TrailerSize}) {
		t.Fatalf("trailer read = %+v", source.reads[0])
	}
	if source.reads[1] != (requestedRange{offset: rootOffset, length: rootLength}) {
		t.Fatalf("root read = %+v", source.reads[1])
	}

	records, stats, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-0100"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || stats.IndexPagesRead != 1 || stats.PagesDecoded != 1 {
		t.Fatalf("records=%d index_pages=%d data_pages=%d, want 4 records from one page each", len(records), stats.IndexPagesRead, stats.PagesDecoded)
	}
	if len(source.reads) != 4 {
		t.Fatalf("total range reads = %d, want trailer + root + index page + data page", len(source.reads))
	}
	indexRead := source.reads[2]
	if !bytes.Equal(pack[indexRead.offset:indexRead.offset+4], indexPageMagic[:]) {
		t.Fatalf("third range is not an index page: %+v", indexRead)
	}
	pageRead := source.reads[3]
	if pageRead.offset < PreambleSize || pageRead.offset+pageRead.length > reader.indexPages[0].offset {
		t.Fatalf("page read = %+v outside page region", pageRead)
	}
	if source.bytesRead() >= uint64(len(pack)) {
		t.Fatalf("range reader fetched %d bytes from %d-byte pack", source.bytesRead(), len(pack))
	}
}

func TestRangeReaderRejectsCorruptIndexRoot(t *testing.T) {
	pack := buildTimelinePack(t, 20, 2, 128, 1024)
	trailerOffset := len(pack) - TrailerSize
	rootOffset := binary.BigEndian.Uint64(pack[trailerOffset+40 : trailerOffset+48])
	pack[rootOffset+IndexRootPreambleSize] ^= 0xff

	_, err := OpenRange(context.Background(), &recordingRangeSource{data: pack})
	if !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("OpenRange() error = %v, want ErrIntegrityMismatch", err)
	}
}

func TestRangeReaderRejectsCorruptSelectedIndexPage(t *testing.T) {
	pack := buildTimelinePack(t, 200, 2, 128, 1024)
	source := &recordingRangeSource{data: pack}
	reader, err := OpenRange(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	hash := hashTimelineKey([]byte("timeline-0100"))
	i := sort.Search(len(reader.indexPages), func(i int) bool {
		return bytes.Compare(reader.indexPages[i].lastHash[:], hash[:]) >= 0
	})
	if i == len(reader.indexPages) {
		t.Fatal("selected index page not found")
	}
	pack[reader.indexPages[i].offset+IndexPagePreambleSize] ^= 0xff

	_, _, err = reader.ReadTimelineContext(context.Background(), []byte("timeline-0100"), 0)
	if !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("ReadTimelineContext() error = %v, want ErrIntegrityMismatch", err)
	}
}

func TestRangeReaderRejectsCorruptSelectedPage(t *testing.T) {
	pack := buildTimelinePack(t, 1, 2, 128, 1024)
	pack[PreambleSize+PagePreambleSize] ^= 0xff

	reader, err := OpenRange(context.Background(), &recordingRangeSource{data: pack})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = reader.ReadTimelineContext(context.Background(), []byte("timeline-0000"), 0)
	if !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("ReadTimelineContext() error = %v, want ErrIntegrityMismatch", err)
	}
}

func TestRangeReaderHonorsCallerCancellation(t *testing.T) {
	pack := buildTimelinePack(t, 20, 2, 128, 1024)
	source := &recordingRangeSource{data: pack}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := OpenRange(ctx, source)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenRange() error = %v, want context.Canceled", err)
	}
	if source.sizeCalls != 0 || len(source.reads) != 0 {
		t.Fatalf("canceled open touched source: size=%d reads=%d", source.sizeCalls, len(source.reads))
	}
}

func TestRangeReaderReusesCachedIndexAndDataPages(t *testing.T) {
	pack := buildTimelinePack(t, 200, 4, 1024, 16<<10)
	source := &recordingRangeSource{data: pack}
	reader, err := OpenRange(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-0100"), 0); err != nil {
		t.Fatal(err)
	}
	source.reset()

	records, stats, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-0100"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || len(source.reads) != 0 || stats.IndexPagesRead != 0 || stats.PagesDecoded != 0 {
		t.Fatalf("warm read records=%d source_reads=%d index_pages=%d data_pages=%d", len(records), len(source.reads), stats.IndexPagesRead, stats.PagesDecoded)
	}
}

func TestLargeTimelineSpansPages(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecNone, RawPageBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := builder.Add(Record{
			TimelineKey: []byte("large"),
			TimelineLSN: uint64(50 + i),
			TimestampMS: int64(i),
			Value:       bytes.Repeat([]byte{byte(i)}, 200),
		}); err != nil {
			t.Fatal(err)
		}
	}
	pack, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Open(pack)
	if err != nil {
		t.Fatal(err)
	}
	records, stats, err := reader.ReadTimeline([]byte("large"), 57)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 13 ||
		records[0].TimelineLSN != 57 ||
		records[len(records)-1].TimelineLSN != 69 {
		t.Fatalf("range = %d records [%d,%d]", len(records), records[0].TimelineLSN, records[len(records)-1].TimelineLSN)
	}
	if stats.PagesDecoded < 2 {
		t.Fatalf("pages decoded = %d, want multiple", stats.PagesDecoded)
	}
}

func TestTimelineSpansRebuildMetadataAcrossExtents(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecNone, RawPageBytes: 512, IndexPageBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	for lsn := 0; lsn < 10; lsn++ {
		if err := builder.Add(Record{
			TimelineKey: []byte("large"), TimelineLSN: uint64(40 + lsn), TimestampMS: int64(lsn), Value: bytes.Repeat([]byte("x"), 160),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for lsn := 0; lsn < 2; lsn++ {
		if err := builder.Add(Record{TimelineKey: []byte("small"), TimelineLSN: uint64(lsn), TimestampMS: int64(lsn), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	body, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Open(body)
	if err != nil {
		t.Fatal(err)
	}
	spans, err := reader.TimelineSpans()
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 || string(spans[0].TimelineKey) != "large" || spans[0].FirstLSN != 40 || spans[0].LastLSN != 49 ||
		string(spans[1].TimelineKey) != "small" || spans[1].FirstLSN != 0 || spans[1].LastLSN != 1 {
		t.Fatalf("TimelineSpans()=%+v", spans)
	}
}

func TestTimelineHashCanSpanIndexPages(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecNone, RawPageBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	const recordCount = 341
	for i := 0; i < recordCount; i++ {
		if err := builder.Add(Record{
			TimelineKey: []byte("large"),
			TimelineLSN: uint64(i),
			TimestampMS: int64(i),
			Value:       bytes.Repeat([]byte{byte(i)}, 200),
		}); err != nil {
			t.Fatal(err)
		}
	}
	pack, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenRange(context.Background(), &recordingRangeSource{data: pack})
	if err != nil {
		t.Fatal(err)
	}
	records, stats, err := reader.ReadTimelineContext(context.Background(), []byte("large"), 0)
	if err != nil {
		t.Fatal(err)
	}
	wantIndexPages := (recordCount + 42 - 1) / 42
	if len(records) != recordCount || stats.IndexPagesRead != wantIndexPages {
		t.Fatalf("records=%d index_pages=%d, want %d records across %d index pages", len(records), stats.IndexPagesRead, recordCount, wantIndexPages)
	}
}

func TestRejectsTimelineGap(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(Record{TimelineKey: []byte("a"), TimelineLSN: 4}); err != nil {
		t.Fatal(err)
	}
	err = builder.Add(Record{TimelineKey: []byte("a"), TimelineLSN: 6})
	if !errors.Is(err, ErrTimelineOrder) {
		t.Fatalf("Add() error = %v, want ErrTimelineOrder", err)
	}
}

func TestBuilderRejectsZeroNamespaceIdentity(t *testing.T) {
	if _, err := NewBuilder(Identity{Shard: 7}, DefaultOptions()); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("NewBuilder(zero namespace) error=%v want=%v", err, ErrInvalidOptions)
	}
}

func TestOpenRejectsPreambleNamespaceDifferentFromTrailer(t *testing.T) {
	pack := buildTimelinePack(t, 1, 1, 8, 1024)
	pack[16] ^= 0xff
	trailer := pack[len(pack)-TrailerSize:]
	binary.BigEndian.PutUint64(trailer[24:32], xxhash.Sum64(pack[:len(pack)-TrailerSize]))
	binary.BigEndian.PutUint64(trailer[120:128], xxhash.Sum64(trailer[:120]))
	if _, err := Open(pack); !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("Open(cross-namespace preamble) error=%v want=%v", err, ErrInvalidPack)
	}
}

func TestOpenRangeRejectsZeroTrailerNamespace(t *testing.T) {
	pack := buildTimelinePack(t, 1, 1, 8, 1024)
	trailer := pack[len(pack)-TrailerSize:]
	clear(trailer[64:96])
	binary.BigEndian.PutUint64(trailer[120:128], xxhash.Sum64(trailer[:120]))
	if _, err := OpenRange(context.Background(), &recordingRangeSource{data: pack}); !errors.Is(err, ErrInvalidPack) {
		t.Fatalf("OpenRange(zero namespace) error=%v want=%v", err, ErrInvalidPack)
	}
}

func TestVersion1CompatibilityVector(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{
		Codec: CodecNone, RawPageBytes: 1024, IndexPageBytes: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []Record{
		{TimelineKey: []byte("timeline-a"), TimelineLSN: 4, TimestampMS: 10, Headers: []Header{{Key: []byte("kind"), Value: []byte("start")}}, Value: []byte("a4")},
		{TimelineKey: []byte("timeline-b"), TimelineLSN: 0, TimestampMS: 11, Value: []byte("b0")},
		{TimelineKey: []byte("timeline-a"), TimelineLSN: 5, TimestampMS: 12, Value: []byte("a5")},
	} {
		if err := builder.Add(item); err != nil {
			t.Fatal(err)
		}
	}
	body, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	const wantSize = 639
	const wantSHA256 = "238b552ffcee8ba8f4be8b9f86a4565d1488c018b96b5b9cc53c9d2a5edb0980"
	gotSHA256 := sha256.Sum256(body)
	if len(body) != wantSize || hex.EncodeToString(gotSHA256[:]) != wantSHA256 {
		t.Fatalf("version-1 vector size=%d sha256=%x", len(body), gotSHA256)
	}
}

func TestIndexPageGeometryIsValidatedAndApplied(t *testing.T) {
	for _, size := range []int{IndexPagePreambleSize + TimelineIndexEntrySize - 1, MaxIndexPageBytes + 1} {
		if _, err := NewBuilder(testPackIdentity(), Options{Codec: CodecZstd, RawPageBytes: 1024, IndexPageBytes: size}); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("NewBuilder(IndexPageBytes=%d) error = %v, want ErrInvalidOptions", size, err)
		}
	}

	pack := buildTimelinePackWithGeometry(t, 2000, 1, 64<<10, 2<<10, func(timeline, _ int) []byte {
		return bytes.Repeat([]byte{byte(timeline)}, 128)
	})
	reader, err := OpenRange(context.Background(), &recordingRangeSource{data: pack})
	if err != nil {
		t.Fatal(err)
	}
	wantPages := (2000 + 42 - 1) / 42
	if len(reader.indexPages) != wantPages {
		t.Fatalf("index pages = %d, want %d", len(reader.indexPages), wantPages)
	}
}

func TestAdaptiveIndexPageGeometry(t *testing.T) {
	tests := []struct {
		name       string
		extents    int
		wantTarget int
		wantPages  int
	}{
		{name: "one", extents: 1, wantTarget: 2 << 10, wantPages: 1},
		{name: "two-thousand", extents: 2_000, wantTarget: 2 << 10, wantPages: 48},
		{name: "twenty-thousand", extents: 20_000, wantTarget: 8 << 10, wantPages: 118},
		{name: "two-hundred-thousand", extents: 200_000, wantTarget: 32 << 10, wantPages: 294},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target, err := selectIndexPageBytes(test.extents, 1, AutoIndexPageBytes)
			if err != nil {
				t.Fatal(err)
			}
			if target != test.wantTarget {
				t.Fatalf("target = %d, want %d", target, test.wantTarget)
			}
			entriesPerPage := (target - IndexPagePreambleSize) / TimelineIndexEntrySize
			if pages := (test.extents + entriesPerPage - 1) / entriesPerPage; pages != test.wantPages {
				t.Fatalf("pages = %d, want %d", pages, test.wantPages)
			}
		})
	}
}

func TestAdaptiveIndexPageGeometryHonorsOverrideAndRootLimit(t *testing.T) {
	const override = 4 << 10
	target, err := selectIndexPageBytes(2_000, 1, override)
	if err != nil {
		t.Fatal(err)
	}
	if target != override {
		t.Fatalf("target = %d, want override %d", target, override)
	}

	tooManyDataPages := (MaxIndexRootBytes-IndexRootPreambleSize-IndexRootEntrySize)/DataPageTableEntrySize + 1
	if _, err := selectIndexPageBytes(1, tooManyDataPages, AutoIndexPageBytes); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("selectIndexPageBytes() error = %v, want ErrRecordTooLarge", err)
	}
}

func TestDefaultIndexPageGeometryIsAdaptive(t *testing.T) {
	build := func() []byte {
		return buildTimelinePackWithGeometry(t, 5_000, 1, 64<<10, AutoIndexPageBytes, func(_, _ int) []byte {
			return nil
		})
	}
	pack := build()
	if second := build(); !bytes.Equal(pack, second) {
		t.Fatal("adaptive geometry produced nondeterministic pack bytes")
	}
	reader, err := OpenRange(context.Background(), &recordingRangeSource{data: pack})
	if err != nil {
		t.Fatal(err)
	}
	const entriesPerPage = (4<<10 - IndexPagePreambleSize) / TimelineIndexEntrySize
	wantPages := (5_000 + entriesPerPage - 1) / entriesPerPage
	if len(reader.indexPages) != wantPages {
		t.Fatalf("index pages = %d, want %d", len(reader.indexPages), wantPages)
	}
	if got := reader.indexPages[0].length; got != IndexPagePreambleSize+entriesPerPage*TimelineIndexEntrySize {
		t.Fatalf("first index page bytes = %d, want 4 KiB adaptive geometry", got)
	}
	records, _, err := reader.ReadTimeline([]byte("timeline-2500"), 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("ReadTimeline() records=%d error=%v", len(records), err)
	}
}

func TestDetectsCorruption(t *testing.T) {
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecNone, RawPageBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(Record{TimelineKey: []byte("a"), TimelineLSN: 0, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	pack, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	pack[PreambleSize+PagePreambleSize] ^= 0xff
	if _, err := Open(pack); !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("Open() error = %v, want ErrIntegrityMismatch", err)
	}
}

func buildTimelinePack(t testing.TB, timelines, recordsPerTimeline, valueBytes, pageBytes int) []byte {
	return buildTimelinePackWithValues(t, timelines, recordsPerTimeline, pageBytes, func(timeline, _ int) []byte {
		return bytes.Repeat([]byte{byte(timeline)}, valueBytes)
	})
}

func buildTimelinePackWithValues(t testing.TB, timelines, recordsPerTimeline, pageBytes int, value func(timeline, record int) []byte) []byte {
	return buildTimelinePackWithGeometry(t, timelines, recordsPerTimeline, pageBytes, DefaultOptions().IndexPageBytes, value)
}

func buildTimelinePackWithGeometry(t testing.TB, timelines, recordsPerTimeline, pageBytes, indexPageBytes int, value func(timeline, record int) []byte) []byte {
	t.Helper()
	builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecZstd, RawPageBytes: pageBytes, IndexPageBytes: indexPageBytes})
	if err != nil {
		t.Fatal(err)
	}
	for record := 0; record < recordsPerTimeline; record++ {
		for timeline := 0; timeline < timelines; timeline++ {
			if err := builder.Add(Record{
				TimelineKey: []byte(fmt.Sprintf("timeline-%04d", timeline)),
				TimelineLSN: uint64(record),
				TimestampMS: int64(record),
				Value:       value(timeline, record),
			}); err != nil {
				t.Fatalf("Add(%d,%d): %v", timeline, record, err)
			}
		}
	}
	pack, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return pack
}

func deterministicIncompressibleValue(timeline, record, size int) []byte {
	value := make([]byte, size)
	state := uint64(timeline+1)<<32 | uint64(record+1)
	for i := range value {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		value[i] = byte(state >> 24)
	}
	return value
}

func BenchmarkTimelineReadByPageSize(b *testing.B) {
	for _, pageBytes := range []int{64 << 10, 128 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("page-%dKiB", pageBytes>>10), func(b *testing.B) {
			builder, err := NewBuilder(testPackIdentity(), Options{Codec: CodecZstd, RawPageBytes: pageBytes})
			if err != nil {
				b.Fatal(err)
			}
			for record := 0; record < 4; record++ {
				for timeline := 0; timeline < 2000; timeline++ {
					if err := builder.Add(Record{
						TimelineKey: []byte(fmt.Sprintf("timeline-%05d", timeline)),
						TimelineLSN: uint64(record),
						TimestampMS: int64(record),
						Value:       bytes.Repeat([]byte{byte(timeline)}, 1024),
					}); err != nil {
						b.Fatal(err)
					}
				}
			}
			pack, err := builder.Seal()
			if err != nil {
				b.Fatal(err)
			}
			reader, err := Open(pack)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.pageCache = make(map[uint64][]byte)
				records, stats, err := reader.ReadTimeline([]byte("timeline-01000"), 0)
				if err != nil || len(records) != 4 {
					b.Fatalf("ReadTimeline() records=%d error=%v", len(records), err)
				}
				b.ReportMetric(float64(stats.StoredBytes), "stored-read/op")
				b.ReportMetric(float64(stats.RawBytes), "raw-read/op")
			}
			b.ReportMetric(float64(len(pack)), "pack-bytes")
		})
	}
}

func BenchmarkColdTimelineRangeReadByPageSize(b *testing.B) {
	for _, pageBytes := range []int{64 << 10, 128 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("page-%dKiB", pageBytes>>10), func(b *testing.B) {
			pack := buildTimelinePackWithGeometry(b, 2000, 4, pageBytes, 16<<10, func(timeline, _ int) []byte {
				return bytes.Repeat([]byte{byte(timeline)}, 1024)
			})
			var source *recordingRangeSource
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source = &recordingRangeSource{data: pack}
				reader, err := OpenRange(context.Background(), source)
				if err != nil {
					b.Fatal(err)
				}
				records, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-1000"), 0)
				if err != nil || len(records) != 4 {
					b.Fatalf("ReadTimelineContext() records=%d error=%v", len(records), err)
				}
			}
			b.ReportMetric(float64(source.sizeCalls), "size-calls/op")
			b.ReportMetric(float64(len(source.reads)), "range-reads/op")
			b.ReportMetric(float64(source.bytesRead()), "range-bytes/op")
			b.ReportMetric(float64(source.reads[1].length), "root-bytes/op")
			b.ReportMetric(float64(source.reads[2].length), "index-page-bytes/op")
			b.ReportMetric(float64(source.reads[3].length), "data-page-bytes/op")
			b.ReportMetric(float64(len(pack)), "pack-bytes")
		})
	}
}

func BenchmarkColdTimelineRangeReadIncompressibleByPageSize(b *testing.B) {
	for _, pageBytes := range []int{64 << 10, 128 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("page-%dKiB", pageBytes>>10), func(b *testing.B) {
			pack := buildTimelinePackWithGeometry(b, 2000, 4, pageBytes, 16<<10, func(timeline, record int) []byte {
				return deterministicIncompressibleValue(timeline, record, 1024)
			})
			var source *recordingRangeSource
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source = &recordingRangeSource{data: pack}
				reader, err := OpenRange(context.Background(), source)
				if err != nil {
					b.Fatal(err)
				}
				records, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-1000"), 0)
				if err != nil || len(records) != 4 {
					b.Fatalf("ReadTimelineContext() records=%d error=%v", len(records), err)
				}
			}
			b.ReportMetric(float64(source.sizeCalls), "size-calls/op")
			b.ReportMetric(float64(len(source.reads)), "range-reads/op")
			b.ReportMetric(float64(source.bytesRead()), "range-bytes/op")
			b.ReportMetric(float64(source.reads[1].length), "root-bytes/op")
			b.ReportMetric(float64(source.reads[2].length), "index-page-bytes/op")
			b.ReportMetric(float64(source.reads[3].length), "data-page-bytes/op")
			b.ReportMetric(float64(len(pack)), "pack-bytes")
		})
	}
}

func BenchmarkColdTimelineRangeReadByIndexPageSize(b *testing.B) {
	for _, indexPageBytes := range []int{2 << 10, 4 << 10, 8 << 10, 16 << 10} {
		b.Run(fmt.Sprintf("index-%dKiB", indexPageBytes>>10), func(b *testing.B) {
			pack := buildTimelinePackWithGeometry(b, 2000, 4, 64<<10, indexPageBytes, func(timeline, _ int) []byte {
				return bytes.Repeat([]byte{byte(timeline)}, 1024)
			})
			var source *recordingRangeSource
			var reader *Reader
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source = &recordingRangeSource{data: pack}
				var err error
				reader, err = OpenRange(context.Background(), source)
				if err != nil {
					b.Fatal(err)
				}
				records, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-1000"), 0)
				if err != nil || len(records) != 4 {
					b.Fatalf("ReadTimelineContext() records=%d error=%v", len(records), err)
				}
			}
			b.ReportMetric(float64(len(reader.indexPages)), "index-pages")
			b.ReportMetric(float64(source.reads[1].length), "root-bytes/op")
			b.ReportMetric(float64(source.reads[2].length), "index-page-bytes/op")
			b.ReportMetric(float64(source.reads[3].length), "data-page-bytes/op")
			b.ReportMetric(float64(source.bytesRead()), "range-bytes/op")
			b.ReportMetric(float64(len(source.reads)), "range-reads/op")
			b.ReportMetric(float64(len(pack)), "pack-bytes")
		})
	}
}

func BenchmarkAdaptiveIndexPageSize(b *testing.B) {
	for _, timelines := range []int{2_000, 20_000, 200_000} {
		for _, geometry := range []struct {
			name           string
			indexPageBytes int
		}{
			{name: "fixed-2KiB", indexPageBytes: 2 << 10},
			{name: "adaptive", indexPageBytes: AutoIndexPageBytes},
		} {
			b.Run(fmt.Sprintf("timelines-%d/%s", timelines, geometry.name), func(b *testing.B) {
				pack := buildTimelinePackWithGeometry(b, timelines, 1, 64<<10, geometry.indexPageBytes, func(_, _ int) []byte {
					return nil
				})
				targetKey := []byte(fmt.Sprintf("timeline-%04d", timelines/2))
				var source *recordingRangeSource
				var reader *Reader
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					source = &recordingRangeSource{data: pack}
					var err error
					reader, err = OpenRange(context.Background(), source)
					if err != nil {
						b.Fatal(err)
					}
					records, _, err := reader.ReadTimelineContext(context.Background(), targetKey, 0)
					if err != nil || len(records) != 1 {
						b.Fatalf("ReadTimelineContext() records=%d error=%v", len(records), err)
					}
				}
				if len(source.reads) != 4 {
					b.Fatalf("range reads = %d, want trailer + root + index + data", len(source.reads))
				}
				b.ReportMetric(float64(len(reader.indexPages)), "index-pages")
				b.ReportMetric(float64(source.reads[1].length), "root-bytes/op")
				b.ReportMetric(float64(source.reads[2].length), "index-page-bytes/op")
				b.ReportMetric(float64(source.reads[1].length+source.reads[2].length), "cold-index-bytes/op")
				b.ReportMetric(float64(source.reads[3].length), "data-page-bytes/op")
				b.ReportMetric(float64(source.bytesRead()), "range-bytes/op")
				b.ReportMetric(float64(len(source.reads)), "range-reads/op")
				b.ReportMetric(float64(len(pack)), "pack-bytes")
			})
		}
	}
}

func BenchmarkWarmTimelineReadByIndexPageSize(b *testing.B) {
	for _, indexPageBytes := range []int{2 << 10, 4 << 10, 8 << 10, 16 << 10} {
		b.Run(fmt.Sprintf("index-%dKiB", indexPageBytes>>10), func(b *testing.B) {
			pack := buildTimelinePackWithGeometry(b, 2000, 4, 64<<10, indexPageBytes, func(timeline, _ int) []byte {
				return bytes.Repeat([]byte{byte(timeline)}, 1024)
			})
			source := &recordingRangeSource{data: pack}
			reader, err := OpenRange(context.Background(), source)
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-1000"), 0); err != nil {
				b.Fatal(err)
			}
			source.reset()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				records, _, err := reader.ReadTimelineContext(context.Background(), []byte("timeline-1000"), 0)
				if err != nil || len(records) != 4 {
					b.Fatalf("ReadTimelineContext() records=%d error=%v", len(records), err)
				}
			}
			b.ReportMetric(float64(len(source.reads))/float64(b.N), "range-reads/op")
			b.ReportMetric(float64(source.bytesRead())/float64(b.N), "range-bytes/op")
		})
	}
}
