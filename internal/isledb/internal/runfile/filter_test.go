package runfile

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
)

type filterHashVector struct {
	Name              string   `json:"name"`
	TimelineHex       string   `json:"timeline_hex"`
	HashInputHex      string   `json:"hash_input_hex"`
	ExpectedSHA256    string   `json:"expected_sha256"`
	ExpectedH1Hex     string   `json:"expected_h1_hex"`
	ExpectedH2OddHex  string   `json:"expected_h2_odd_hex"`
	LineCount         uint32   `json:"line_count"`
	Probes            uint8    `json:"probes"`
	ExpectedLine      uint32   `json:"expected_line"`
	ExpectedPage      uint32   `json:"expected_page"`
	ExpectedProbeBits []uint16 `json:"expected_probe_bits"`
}

type filterPageVector struct {
	Page                   uint32 `json:"page"`
	OffsetRelativeToRegion uint64 `json:"offset_relative_to_region"`
	DataLength             uint64 `json:"data_length"`
	RecordLength           uint64 `json:"record_length"`
	ExpectedDataSHA256     string `json:"expected_data_sha256"`
	ExpectedCRCInputSHA256 string `json:"expected_crc_input_sha256"`
	ExpectedCRC32C         string `json:"expected_crc32c"`
	ExpectedRecordSHA256   string `json:"expected_record_sha256"`
}

type filterQueryVector struct {
	Name                   string   `json:"name"`
	TimelineHex            string   `json:"timeline_hex"`
	ExpectedLine           uint32   `json:"expected_line"`
	ExpectedPage           uint32   `json:"expected_page"`
	ExpectedLineByteOffset *uint32  `json:"expected_line_byte_offset"`
	ExpectedProbeBits      []uint16 `json:"expected_probe_bits"`
	ExpectedMayContain     bool     `json:"expected_may_contain"`
}

type timelineFilterVector struct {
	Name         string `json:"name"`
	HeaderFields struct {
		BitsPerKey uint16 `json:"bits_per_key"`
		KeyCount   uint64 `json:"key_count"`
		RunIDHex   string `json:"run_id_hex"`
	} `json:"header_fields"`
	ExpectedHeaderHex    string              `json:"expected_header_hex"`
	ExpectedHeaderSHA256 string              `json:"expected_header_sha256"`
	PageRecords          []filterPageVector  `json:"page_records"`
	ExpectedRegionLength uint64              `json:"expected_region_length"`
	ExpectedRegionSHA256 string              `json:"expected_region_sha256"`
	Queries              []filterQueryVector `json:"queries"`
}

type filterCompatibilityManifest struct {
	HashVectors   []filterHashVector     `json:"hash_vectors"`
	FilterVectors []timelineFilterVector `json:"filter_vectors"`
}

func TestTimelineFilterHashVectors(t *testing.T) {
	manifest := loadFilterCompatibilityManifest(t)
	for _, vector := range manifest.HashVectors {
		if vector.TimelineHex == "" {
			continue
		}
		t.Run(vector.Name, func(t *testing.T) {
			timeline := decodeHex(t, vector.TimelineHex)
			digest, h1, h2, err := timelineFilterToken(timeline)
			if err != nil {
				t.Fatal(err)
			}
			if got := encodeHex(digest[:]); got != vector.ExpectedSHA256 {
				t.Fatalf("digest=%s, want %s", got, vector.ExpectedSHA256)
			}
			if got := fmt.Sprintf("%08x", h1); got != vector.ExpectedH1Hex {
				t.Fatalf("h1=%s, want %s", got, vector.ExpectedH1Hex)
			}
			if got := fmt.Sprintf("%08x", h2); got != vector.ExpectedH2OddHex {
				t.Fatalf("h2=%s, want %s", got, vector.ExpectedH2OddHex)
			}
			if got := encodeHex(timelineFilterHashInput(timeline)); got != vector.HashInputHex {
				t.Fatalf("hash input=%s, want %s", got, vector.HashInputHex)
			}

			header := FilterHeader{
				Probes:       vector.Probes,
				LineCount:    vector.LineCount,
				LineBytes:    TimelineFilterLineBytes,
				LinesPerPage: TimelineFilterLinesPerPage,
			}
			location, err := locateTimeline(header, timeline)
			if err != nil {
				t.Fatal(err)
			}
			if location.line != vector.ExpectedLine || location.page != vector.ExpectedPage {
				t.Fatalf("location=(line %d,page %d), want (%d,%d)", location.line, location.page, vector.ExpectedLine, vector.ExpectedPage)
			}
			if got := locationProbeBits(location, vector.Probes); !reflect.DeepEqual(got, vector.ExpectedProbeBits) {
				t.Fatalf("probe bits=%v, want %v", got, vector.ExpectedProbeBits)
			}
		})
	}
}

func TestTimelineFilterCompatibilityVector(t *testing.T) {
	vector := onlyFilterVector(t)
	runIDBytes := decodeHex(t, vector.HeaderFields.RunIDHex)
	var runID [RunIDBytes]byte
	copy(runID[:], runIDBytes)
	timelines := generatedFilterTimelines(int(vector.HeaderFields.KeyCount))
	encoded, header, err := BuildTimelineFilter(runID, timelines, FilterOptions{BitsPerKey: vector.HeaderFields.BitsPerKey})
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := decodeHex(t, vector.ExpectedHeaderHex)
	if !bytes.Equal(encoded[:TimelineFilterHeaderBytes], expectedHeader) {
		t.Fatal("filter header does not reproduce frozen compatibility bytes")
	}
	if got := sha256Hex(expectedHeader); got != vector.ExpectedHeaderSHA256 {
		t.Fatalf("header SHA-256=%s, want %s", got, vector.ExpectedHeaderSHA256)
	}
	decodedHeader, err := UnmarshalFilterHeader(expectedHeader)
	if err != nil {
		t.Fatal(err)
	}
	if decodedHeader != header {
		t.Fatalf("decoded header=%+v, want %+v", decodedHeader, header)
	}
	if uint64(len(encoded)) != vector.ExpectedRegionLength {
		t.Fatalf("filter length=%d, want %d", len(encoded), vector.ExpectedRegionLength)
	}
	if got := sha256Hex(encoded); got != vector.ExpectedRegionSHA256 {
		t.Fatalf("filter SHA-256=%s, want %s", got, vector.ExpectedRegionSHA256)
	}

	for _, pageVector := range vector.PageRecords {
		pageVector := pageVector
		t.Run(fmt.Sprintf("page-%d", pageVector.Page), func(t *testing.T) {
			record, data := filterPageRecord(t, encoded, header, pageVector.Page)
			offset, _ := filterPageRelativeOffset(pageVector.Page)
			if offset != pageVector.OffsetRelativeToRegion || uint64(len(data)) != pageVector.DataLength || uint64(len(record)) != pageVector.RecordLength {
				t.Fatalf("page shape offset=%d data=%d record=%d", offset, len(data), len(record))
			}
			if got := sha256Hex(data); got != pageVector.ExpectedDataSHA256 {
				t.Fatalf("page data SHA-256=%s, want %s", got, pageVector.ExpectedDataSHA256)
			}
			crcInput := timelineFilterPageCRCInput(encoded[:TimelineFilterHeaderBytes], pageVector.Page, data)
			if got := sha256Hex(crcInput); got != pageVector.ExpectedCRCInputSHA256 {
				t.Fatalf("CRC input SHA-256=%s, want %s", got, pageVector.ExpectedCRCInputSHA256)
			}
			if got := encodeHex(record[len(data):]); got != pageVector.ExpectedCRC32C {
				t.Fatalf("CRC-32C=%s, want %s", got, pageVector.ExpectedCRC32C)
			}
			if got := sha256Hex(record); got != pageVector.ExpectedRecordSHA256 {
				t.Fatalf("page record SHA-256=%s, want %s", got, pageVector.ExpectedRecordSHA256)
			}
		})
	}

	ref := testFilterRef(header, 176, uint64(len(encoded)), 5027)
	for _, query := range vector.Queries {
		query := query
		t.Run(query.Name, func(t *testing.T) {
			timeline := decodeHex(t, query.TimelineHex)
			location, err := locateTimeline(header, timeline)
			if err != nil {
				t.Fatal(err)
			}
			if location.line != query.ExpectedLine || location.page != query.ExpectedPage {
				t.Fatalf("location=(line %d,page %d), want (%d,%d)", location.line, location.page, query.ExpectedLine, query.ExpectedPage)
			}
			if query.ExpectedLineByteOffset != nil && location.lineByteOffset != *query.ExpectedLineByteOffset {
				t.Fatalf("line byte offset=%d, want %d", location.lineByteOffset, *query.ExpectedLineByteOffset)
			}
			if got := locationProbeBits(location, header.Probes); !reflect.DeepEqual(got, query.ExpectedProbeBits) {
				t.Fatalf("probe bits=%v, want %v", got, query.ExpectedProbeBits)
			}
			request, err := PlanFilterPage(ref, timeline)
			if err != nil {
				t.Fatal(err)
			}
			pageVector := vector.PageRecords[request.Page]
			if request.Offset != int64(ref.Region.Offset+pageVector.OffsetRelativeToRegion) || request.Length != int64(pageVector.RecordLength) {
				t.Fatalf("request=%+v, want offset=%d length=%d", request, ref.Region.Offset+pageVector.OffsetRelativeToRegion, pageVector.RecordLength)
			}
			record, _ := filterPageRecord(t, encoded, header, request.Page)
			mayContain, err := CheckFilterPage(ref, timeline, record)
			if err != nil {
				t.Fatal(err)
			}
			if mayContain != query.ExpectedMayContain {
				t.Fatalf("mayContain=%v, want %v", mayContain, query.ExpectedMayContain)
			}
		})
	}

	for _, timeline := range timelines {
		if !checkEncodedFilter(t, ref, encoded, timeline) {
			t.Fatalf("false negative for %q", timeline)
		}
	}
}

func TestTimelineFilterGeometryBoundaries(t *testing.T) {
	runID := testRunID()
	oneLine, err := NewFilterHeader(runID, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if oneLine.BitsPerKey != DefaultTimelineFilterBitsPerKey || oneLine.Probes != 6 || oneLine.LineCount != 1 || oneLine.PageCount != 1 {
		t.Fatalf("one-line geometry=%+v", oneLine)
	}
	if length, err := oneLine.EncodedLength(); err != nil || length != 132 {
		t.Fatalf("one-line length=%d error=%v, want 132", length, err)
	}

	fullPage, err := NewFilterHeader(runID, 3276, 10)
	if err != nil {
		t.Fatal(err)
	}
	if fullPage.LineCount != 64 || fullPage.PageCount != 1 {
		t.Fatalf("full-page geometry=%+v", fullPage)
	}
	if length, _ := fullPage.EncodedLength(); length != 4164 {
		t.Fatalf("full-page length=%d, want 4164", length)
	}

	twoPages, err := NewFilterHeader(runID, 3277, 10)
	if err != nil {
		t.Fatal(err)
	}
	if twoPages.LineCount != 65 || twoPages.PageCount != 2 {
		t.Fatalf("two-page geometry=%+v", twoPages)
	}
	if length, _ := twoPages.EncodedLength(); length != 4232 {
		t.Fatalf("two-page length=%d, want 4232", length)
	}
	if finalLength, ok := filterPageDataLength(twoPages, 1); !ok || finalLength != 64 {
		t.Fatalf("final page data length=(%d,%v), want (64,true)", finalLength, ok)
	}

	maximumKeys := MaxTimelineFilterLines * 16
	maximum, err := NewFilterHeader(runID, maximumKeys, MaxTimelineFilterBitsPerKey)
	if err != nil {
		t.Fatal(err)
	}
	if maximum.Probes != MaxTimelineFilterProbes || uint64(maximum.LineCount) != MaxTimelineFilterLines || maximum.PageCount != 67_108_864 {
		t.Fatalf("maximum geometry=%+v", maximum)
	}
	if length, _ := maximum.EncodedLength(); length != 275_146_342_400 {
		t.Fatalf("maximum filter length=%d, want 275146342400", length)
	}
	maximumHeaderBytes, err := MarshalFilterHeader(maximum)
	if err != nil {
		t.Fatal(err)
	}
	decodedMaximum, err := UnmarshalFilterHeader(maximumHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	if decodedMaximum != maximum {
		t.Fatal("maximum filter header did not round trip")
	}
	if finalLength, ok := filterPageDataLength(maximum, maximum.PageCount-1); !ok || finalLength != 4032 {
		t.Fatalf("maximum final page data length=(%d,%v), want (4032,true)", finalLength, ok)
	}
	lastOffset, ok := filterPageRelativeOffset(maximum.PageCount - 1)
	if !ok {
		t.Fatal("maximum final page offset overflowed")
	}
	if lastOffset+4032+TimelineFilterPageChecksumBytes != 275_146_342_400 {
		t.Fatalf("maximum final page end=%d", lastOffset+4032+TimelineFilterPageChecksumBytes)
	}
	maximumRef := testFilterRef(maximum, PreambleBytes, 275_146_342_400, PreambleBytes+275_146_342_400)
	if request, err := PlanFilterPage(maximumRef, []byte("maximum-geometry-query")); err != nil || request.Length < TimelineFilterLineBytes+TimelineFilterPageChecksumBytes || request.Length > TimelineFilterPageDataBytes+TimelineFilterPageChecksumBytes {
		t.Fatalf("maximum filter request=%+v error=%v", request, err)
	}
	if _, err := NewFilterHeader(runID, maximumKeys+1, MaxTimelineFilterBitsPerKey); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("over-maximum error=%v, want too large", err)
	}
	if _, err := NewFilterHeader(runID, math.MaxUint64, MaxTimelineFilterBitsPerKey); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("overflow error=%v, want too large", err)
	}
	minimumProbes, err := NewFilterHeader(runID, 1, MinTimelineFilterBitsPerKey)
	if err != nil || minimumProbes.Probes != 1 {
		t.Fatalf("minimum probes=%d error=%v", minimumProbes.Probes, err)
	}
}

func TestTimelineFilterHeaderRejectsMutations(t *testing.T) {
	vector := onlyFilterVector(t)
	valid := decodeHex(t, vector.ExpectedHeaderHex)
	tests := []struct {
		name   string
		want   error
		mutate func([]byte)
	}{
		{name: "magic", want: ErrCorruptRun, mutate: func(data []byte) { data[0] ^= 1 }},
		{name: "version", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[4:6], 2) }},
		{name: "header bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[6:8], 63) }},
		{name: "algorithm", want: ErrUnsupportedRunVersion, mutate: func(data []byte) { data[8] = 2 }},
		{name: "probes", want: ErrCorruptRun, mutate: func(data []byte) { data[9]++ }},
		{name: "bits per key zero", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[10:12], 0) }},
		{name: "bits per key high", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[10:12], MaxTimelineFilterBitsPerKey+1) }},
		{name: "flags", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[12:16], 1) }},
		{name: "key count", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint64(data[16:24], 0) }},
		{name: "line count", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[24:28], 64) }},
		{name: "page count", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[28:32], 1) }},
		{name: "line bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[32:34], TimelineFilterLineBytes-1) }},
		{name: "lines per page", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint16(data[34:36], TimelineFilterLinesPerPage-1) }},
		{name: "page data bytes", want: ErrCorruptRun, mutate: func(data []byte) { binary.BigEndian.PutUint32(data[36:40], TimelineFilterPageDataBytes-1) }},
		{name: "run ID", want: ErrCorruptRun, mutate: func(data []byte) { clear(data[40:56]) }},
		{name: "reserved", want: ErrCorruptRun, mutate: func(data []byte) { data[56] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if _, err := UnmarshalFilterHeader(data); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
	for _, data := range [][]byte{valid[:len(valid)-1], append(bytes.Clone(valid), 0)} {
		if _, err := UnmarshalFilterHeader(data); !errors.Is(err, ErrCorruptRun) {
			t.Fatalf("length %d error=%v, want corruption", len(data), err)
		}
	}
}

func TestBuildTimelineFilterValidationAndDeduplication(t *testing.T) {
	runID := testRunID()
	first := []byte("first")
	second := []byte("second")
	withDuplicates, duplicateHeader, err := BuildTimelineFilter(runID, [][]byte{first, second, first, second}, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	withoutDuplicates, uniqueHeader, err := BuildTimelineFilter(runID, [][]byte{second, first}, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if duplicateHeader.KeyCount != 2 || duplicateHeader != uniqueHeader || !bytes.Equal(withDuplicates, withoutDuplicates) {
		t.Fatal("duplicate timelines changed canonical filter bytes or key count")
	}
	clear(first)
	clear(second)
	if !bytes.Equal(withDuplicates, withoutDuplicates) {
		t.Fatal("filter bytes alias caller timelines")
	}

	if _, _, err := BuildTimelineFilter(runID, nil, FilterOptions{}); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("empty input error=%v, want invalid run", err)
	}
	if _, _, err := BuildTimelineFilter(runID, [][]byte{{}}, FilterOptions{}); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("empty timeline error=%v, want invalid run", err)
	}
	if _, _, err := BuildTimelineFilter(runID, [][]byte{make([]byte, MaxTimelineBytes+1)}, FilterOptions{}); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("long timeline error=%v, want too large", err)
	}
	if _, _, err := BuildTimelineFilter(runID, [][]byte{{1}}, FilterOptions{BitsPerKey: MaxTimelineFilterBitsPerKey + 1}); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("bits per key error=%v, want invalid run", err)
	}
	if _, _, err := BuildTimelineFilter([RunIDBytes]byte{}, [][]byte{{1}}, FilterOptions{}); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("zero run ID error=%v, want invalid run", err)
	}
}

func TestFilterPageRejectsSwapsAndCorruption(t *testing.T) {
	runID := testRunID()
	timelines := generatedFilterTimelines(7_000)
	encoded, header, err := BuildTimelineFilter(runID, timelines, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if header.PageCount < 3 {
		t.Fatalf("page count=%d, want at least 3", header.PageCount)
	}
	ref := testFilterRef(header, 128, uint64(len(encoded)), 128+uint64(len(encoded)))
	pageZeroTimeline := findTimelineForPage(t, header, timelines, 0)
	pageZeroRecord, _ := filterPageRecord(t, encoded, header, 0)
	pageOneRecord, _ := filterPageRecord(t, encoded, header, 1)
	if len(pageZeroRecord) != len(pageOneRecord) {
		t.Fatal("first two page records are not both full")
	}

	tests := []struct {
		name      string
		candidate FilterRef
		page      []byte
	}{
		{name: "swapped full page", candidate: ref, page: pageOneRecord},
		{name: "short page", candidate: ref, page: pageZeroRecord[:len(pageZeroRecord)-1]},
		{name: "long page", candidate: ref, page: append(bytes.Clone(pageZeroRecord), 0)},
		{name: "bad data", candidate: ref, page: mutateClone(pageZeroRecord, 0)},
		{name: "bad CRC", candidate: ref, page: mutateClone(pageZeroRecord, len(pageZeroRecord)-1)},
	}
	swappedRunID := ref
	swappedRunID.Header.RunID[0] ^= 1
	tests = append(tests, struct {
		name      string
		candidate FilterRef
		page      []byte
	}{name: "swapped run ID", candidate: swappedRunID, page: pageZeroRecord})
	changedGeometry := ref
	changedGeometry.Header, err = NewFilterHeader(runID, header.KeyCount, 11)
	if err != nil {
		t.Fatal(err)
	}
	changedGeometry.Region.Length, err = changedGeometry.Header.EncodedLength()
	if err != nil {
		t.Fatal(err)
	}
	changedGeometry.Region.EntryCount = changedGeometry.Header.KeyCount
	changedGeometry.ObjectSize = changedGeometry.Region.Offset + changedGeometry.Region.Length
	tests = append(tests, struct {
		name      string
		candidate FilterRef
		page      []byte
	}{name: "changed geometry", candidate: changedGeometry, page: pageZeroRecord})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if mayContain, err := CheckFilterPage(test.candidate, pageZeroTimeline, test.page); mayContain || !errors.Is(err, ErrCorruptRun) {
				t.Fatalf("mayContain=%v error=%v, want corruption", mayContain, err)
			}
		})
	}
}

func TestFilterTransportFailureIsNotCorruption(t *testing.T) {
	runID := testRunID()
	encoded, header, err := BuildTimelineFilter(runID, [][]byte{[]byte("present")}, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := testFilterRef(header, 128, uint64(len(encoded)), 128+uint64(len(encoded)))
	transport := errors.New("temporary object-store failure")
	mayContain, err := ReadFilterMembership(ref, []byte("present"), func(FilterPageRequest) ([]byte, error) {
		return nil, transport
	})
	if mayContain || !errors.Is(err, ErrFilterUnavailable) || !errors.Is(err, transport) || errors.Is(err, ErrCorruptRun) {
		t.Fatalf("transport result mayContain=%v error=%v", mayContain, err)
	}

	mayContain, err = ReadFilterMembership(ref, []byte("present"), func(FilterPageRequest) ([]byte, error) {
		return []byte{1}, nil
	})
	if mayContain || !errors.Is(err, ErrCorruptRun) || errors.Is(err, ErrFilterUnavailable) {
		t.Fatalf("short successful read mayContain=%v error=%v", mayContain, err)
	}
}

func TestFilterRefRejectsInvalidGeometryAndBounds(t *testing.T) {
	runID := testRunID()
	header, err := NewFilterHeader(runID, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	length, err := header.EncodedLength()
	if err != nil {
		t.Fatal(err)
	}
	valid := testFilterRef(header, 128, length, 128+length)
	tests := []struct {
		name   string
		want   error
		mutate func(*FilterRef)
	}{
		{name: "probes", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Header.Probes++ }},
		{name: "line count", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Header.LineCount++ }},
		{name: "region kind", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.Kind = RegionKindHeadsSST }},
		{name: "required", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.Required = true }},
		{name: "encoding", want: ErrUnsupportedRunVersion, mutate: func(ref *FilterRef) { ref.Region.Encoding++ }},
		{name: "region length", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.Length++ }},
		{name: "entry count", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.EntryCount++ }},
		{name: "sequence", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.SeqLo = 1 }},
		{name: "key bounds", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.MinKey = []byte{1} }},
		{name: "content hash", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.ContentHash = [SHA256Bytes]byte{} }},
		{name: "before preamble", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.Offset = PreambleBytes - 1 }},
		{name: "unaligned", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.Region.Offset++ }},
		{name: "outside object", want: ErrCorruptRun, mutate: func(ref *FilterRef) { ref.ObjectSize-- }},
		{name: "range overflow", want: ErrCorruptRun, mutate: func(ref *FilterRef) {
			ref.Region.Offset = math.MaxUint64 - 7
			ref.ObjectSize = MaxRunObjectBytes
		}},
		{name: "large object", want: ErrRunTooLarge, mutate: func(ref *FilterRef) { ref.ObjectSize = MaxRunObjectBytes + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if _, err := PlanFilterPage(candidate, []byte("query")); !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
	if _, err := PlanFilterPage(valid, nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("empty timeline error=%v, want invalid run", err)
	}
	if _, err := PlanFilterPage(valid, make([]byte, MaxTimelineBytes+1)); !errors.Is(err, ErrRunTooLarge) {
		t.Fatalf("long timeline error=%v, want too large", err)
	}
}

func TestTimelineFilterFalsePositiveRate(t *testing.T) {
	// With ten bits/key and six local probes the expected rate is about 0.8%.
	// The deterministic 20,000-query sample is accepted in [0.3%, 2.0%], a
	// deliberately wide interval that catches broken geometry without flakes.
	runID := testRunID()
	present := make([][]byte, 10_000)
	for i := range present {
		present[i] = []byte(fmt.Sprintf("present-%08d", i))
	}
	encoded, header, err := BuildTimelineFilter(runID, present, FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := testFilterRef(header, 128, uint64(len(encoded)), 128+uint64(len(encoded)))
	const queryCount = 20_000
	falsePositives := 0
	for i := 0; i < queryCount; i++ {
		timeline := []byte(fmt.Sprintf("absent-%08d", i))
		if checkEncodedFilter(t, ref, encoded, timeline) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / queryCount
	t.Logf("false-positive rate %.4f (%d/%d)", rate, falsePositives, queryCount)
	if rate < 0.003 || rate > 0.020 {
		t.Fatalf("false-positive rate %.4f (%d/%d), want [0.003,0.020]", rate, falsePositives, queryCount)
	}
}

func loadFilterCompatibilityManifest(t *testing.T) filterCompatibilityManifest {
	t.Helper()
	data, err := os.ReadFile("testdata/compat/v1/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest filterCompatibilityManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func onlyFilterVector(t *testing.T) timelineFilterVector {
	t.Helper()
	manifest := loadFilterCompatibilityManifest(t)
	if len(manifest.FilterVectors) != 1 {
		t.Fatalf("filter vectors=%d, want 1", len(manifest.FilterVectors))
	}
	return manifest.FilterVectors[0]
}

func testRunID() [RunIDBytes]byte {
	var runID [RunIDBytes]byte
	for i := range runID {
		runID[i] = byte(i + 1)
	}
	return runID
}

func generatedFilterTimelines(count int) [][]byte {
	timelines := make([][]byte, count)
	for i := range timelines {
		timelines[i] = []byte(fmt.Sprintf("timeline-%04d", i))
	}
	return timelines
}

func timelineFilterHashInput(timeline []byte) []byte {
	input := make([]byte, 0, len(timelineFilterKeyDomain)+1+2+len(timeline))
	input = append(input, timelineFilterKeyDomain...)
	input = append(input, 0)
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(timeline)))
	input = append(input, length[:]...)
	return append(input, timeline...)
}

func timelineFilterPageCRCInput(header []byte, page uint32, data []byte) []byte {
	input := make([]byte, 0, len(timelineFilterPageDomain)+1+len(header)+4+len(data))
	input = append(input, timelineFilterPageDomain...)
	input = append(input, 0)
	input = append(input, header...)
	var pageBytes [4]byte
	binary.BigEndian.PutUint32(pageBytes[:], page)
	input = append(input, pageBytes[:]...)
	return append(input, data...)
}

func filterPageRecord(t *testing.T, encoded []byte, header FilterHeader, page uint32) ([]byte, []byte) {
	t.Helper()
	offset, ok := filterPageRelativeOffset(page)
	if !ok {
		t.Fatal("page offset overflow")
	}
	dataLength, ok := filterPageDataLength(header, page)
	if !ok {
		t.Fatal("invalid page geometry")
	}
	dataEnd := offset + dataLength
	recordEnd := dataEnd + TimelineFilterPageChecksumBytes
	return encoded[int(offset):int(recordEnd)], encoded[int(offset):int(dataEnd)]
}

func checkEncodedFilter(t *testing.T, ref FilterRef, encoded, timeline []byte) bool {
	t.Helper()
	request, err := PlanFilterPage(ref, timeline)
	if err != nil {
		t.Fatal(err)
	}
	start := uint64(request.Offset) - ref.Region.Offset
	end := start + uint64(request.Length)
	mayContain, err := CheckFilterPage(ref, timeline, encoded[int(start):int(end)])
	if err != nil {
		t.Fatal(err)
	}
	return mayContain
}

func locationProbeBits(location filterLocation, probes uint8) []uint16 {
	return append([]uint16(nil), location.probeBits[:probes]...)
}

func findTimelineForPage(t *testing.T, header FilterHeader, timelines [][]byte, page uint32) []byte {
	t.Helper()
	for _, timeline := range timelines {
		location, err := locateTimeline(header, timeline)
		if err != nil {
			t.Fatal(err)
		}
		if location.page == page {
			return timeline
		}
	}
	t.Fatalf("no timeline mapped to page %d", page)
	return nil
}

func mutateClone(source []byte, index int) []byte {
	clone := bytes.Clone(source)
	clone[index] ^= 1
	return clone
}

func testFilterRef(header FilterHeader, offset, length, objectSize uint64) FilterRef {
	region := RegionDescriptor{
		Kind:       RegionKindTimelineFilter,
		Encoding:   RegionEncodingV1,
		Offset:     offset,
		Length:     length,
		EntryCount: header.KeyCount,
	}
	region.ContentHash[0] = 1
	return FilterRef{Header: header, Region: region, ObjectSize: objectSize}
}
