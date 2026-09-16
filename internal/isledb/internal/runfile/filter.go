package runfile

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	TimelineFilterAlgorithmLocalBloom uint8  = 1
	MinTimelineFilterBitsPerKey       uint16 = 1
	MaxTimelineFilterBitsPerKey       uint16 = 32
	MaxTimelineFilterProbes           uint8  = 22
	TimelineFilterPageChecksumBytes          = 4
)

const timelineFilterKeyDomain = "unijord/run/timeline-filter-key/v1"

// FilterOptions controls construction of a version-1 timeline filter. A zero
// BitsPerKey selects DefaultTimelineFilterBitsPerKey.
type FilterOptions struct {
	BitsPerKey uint16
}

// FilterHeader is the variable portion of one version-1 UJTF header. Fields
// repeated in a manifest FilterRef remain explicit so readers can reconstruct
// the exact header without fetching it.
type FilterHeader struct {
	Algorithm     uint8
	Probes        uint8
	BitsPerKey    uint16
	KeyCount      uint64
	LineCount     uint32
	PageCount     uint32
	LineBytes     uint16
	LinesPerPage  uint16
	PageDataBytes uint32
	RunID         [RunIDBytes]byte
}

// FilterRef supplies the complete manifest projection needed to plan and
// authenticate a single filter-page read.
type FilterRef struct {
	Header     FilterHeader
	Region     RegionDescriptor
	ObjectSize uint64
}

// FilterPageRequest is one exact object-relative page-record read.
type FilterPageRequest struct {
	Offset int64
	Length int64
	Page   uint32
}

type filterGeometry struct {
	probes        uint8
	lineCount     uint32
	pageCount     uint32
	encodedLength uint64
}

type filterLocation struct {
	line           uint32
	page           uint32
	lineByteOffset uint32
	probeBits      [MaxTimelineFilterProbes]uint16
}

// NewFilterHeader derives canonical version-1 geometry. A zero bitsPerKey
// selects DefaultTimelineFilterBitsPerKey.
func NewFilterHeader(runID [RunIDBytes]byte, keyCount uint64, bitsPerKey uint16) (FilterHeader, error) {
	if bitsPerKey == 0 {
		bitsPerKey = DefaultTimelineFilterBitsPerKey
	}
	if allZero(runID[:]) {
		return FilterHeader{}, invalidRunf("run ID is zero")
	}
	geometry, err := deriveFilterGeometry(keyCount, bitsPerKey, invalidRunf)
	if err != nil {
		return FilterHeader{}, err
	}
	return FilterHeader{
		Algorithm:     TimelineFilterAlgorithmLocalBloom,
		Probes:        geometry.probes,
		BitsPerKey:    bitsPerKey,
		KeyCount:      keyCount,
		LineCount:     geometry.lineCount,
		PageCount:     geometry.pageCount,
		LineBytes:     TimelineFilterLineBytes,
		LinesPerPage:  TimelineFilterLinesPerPage,
		PageDataBytes: TimelineFilterPageDataBytes,
		RunID:         runID,
	}, nil
}

func deriveFilterGeometry(keyCount uint64, bitsPerKey uint16, errorf func(string, ...any) error) (filterGeometry, error) {
	var geometry filterGeometry
	if keyCount == 0 {
		return geometry, errorf("timeline filter key count is zero")
	}
	if bitsPerKey < MinTimelineFilterBitsPerKey || bitsPerKey > MaxTimelineFilterBitsPerKey {
		return geometry, errorf("timeline filter bits per key %d outside [%d,%d]", bitsPerKey, MinTimelineFilterBitsPerKey, MaxTimelineFilterBitsPerKey)
	}

	requestedBits, ok := checkedMultiply(keyCount, uint64(bitsPerKey))
	if !ok {
		return geometry, runTooLargef("timeline filter requested-bit calculation overflows")
	}
	lineCount, ok := checkedCeilingDivide(requestedBits, TimelineFilterLineBytes*8)
	if !ok {
		return geometry, runTooLargef("timeline filter line calculation overflows")
	}
	if lineCount == 0 {
		lineCount = 1
	}
	if lineCount > MaxTimelineFilterLines {
		return geometry, runTooLargef("timeline filter line count %d exceeds %d", lineCount, MaxTimelineFilterLines)
	}
	pageCount, ok := checkedCeilingDivide(lineCount, TimelineFilterLinesPerPage)
	if !ok {
		return geometry, runTooLargef("timeline filter page calculation overflows")
	}

	lineDataBytes, ok := checkedMultiply(lineCount, TimelineFilterLineBytes)
	if !ok {
		return geometry, runTooLargef("timeline filter data length overflows")
	}
	checksumBytes, ok := checkedMultiply(pageCount, TimelineFilterPageChecksumBytes)
	if !ok {
		return geometry, runTooLargef("timeline filter checksum length overflows")
	}
	encodedLength, ok := checkedAdd(TimelineFilterHeaderBytes, lineDataBytes)
	if !ok {
		return geometry, runTooLargef("timeline filter length overflows")
	}
	encodedLength, ok = checkedAdd(encodedLength, checksumBytes)
	if !ok {
		return geometry, runTooLargef("timeline filter length overflows")
	}

	probes := uint64(bitsPerKey) * 69 / 100
	if probes < 1 {
		probes = 1
	}
	if probes > 30 {
		probes = 30
	}
	geometry.probes = uint8(probes)
	geometry.lineCount = uint32(lineCount)
	geometry.pageCount = uint32(pageCount)
	geometry.encodedLength = encodedLength
	return geometry, nil
}

// Validate checks a filter header as caller-provided build input.
func (h FilterHeader) Validate() error {
	return validateFilterHeader(h, false)
}

func validateFilterHeader(h FilterHeader, persisted bool) error {
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}
	if h.Algorithm != TimelineFilterAlgorithmLocalBloom {
		return unsupportedRunf("timeline filter algorithm %d", h.Algorithm)
	}
	if allZero(h.RunID[:]) {
		return errorf("timeline filter run ID is zero")
	}
	geometry, err := deriveFilterGeometry(h.KeyCount, h.BitsPerKey, errorf)
	if err != nil {
		return err
	}
	if h.Probes != geometry.probes {
		return errorf("timeline filter probes %d, want %d", h.Probes, geometry.probes)
	}
	if h.LineCount != geometry.lineCount {
		return errorf("timeline filter line count %d, want %d", h.LineCount, geometry.lineCount)
	}
	if h.PageCount != geometry.pageCount {
		return errorf("timeline filter page count %d, want %d", h.PageCount, geometry.pageCount)
	}
	if h.LineBytes != TimelineFilterLineBytes {
		return errorf("timeline filter line bytes %d, want %d", h.LineBytes, TimelineFilterLineBytes)
	}
	if h.LinesPerPage != TimelineFilterLinesPerPage {
		return errorf("timeline filter lines per page %d, want %d", h.LinesPerPage, TimelineFilterLinesPerPage)
	}
	if h.PageDataBytes != TimelineFilterPageDataBytes {
		return errorf("timeline filter page data bytes %d, want %d", h.PageDataBytes, TimelineFilterPageDataBytes)
	}
	return nil
}

// EncodedLength returns the canonical complete filter-region length.
func (h FilterHeader) EncodedLength() (uint64, error) {
	if err := h.Validate(); err != nil {
		return 0, err
	}
	geometry, err := deriveFilterGeometry(h.KeyCount, h.BitsPerKey, invalidRunf)
	if err != nil {
		return 0, err
	}
	return geometry.encodedLength, nil
}

// MarshalFilterHeader encodes one exact version-1 UJTF header.
func MarshalFilterHeader(h FilterHeader) ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	encoded := make([]byte, TimelineFilterHeaderBytes)
	encodeFilterHeader(encoded, h)
	return encoded, nil
}

func encodeFilterHeader(encoded []byte, h FilterHeader) {
	copy(encoded[0:4], TimelineFilterMagic)
	binary.BigEndian.PutUint16(encoded[4:6], FormatVersion)
	binary.BigEndian.PutUint16(encoded[6:8], TimelineFilterHeaderBytes)
	encoded[8] = h.Algorithm
	encoded[9] = h.Probes
	binary.BigEndian.PutUint16(encoded[10:12], h.BitsPerKey)
	binary.BigEndian.PutUint32(encoded[12:16], 0)
	binary.BigEndian.PutUint64(encoded[16:24], h.KeyCount)
	binary.BigEndian.PutUint32(encoded[24:28], h.LineCount)
	binary.BigEndian.PutUint32(encoded[28:32], h.PageCount)
	binary.BigEndian.PutUint16(encoded[32:34], h.LineBytes)
	binary.BigEndian.PutUint16(encoded[34:36], h.LinesPerPage)
	binary.BigEndian.PutUint32(encoded[36:40], h.PageDataBytes)
	copy(encoded[40:56], h.RunID[:])
}

// UnmarshalFilterHeader decodes and validates one exact version-1 UJTF header.
func UnmarshalFilterHeader(encoded []byte) (FilterHeader, error) {
	var h FilterHeader
	if len(encoded) != TimelineFilterHeaderBytes {
		return h, corruptRunf("timeline filter header length %d, want %d", len(encoded), TimelineFilterHeaderBytes)
	}
	if string(encoded[0:4]) != TimelineFilterMagic {
		return h, corruptRunf("invalid timeline filter magic %x", encoded[0:4])
	}
	version := binary.BigEndian.Uint16(encoded[4:6])
	if version != FormatVersion {
		return h, unsupportedRunf("timeline filter version %d", version)
	}
	if headerBytes := binary.BigEndian.Uint16(encoded[6:8]); headerBytes != TimelineFilterHeaderBytes {
		return h, corruptRunf("timeline filter header bytes %d, want %d", headerBytes, TimelineFilterHeaderBytes)
	}
	if algorithm := encoded[8]; algorithm != TimelineFilterAlgorithmLocalBloom {
		return h, unsupportedRunf("timeline filter algorithm %d", algorithm)
	}
	if flags := binary.BigEndian.Uint32(encoded[12:16]); flags != 0 {
		return h, corruptRunf("unknown timeline filter flags %#x", flags)
	}
	if !allZero(encoded[56:64]) {
		return h, corruptRunf("non-zero timeline filter reserved0")
	}

	h.Algorithm = encoded[8]
	h.Probes = encoded[9]
	h.BitsPerKey = binary.BigEndian.Uint16(encoded[10:12])
	h.KeyCount = binary.BigEndian.Uint64(encoded[16:24])
	h.LineCount = binary.BigEndian.Uint32(encoded[24:28])
	h.PageCount = binary.BigEndian.Uint32(encoded[28:32])
	h.LineBytes = binary.BigEndian.Uint16(encoded[32:34])
	h.LinesPerPage = binary.BigEndian.Uint16(encoded[34:36])
	h.PageDataBytes = binary.BigEndian.Uint32(encoded[36:40])
	copy(h.RunID[:], encoded[40:56])
	if err := validateFilterHeader(h, true); err != nil {
		return FilterHeader{}, err
	}
	return h, nil
}

// Validate checks a filter reference as caller-provided input.
func (r FilterRef) Validate() error {
	return validateFilterRef(r, false)
}

func validateFilterRef(r FilterRef, persisted bool) error {
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}
	if err := validateFilterHeader(r.Header, persisted); err != nil {
		return err
	}
	geometry, err := deriveFilterGeometry(r.Header.KeyCount, r.Header.BitsPerKey, errorf)
	if err != nil {
		return err
	}
	if r.Region.Kind != RegionKindTimelineFilter {
		return errorf("timeline filter reference has region kind %d", r.Region.Kind)
	}
	if r.Region.Required {
		return errorf("timeline filter reference is marked required")
	}
	if r.Region.Encoding != RegionEncodingV1 {
		return unsupportedRunf("timeline filter region encoding %d", r.Region.Encoding)
	}
	if r.Region.Offset < PreambleBytes {
		return errorf("timeline filter offset %d precedes preamble", r.Region.Offset)
	}
	if r.Region.Offset%RegionAlignment != 0 {
		return errorf("timeline filter offset %d is not %d-byte aligned", r.Region.Offset, RegionAlignment)
	}
	if r.Region.Length != geometry.encodedLength {
		return errorf("timeline filter length %d, want %d", r.Region.Length, geometry.encodedLength)
	}
	if r.Region.EntryCount != r.Header.KeyCount {
		return errorf("timeline filter entry count %d, want %d", r.Region.EntryCount, r.Header.KeyCount)
	}
	if r.Region.SeqLo != 0 || r.Region.SeqHi != 0 {
		return errorf("timeline filter reference has non-zero sequence bounds")
	}
	if len(r.Region.MinKey) != 0 || len(r.Region.MaxKey) != 0 {
		return errorf("timeline filter reference has key bounds")
	}
	if allZero(r.Region.ContentHash[:]) {
		return errorf("timeline filter content hash is zero")
	}
	if r.ObjectSize > MaxRunObjectBytes {
		return runTooLargef("object size %d exceeds %d", r.ObjectSize, MaxRunObjectBytes)
	}
	regionEnd, ok := checkedRangeEnd(r.Region.Offset, r.Region.Length)
	if !ok {
		return errorf("timeline filter range overflows")
	}
	if regionEnd > r.ObjectSize {
		return errorf("timeline filter ends at %d beyond object size %d", regionEnd, r.ObjectSize)
	}
	return nil
}

func timelineFilterToken(timeline []byte) ([SHA256Bytes]byte, uint32, uint32, error) {
	var digest [SHA256Bytes]byte
	if len(timeline) == 0 {
		return digest, 0, 0, invalidRunf("timeline is empty")
	}
	if uint64(len(timeline)) > MaxTimelineBytes {
		return digest, 0, 0, runTooLargef("timeline length %d exceeds %d", len(timeline), MaxTimelineBytes)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(timelineFilterKeyDomain))
	_, _ = hasher.Write([]byte{0})
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(timeline)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(timeline)
	hasher.Sum(digest[:0])
	h1 := binary.BigEndian.Uint32(digest[0:4])
	h2 := binary.BigEndian.Uint32(digest[4:8]) | 1
	return digest, h1, h2, nil
}

func locateTimeline(h FilterHeader, timeline []byte) (filterLocation, error) {
	var location filterLocation
	_, h1, probeHash, err := timelineFilterToken(timeline)
	if err != nil {
		return location, err
	}
	location.line = uint32((uint64(h1) * uint64(h.LineCount)) >> 32)
	location.page = location.line / uint32(h.LinesPerPage)
	lineInPage := location.line % uint32(h.LinesPerPage)
	location.lineByteOffset = lineInPage * uint32(h.LineBytes)

	// The 512-bit probe-local geometry follows RocksDB FastLocalBloomImpl and
	// was cross-checked against Pebble v2.1.4 bloom/bloom.go. UJTF's hash,
	// header, pages, and checksums remain Unijord-specific wire bytes.
	for i := uint8(0); i < h.Probes; i++ {
		location.probeBits[i] = uint16(probeHash >> 23)
		probeHash *= 0x9e3779b9
	}
	return location, nil
}

func filterPageDataLength(h FilterHeader, page uint32) (uint64, bool) {
	if page >= h.PageCount {
		return 0, false
	}
	if page < h.PageCount-1 {
		return uint64(h.PageDataBytes), true
	}
	priorLines, ok := checkedMultiply(uint64(page), uint64(h.LinesPerPage))
	if !ok || priorLines >= uint64(h.LineCount) {
		return 0, false
	}
	lastPageLines := uint64(h.LineCount) - priorLines
	return checkedMultiply(lastPageLines, uint64(h.LineBytes))
}

func filterPageRelativeOffset(page uint32) (uint64, bool) {
	fullRecordBytes, ok := checkedAdd(TimelineFilterPageDataBytes, TimelineFilterPageChecksumBytes)
	if !ok {
		return 0, false
	}
	priorBytes, ok := checkedMultiply(uint64(page), fullRecordBytes)
	if !ok {
		return 0, false
	}
	return checkedAdd(TimelineFilterHeaderBytes, priorBytes)
}
