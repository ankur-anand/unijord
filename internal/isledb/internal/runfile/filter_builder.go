package runfile

import (
	"encoding/binary"
	"hash/crc32"
)

const timelineFilterPageDomain = "unijord/run/timeline-filter-page/v1"

// BuildTimelineFilter constructs one exact version-1 UJTF region from the
// distinct opaque timeline byte strings in timelines.
func BuildTimelineFilter(runID [RunIDBytes]byte, timelines [][]byte, options FilterOptions) ([]byte, FilterHeader, error) {
	bitsPerKey := options.BitsPerKey
	if bitsPerKey == 0 {
		bitsPerKey = DefaultTimelineFilterBitsPerKey
	}
	if bitsPerKey < MinTimelineFilterBitsPerKey || bitsPerKey > MaxTimelineFilterBitsPerKey {
		return nil, FilterHeader{}, invalidRunf("timeline filter bits per key %d outside [%d,%d]", bitsPerKey, MinTimelineFilterBitsPerKey, MaxTimelineFilterBitsPerKey)
	}
	if allZero(runID[:]) {
		return nil, FilterHeader{}, invalidRunf("run ID is zero")
	}
	if len(timelines) == 0 {
		return nil, FilterHeader{}, invalidRunf("timeline filter key count is zero")
	}
	for _, timeline := range timelines {
		if len(timeline) == 0 {
			return nil, FilterHeader{}, invalidRunf("timeline is empty")
		}
		if uint64(len(timeline)) > MaxTimelineBytes {
			return nil, FilterHeader{}, runTooLargef("timeline length %d exceeds %d", len(timeline), MaxTimelineBytes)
		}
	}
	unique := make(map[string]struct{})
	for _, timeline := range timelines {
		unique[string(timeline)] = struct{}{}
	}
	header, err := NewFilterHeader(runID, uint64(len(unique)), bitsPerKey)
	if err != nil {
		return nil, FilterHeader{}, err
	}
	regionLength, err := header.EncodedLength()
	if err != nil {
		return nil, FilterHeader{}, err
	}
	maxInt := uint64(^uint(0) >> 1)
	if regionLength > maxInt {
		return nil, FilterHeader{}, runTooLargef("timeline filter length %d exceeds platform allocation limit %d", regionLength, maxInt)
	}

	encoded := make([]byte, int(regionLength))
	encodeFilterHeader(encoded[:TimelineFilterHeaderBytes], header)
	for timeline := range unique {
		location, err := locateTimeline(header, []byte(timeline))
		if err != nil {
			return nil, FilterHeader{}, err
		}
		pageOffset, ok := filterPageRelativeOffset(location.page)
		if !ok {
			return nil, FilterHeader{}, invalidRunf("timeline filter page offset overflows")
		}
		lineOffset, ok := checkedAdd(pageOffset, uint64(location.lineByteOffset))
		if !ok {
			return nil, FilterHeader{}, invalidRunf("timeline filter line offset overflows")
		}
		for i := uint8(0); i < header.Probes; i++ {
			bit := location.probeBits[i]
			encoded[int(lineOffset)+int(bit/8)] |= byte(1 << (bit % 8))
		}
	}

	headerBytes := encoded[:TimelineFilterHeaderBytes]
	for page := uint32(0); page < header.PageCount; page++ {
		pageOffset, ok := filterPageRelativeOffset(page)
		if !ok {
			return nil, FilterHeader{}, invalidRunf("timeline filter page offset overflows")
		}
		dataLength, ok := filterPageDataLength(header, page)
		if !ok {
			return nil, FilterHeader{}, invalidRunf("timeline filter page %d has invalid geometry", page)
		}
		dataEnd, ok := checkedAdd(pageOffset, dataLength)
		if !ok {
			return nil, FilterHeader{}, invalidRunf("timeline filter page %d range overflows", page)
		}
		pageData := encoded[int(pageOffset):int(dataEnd)]
		binary.BigEndian.PutUint32(encoded[int(dataEnd):int(dataEnd)+TimelineFilterPageChecksumBytes], filterPageCRC32C(headerBytes, page, pageData))
	}
	return encoded, header, nil
}

func filterPageCRC32C(header []byte, page uint32, pageData []byte) uint32 {
	checksum := crc32.Update(0, castagnoliTable, []byte(timelineFilterPageDomain))
	checksum = crc32.Update(checksum, castagnoliTable, []byte{0})
	checksum = crc32.Update(checksum, castagnoliTable, header)
	var pageBytes [4]byte
	binary.BigEndian.PutUint32(pageBytes[:], page)
	checksum = crc32.Update(checksum, castagnoliTable, pageBytes[:])
	return crc32.Update(checksum, castagnoliTable, pageData)
}
