package runfile

import (
	"encoding/binary"
	"fmt"
)

// FilterPageReader fetches one exact page record for a filter request.
type FilterPageReader func(FilterPageRequest) ([]byte, error)

// PlanFilterPage calculates the only filter page needed for timeline.
func PlanFilterPage(ref FilterRef, timeline []byte) (FilterPageRequest, error) {
	var request FilterPageRequest
	if err := validateFilterRef(ref, true); err != nil {
		return request, err
	}
	location, err := locateTimeline(ref.Header, timeline)
	if err != nil {
		return request, err
	}
	relativeOffset, ok := filterPageRelativeOffset(location.page)
	if !ok {
		return request, corruptRunf("timeline filter page offset overflows")
	}
	dataLength, ok := filterPageDataLength(ref.Header, location.page)
	if !ok {
		return request, corruptRunf("timeline filter page %d has invalid geometry", location.page)
	}
	recordLength, ok := checkedAdd(dataLength, TimelineFilterPageChecksumBytes)
	if !ok {
		return request, corruptRunf("timeline filter page record length overflows")
	}
	absoluteOffset, ok := checkedAdd(ref.Region.Offset, relativeOffset)
	if !ok {
		return request, corruptRunf("timeline filter page absolute offset overflows")
	}
	recordEnd, ok := checkedAdd(absoluteOffset, recordLength)
	if !ok {
		return request, corruptRunf("timeline filter page range overflows")
	}
	regionEnd, ok := checkedAdd(ref.Region.Offset, ref.Region.Length)
	if !ok || recordEnd > regionEnd || recordEnd > ref.ObjectSize {
		return request, corruptRunf("timeline filter page range exceeds filter region or object")
	}
	request.Offset = int64(absoluteOffset)
	request.Length = int64(recordLength)
	request.Page = location.page
	return request, nil
}

// CheckFilterPage authenticates one successfully fetched page record before
// using it to answer membership. A malformed successful read is corruption.
func CheckFilterPage(ref FilterRef, timeline, pageBytes []byte) (bool, error) {
	request, err := PlanFilterPage(ref, timeline)
	if err != nil {
		return false, err
	}
	if uint64(len(pageBytes)) != uint64(request.Length) {
		return false, corruptRunf("timeline filter page %d length %d, want %d", request.Page, len(pageBytes), request.Length)
	}
	dataLength := len(pageBytes) - TimelineFilterPageChecksumBytes
	storedChecksum := binary.BigEndian.Uint32(pageBytes[dataLength:])
	headerBytes := make([]byte, TimelineFilterHeaderBytes)
	encodeFilterHeader(headerBytes, ref.Header)
	computedChecksum := filterPageCRC32C(headerBytes, request.Page, pageBytes[:dataLength])
	if storedChecksum != computedChecksum {
		return false, corruptRunf("timeline filter page %d CRC-32C %#08x, want %#08x", request.Page, storedChecksum, computedChecksum)
	}

	location, err := locateTimeline(ref.Header, timeline)
	if err != nil {
		return false, err
	}
	lineEnd, ok := checkedAdd(uint64(location.lineByteOffset), uint64(ref.Header.LineBytes))
	if !ok || lineEnd > uint64(dataLength) {
		return false, corruptRunf("timeline filter line %d exceeds page %d data", location.line, location.page)
	}
	line := pageBytes[int(location.lineByteOffset):int(lineEnd)]
	for i := uint8(0); i < ref.Header.Probes; i++ {
		bit := location.probeBits[i]
		if line[bit/8]&(byte(1)<<(bit%8)) == 0 {
			return false, nil
		}
	}
	return true, nil
}

// ReadFilterMembership plans, fetches, and verifies one page. Transport
// failures preserve their original cause and are never reported as corruption
// or interpreted as definite non-membership.
func ReadFilterMembership(ref FilterRef, timeline []byte, read FilterPageReader) (bool, error) {
	if read == nil {
		return false, invalidRunf("nil filter page reader")
	}
	request, err := PlanFilterPage(ref, timeline)
	if err != nil {
		return false, err
	}
	pageBytes, err := read(request)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrFilterUnavailable, err)
	}
	return CheckFilterPage(ref, timeline, pageBytes)
}
