package catformat

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func MarshalLeafPage(page LeafPage) ([]byte, [16]byte, error) {
	if err := validateLeafPage(page); err != nil {
		return nil, [16]byte{}, err
	}
	body := make([]byte, PageHeaderSize+len(page.Entries)*LeafEntrySize)
	marshalPageHeader(body[:PageHeaderSize], leafMagic, page.Header)
	for i := range page.Entries {
		marshalLeafEntry(body[PageHeaderSize+i*LeafEntrySize:], page.Entries[i])
	}
	encoded, err := appendTrailer(body, page.Header.HashAlgo)
	if err != nil {
		return nil, [16]byte{}, err
	}
	return encoded, pageID(body), nil
}

func MarshalIndexPage(page IndexPage) ([]byte, [16]byte, error) {
	if err := validateIndexPage(page); err != nil {
		return nil, [16]byte{}, err
	}
	body := make([]byte, PageHeaderSize+len(page.Entries)*IndexEntrySize)
	marshalPageHeader(body[:PageHeaderSize], indexMagic, page.Header)
	for i := range page.Entries {
		marshalIndexEntry(body[PageHeaderSize+i*IndexEntrySize:], page.Entries[i])
	}
	encoded, err := appendTrailer(body, page.Header.HashAlgo)
	if err != nil {
		return nil, [16]byte{}, err
	}
	return encoded, pageID(body), nil
}

func MarshalHead(head Head) ([]byte, error) {
	if err := validateHead(head); err != nil {
		return nil, err
	}
	bodySize := HeadHeaderSize + LeafEntrySize + len(head.Active)*LeafEntrySize
	for i := range head.Sections {
		bodySize += SectionHeaderSize + len(head.Sections[i].Entries)*IndexEntrySize
	}
	body := make([]byte, bodySize)
	marshalHeadHeader(body[:HeadHeaderSize], head.Header)
	marshalLeafEntry(body[HeadHeaderSize:HeadHeaderSize+LeafEntrySize], head.LastSegment)
	offset := HeadHeaderSize + LeafEntrySize
	for i := range head.Active {
		marshalLeafEntry(body[offset:offset+LeafEntrySize], head.Active[i])
		offset += LeafEntrySize
	}
	for i := range head.Sections {
		section := head.Sections[i]
		marshalSectionHeader(body[offset:offset+SectionHeaderSize], section)
		offset += SectionHeaderSize
		for j := range section.Entries {
			marshalIndexEntry(body[offset:offset+IndexEntrySize], section.Entries[j])
			offset += IndexEntrySize
		}
	}
	return appendTrailer(body, head.Header.HashAlgo)
}

func ParseLeafPage(encoded []byte) (LeafPage, [16]byte, error) {
	page, err := parsePage(encoded, leafMagic)
	if err != nil {
		return LeafPage{}, [16]byte{}, err
	}
	out := LeafPage{Header: page.header, Entries: make([]LeafEntry, page.header.EntryCount)}
	offset := PageHeaderSize
	for i := range out.Entries {
		entry, err := parseLeafEntry(encoded[offset : offset+LeafEntrySize])
		if err != nil {
			return LeafPage{}, [16]byte{}, invalidf("leaf entry %d: %v", i, err)
		}
		out.Entries[i] = entry
		offset += LeafEntrySize
	}
	if err := validateLeafPage(out); err != nil {
		return LeafPage{}, [16]byte{}, err
	}
	return out, pageID(encoded[:len(encoded)-TrailerSize]), nil
}

func ParseIndexPage(encoded []byte) (IndexPage, [16]byte, error) {
	page, err := parsePage(encoded, indexMagic)
	if err != nil {
		return IndexPage{}, [16]byte{}, err
	}
	out := IndexPage{Header: page.header, Entries: make([]IndexEntry, page.header.EntryCount)}
	offset := PageHeaderSize
	for i := range out.Entries {
		entry, err := parseIndexEntry(encoded[offset : offset+IndexEntrySize])
		if err != nil {
			return IndexPage{}, [16]byte{}, invalidf("index entry %d: %v", i, err)
		}
		out.Entries[i] = entry
		offset += IndexEntrySize
	}
	if err := validateIndexPage(out); err != nil {
		return IndexPage{}, [16]byte{}, err
	}
	return out, pageID(encoded[:len(encoded)-TrailerSize]), nil
}

func ParseHead(encoded []byte) (Head, error) {
	if len(encoded) < HeadHeaderSize+LeafEntrySize+TrailerSize {
		return Head{}, invalidf("head size=%d is below minimum=%d", len(encoded), HeadHeaderSize+LeafEntrySize+TrailerSize)
	}
	header, err := parseHeadHeader(encoded[:HeadHeaderSize])
	if err != nil {
		return Head{}, err
	}
	if header.ActiveCount >= header.LeafLimit || header.ActiveCount > MaxLeafEntries {
		return Head{}, invalidf("head active_count=%d is invalid for leaf_limit=%d", header.ActiveCount, header.LeafLimit)
	}
	if header.LevelCount > MaxOpenIndexLevel {
		return Head{}, invalidf("head level_count=%d exceeds maximum=%d", header.LevelCount, MaxOpenIndexLevel)
	}
	if err := verifyTrailer(encoded, header.HashAlgo); err != nil {
		return Head{}, err
	}

	lastSegment, err := parseLeafEntry(encoded[HeadHeaderSize : HeadHeaderSize+LeafEntrySize])
	if err != nil {
		return Head{}, invalidf("head last_segment: %v", err)
	}
	head := Head{Header: header, LastSegment: lastSegment}
	if header.ActiveCount > 0 {
		head.Active = make([]LeafEntry, header.ActiveCount)
	}
	offset := HeadHeaderSize + LeafEntrySize
	bodyEnd := len(encoded) - TrailerSize
	for i := range head.Active {
		if bodyEnd-offset < LeafEntrySize {
			return Head{}, invalidf("head truncated in active leaf %d", i)
		}
		entry, err := parseLeafEntry(encoded[offset : offset+LeafEntrySize])
		if err != nil {
			return Head{}, invalidf("head active leaf %d: %v", i, err)
		}
		head.Active[i] = entry
		offset += LeafEntrySize
	}
	if header.LevelCount > 0 {
		head.Sections = make([]OpenIndexSection, header.LevelCount)
	}
	for i := range head.Sections {
		if bodyEnd-offset < SectionHeaderSize {
			return Head{}, invalidf("head truncated before section %d", i+1)
		}
		section, count, err := parseSectionHeader(encoded[offset : offset+SectionHeaderSize])
		if err != nil {
			return Head{}, invalidf("head section %d: %v", i+1, err)
		}
		offset += SectionHeaderSize
		if count >= header.IndexLimit || count > MaxIndexEntries {
			return Head{}, invalidf("head section level=%d count=%d is invalid for index_limit=%d", section.Level, count, header.IndexLimit)
		}
		needed := int(count) * IndexEntrySize
		if bodyEnd-offset < needed {
			return Head{}, invalidf("head truncated in section level=%d", section.Level)
		}
		if count > 0 {
			section.Entries = make([]IndexEntry, count)
		}
		for j := range section.Entries {
			entry, err := parseIndexEntry(encoded[offset : offset+IndexEntrySize])
			if err != nil {
				return Head{}, invalidf("head section level=%d entry=%d: %v", section.Level, j, err)
			}
			section.Entries[j] = entry
			offset += IndexEntrySize
		}
		head.Sections[i] = section
	}
	if offset != bodyEnd {
		return Head{}, invalidf("head has %d unclaimed body bytes", bodyEnd-offset)
	}
	if err := validateHead(head); err != nil {
		return Head{}, err
	}
	return head, nil
}

type parsedPage struct {
	header PageHeader
}

func parsePage(encoded []byte, wantMagic [4]byte) (parsedPage, error) {
	if len(encoded) < PageHeaderSize+TrailerSize {
		return parsedPage{}, invalidf("page size=%d is below minimum=%d", len(encoded), PageHeaderSize+TrailerSize)
	}
	header, magic, err := parsePageHeader(encoded[:PageHeaderSize])
	if err != nil {
		return parsedPage{}, err
	}
	if magic != wantMagic {
		return parsedPage{}, invalidf("page magic=%q, want %q", magic, wantMagic)
	}
	wantEntrySize := uint32(LeafEntrySize)
	wantMax := uint32(MaxLeafEntries)
	if wantMagic == indexMagic {
		wantEntrySize = IndexEntrySize
		wantMax = MaxIndexEntries
	}
	if header.EntrySize != wantEntrySize {
		return parsedPage{}, invalidf("page entry_size=%d, want %d", header.EntrySize, wantEntrySize)
	}
	if header.EntryCount == 0 || header.EntryCount > wantMax {
		return parsedPage{}, invalidf("page entry_count=%d outside [1,%d]", header.EntryCount, wantMax)
	}
	wantSize := PageHeaderSize + int(header.EntryCount)*int(wantEntrySize) + TrailerSize
	if len(encoded) != wantSize {
		return parsedPage{}, invalidf("page size=%d, want %d", len(encoded), wantSize)
	}
	if err := verifyTrailer(encoded, header.HashAlgo); err != nil {
		return parsedPage{}, err
	}
	return parsedPage{header: header}, nil
}

func marshalLeafEntry(dst []byte, entry LeafEntry) {
	binary.BigEndian.PutUint64(dst[0:8], entry.BaseLSN)
	binary.BigEndian.PutUint64(dst[8:16], entry.LastLSN)
	putInt64(dst[16:24], entry.MinTimestampMS)
	putInt64(dst[24:32], entry.MaxTimestampMS)
	binary.BigEndian.PutUint32(dst[32:36], entry.RecordCount)
	binary.BigEndian.PutUint32(dst[36:40], entry.BlockCount)
	binary.BigEndian.PutUint64(dst[40:48], entry.SizeBytes)
	binary.BigEndian.PutUint64(dst[48:56], entry.BlockIndexOffset)
	binary.BigEndian.PutUint32(dst[56:60], entry.BlockIndexLength)
	binary.BigEndian.PutUint16(dst[60:62], uint16(entry.Codec))
	binary.BigEndian.PutUint16(dst[62:64], uint16(entry.HashAlgo))
	binary.BigEndian.PutUint64(dst[64:72], entry.SegmentHash)
	binary.BigEndian.PutUint64(dst[72:80], entry.TrailerHash)
	binary.BigEndian.PutUint64(dst[80:88], entry.WriterEpoch)
	copy(dst[88:104], entry.SegmentUUID[:])
	copy(dst[104:120], entry.WriterTag[:])
}

func parseLeafEntry(src []byte) (LeafEntry, error) {
	if len(src) != LeafEntrySize {
		return LeafEntry{}, invalidf("leaf entry size=%d, want %d", len(src), LeafEntrySize)
	}
	if !allZero(src[120:128]) {
		return LeafEntry{}, invalidf("leaf entry reserved bytes are non-zero")
	}
	entry := LeafEntry{
		BaseLSN:          binary.BigEndian.Uint64(src[0:8]),
		LastLSN:          binary.BigEndian.Uint64(src[8:16]),
		MinTimestampMS:   getInt64(src[16:24]),
		MaxTimestampMS:   getInt64(src[24:32]),
		RecordCount:      binary.BigEndian.Uint32(src[32:36]),
		BlockCount:       binary.BigEndian.Uint32(src[36:40]),
		SizeBytes:        binary.BigEndian.Uint64(src[40:48]),
		BlockIndexOffset: binary.BigEndian.Uint64(src[48:56]),
		BlockIndexLength: binary.BigEndian.Uint32(src[56:60]),
		Codec:            segformat.Codec(binary.BigEndian.Uint16(src[60:62])),
		HashAlgo:         segformat.HashAlgo(binary.BigEndian.Uint16(src[62:64])),
		SegmentHash:      binary.BigEndian.Uint64(src[64:72]),
		TrailerHash:      binary.BigEndian.Uint64(src[72:80]),
		WriterEpoch:      binary.BigEndian.Uint64(src[80:88]),
	}
	copy(entry.SegmentUUID[:], src[88:104])
	copy(entry.WriterTag[:], src[104:120])
	return entry, nil
}

func marshalIndexEntry(dst []byte, entry IndexEntry) {
	dst[0] = entry.Level
	binary.BigEndian.PutUint32(dst[4:8], entry.EntryCount)
	binary.BigEndian.PutUint64(dst[8:16], entry.SeqLo)
	binary.BigEndian.PutUint64(dst[16:24], entry.SeqHi)
	putInt64(dst[24:32], entry.MinTimestampMS)
	putInt64(dst[32:40], entry.MaxTimestampMS)
	binary.BigEndian.PutUint64(dst[40:48], entry.Generation)
	copy(dst[48:64], entry.PageID[:])
	binary.BigEndian.PutUint64(dst[64:72], entry.SegmentCount)
}

func parseIndexEntry(src []byte) (IndexEntry, error) {
	if len(src) != IndexEntrySize {
		return IndexEntry{}, invalidf("index entry size=%d, want %d", len(src), IndexEntrySize)
	}
	if !allZero(src[1:4]) || !allZero(src[72:80]) {
		return IndexEntry{}, invalidf("index entry reserved bytes are non-zero")
	}
	entry := IndexEntry{
		Level:          src[0],
		EntryCount:     binary.BigEndian.Uint32(src[4:8]),
		SeqLo:          binary.BigEndian.Uint64(src[8:16]),
		SeqHi:          binary.BigEndian.Uint64(src[16:24]),
		MinTimestampMS: getInt64(src[24:32]),
		MaxTimestampMS: getInt64(src[32:40]),
		Generation:     binary.BigEndian.Uint64(src[40:48]),
		SegmentCount:   binary.BigEndian.Uint64(src[64:72]),
	}
	copy(entry.PageID[:], src[48:64])
	return entry, nil
}

func marshalPageHeader(dst []byte, magic [4]byte, header PageHeader) {
	copy(dst[0:4], magic[:])
	binary.BigEndian.PutUint16(dst[4:6], Version)
	binary.BigEndian.PutUint16(dst[6:8], PageHeaderSize)
	binary.BigEndian.PutUint32(dst[8:12], header.Flags)
	binary.BigEndian.PutUint32(dst[12:16], header.Partition)
	dst[16] = header.Level
	binary.BigEndian.PutUint16(dst[18:20], uint16(header.HashAlgo))
	binary.BigEndian.PutUint32(dst[20:24], header.EntryCount)
	binary.BigEndian.PutUint32(dst[24:28], header.EntrySize)
	binary.BigEndian.PutUint64(dst[32:40], header.SeqLo)
	binary.BigEndian.PutUint64(dst[40:48], header.SeqHi)
	putInt64(dst[48:56], header.MinTimestampMS)
	putInt64(dst[56:64], header.MaxTimestampMS)
	binary.BigEndian.PutUint64(dst[64:72], header.Generation)
	binary.BigEndian.PutUint64(dst[72:80], header.WriterEpoch)
	copy(dst[80:112], header.StreamKey[:])
	binary.BigEndian.PutUint64(dst[112:120], header.SegmentCount)
}

func parsePageHeader(src []byte) (PageHeader, [4]byte, error) {
	if len(src) != PageHeaderSize {
		return PageHeader{}, [4]byte{}, invalidf("page header size=%d, want %d", len(src), PageHeaderSize)
	}
	var magic [4]byte
	copy(magic[:], src[0:4])
	if magic != leafMagic && magic != indexMagic {
		return PageHeader{}, magic, invalidf("unknown page magic=%q", magic)
	}
	if version := binary.BigEndian.Uint16(src[4:6]); version != Version {
		return PageHeader{}, magic, fmt.Errorf("%w: page version=%d", ErrUnsupportedVersion, version)
	}
	if length := binary.BigEndian.Uint16(src[6:8]); length != PageHeaderSize {
		return PageHeader{}, magic, invalidf("page header_len=%d, want %d", length, PageHeaderSize)
	}
	if src[17] != 0 || !allZero(src[28:32]) || !allZero(src[120:128]) {
		return PageHeader{}, magic, invalidf("page header reserved bytes are non-zero")
	}
	header := PageHeader{
		Flags:          binary.BigEndian.Uint32(src[8:12]),
		Partition:      binary.BigEndian.Uint32(src[12:16]),
		Level:          src[16],
		HashAlgo:       segformat.HashAlgo(binary.BigEndian.Uint16(src[18:20])),
		EntryCount:     binary.BigEndian.Uint32(src[20:24]),
		EntrySize:      binary.BigEndian.Uint32(src[24:28]),
		SeqLo:          binary.BigEndian.Uint64(src[32:40]),
		SeqHi:          binary.BigEndian.Uint64(src[40:48]),
		MinTimestampMS: getInt64(src[48:56]),
		MaxTimestampMS: getInt64(src[56:64]),
		Generation:     binary.BigEndian.Uint64(src[64:72]),
		WriterEpoch:    binary.BigEndian.Uint64(src[72:80]),
		SegmentCount:   binary.BigEndian.Uint64(src[112:120]),
	}
	copy(header.StreamKey[:], src[80:112])
	if err := validateCommonPageHeader(header); err != nil {
		return PageHeader{}, magic, err
	}
	return header, magic, nil
}

func marshalHeadHeader(dst []byte, header HeadHeader) {
	copy(dst[0:4], headMagic[:])
	binary.BigEndian.PutUint16(dst[4:6], Version)
	binary.BigEndian.PutUint16(dst[6:8], HeadHeaderSize)
	binary.BigEndian.PutUint32(dst[8:12], header.Flags)
	binary.BigEndian.PutUint32(dst[12:16], header.Partition)
	binary.BigEndian.PutUint16(dst[16:18], uint16(header.HashAlgo))
	binary.BigEndian.PutUint16(dst[18:20], header.SegmentLayoutVersion)
	binary.BigEndian.PutUint32(dst[20:24], header.LeafLimit)
	binary.BigEndian.PutUint32(dst[24:28], header.IndexLimit)
	binary.BigEndian.PutUint32(dst[28:32], header.ActiveCount)
	binary.BigEndian.PutUint32(dst[32:36], header.LevelCount)
	binary.BigEndian.PutUint64(dst[40:48], header.NextLSN)
	binary.BigEndian.PutUint64(dst[48:56], header.OldestLSN)
	binary.BigEndian.PutUint64(dst[56:64], header.AppliedRetentionLSN)
	binary.BigEndian.PutUint64(dst[64:72], header.AppliedRetentionVersion)
	binary.BigEndian.PutUint64(dst[72:80], header.WriterEpoch)
	binary.BigEndian.PutUint64(dst[80:88], header.Generation)
	binary.BigEndian.PutUint64(dst[88:96], header.SegmentCount)
	copy(dst[96:112], header.WriterID[:])
	copy(dst[112:144], header.StreamKey[:])
	copy(dst[144:176], header.DataRootKey[:])
	binary.BigEndian.PutUint64(dst[176:184], header.ReachableSegmentCount)
}

func parseHeadHeader(src []byte) (HeadHeader, error) {
	if len(src) != HeadHeaderSize {
		return HeadHeader{}, invalidf("head header size=%d, want %d", len(src), HeadHeaderSize)
	}
	if !equalMagic(src[:4], headMagic) {
		return HeadHeader{}, invalidf("head magic=%q, want %q", src[:4], headMagic)
	}
	if version := binary.BigEndian.Uint16(src[4:6]); version != Version {
		return HeadHeader{}, fmt.Errorf("%w: head version=%d", ErrUnsupportedVersion, version)
	}
	if length := binary.BigEndian.Uint16(src[6:8]); length != HeadHeaderSize {
		return HeadHeader{}, invalidf("head header_len=%d, want %d", length, HeadHeaderSize)
	}
	if !allZero(src[36:40]) || !allZero(src[184:192]) {
		return HeadHeader{}, invalidf("head header reserved bytes are non-zero")
	}
	header := HeadHeader{
		Flags:                   binary.BigEndian.Uint32(src[8:12]),
		Partition:               binary.BigEndian.Uint32(src[12:16]),
		HashAlgo:                segformat.HashAlgo(binary.BigEndian.Uint16(src[16:18])),
		SegmentLayoutVersion:    binary.BigEndian.Uint16(src[18:20]),
		LeafLimit:               binary.BigEndian.Uint32(src[20:24]),
		IndexLimit:              binary.BigEndian.Uint32(src[24:28]),
		ActiveCount:             binary.BigEndian.Uint32(src[28:32]),
		LevelCount:              binary.BigEndian.Uint32(src[32:36]),
		NextLSN:                 binary.BigEndian.Uint64(src[40:48]),
		OldestLSN:               binary.BigEndian.Uint64(src[48:56]),
		AppliedRetentionLSN:     binary.BigEndian.Uint64(src[56:64]),
		AppliedRetentionVersion: binary.BigEndian.Uint64(src[64:72]),
		WriterEpoch:             binary.BigEndian.Uint64(src[72:80]),
		Generation:              binary.BigEndian.Uint64(src[80:88]),
		SegmentCount:            binary.BigEndian.Uint64(src[88:96]),
		ReachableSegmentCount:   binary.BigEndian.Uint64(src[176:184]),
	}
	copy(header.WriterID[:], src[96:112])
	copy(header.StreamKey[:], src[112:144])
	copy(header.DataRootKey[:], src[144:176])
	if err := validateHashAlgo(header.HashAlgo); err != nil {
		return HeadHeader{}, err
	}
	if header.SegmentLayoutVersion != SegmentLayoutV1 {
		return HeadHeader{}, fmt.Errorf("%w: %d", ErrUnsupportedLayout, header.SegmentLayoutVersion)
	}
	if header.LeafLimit == 0 || header.LeafLimit > MaxLeafEntries || header.IndexLimit < 2 || header.IndexLimit > MaxIndexEntries {
		return HeadHeader{}, invalidf("head limits leaf=%d index=%d are invalid", header.LeafLimit, header.IndexLimit)
	}
	return header, nil
}

func marshalSectionHeader(dst []byte, section OpenIndexSection) {
	copy(dst[0:4], sectionMagic[:])
	dst[4] = section.Level
	binary.BigEndian.PutUint32(dst[8:12], uint32(len(section.Entries)))
}

func parseSectionHeader(src []byte) (OpenIndexSection, uint32, error) {
	if len(src) != SectionHeaderSize {
		return OpenIndexSection{}, 0, invalidf("section header size=%d, want %d", len(src), SectionHeaderSize)
	}
	if !equalMagic(src[:4], sectionMagic) {
		return OpenIndexSection{}, 0, invalidf("section magic=%q, want %q", src[:4], sectionMagic)
	}
	if !allZero(src[5:8]) || !allZero(src[12:16]) {
		return OpenIndexSection{}, 0, invalidf("section reserved bytes are non-zero")
	}
	return OpenIndexSection{Level: src[4]}, binary.BigEndian.Uint32(src[8:12]), nil
}

func appendTrailer(body []byte, algo segformat.HashAlgo) ([]byte, error) {
	if err := validateHashAlgo(algo); err != nil {
		return nil, err
	}
	bodyHash, err := segformat.HashBytes(algo, body)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, len(body)+TrailerSize)
	copy(encoded, body)
	trailer := encoded[len(body):]
	copy(trailer[0:4], trailerMagic[:])
	binary.BigEndian.PutUint16(trailer[4:6], TrailerSize)
	binary.BigEndian.PutUint64(trailer[8:16], bodyHash)
	trailerHash, err := segformat.HashBytes(algo, trailer)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint64(trailer[16:24], trailerHash)
	return encoded, nil
}

func verifyTrailer(encoded []byte, algo segformat.HashAlgo) error {
	if len(encoded) < TrailerSize {
		return invalidf("object size=%d is below trailer size", len(encoded))
	}
	if err := validateHashAlgo(algo); err != nil {
		return err
	}
	trailer := encoded[len(encoded)-TrailerSize:]
	if !equalMagic(trailer[:4], trailerMagic) {
		return invalidf("trailer magic=%q, want %q", trailer[:4], trailerMagic)
	}
	if length := binary.BigEndian.Uint16(trailer[4:6]); length != TrailerSize {
		return invalidf("trailer_len=%d, want %d", length, TrailerSize)
	}
	if !allZero(trailer[6:8]) || !allZero(trailer[24:32]) {
		return invalidf("trailer reserved bytes are non-zero")
	}
	wantTrailerHash := binary.BigEndian.Uint64(trailer[16:24])
	check := [TrailerSize]byte{}
	copy(check[:], trailer)
	for i := 16; i < 24; i++ {
		check[i] = 0
	}
	gotTrailerHash, err := segformat.HashBytes(algo, check[:])
	if err != nil {
		return err
	}
	if gotTrailerHash != wantTrailerHash {
		return fmt.Errorf("%w: trailer hash got=%016x want=%016x", ErrIntegrityMismatch, gotTrailerHash, wantTrailerHash)
	}
	wantBodyHash := binary.BigEndian.Uint64(trailer[8:16])
	gotBodyHash, err := segformat.HashBytes(algo, encoded[:len(encoded)-TrailerSize])
	if err != nil {
		return err
	}
	if gotBodyHash != wantBodyHash {
		return fmt.Errorf("%w: body hash got=%016x want=%016x", ErrIntegrityMismatch, gotBodyHash, wantBodyHash)
	}
	return nil
}

func pageID(body []byte) [16]byte {
	sum := sha256.Sum256(body)
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

func putInt64(dst []byte, value int64) { binary.BigEndian.PutUint64(dst, uint64(value)) }
func getInt64(src []byte) int64        { return int64(binary.BigEndian.Uint64(src)) }

func allZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

func equalMagic(value []byte, magic [4]byte) bool {
	return len(value) == len(magic) && string(value) == string(magic[:])
}

func IsIntegrityError(err error) bool {
	return errors.Is(err, ErrIntegrityMismatch)
}
