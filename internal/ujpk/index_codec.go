package ujpk

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
)

func hashTimelineKey(key []byte) timelineHash {
	sum := sha256.Sum256(key)
	var hash timelineHash
	copy(hash[:], sum[:len(hash)])
	return hash
}

func appendIndexPages(dst []byte, entries []indexEntry, targetBytes int) ([]byte, []indexPageRef) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := bytes.Compare(entries[i].keyHash[:], entries[j].keyHash[:]); cmp != 0 {
			return cmp < 0
		}
		if cmp := bytes.Compare(entries[i].key, entries[j].key); cmp != 0 {
			return cmp < 0
		}
		return entries[i].firstLSN < entries[j].firstLSN
	})

	entriesPerPage := (targetBytes - IndexPagePreambleSize) / TimelineIndexEntrySize
	refs := make([]indexPageRef, 0, (len(entries)+entriesPerPage-1)/entriesPerPage)
	for start := 0; start < len(entries); start += entriesPerPage {
		end := min(start+entriesPerPage, len(entries))
		page := encodeIndexPage(entries[start:end])
		offset := uint64(len(dst))
		var recordCount uint32
		for _, entry := range entries[start:end] {
			recordCount += entry.recordCount
		}
		refs = append(refs, indexPageRef{
			firstHash:   entries[start].keyHash,
			lastHash:    entries[end-1].keyHash,
			offset:      offset,
			length:      uint32(len(page)),
			entryCount:  uint32(end - start),
			recordCount: recordCount,
			hash:        xxhash.Sum64(page),
		})
		dst = append(dst, page...)
	}
	return dst, refs
}

func encodeIndexPage(entries []indexEntry) []byte {
	out := make([]byte, IndexPagePreambleSize, IndexPagePreambleSize+len(entries)*TimelineIndexEntrySize)
	copy(out[0:4], indexPageMagic[:])
	binary.BigEndian.PutUint16(out[4:6], Version)
	binary.BigEndian.PutUint16(out[6:8], IndexPagePreambleSize)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(entries)))
	var recordCount uint32
	for _, entry := range entries {
		recordCount += entry.recordCount
	}
	binary.BigEndian.PutUint32(out[12:16], recordCount)
	for _, entry := range entries {
		header := make([]byte, TimelineIndexEntrySize)
		copy(header[0:16], entry.keyHash[:])
		binary.BigEndian.PutUint32(header[16:20], entry.pageNo)
		binary.BigEndian.PutUint32(header[20:24], entry.extentOffset)
		binary.BigEndian.PutUint32(header[24:28], entry.extentLength)
		binary.BigEndian.PutUint32(header[28:32], entry.recordCount)
		binary.BigEndian.PutUint64(header[32:40], entry.firstLSN)
		out = append(out, header...)
	}
	return out
}

func encodeIndexRoot(dataPages []pageEntry, indexPages []indexPageRef) []byte {
	dataBytes := len(dataPages) * DataPageTableEntrySize
	indexBytes := len(indexPages) * IndexRootEntrySize
	out := make([]byte, IndexRootPreambleSize, IndexRootPreambleSize+dataBytes+indexBytes)
	copy(out[0:4], indexRootMagic[:])
	binary.BigEndian.PutUint16(out[4:6], Version)
	binary.BigEndian.PutUint16(out[6:8], IndexRootPreambleSize)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(dataPages)))
	binary.BigEndian.PutUint32(out[12:16], uint32(len(indexPages)))
	binary.BigEndian.PutUint32(out[16:20], uint32(dataBytes))
	binary.BigEndian.PutUint32(out[20:24], uint32(indexBytes))
	for _, page := range dataPages {
		entry := make([]byte, DataPageTableEntrySize)
		binary.BigEndian.PutUint64(entry[0:8], page.offset)
		binary.BigEndian.PutUint32(entry[8:12], page.storedSize)
		binary.BigEndian.PutUint32(entry[12:16], page.rawSize)
		binary.BigEndian.PutUint32(entry[16:20], page.extentCount)
		binary.BigEndian.PutUint64(entry[24:32], page.hash)
		out = append(out, entry...)
	}
	for _, page := range indexPages {
		entry := make([]byte, IndexRootEntrySize)
		copy(entry[0:16], page.firstHash[:])
		copy(entry[16:32], page.lastHash[:])
		binary.BigEndian.PutUint64(entry[32:40], page.offset)
		binary.BigEndian.PutUint32(entry[40:44], page.length)
		binary.BigEndian.PutUint32(entry[44:48], page.entryCount)
		binary.BigEndian.PutUint32(entry[48:52], page.recordCount)
		binary.BigEndian.PutUint64(entry[56:64], page.hash)
		out = append(out, entry...)
	}
	return out
}

func parseIndexRoot(buf []byte, rootOffset uint64, wantDataPages, wantExtents, wantRecords uint32) ([]pageEntry, []indexPageRef, error) {
	if len(buf) < IndexRootPreambleSize || !bytes.Equal(buf[0:4], indexRootMagic[:]) {
		return nil, nil, fmt.Errorf("%w: index root preamble", ErrInvalidPack)
	}
	if binary.BigEndian.Uint16(buf[4:6]) != Version ||
		binary.BigEndian.Uint16(buf[6:8]) != IndexRootPreambleSize ||
		!allZero(buf[24:32]) {
		return nil, nil, fmt.Errorf("%w: index root version or reserved bytes", ErrInvalidPack)
	}
	dataCount := binary.BigEndian.Uint32(buf[8:12])
	indexCount := binary.BigEndian.Uint32(buf[12:16])
	dataBytes := binary.BigEndian.Uint32(buf[16:20])
	indexBytes := binary.BigEndian.Uint32(buf[20:24])
	if dataCount != wantDataPages || dataCount == 0 || indexCount == 0 ||
		uint64(dataBytes) != uint64(dataCount)*DataPageTableEntrySize ||
		uint64(indexBytes) != uint64(indexCount)*IndexRootEntrySize ||
		uint64(IndexRootPreambleSize)+uint64(dataBytes)+uint64(indexBytes) != uint64(len(buf)) {
		return nil, nil, fmt.Errorf("%w: index root size or counts", ErrInvalidPack)
	}

	dataPages := make([]pageEntry, 0, dataCount)
	off := IndexRootPreambleSize
	var extentCount uint64
	for i := uint32(0); i < dataCount; i++ {
		entry := buf[off : off+DataPageTableEntrySize]
		page := pageEntry{
			offset:      binary.BigEndian.Uint64(entry[0:8]),
			storedSize:  binary.BigEndian.Uint32(entry[8:12]),
			rawSize:     binary.BigEndian.Uint32(entry[12:16]),
			extentCount: binary.BigEndian.Uint32(entry[16:20]),
			hash:        binary.BigEndian.Uint64(entry[24:32]),
		}
		if page.storedSize == 0 || page.storedSize > MaxStoredPageBytes ||
			page.rawSize == 0 || page.rawSize > MaxRawPageBytes ||
			page.extentCount == 0 || !allZero(entry[20:24]) {
			return nil, nil, fmt.Errorf("%w: data page entry=%d", ErrInvalidPack, i)
		}
		extentCount += uint64(page.extentCount)
		dataPages = append(dataPages, page)
		off += DataPageTableEntrySize
	}
	if extentCount != uint64(wantExtents) {
		return nil, nil, fmt.Errorf("%w: data page extents=%d want=%d", ErrInvalidPack, extentCount, wantExtents)
	}

	indexPages := make([]indexPageRef, 0, indexCount)
	var indexedExtents uint64
	var indexedRecords uint64
	for i := uint32(0); i < indexCount; i++ {
		entry := buf[off : off+IndexRootEntrySize]
		var page indexPageRef
		copy(page.firstHash[:], entry[0:16])
		copy(page.lastHash[:], entry[16:32])
		page.offset = binary.BigEndian.Uint64(entry[32:40])
		page.length = binary.BigEndian.Uint32(entry[40:44])
		page.entryCount = binary.BigEndian.Uint32(entry[44:48])
		page.recordCount = binary.BigEndian.Uint32(entry[48:52])
		page.hash = binary.BigEndian.Uint64(entry[56:64])
		if bytes.Compare(page.firstHash[:], page.lastHash[:]) > 0 ||
			page.entryCount == 0 || page.recordCount == 0 ||
			uint64(page.length) != uint64(IndexPagePreambleSize)+uint64(page.entryCount)*TimelineIndexEntrySize ||
			page.length > MaxIndexPageBytes || !allZero(entry[52:56]) {
			return nil, nil, fmt.Errorf("%w: index root entry=%d", ErrInvalidPack, i)
		}
		if i > 0 && bytes.Compare(page.firstHash[:], indexPages[i-1].lastHash[:]) < 0 {
			return nil, nil, fmt.Errorf("%w: overlapping index hash range=%d", ErrInvalidPack, i)
		}
		indexedExtents += uint64(page.entryCount)
		indexedRecords += uint64(page.recordCount)
		indexPages = append(indexPages, page)
		off += IndexRootEntrySize
	}
	if off != len(buf) || indexedExtents != uint64(wantExtents) || indexedRecords != uint64(wantRecords) {
		return nil, nil, fmt.Errorf("%w: index root totals extents=%d/%d records=%d/%d", ErrInvalidPack, indexedExtents, wantExtents, indexedRecords, wantRecords)
	}

	wantOffset := uint64(PreambleSize)
	for i, page := range dataPages {
		if page.offset != wantOffset {
			return nil, nil, fmt.Errorf("%w: data page=%d offset=%d want=%d", ErrInvalidPack, i, page.offset, wantOffset)
		}
		wantOffset += uint64(PagePreambleSize) + uint64(page.storedSize)
		if wantOffset > rootOffset {
			return nil, nil, fmt.Errorf("%w: data page=%d crosses index", ErrInvalidPack, i)
		}
	}
	for i, page := range indexPages {
		if page.offset != wantOffset {
			return nil, nil, fmt.Errorf("%w: index page=%d offset=%d want=%d", ErrInvalidPack, i, page.offset, wantOffset)
		}
		wantOffset += uint64(page.length)
		if wantOffset > rootOffset {
			return nil, nil, fmt.Errorf("%w: index page=%d crosses root", ErrInvalidPack, i)
		}
	}
	if wantOffset != rootOffset {
		return nil, nil, fmt.Errorf("%w: indexed regions end=%d root=%d", ErrInvalidPack, wantOffset, rootOffset)
	}
	return dataPages, indexPages, nil
}

func parseIndexPage(buf []byte, ref indexPageRef, dataPages []pageEntry) ([]indexEntry, error) {
	if len(buf) != int(ref.length) || len(buf) < IndexPagePreambleSize ||
		!bytes.Equal(buf[0:4], indexPageMagic[:]) {
		return nil, fmt.Errorf("%w: index page preamble", ErrInvalidPack)
	}
	if binary.BigEndian.Uint16(buf[4:6]) != Version ||
		binary.BigEndian.Uint16(buf[6:8]) != IndexPagePreambleSize ||
		binary.BigEndian.Uint32(buf[8:12]) != ref.entryCount ||
		binary.BigEndian.Uint32(buf[12:16]) != ref.recordCount ||
		!allZero(buf[16:32]) {
		return nil, fmt.Errorf("%w: index page metadata", ErrInvalidPack)
	}
	entries := make([]indexEntry, 0, ref.entryCount)
	off := IndexPagePreambleSize
	var recordCount uint64
	for i := uint32(0); i < ref.entryCount; i++ {
		raw := buf[off : off+TimelineIndexEntrySize]
		var entry indexEntry
		copy(entry.keyHash[:], raw[0:16])
		entry.pageNo = binary.BigEndian.Uint32(raw[16:20])
		entry.extentOffset = binary.BigEndian.Uint32(raw[20:24])
		entry.extentLength = binary.BigEndian.Uint32(raw[24:28])
		entry.recordCount = binary.BigEndian.Uint32(raw[28:32])
		entry.firstLSN = binary.BigEndian.Uint64(raw[32:40])
		if entry.pageNo >= uint32(len(dataPages)) || entry.recordCount == 0 ||
			entry.extentLength < ExtentPreambleSize+1 ||
			entry.firstLSN > math.MaxUint64-uint64(entry.recordCount) ||
			!allZero(raw[40:48]) {
			return nil, fmt.Errorf("%w: timeline index entry=%d", ErrInvalidPack, i)
		}
		page := dataPages[entry.pageNo]
		if uint64(entry.extentOffset)+uint64(entry.extentLength) > uint64(page.rawSize) {
			return nil, fmt.Errorf("%w: timeline index extent=%d", ErrInvalidPack, i)
		}
		if i > 0 && bytes.Compare(entry.keyHash[:], entries[i-1].keyHash[:]) < 0 {
			return nil, fmt.Errorf("%w: unsorted timeline index entry=%d", ErrInvalidPack, i)
		}
		if bytes.Compare(entry.keyHash[:], ref.firstHash[:]) < 0 ||
			bytes.Compare(entry.keyHash[:], ref.lastHash[:]) > 0 {
			return nil, fmt.Errorf("%w: timeline index hash range entry=%d", ErrInvalidPack, i)
		}
		recordCount += uint64(entry.recordCount)
		entries = append(entries, entry)
		off += TimelineIndexEntrySize
	}
	if off != len(buf) || recordCount != uint64(ref.recordCount) ||
		entries[0].keyHash != ref.firstHash || entries[len(entries)-1].keyHash != ref.lastHash {
		return nil, fmt.Errorf("%w: index page totals or bounds", ErrInvalidPack)
	}
	return entries, nil
}
