package runfile

import (
	"bytes"
	"encoding/binary"
)

// RegionKind identifies a region descriptor's contents.
type RegionKind uint16

const (
	RegionKindEventsSST      RegionKind = 1
	RegionKindHeadsSST       RegionKind = 2
	RegionKindTimelineFilter RegionKind = 3
)

// RegionEncoding identifies a kind-specific region encoding.
type RegionEncoding uint16

const (
	// RegionEncodingV1 is the version-1 encoding for each known region kind.
	// The numeric value is interpreted in the context of RegionKind.
	RegionEncodingV1 RegionEncoding = 1
)

const regionFlagRequired uint16 = 1

// RegionDescriptor is the decoded, owned form of one UJRD region descriptor.
// Key byte slices are copied during decoding.
type RegionDescriptor struct {
	Kind        RegionKind
	Required    bool
	Encoding    RegionEncoding
	Offset      uint64
	Length      uint64
	EntryCount  uint64
	SeqLo       uint64
	SeqHi       uint64
	MinKey      []byte
	MaxKey      []byte
	ContentHash [SHA256Bytes]byte
}

// Directory is the decoded, owned form of one UJRD directory.
// DirectoryOffset is object-relative context supplied by the trailer and is
// not repeated in the directory bytes.
type Directory struct {
	DirectoryOffset uint64
	MinTimeline     []byte
	MaxTimeline     []byte
	Regions         []RegionDescriptor
}

type blobReference struct {
	offset uint32
	length uint32
}

type regionLayout struct {
	minKey blobReference
	maxKey blobReference
}

type directoryLayout struct {
	entriesLength uint64
	keyBlobOffset uint64
	keyBlobLength uint64
	totalBytes    uint64
	minTimeline   blobReference
	maxTimeline   blobReference
	regions       []regionLayout
}

type wireRegion struct {
	value  RegionDescriptor
	minKey blobReference
	maxKey blobReference
}

// Validate checks a directory as caller-provided build input.
func (d Directory) Validate() error {
	_, err := planDirectory(d, false)
	return err
}

func planDirectory(d Directory, persisted bool) (directoryLayout, error) {
	var layout directoryLayout
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}

	if d.DirectoryOffset < PreambleBytes {
		return layout, errorf("directory offset %d precedes preamble", d.DirectoryOffset)
	}
	if d.DirectoryOffset%RegionAlignment != 0 {
		return layout, errorf("directory offset %d is not %d-byte aligned", d.DirectoryOffset, RegionAlignment)
	}
	if len(d.Regions) < int(MinRegionCount) || len(d.Regions) > int(MaxRegionCount) {
		return layout, errorf("region count %d outside [%d,%d]", len(d.Regions), MinRegionCount, MaxRegionCount)
	}

	if err := validateRegionDescriptors(d, errorf); err != nil {
		return layout, err
	}
	if bytes.Compare(d.MinTimeline, d.MaxTimeline) > 0 {
		return layout, errorf("minimum timeline is greater than maximum timeline")
	}

	var cursor uint64
	var err error
	if layout.minTimeline, err = appendBlobReference(&cursor, d.MinTimeline, false, MaxTimelineBytes, "minimum timeline", errorf); err != nil {
		return directoryLayout{}, err
	}
	if layout.maxTimeline, err = appendBlobReference(&cursor, d.MaxTimeline, false, MaxTimelineBytes, "maximum timeline", errorf); err != nil {
		return directoryLayout{}, err
	}
	layout.regions = make([]regionLayout, len(d.Regions))
	for i := range d.Regions {
		if layout.regions[i].minKey, err = appendBlobReference(&cursor, d.Regions[i].MinKey, true, MaxTableKeyBytes, "minimum key", errorf); err != nil {
			return directoryLayout{}, err
		}
		if layout.regions[i].maxKey, err = appendBlobReference(&cursor, d.Regions[i].MaxKey, true, MaxTableKeyBytes, "maximum key", errorf); err != nil {
			return directoryLayout{}, err
		}
	}
	layout.keyBlobLength = cursor

	entriesLength, ok := checkedMultiply(uint64(len(d.Regions)), RegionDescriptorBytes)
	if !ok {
		return directoryLayout{}, errorf("region descriptor length overflows")
	}
	layout.entriesLength = entriesLength
	keyBlobOffset, ok := checkedAdd(DirectoryHeaderBytes, entriesLength)
	if !ok {
		return directoryLayout{}, errorf("key blob offset overflows")
	}
	layout.keyBlobOffset = keyBlobOffset
	totalBytes, ok := checkedAdd(keyBlobOffset, layout.keyBlobLength)
	if !ok {
		return directoryLayout{}, errorf("directory length overflows")
	}
	if totalBytes > MaxDirectoryBytes {
		return directoryLayout{}, runTooLargef("directory length %d exceeds %d", totalBytes, MaxDirectoryBytes)
	}
	directoryEnd, ok := checkedAdd(d.DirectoryOffset, totalBytes)
	if !ok {
		return directoryLayout{}, errorf("directory range overflows")
	}
	objectSize, ok := checkedAdd(directoryEnd, TrailerBytes)
	if !ok {
		return directoryLayout{}, errorf("object size overflows")
	}
	if objectSize > MaxRunObjectBytes {
		return directoryLayout{}, runTooLargef("object size %d exceeds %d", objectSize, MaxRunObjectBytes)
	}
	layout.totalBytes = totalBytes
	return layout, nil
}

func validateRegionDescriptors(d Directory, errorf func(string, ...any) error) error {
	var (
		foundEvents  bool
		foundHeads   bool
		previousEnd  uint64
		previousKind RegionKind
	)
	for i := range d.Regions {
		region := d.Regions[i]
		if i > 0 {
			if region.Kind == previousKind {
				return errorf("duplicate region kind %d", region.Kind)
			}
			if region.Kind < previousKind {
				return errorf("region kind %d follows %d", region.Kind, previousKind)
			}
		}
		if region.Offset < PreambleBytes {
			return errorf("region kind %d offset %d precedes preamble", region.Kind, region.Offset)
		}
		if region.Offset%RegionAlignment != 0 {
			return errorf("region kind %d offset %d is not %d-byte aligned", region.Kind, region.Offset, RegionAlignment)
		}
		if region.Length == 0 {
			return errorf("region kind %d has zero length", region.Kind)
		}
		regionEnd, ok := checkedRangeEnd(region.Offset, region.Length)
		if !ok {
			return errorf("region kind %d range overflows", region.Kind)
		}
		if regionEnd > d.DirectoryOffset {
			return errorf("region kind %d ends at %d beyond directory offset %d", region.Kind, regionEnd, d.DirectoryOffset)
		}
		if i > 0 && region.Offset < previousEnd {
			return errorf("region kind %d overlaps or precedes prior region", region.Kind)
		}
		if allZero(region.ContentHash[:]) {
			return errorf("region kind %d content hash is zero", region.Kind)
		}

		switch region.Kind {
		case RegionKindEventsSST, RegionKindHeadsSST:
			if !region.Required {
				return errorf("region kind %d is not marked required", region.Kind)
			}
			if region.Encoding != RegionEncodingV1 {
				return unsupportedRunf("required region kind %d encoding %d", region.Kind, region.Encoding)
			}
			if region.EntryCount == 0 {
				return errorf("required region kind %d has zero entries", region.Kind)
			}
			if region.SeqLo > region.SeqHi {
				return errorf("required region kind %d sequence range [%d,%d] is inverted", region.Kind, region.SeqLo, region.SeqHi)
			}
			if len(region.MinKey) == 0 || len(region.MaxKey) == 0 {
				return errorf("required region kind %d is missing key bounds", region.Kind)
			}
			if bytes.Compare(region.MinKey, region.MaxKey) > 0 {
				return errorf("required region kind %d minimum key is greater than maximum key", region.Kind)
			}
			if region.Kind == RegionKindEventsSST {
				foundEvents = true
			} else {
				foundHeads = true
			}

		case RegionKindTimelineFilter:
			if region.Required {
				return errorf("timeline filter is marked required")
			}
			// An unknown encoding remains ignorable because the filter is an
			// optional region. Only encoding 1 has version-1 semantics to check.
			if region.Encoding != RegionEncodingV1 {
				break
			}
			if region.EntryCount == 0 {
				return errorf("timeline filter has zero entries")
			}
			if region.SeqLo != 0 || region.SeqHi != 0 {
				return errorf("timeline filter has non-zero sequence bounds")
			}
			if len(region.MinKey) != 0 || len(region.MaxKey) != 0 {
				return errorf("timeline filter has key bounds")
			}

		default:
			if region.Kind <= RegionKindTimelineFilter {
				return errorf("invalid region kind %d", region.Kind)
			}
			if region.Required {
				return unsupportedRunf("unknown required region kind %d encoding %d", region.Kind, region.Encoding)
			}
		}

		previousKind = region.Kind
		previousEnd = regionEnd
	}
	if !foundEvents || !foundHeads {
		return errorf("directory is missing required Events or Heads region")
	}
	return nil
}

func appendBlobReference(cursor *uint64, value []byte, allowEmpty bool, maximum uint64, name string, errorf func(string, ...any) error) (blobReference, error) {
	if len(value) == 0 {
		if !allowEmpty {
			return blobReference{}, errorf("%s is empty", name)
		}
		return blobReference{}, nil
	}
	length := uint64(len(value))
	if length > maximum {
		return blobReference{}, runTooLargef("%s length %d exceeds %d", name, length, maximum)
	}
	end, ok := checkedAdd(*cursor, length)
	if !ok {
		return blobReference{}, errorf("%s reference overflows", name)
	}
	reference := blobReference{offset: uint32(*cursor), length: uint32(length)}
	*cursor = end
	return reference, nil
}

// MarshalDirectory encodes one canonical version-1 UJRD directory.
func MarshalDirectory(d Directory) ([]byte, error) {
	layout, err := planDirectory(d, false)
	if err != nil {
		return nil, err
	}

	encoded := make([]byte, int(layout.totalBytes))
	copy(encoded[0:4], DirectoryMagic)
	binary.BigEndian.PutUint16(encoded[4:6], FormatVersion)
	binary.BigEndian.PutUint16(encoded[6:8], DirectoryHeaderBytes)
	binary.BigEndian.PutUint16(encoded[8:10], uint16(len(d.Regions)))
	binary.BigEndian.PutUint16(encoded[10:12], RegionDescriptorBytes)
	binary.BigEndian.PutUint32(encoded[12:16], 0)
	binary.BigEndian.PutUint64(encoded[16:24], DirectoryHeaderBytes)
	binary.BigEndian.PutUint64(encoded[24:32], layout.entriesLength)
	binary.BigEndian.PutUint64(encoded[32:40], layout.keyBlobOffset)
	binary.BigEndian.PutUint64(encoded[40:48], layout.keyBlobLength)
	putBlobReference(encoded[48:56], layout.minTimeline)
	putBlobReference(encoded[56:64], layout.maxTimeline)

	for i := range d.Regions {
		base := DirectoryHeaderBytes + i*RegionDescriptorBytes
		region := d.Regions[i]
		binary.BigEndian.PutUint16(encoded[base:base+2], uint16(region.Kind))
		if region.Required {
			binary.BigEndian.PutUint16(encoded[base+2:base+4], regionFlagRequired)
		}
		binary.BigEndian.PutUint16(encoded[base+4:base+6], uint16(region.Encoding))
		binary.BigEndian.PutUint64(encoded[base+8:base+16], region.Offset)
		binary.BigEndian.PutUint64(encoded[base+16:base+24], region.Length)
		binary.BigEndian.PutUint64(encoded[base+24:base+32], region.EntryCount)
		binary.BigEndian.PutUint64(encoded[base+32:base+40], region.SeqLo)
		binary.BigEndian.PutUint64(encoded[base+40:base+48], region.SeqHi)
		putBlobReference(encoded[base+48:base+56], layout.regions[i].minKey)
		putBlobReference(encoded[base+56:base+64], layout.regions[i].maxKey)
		copy(encoded[base+64:base+96], region.ContentHash[:])
	}

	keyBlob := encoded[int(layout.keyBlobOffset):]
	copyBlobReference(keyBlob, layout.minTimeline, d.MinTimeline)
	copyBlobReference(keyBlob, layout.maxTimeline, d.MaxTimeline)
	for i := range d.Regions {
		copyBlobReference(keyBlob, layout.regions[i].minKey, d.Regions[i].MinKey)
		copyBlobReference(keyBlob, layout.regions[i].maxKey, d.Regions[i].MaxKey)
	}
	return encoded, nil
}

func putBlobReference(dst []byte, reference blobReference) {
	binary.BigEndian.PutUint32(dst[0:4], reference.offset)
	binary.BigEndian.PutUint32(dst[4:8], reference.length)
}

func copyBlobReference(blob []byte, reference blobReference, value []byte) {
	if reference.length == 0 {
		return
	}
	start := int(reference.offset)
	copy(blob[start:start+int(reference.length)], value)
}

// UnmarshalDirectory decodes and validates one exact version-1 UJRD directory.
// directoryOffset is the absolute object offset recorded in the trailer.
func UnmarshalDirectory(encoded []byte, directoryOffset uint64) (Directory, error) {
	var d Directory
	if uint64(len(encoded)) > MaxDirectoryBytes {
		return d, runTooLargef("directory length %d exceeds %d", len(encoded), MaxDirectoryBytes)
	}
	if len(encoded) < DirectoryHeaderBytes {
		return d, corruptRunf("directory length %d is smaller than header %d", len(encoded), DirectoryHeaderBytes)
	}
	if directoryOffset < PreambleBytes {
		return d, corruptRunf("directory offset %d precedes preamble", directoryOffset)
	}
	if directoryOffset%RegionAlignment != 0 {
		return d, corruptRunf("directory offset %d is not %d-byte aligned", directoryOffset, RegionAlignment)
	}
	directoryEnd, ok := checkedAdd(directoryOffset, uint64(len(encoded)))
	if !ok {
		return d, corruptRunf("directory range overflows")
	}
	objectSize, ok := checkedAdd(directoryEnd, TrailerBytes)
	if !ok {
		return d, corruptRunf("object size overflows")
	}
	if objectSize > MaxRunObjectBytes {
		return d, runTooLargef("object size %d exceeds %d", objectSize, MaxRunObjectBytes)
	}
	if string(encoded[0:4]) != DirectoryMagic {
		return d, corruptRunf("invalid directory magic %x", encoded[0:4])
	}
	version := binary.BigEndian.Uint16(encoded[4:6])
	if version != FormatVersion {
		return d, unsupportedRunf("directory version %d", version)
	}
	if headerBytes := binary.BigEndian.Uint16(encoded[6:8]); headerBytes != DirectoryHeaderBytes {
		return d, corruptRunf("directory header bytes %d, want %d", headerBytes, DirectoryHeaderBytes)
	}
	regionCount := binary.BigEndian.Uint16(encoded[8:10])
	if regionCount < MinRegionCount || regionCount > MaxRegionCount {
		return d, corruptRunf("region count %d outside [%d,%d]", regionCount, MinRegionCount, MaxRegionCount)
	}
	if entryBytes := binary.BigEndian.Uint16(encoded[10:12]); entryBytes != RegionDescriptorBytes {
		return d, corruptRunf("region entry bytes %d, want %d", entryBytes, RegionDescriptorBytes)
	}
	if flags := binary.BigEndian.Uint32(encoded[12:16]); flags != 0 {
		return d, corruptRunf("unknown directory flags %#x", flags)
	}
	if entriesOffset := binary.BigEndian.Uint64(encoded[16:24]); entriesOffset != DirectoryHeaderBytes {
		return d, corruptRunf("entries offset %d, want %d", entriesOffset, DirectoryHeaderBytes)
	}
	expectedEntriesLength, ok := checkedMultiply(uint64(regionCount), RegionDescriptorBytes)
	if !ok {
		return d, corruptRunf("region descriptor length overflows")
	}
	if entriesLength := binary.BigEndian.Uint64(encoded[24:32]); entriesLength != expectedEntriesLength {
		return d, corruptRunf("entries length %d, want %d", entriesLength, expectedEntriesLength)
	}
	expectedKeyBlobOffset, ok := checkedAdd(DirectoryHeaderBytes, expectedEntriesLength)
	if !ok {
		return d, corruptRunf("key blob offset overflows")
	}
	if keyBlobOffset := binary.BigEndian.Uint64(encoded[32:40]); keyBlobOffset != expectedKeyBlobOffset {
		return d, corruptRunf("key blob offset %d, want %d", keyBlobOffset, expectedKeyBlobOffset)
	}
	if expectedKeyBlobOffset > uint64(len(encoded)) {
		return d, corruptRunf("key blob offset %d exceeds directory length %d", expectedKeyBlobOffset, len(encoded))
	}
	expectedKeyBlobLength := uint64(len(encoded)) - expectedKeyBlobOffset
	if keyBlobLength := binary.BigEndian.Uint64(encoded[40:48]); keyBlobLength != expectedKeyBlobLength {
		return d, corruptRunf("key blob length %d, want %d", keyBlobLength, expectedKeyBlobLength)
	}

	minimumTimeline := blobReferenceFrom(encoded[48:56])
	maximumTimeline := blobReferenceFrom(encoded[56:64])
	var wireRegions [MaxRegionCount]wireRegion
	for i := 0; i < int(regionCount); i++ {
		base := DirectoryHeaderBytes + i*RegionDescriptorBytes
		flags := binary.BigEndian.Uint16(encoded[base+2 : base+4])
		if flags & ^regionFlagRequired != 0 {
			return d, corruptRunf("region %d has unknown flags %#x", i, flags)
		}
		if reserved := binary.BigEndian.Uint16(encoded[base+6 : base+8]); reserved != 0 {
			return d, corruptRunf("region %d has non-zero reserved0", i)
		}
		if !allZero(encoded[base+96 : base+128]) {
			return d, corruptRunf("region %d has non-zero reserved1", i)
		}

		wireRegions[i] = wireRegion{
			value: RegionDescriptor{
				Kind:       RegionKind(binary.BigEndian.Uint16(encoded[base : base+2])),
				Required:   flags&regionFlagRequired != 0,
				Encoding:   RegionEncoding(binary.BigEndian.Uint16(encoded[base+4 : base+6])),
				Offset:     binary.BigEndian.Uint64(encoded[base+8 : base+16]),
				Length:     binary.BigEndian.Uint64(encoded[base+16 : base+24]),
				EntryCount: binary.BigEndian.Uint64(encoded[base+24 : base+32]),
				SeqLo:      binary.BigEndian.Uint64(encoded[base+32 : base+40]),
				SeqHi:      binary.BigEndian.Uint64(encoded[base+40 : base+48]),
			},
			minKey: blobReferenceFrom(encoded[base+48 : base+56]),
			maxKey: blobReferenceFrom(encoded[base+56 : base+64]),
		}
		copy(wireRegions[i].value.ContentHash[:], encoded[base+64:base+96])
	}

	keyBlob := encoded[int(expectedKeyBlobOffset):]
	var cursor uint64
	if err := consumeBlobReference(uint64(len(keyBlob)), &cursor, minimumTimeline, false, MaxTimelineBytes, "minimum timeline"); err != nil {
		return d, err
	}
	if err := consumeBlobReference(uint64(len(keyBlob)), &cursor, maximumTimeline, false, MaxTimelineBytes, "maximum timeline"); err != nil {
		return d, err
	}
	for i := 0; i < int(regionCount); i++ {
		if err := consumeBlobReference(uint64(len(keyBlob)), &cursor, wireRegions[i].minKey, true, MaxTableKeyBytes, "minimum key"); err != nil {
			return d, err
		}
		if err := consumeBlobReference(uint64(len(keyBlob)), &cursor, wireRegions[i].maxKey, true, MaxTableKeyBytes, "maximum key"); err != nil {
			return d, err
		}
	}
	if cursor != uint64(len(keyBlob)) {
		return d, corruptRunf("key blob has %d unreferenced trailing bytes", uint64(len(keyBlob))-cursor)
	}

	d.DirectoryOffset = directoryOffset
	d.MinTimeline = cloneBlobReference(keyBlob, minimumTimeline)
	d.MaxTimeline = cloneBlobReference(keyBlob, maximumTimeline)
	d.Regions = make([]RegionDescriptor, int(regionCount))
	for i := range d.Regions {
		d.Regions[i] = wireRegions[i].value
		d.Regions[i].MinKey = cloneBlobReference(keyBlob, wireRegions[i].minKey)
		d.Regions[i].MaxKey = cloneBlobReference(keyBlob, wireRegions[i].maxKey)
	}
	if _, err := planDirectory(d, true); err != nil {
		return Directory{}, err
	}
	return d, nil
}

func blobReferenceFrom(src []byte) blobReference {
	return blobReference{
		offset: binary.BigEndian.Uint32(src[0:4]),
		length: binary.BigEndian.Uint32(src[4:8]),
	}
}

func consumeBlobReference(blobLength uint64, cursor *uint64, reference blobReference, allowEmpty bool, maximum uint64, name string) error {
	if reference.length == 0 {
		if !allowEmpty {
			return corruptRunf("%s is empty", name)
		}
		if reference.offset != 0 {
			return corruptRunf("empty %s has offset %d", name, reference.offset)
		}
		return nil
	}
	if uint64(reference.length) > maximum {
		return runTooLargef("%s length %d exceeds %d", name, reference.length, maximum)
	}
	end, ok := checkedRangeEnd(uint64(reference.offset), uint64(reference.length))
	if !ok || end > blobLength {
		return corruptRunf("%s reference [%d,%d) exceeds key blob length %d", name, reference.offset, end, blobLength)
	}
	if uint64(reference.offset) != *cursor {
		return corruptRunf("noncanonical %s offset %d, want %d", name, reference.offset, *cursor)
	}
	*cursor = end
	return nil
}

func cloneBlobReference(blob []byte, reference blobReference) []byte {
	if reference.length == 0 {
		return nil
	}
	start := int(reference.offset)
	return bytes.Clone(blob[start : start+int(reference.length)])
}
