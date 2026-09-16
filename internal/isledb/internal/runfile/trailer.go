package runfile

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	trailerCRCOffset = 12
	trailerCRCEnd    = 16
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Validate checks the variable trailer fields as caller-provided build input.
func (t Trailer) Validate() error {
	return validateTrailer(t, false)
}

func validateTrailer(t Trailer, persisted bool) error {
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}
	if t.DirectoryOffset < PreambleBytes {
		return errorf("directory offset %d precedes preamble", t.DirectoryOffset)
	}
	if t.DirectoryOffset%RegionAlignment != 0 {
		return errorf("directory offset %d is not %d-byte aligned", t.DirectoryOffset, RegionAlignment)
	}
	if t.DirectoryLength == 0 {
		return errorf("directory length is zero")
	}
	if t.DirectoryLength > MaxDirectoryBytes {
		return runTooLargef("directory length %d exceeds %d", t.DirectoryLength, MaxDirectoryBytes)
	}
	if t.RegionCount < MinRegionCount || t.RegionCount > MaxRegionCount {
		return errorf("region count %d outside [%d,%d]", t.RegionCount, MinRegionCount, MaxRegionCount)
	}
	if t.ObjectSize > MaxRunObjectBytes {
		return runTooLargef("object size %d exceeds %d", t.ObjectSize, MaxRunObjectBytes)
	}
	directoryEnd, ok := checkedAdd(t.DirectoryOffset, t.DirectoryLength)
	if !ok {
		return errorf("directory range overflows")
	}
	expectedObjectSize, ok := checkedAdd(directoryEnd, TrailerBytes)
	if !ok {
		return errorf("object size overflows")
	}
	if expectedObjectSize != t.ObjectSize {
		return errorf("object size %d, want %d from directory framing", t.ObjectSize, expectedObjectSize)
	}
	if allZero(t.RunID[:]) {
		return errorf("run ID is zero")
	}
	return nil
}

// ValidateObjectSize checks a decoded trailer against the object's actual
// finalized size.
func (t Trailer) ValidateObjectSize(actual uint64) error {
	if actual > MaxRunObjectBytes {
		return runTooLargef("actual object size %d exceeds %d", actual, MaxRunObjectBytes)
	}
	if actual != t.ObjectSize {
		return corruptRunf("actual object size %d, trailer records %d", actual, t.ObjectSize)
	}
	return nil
}

func trailerCRC32C(encoded []byte) (uint32, bool) {
	if len(encoded) != TrailerBytes {
		return 0, false
	}
	var scratch [TrailerBytes]byte
	copy(scratch[:], encoded)
	clear(scratch[trailerCRCOffset:trailerCRCEnd])
	return crc32.Checksum(scratch[:], castagnoliTable), true
}

// MarshalTrailer encodes one exact version-1 UJRT trailer and derives its
// CRC-32C with the checksum field zeroed.
func MarshalTrailer(t Trailer) ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}

	encoded := make([]byte, TrailerBytes)
	copy(encoded[0:4], TrailerMagic)
	binary.BigEndian.PutUint16(encoded[4:6], FormatVersion)
	binary.BigEndian.PutUint16(encoded[6:8], TrailerBytes)
	binary.BigEndian.PutUint32(encoded[8:12], 0)
	binary.BigEndian.PutUint64(encoded[16:24], t.DirectoryOffset)
	binary.BigEndian.PutUint64(encoded[24:32], t.DirectoryLength)
	binary.BigEndian.PutUint64(encoded[32:40], t.ObjectSize)
	binary.BigEndian.PutUint16(encoded[40:42], t.RegionCount)
	encoded[42] = byte(HashAlgorithmSHA256)
	copy(encoded[48:80], t.DirectoryHash[:])
	copy(encoded[80:112], t.PayloadHash[:])
	copy(encoded[112:128], t.RunID[:])
	checksum, _ := trailerCRC32C(encoded)
	binary.BigEndian.PutUint32(encoded[trailerCRCOffset:trailerCRCEnd], checksum)
	return encoded, nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (t Trailer) MarshalBinary() ([]byte, error) {
	return MarshalTrailer(t)
}

// UnmarshalTrailer decodes and validates one exact version-1 UJRT trailer.
func UnmarshalTrailer(encoded []byte) (Trailer, error) {
	var t Trailer
	if len(encoded) != TrailerBytes {
		return t, corruptRunf("trailer length %d, want %d", len(encoded), TrailerBytes)
	}
	if string(encoded[0:4]) != TrailerMagic {
		return t, corruptRunf("invalid trailer magic %x", encoded[0:4])
	}
	version := binary.BigEndian.Uint16(encoded[4:6])
	if version != FormatVersion {
		return t, unsupportedRunf("trailer version %d", version)
	}
	if trailerBytes := binary.BigEndian.Uint16(encoded[6:8]); trailerBytes != TrailerBytes {
		return t, corruptRunf("trailer bytes %d, want %d", trailerBytes, TrailerBytes)
	}
	if flags := binary.BigEndian.Uint32(encoded[8:12]); flags != 0 {
		return t, corruptRunf("unknown trailer flags %#x", flags)
	}
	storedChecksum := binary.BigEndian.Uint32(encoded[trailerCRCOffset:trailerCRCEnd])
	computedChecksum, _ := trailerCRC32C(encoded)
	if storedChecksum != computedChecksum {
		return t, corruptRunf("trailer CRC-32C %#08x, want %#08x", storedChecksum, computedChecksum)
	}
	if algorithm := HashAlgorithm(encoded[42]); algorithm != HashAlgorithmSHA256 {
		return t, unsupportedRunf("trailer hash algorithm %d", algorithm)
	}
	if !allZero(encoded[43:48]) {
		return t, corruptRunf("non-zero trailer reserved0")
	}
	if !allZero(encoded[128:160]) {
		return t, corruptRunf("non-zero trailer reserved1")
	}
	t.DirectoryOffset = binary.BigEndian.Uint64(encoded[16:24])
	t.DirectoryLength = binary.BigEndian.Uint64(encoded[24:32])
	t.ObjectSize = binary.BigEndian.Uint64(encoded[32:40])
	t.RegionCount = binary.BigEndian.Uint16(encoded[40:42])
	copy(t.DirectoryHash[:], encoded[48:80])
	copy(t.PayloadHash[:], encoded[80:112])
	copy(t.RunID[:], encoded[112:128])
	if err := validateTrailer(t, true); err != nil {
		return Trailer{}, err
	}
	return t, nil
}

// UnmarshalTrailerForObject additionally binds the trailer to an actual object
// size supplied by the range source.
func UnmarshalTrailerForObject(encoded []byte, actualObjectSize uint64) (Trailer, error) {
	if actualObjectSize > MaxRunObjectBytes {
		return Trailer{}, runTooLargef("actual object size %d exceeds %d", actualObjectSize, MaxRunObjectBytes)
	}
	t, err := UnmarshalTrailer(encoded)
	if err != nil {
		return Trailer{}, err
	}
	if err := t.ValidateObjectSize(actualObjectSize); err != nil {
		return Trailer{}, err
	}
	return t, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The receiver is
// changed only after complete validation succeeds. Object-size binding remains
// the caller's responsibility through ValidateObjectSize.
func (t *Trailer) UnmarshalBinary(encoded []byte) error {
	if t == nil {
		return invalidRunf("nil trailer receiver")
	}
	decoded, err := UnmarshalTrailer(encoded)
	if err != nil {
		return err
	}
	*t = decoded
	return nil
}
