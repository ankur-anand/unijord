package runfile

import (
	"encoding/binary"
)

// Validate checks the variable preamble fields as caller-provided build input.
func (p Preamble) Validate() error {
	return validatePreamble(p, false)
}

func validatePreamble(p Preamble, persisted bool) error {
	errorf := invalidRunf
	if persisted {
		errorf = corruptRunf
	}
	if p.CreatorRole != CreatorRoleWriterFlush && p.CreatorRole != CreatorRoleCompactionOutput {
		return errorf("invalid creator role %d", p.CreatorRole)
	}
	if p.CreatorEpoch == 0 {
		return errorf("creator epoch is zero")
	}
	if p.SeqLo > p.SeqHi {
		return errorf("sequence range [%d,%d] is inverted", p.SeqLo, p.SeqHi)
	}
	if allZero(p.NamespaceHash[:]) {
		return errorf("namespace hash is zero")
	}
	if allZero(p.RunID[:]) {
		return errorf("run ID is zero")
	}
	if allZero(p.PublicationHash[:]) {
		return errorf("publication hash is zero")
	}
	return nil
}

// MarshalPreamble encodes one exact version-1 UJRN preamble.
func MarshalPreamble(p Preamble) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	encoded := make([]byte, PreambleBytes)
	copy(encoded[0:4], PreambleMagic)
	binary.BigEndian.PutUint16(encoded[4:6], FormatVersion)
	binary.BigEndian.PutUint16(encoded[6:8], PreambleBytes)
	binary.BigEndian.PutUint32(encoded[8:12], 0)
	encoded[12] = byte(p.CreatorRole)
	encoded[13] = byte(HashAlgorithmSHA256)
	binary.BigEndian.PutUint32(encoded[16:20], p.Shard)
	binary.BigEndian.PutUint64(encoded[24:32], p.CreatorEpoch)
	binary.BigEndian.PutUint64(encoded[32:40], p.SeqLo)
	binary.BigEndian.PutUint64(encoded[40:48], p.SeqHi)
	copy(encoded[48:80], p.NamespaceHash[:])
	copy(encoded[80:96], p.RunID[:])
	copy(encoded[96:128], p.PublicationHash[:])
	return encoded, nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (p Preamble) MarshalBinary() ([]byte, error) {
	return MarshalPreamble(p)
}

// UnmarshalPreamble decodes and validates one exact version-1 UJRN preamble.
func UnmarshalPreamble(encoded []byte) (Preamble, error) {
	var p Preamble
	if len(encoded) != PreambleBytes {
		return p, corruptRunf("preamble length %d, want %d", len(encoded), PreambleBytes)
	}
	if string(encoded[0:4]) != PreambleMagic {
		return p, corruptRunf("invalid preamble magic %x", encoded[0:4])
	}
	version := binary.BigEndian.Uint16(encoded[4:6])
	if version != FormatVersion {
		return p, unsupportedRunf("preamble version %d", version)
	}
	if headerBytes := binary.BigEndian.Uint16(encoded[6:8]); headerBytes != PreambleBytes {
		return p, corruptRunf("preamble header bytes %d, want %d", headerBytes, PreambleBytes)
	}
	if flags := binary.BigEndian.Uint32(encoded[8:12]); flags != 0 {
		return p, corruptRunf("unknown preamble flags %#x", flags)
	}
	if algorithm := HashAlgorithm(encoded[13]); algorithm != HashAlgorithmSHA256 {
		return p, unsupportedRunf("preamble hash algorithm %d", algorithm)
	}
	if !allZero(encoded[14:16]) {
		return p, corruptRunf("non-zero preamble reserved0")
	}
	if !allZero(encoded[20:24]) {
		return p, corruptRunf("non-zero preamble reserved1")
	}

	p.CreatorRole = CreatorRole(encoded[12])
	p.Shard = binary.BigEndian.Uint32(encoded[16:20])
	p.CreatorEpoch = binary.BigEndian.Uint64(encoded[24:32])
	p.SeqLo = binary.BigEndian.Uint64(encoded[32:40])
	p.SeqHi = binary.BigEndian.Uint64(encoded[40:48])
	copy(p.NamespaceHash[:], encoded[48:80])
	copy(p.RunID[:], encoded[80:96])
	copy(p.PublicationHash[:], encoded[96:128])
	if err := validatePreamble(p, true); err != nil {
		return Preamble{}, err
	}
	return p, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The receiver is
// changed only after complete validation succeeds.
func (p *Preamble) UnmarshalBinary(encoded []byte) error {
	if p == nil {
		return invalidRunf("nil preamble receiver")
	}
	decoded, err := UnmarshalPreamble(encoded)
	if err != nil {
		return err
	}
	*p = decoded
	return nil
}
