package postgres

import (
	"encoding/binary"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func decodeUint64(encoded []byte) (uint64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("%w: uint64 bytes=%d", metastore.ErrCorrupt, len(encoded))
	}
	return binary.BigEndian.Uint64(encoded), nil
}

func decodeHash(encoded []byte) ([32]byte, error) {
	if len(encoded) != 32 {
		return [32]byte{}, fmt.Errorf("%w: hash bytes=%d", metastore.ErrCorrupt, len(encoded))
	}
	var hash [32]byte
	copy(hash[:], encoded)
	return hash, nil
}
