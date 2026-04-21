package segformat

import (
	"fmt"
	"hash/crc32"

	"github.com/cespare/xxhash/v2"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func NewCRC32C() hash.Hash32 {
	return crc32.New(crc32cTable)
}

func HashBytes(algo HashAlgo, data []byte) (uint64, error) {
	switch algo {
	case HashCRC32C:
		return uint64(crc32.Checksum(data, crc32cTable)), nil
	case HashXXH64:
		return xxhash.Sum64(data), nil
	default:
		return 0, fmt.Errorf("%w: %d", ErrUnsupportedHashAlgo, uint16(algo))
	}
}
