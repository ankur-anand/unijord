// Package keylayout owns the canonical bucket hashing used in object-store keys
// for partitionlog catalog metadata and segment objects.
//
// The bucket scheme is part of the storage layout contract. Writers, readers,
// and GC tooling in any language must produce byte-identical keys.
package keylayout

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
)

const (
	// BucketBits is the number of low CRC32C bits used to derive the bucket.
	// 12 bits gives 4,096 buckets. Changing this rewrites every object-store key.
	BucketBits = 12

	// BucketHexLen is the number of lowercase hex characters used to format a
	// bucket. ceil(BucketBits / 4) = 3.
	BucketHexLen = 3

	// BucketCount is the number of distinct bucket values.
	BucketCount = 1 << BucketBits
)

const bucketMask uint32 = BucketCount - 1

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Bucket returns the layout bucket string for a stream partition.
//
// streamID is normalized before hashing. The storage layout treats leading and
// trailing slashes as path separators, not as part of stream identity.
//
// Computation:
//   - input = uint32 byte length of streamID, big-endian
//   - input += streamID bytes
//   - input += uint32 partition encoded as 4 bytes, big-endian
//   - hash = CRC32C Castagnoli of input
//   - bucket = hash & 0xfff
//   - output = lowercase hex, zero-padded to 3 characters
func Bucket(streamID string, partition uint32) string {
	sum := Checksum(streamID, partition)
	return fmt.Sprintf("%0*x", BucketHexLen, sum&bucketMask)
}

// Checksum returns the full CRC32C value behind Bucket.
//
// It is exported so conformance tests in other implementations can pin the
// complete hash, not only the low bucket bits.
func Checksum(streamID string, partition uint32) uint32 {
	streamID = NormalizeStreamID(streamID)
	h := crc32.New(castagnoliTable)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(streamID)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(streamID))

	var partitionBuf [4]byte
	binary.BigEndian.PutUint32(partitionBuf[:], partition)
	_, _ = h.Write(partitionBuf[:])

	return h.Sum32()
}

// NormalizeStreamID returns the canonical stream ID used by object-store key
// hashing and path builders.
func NormalizeStreamID(streamID string) string {
	return strings.Trim(streamID, "/")
}
