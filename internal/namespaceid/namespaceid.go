package namespaceid

import (
	"crypto/sha256"
	"encoding/binary"
)

const domain = "unijord/timeline-index/namespace/v1\x00"

// Sum returns the domain-separated digest of one exact opaque namespace key.
// The length prefix makes the input encoding unambiguous and matches the
// namespace identity already used by the metadata index.
func Sum(namespace []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(domain))
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(namespace)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(namespace)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func IsZero(digest [32]byte) bool { return digest == ([32]byte{}) }
