package runfile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

type outerFramingVector struct {
	Preamble struct {
		ExpectedHex    string `json:"expected_hex"`
		ExpectedSHA256 string `json:"expected_sha256"`
	} `json:"preamble"`
	Directory struct {
		Offset         uint64 `json:"offset"`
		Length         uint64 `json:"length"`
		KeyBlobHex     string `json:"key_blob_hex"`
		ExpectedHex    string `json:"expected_hex"`
		ExpectedSHA256 string `json:"expected_sha256"`
	} `json:"directory"`
	Payload struct {
		ExpectedSHA256 string `json:"expected_sha256"`
	} `json:"payload"`
	Trailer struct {
		ExpectedHex    string `json:"expected_hex"`
		ExpectedCRC32C string `json:"expected_crc32c"`
		ExpectedSHA256 string `json:"expected_sha256"`
	} `json:"trailer"`
	ObjectSize uint64 `json:"object_size"`
}

func loadOuterFramingVector(t *testing.T) outerFramingVector {
	t.Helper()
	data, err := os.ReadFile("testdata/compat/v1/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Vectors []outerFramingVector `json:"outer_framing_vectors"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Vectors) != 1 {
		t.Fatalf("outer framing vectors=%d, want 1", len(manifest.Vectors))
	}
	return manifest.Vectors[0]
}

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func encodeHex(data []byte) string {
	return hex.EncodeToString(data)
}

func fillBytes[T ~[]byte](dst T, value byte) {
	for i := range dst {
		dst[i] = value
	}
}
