package segformat

import "testing"

func TestCRC32CCompatibilityVector(t *testing.T) {
	got, err := HashBytes(HashCRC32C, []byte("123456789"))
	if err != nil {
		t.Fatalf("HashBytes() error = %v", err)
	}
	if got != 0xe3069283 {
		t.Fatalf("CRC32C(123456789) = %016x, want 00000000e3069283", got)
	}
}
