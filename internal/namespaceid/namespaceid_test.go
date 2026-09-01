package namespaceid

import (
	"encoding/hex"
	"testing"
)

func TestStableVector(t *testing.T) {
	const want = "3ff1ae2db4885a0ade2ecd7d6de3273370a6ac4d6e2f2f75643af361d1f288d7"
	got := Sum([]byte("tenant-a"))
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("Sum(tenant-a)=%x want=%s", got, want)
	}
}

func TestEncodingIsLengthDelimited(t *testing.T) {
	if Sum([]byte("ab")) == Sum([]byte("a")) || Sum(nil) == ([32]byte{}) {
		t.Fatal("namespace digest is not distinct and non-zero")
	}
}
