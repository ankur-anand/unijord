package rangeread

import (
	"bytes"
	"math"
	"testing"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	bounds, err := Validate(7, 5)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if bounds.Offset != 7 || bounds.Count != 5 || bounds.End != 11 {
		t.Fatalf("Validate() = %+v", bounds)
	}
	if _, err := Validate(math.MaxInt64, 2); err == nil {
		t.Fatal("Validate(overflow) error = nil")
	}
}

func TestReadExactRejectsShortAndLongBodies(t *testing.T) {
	t.Parallel()

	if _, err := ReadExact(bytes.NewReader([]byte("ab")), 3); err == nil {
		t.Fatal("ReadExact(short) error = nil")
	}
	if _, err := ReadExact(bytes.NewReader([]byte("abcd")), 3); err == nil {
		t.Fatal("ReadExact(long) error = nil")
	}
	got, err := ReadExact(bytes.NewReader([]byte("abc")), 3)
	if err != nil {
		t.Fatalf("ReadExact() error = %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("ReadExact() = %q", got)
	}
}
