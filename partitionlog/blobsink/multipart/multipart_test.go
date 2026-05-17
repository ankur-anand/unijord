package multipart

import (
	"errors"
	"testing"
)

func TestValidateReceiptsRejectsEmpty(t *testing.T) {
	t.Parallel()

	if err := ValidateReceipts(nil); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("ValidateReceipts(nil) error = %v, want %v", err, ErrInvalidStore)
	}
}

func TestValidateReceiptsRejectsNonContiguous(t *testing.T) {
	t.Parallel()

	err := ValidateReceipts([]Receipt{
		{Number: 1},
		{Number: 3},
	})
	if !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("ValidateReceipts(non-contiguous) error = %v, want %v", err, ErrInvalidStore)
	}
}
