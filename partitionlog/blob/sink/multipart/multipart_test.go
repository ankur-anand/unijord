package multipart

import (
	"context"
	"crypto/sha256"
	"errors"
	"path"
	"testing"

	"github.com/google/uuid"
)

func TestValidateReceiptsRejectsEmpty(t *testing.T) {
	t.Parallel()

	if err := ValidateReceipts(nil); !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("ValidateReceipts(nil) error = %v, want %v", err, ErrInvalidStore)
	}
}

func TestNormalizeOptionsCreatesIsolatedSessionPrefix(t *testing.T) {
	t.Parallel()

	opts, err := NormalizeOptions("segments/final", Options{StagingPrefix: "segments/staging"})
	if err != nil {
		t.Fatalf("NormalizeOptions() error = %v", err)
	}
	if _, err := uuid.Parse(opts.SessionID); err != nil {
		t.Fatalf("SessionID = %q is not a UUID: %v", opts.SessionID, err)
	}
	if want := path.Join("segments/staging", opts.SessionID); opts.StagingPrefix != want {
		t.Fatalf("StagingPrefix = %q, want %q", opts.StagingPrefix, want)
	}
}

func TestNormalizeOptionsRejectsReservedMetadata(t *testing.T) {
	t.Parallel()

	_, err := NormalizeOptions("segments/final", Options{Metadata: map[string]string{
		"Unijord-Upload-Session": "caller-value",
	}})
	if !errors.Is(err, ErrInvalidStore) {
		t.Fatalf("NormalizeOptions() error = %v, want ErrInvalidStore", err)
	}
}

func TestValidatePartDetectsMutation(t *testing.T) {
	t.Parallel()

	bytes := []byte("original")
	part := NewPart(1, bytes)
	bytes[0] = 'X'
	if err := ValidatePart(part); !errors.Is(err, ErrPartConflict) {
		t.Fatalf("ValidatePart() error = %v, want ErrPartConflict", err)
	}
}

func TestMemorySessionRetryAndIdempotentCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryStore()
	session, err := store.Begin(ctx, "segments/final", Options{})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	part := NewPart(1, []byte("payload"))
	first, err := session.PutPart(ctx, part)
	if err != nil {
		t.Fatalf("PutPart(first) error = %v", err)
	}
	retry, err := session.PutPart(ctx, part)
	if err != nil {
		t.Fatalf("PutPart(retry) error = %v", err)
	}
	if retry != first {
		t.Fatalf("retry receipt = %+v, want %+v", retry, first)
	}
	if _, err := session.PutPart(ctx, NewPart(1, []byte("different"))); !errors.Is(err, ErrPartConflict) {
		t.Fatalf("PutPart(conflict) error = %v, want ErrPartConflict", err)
	}

	request := NewCommitRequest([]Receipt{first})
	request.ObjectSHA256 = sha256.Sum256([]byte("payload"))
	attrs, err := session.Commit(ctx, request)
	if err != nil {
		t.Fatalf("Commit(first) error = %v", err)
	}
	if attrs.SessionID == "" || attrs.ObjectSHA256 != request.ObjectSHA256 {
		t.Fatalf("committed identity = %+v", attrs)
	}
	retriedAttrs, err := session.Commit(ctx, request)
	if err != nil {
		t.Fatalf("Commit(retry) error = %v", err)
	}
	if retriedAttrs != attrs {
		t.Fatalf("retried attrs = %+v, want %+v", retriedAttrs, attrs)
	}
	if err := session.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup(after commit) error = %v", err)
	}
}

func TestMemorySessionCleanupIsTerminalForStaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	session, err := NewMemoryStore().Begin(ctx, "segments/final", Options{})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if err := session.Cleanup(ctx); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := session.PutPart(ctx, NewPart(1, []byte("x"))); !errors.Is(err, ErrCleaned) {
		t.Fatalf("PutPart(after cleanup) error = %v, want ErrCleaned", err)
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
