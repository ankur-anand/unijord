package segwriter

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/cespare/xxhash/v2"
)

func TestPackerForwardsOneOrderedByteStream(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.WriteBody(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("WriteBody(first) error = %v", err)
	}
	if err := p.WriteBody(context.Background(), []byte("def")); err != nil {
		t.Fatalf("WriteBody(second) error = %v", err)
	}
	if got := p.Offset(); got != 6 {
		t.Fatalf("Offset() = %d, want 6", got)
	}
	wantHash := xxhash.Sum64([]byte("abcdef"))
	if got := p.BodyHash(); got != wantHash {
		t.Fatalf("BodyHash() = %d, want %d", got, wantHash)
	}
	if err := p.WriteFinal(context.Background(), []byte("!")); err != nil {
		t.Fatalf("WriteFinal() error = %v", err)
	}
	obj, err := p.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if obj.SizeBytes != 7 || obj.URI == "" {
		t.Fatalf("Complete() = %+v, want size 7 and URI", obj)
	}
	if got := txn.objectBytes(); !bytes.Equal(got, []byte("abcdef!")) {
		t.Fatalf("committed bytes = %q, want %q", got, "abcdef!")
	}
	if got := txn.writeCalls(); got != 3 {
		t.Fatalf("Write calls = %d, want 3", got)
	}
}

func TestPackerBodyHashExcludesFinalBytes(t *testing.T) {
	t.Parallel()

	p := newTestPacker(t, newRecordingTxn(), segformat.HashCRC32C)
	if err := p.WriteBody(context.Background(), []byte("body")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	hasher := segformat.NewCRC32C()
	_, _ = hasher.Write([]byte("body"))
	want := uint64(hasher.Sum32())
	if got := p.BodyHash(); got != want {
		t.Fatalf("BodyHash() = %d, want %d", got, want)
	}
	if err := p.WriteFinal(context.Background(), []byte("trailer")); err != nil {
		t.Fatalf("WriteFinal() error = %v", err)
	}
	if got := p.BodyHash(); got != want {
		t.Fatalf("BodyHash(after final) = %d, want %d", got, want)
	}
}

func TestPackerEnforcesBodyAndFinalOrdering(t *testing.T) {
	t.Parallel()

	p := newTestPacker(t, newRecordingTxn(), segformat.HashXXH64)
	if err := p.WriteFinal(context.Background(), []byte("final")); !errors.Is(err, ErrBodyNotSealed) {
		t.Fatalf("WriteFinal(before BodyHash) error = %v, want ErrBodyNotSealed", err)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrBodyNotSealed) {
		t.Fatalf("Complete(before BodyHash) error = %v, want ErrBodyNotSealed", err)
	}
	_ = p.BodyHash()
	if err := p.WriteBody(context.Background(), []byte("body")); !errors.Is(err, ErrBodySealed) {
		t.Fatalf("WriteBody(after BodyHash) error = %v, want ErrBodySealed", err)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrEmptyObject) {
		t.Fatalf("Complete(empty) error = %v, want ErrEmptyObject", err)
	}
}

func TestPackerWriteFailureIsSticky(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("write failed")
	txn := newRecordingTxn()
	txn.writeErr = wantErr
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.WriteBody(context.Background(), []byte("body")); !errors.Is(err, wantErr) {
		t.Fatalf("WriteBody() error = %v, want %v", err, wantErr)
	}
	if got := p.Offset(); got != 0 {
		t.Fatalf("Offset() after failed Write = %d, want 0", got)
	}
	if err := p.WriteBody(context.Background(), []byte("again")); !errors.Is(err, wantErr) {
		t.Fatalf("later WriteBody() error = %v, want %v", err, wantErr)
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Complete() error = %v, want %v", err, wantErr)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if got := txn.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
}

func TestPackerWriteHonorsCallerContext(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	txn.writeGate = make(chan struct{})
	p := newTestPacker(t, txn, segformat.HashXXH64)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.WriteBody(ctx, []byte("body")); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteBody() error = %v, want context.Canceled", err)
	}
	if got := txn.writeCalls(); got != 0 {
		t.Fatalf("transaction Write calls = %d, want 0", got)
	}
}

func TestPackerRejectsInvalidCommittedObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(CommittedObject) CommittedObject
		contains string
	}{
		{
			name: "empty URI",
			mutate: func(obj CommittedObject) CommittedObject {
				obj.URI = ""
				return obj
			},
			contains: "empty object URI",
		},
		{
			name: "wrong size",
			mutate: func(obj CommittedObject) CommittedObject {
				obj.SizeBytes++
				return obj
			},
			contains: "accepted_bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := newRecordingTxn()
			txn.commitMutate = tt.mutate
			p := newTestPacker(t, txn, segformat.HashXXH64)
			if err := p.WriteBody(context.Background(), []byte("body")); err != nil {
				t.Fatalf("WriteBody() error = %v", err)
			}
			_ = p.BodyHash()
			_, err := p.Complete(context.Background())
			if !errors.Is(err, ErrSinkContract) || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("Complete() error = %v, want ErrSinkContract containing %q", err, tt.contains)
			}
			if got := txn.abortCount(); got != 1 {
				t.Fatalf("Abort calls = %d, want 1", got)
			}
		})
	}
}

func TestPackerPreservesContractAndCleanupFailures(t *testing.T) {
	t.Parallel()

	cleanupErr := errors.New("cleanup failed")
	txn := newRecordingTxn()
	txn.abortErr = cleanupErr
	txn.commitMutate = func(obj CommittedObject) CommittedObject {
		obj.URI = ""
		return obj
	}
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.WriteBody(context.Background(), []byte("body")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	_, err := p.Complete(context.Background())
	if !errors.Is(err, ErrSinkContract) {
		t.Fatalf("Complete() error = %v, want ErrSinkContract", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("Complete() error = %v, want cleanup error", err)
	}
}

func TestPackerCommitAndAbortLifecycle(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.WriteBody(context.Background(), []byte("body")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrPackerClosed) {
		t.Fatalf("second Complete() error = %v, want ErrPackerClosed", err)
	}
	if err := p.WriteFinal(context.Background(), []byte("x")); !errors.Is(err, ErrPackerClosed) {
		t.Fatalf("WriteFinal(after Complete) error = %v, want ErrPackerClosed", err)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort(after Complete) error = %v", err)
	}
	if got := txn.abortCount(); got != 0 {
		t.Fatalf("Abort calls after successful Complete = %d, want 0", got)
	}
}

func TestPackerAbortIsIdempotent(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort(first) error = %v", err)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort(second) error = %v", err)
	}
	if got := txn.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
	if err := p.WriteBody(context.Background(), []byte("body")); !errors.Is(err, ErrPackerAborted) {
		t.Fatalf("WriteBody(after Abort) error = %v, want ErrPackerAborted", err)
	}
}

func TestPackerRetriesFailedAbort(t *testing.T) {
	t.Parallel()

	cleanupErr := errors.New("cleanup timed out")
	txn := newRecordingTxn()
	txn.abortErrOnce = cleanupErr
	p := newTestPacker(t, txn, segformat.HashXXH64)
	if err := p.Abort(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("Abort(first) error = %v, want cleanup error", err)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort(retry) error = %v", err)
	}
	if got := txn.abortCount(); got != 2 {
		t.Fatalf("Abort calls = %d, want 2", got)
	}
}

func TestNewPackerValidatesInputs(t *testing.T) {
	t.Parallel()

	if _, err := newPacker(nil, segformat.HashXXH64); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("newPacker(nil) error = %v, want ErrInvalidOptions", err)
	}
	if _, err := newPacker(newRecordingTxn(), segformat.HashAlgo(99)); !errors.Is(err, ErrInvalidOptions) || !errors.Is(err, segformat.ErrUnsupportedHashAlgo) {
		t.Fatalf("newPacker(bad hash) error = %v", err)
	}
}

func newTestPacker(t *testing.T, txn Txn, hashAlgo segformat.HashAlgo) *packer {
	t.Helper()
	p, err := newPacker(txn, hashAlgo)
	if err != nil {
		t.Fatalf("newPacker() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Abort(context.Background()) })
	return p
}

type recordingTxn struct {
	mu sync.Mutex

	bytes        []byte
	writes       int
	aborts       int
	writeErr     error
	commitErr    error
	abortErr     error
	abortErrOnce error
	writeGate    chan struct{}
	commitMutate func(CommittedObject) CommittedObject
}

func newRecordingTxn() *recordingTxn { return &recordingTxn{} }

func (t *recordingTxn) Write(ctx context.Context, value []byte) error {
	if t.writeGate != nil {
		select {
		case <-t.writeGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes++
	if t.writeErr != nil {
		return t.writeErr
	}
	t.bytes = append(t.bytes, value...)
	return nil
}

func (t *recordingTxn) Commit(ctx context.Context) (CommittedObject, error) {
	if err := ctx.Err(); err != nil {
		return CommittedObject{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.commitErr != nil {
		return CommittedObject{}, t.commitErr
	}
	obj := CommittedObject{URI: "memory://recording", SizeBytes: uint64(len(t.bytes)), Token: "commit"}
	if t.commitMutate != nil {
		obj = t.commitMutate(obj)
	}
	return obj, nil
}

func (t *recordingTxn) Abort(context.Context) error {
	t.mu.Lock()
	t.aborts++
	err := t.abortErr
	if t.abortErrOnce != nil {
		err = t.abortErrOnce
		t.abortErrOnce = nil
	}
	t.mu.Unlock()
	return err
}

func (t *recordingTxn) objectBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.bytes...)
}

func (t *recordingTxn) writeCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writes
}

func (t *recordingTxn) abortCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.aborts
}
