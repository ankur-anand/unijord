package segwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestPackerSmallWriteProducesOnePart(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 16, UploadParallelism: 2})

	if err := p.WriteBody(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	hash := p.BodyHash()
	if err := p.WriteFinal(context.Background(), []byte("!")); err != nil {
		t.Fatalf("WriteFinal() error = %v", err)
	}

	obj, err := p.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if got, want := obj.SizeBytes, uint64(len("hello!")); got != want {
		t.Fatalf("object size = %d, want %d", got, want)
	}
	if got, want := txn.objectBytes(), []byte("hello!"); !bytes.Equal(got, want) {
		t.Fatalf("object bytes = %q, want %q", got, want)
	}
	if got, want := len(txn.partsSnapshot()), 1; got != want {
		t.Fatalf("uploaded parts = %d, want %d", got, want)
	}
	wantHash, err := segformat.HashBytes(segformat.HashXXH64, []byte("hello"))
	if err != nil {
		t.Fatalf("HashBytes() error = %v", err)
	}
	if hash != wantHash {
		t.Fatalf("BodyHash() = %x, want %x", hash, wantHash)
	}
}

func TestPackerSplitsLargeWriteIntoParts(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 2})

	if err := p.WriteBody(context.Background(), []byte("abcdefghijkl")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()

	obj, err := p.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if got, want := obj.SizeBytes, uint64(12); got != want {
		t.Fatalf("object size = %d, want %d", got, want)
	}

	parts := txn.partsSnapshot()
	if got, want := len(parts), 3; got != want {
		t.Fatalf("uploaded parts = %d, want %d", got, want)
	}
	for _, part := range parts {
		if got, want := len(part.Bytes), 4; got != want {
			t.Fatalf("part %d size = %d, want %d", part.Number, got, want)
		}
	}
	if got, want := txn.objectBytes(), []byte("abcdefghijkl"); !bytes.Equal(got, want) {
		t.Fatalf("object bytes = %q, want %q", got, want)
	}
}

func TestPackerSplitsAcrossSmallWrites(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 2})

	for _, s := range []string{"ab", "cd", "ef", "gh"} {
		if err := p.WriteBody(context.Background(), []byte(s)); err != nil {
			t.Fatalf("WriteBody(%q) error = %v", s, err)
		}
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	parts := txn.partsSnapshot()
	if got, want := len(parts), 2; got != want {
		t.Fatalf("uploaded parts = %d, want %d", got, want)
	}
	for _, part := range parts {
		if got, want := len(part.Bytes), 4; got != want {
			t.Fatalf("part %d size = %d, want %d", part.Number, got, want)
		}
	}
	if got, want := txn.objectBytes(), []byte("abcdefgh"); !bytes.Equal(got, want) {
		t.Fatalf("object bytes = %q, want %q", got, want)
	}
}

func TestPackerOffsetAdvances(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if got := p.Offset(); got != 0 {
		t.Fatalf("initial Offset() = %d, want 0", got)
	}
	if err := p.WriteBody(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	if got, want := p.Offset(), uint64(3); got != want {
		t.Fatalf("Offset() = %d, want %d", got, want)
	}
	_ = p.BodyHash()
	if err := p.WriteFinal(context.Background(), []byte("defgh")); err != nil {
		t.Fatalf("WriteFinal() error = %v", err)
	}
	if got, want := p.Offset(), uint64(8); got != want {
		t.Fatalf("Offset() = %d, want %d", got, want)
	}
}

func TestPackerBodyHashExcludesFinalBytes(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 32, UploadParallelism: 1})

	if err := p.WriteBody(context.Background(), []byte("body")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	got := p.BodyHash()
	if err := p.WriteFinal(context.Background(), []byte("trailer")); err != nil {
		t.Fatalf("WriteFinal() error = %v", err)
	}
	again := p.BodyHash()

	want, err := segformat.HashBytes(segformat.HashXXH64, []byte("body"))
	if err != nil {
		t.Fatalf("HashBytes() error = %v", err)
	}
	if got != want {
		t.Fatalf("BodyHash() = %x, want %x", got, want)
	}
	if again != want {
		t.Fatalf("BodyHash() after WriteFinal = %x, want %x", again, want)
	}
}

func TestPackerSortsReceiptsBeforeComplete(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	txn.delayPart[1] = 20 * time.Millisecond
	p := newTestPacker(t, txn, packerOptions{PartSize: 2, UploadParallelism: 3, UploadQueueSize: 3})

	if err := p.WriteBody(context.Background(), []byte("abcdef")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	receipts := txn.completeReceiptsSnapshot()
	if got, want := len(receipts), 3; got != want {
		t.Fatalf("complete receipts = %d, want %d", got, want)
	}
	for i, receipt := range receipts {
		if got, want := receipt.Number, i+1; got != want {
			t.Fatalf("receipt[%d].Number = %d, want %d", i, got, want)
		}
	}
}

func TestPackerUploadErrorPropagatesAndAbortIsAllowed(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("upload failed")
	txn := newRecordingTxn()
	txn.failPart = 1
	txn.failErr = wantErr
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if err := p.WriteBody(context.Background(), []byte("abcd")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	_, err := p.Complete(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Complete() error = %v, want %v", err, wantErr)
	}
	if err := p.WriteFinal(context.Background(), []byte("x")); !errors.Is(err, wantErr) {
		t.Fatalf("WriteFinal() after upload error = %v, want %v", err, wantErr)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if got, want := txn.abortCount(), 1; got != want {
		t.Fatalf("Abort calls = %d, want %d", got, want)
	}
}

func TestPackerAbortIsIdempotent(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("second Abort() error = %v", err)
	}
	if got, want := txn.abortCount(), 1; got != want {
		t.Fatalf("Abort calls = %d, want %d", got, want)
	}
}

func TestPackerRejectsWritesAfterComplete(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if err := p.WriteBody(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if err := p.WriteFinal(context.Background(), []byte("x")); !errors.Is(err, ErrPackerClosed) {
		t.Fatalf("WriteFinal() after Complete = %v, want %v", err, ErrPackerClosed)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrPackerClosed) {
		t.Fatalf("second Complete() error = %v, want %v", err, ErrPackerClosed)
	}
	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() after Complete error = %v", err)
	}
}

func TestPackerRejectsWritesAfterAbort(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if err := p.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := p.WriteBody(context.Background(), []byte("x")); !errors.Is(err, ErrPackerAborted) {
		t.Fatalf("WriteBody() after Abort = %v, want %v", err, ErrPackerAborted)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrPackerAborted) {
		t.Fatalf("Complete() after Abort = %v, want %v", err, ErrPackerAborted)
	}
}

func TestPackerRequiresBodyHashBeforeFinalAndComplete(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	if err := p.WriteBody(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	if err := p.WriteFinal(context.Background(), []byte("x")); !errors.Is(err, ErrBodyNotSealed) {
		t.Fatalf("WriteFinal() before BodyHash = %v, want %v", err, ErrBodyNotSealed)
	}
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrBodyNotSealed) {
		t.Fatalf("Complete() before BodyHash = %v, want %v", err, ErrBodyNotSealed)
	}
	_ = p.BodyHash()
	if err := p.WriteBody(context.Background(), []byte("more")); !errors.Is(err, ErrBodySealed) {
		t.Fatalf("WriteBody() after BodyHash = %v, want %v", err, ErrBodySealed)
	}
}

func TestPackerRejectsEmptyObject(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	p := newTestPacker(t, txn, packerOptions{PartSize: 4, UploadParallelism: 1})

	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); !errors.Is(err, ErrEmptyObject) {
		t.Fatalf("Complete() empty error = %v, want %v", err, ErrEmptyObject)
	}
}

func TestPackerUploadLimiterBoundsConcurrency(t *testing.T) {
	t.Parallel()

	limiter, err := NewSemaphoreUploadLimiter(1)
	if err != nil {
		t.Fatalf("NewSemaphoreUploadLimiter() error = %v", err)
	}
	txn := newRecordingTxn()
	txn.uploadDelay = 10 * time.Millisecond
	p := newTestPacker(t, txn, packerOptions{
		PartSize:          1,
		UploadParallelism: 4,
		UploadQueueSize:   4,
		UploadLimiter:     limiter,
	})

	if err := p.WriteBody(context.Background(), []byte("abcdef")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	if _, err := p.Complete(context.Background()); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if got, want := txn.maxActiveUploads(), 1; got != want {
		t.Fatalf("max active uploads = %d, want %d", got, want)
	}
}

func TestPackerRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	if _, err := newPacker(context.Background(), nil, packerOptions{PartSize: 1, HashAlgo: segformat.HashXXH64}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("nil txn error = %v, want %v", err, ErrInvalidOptions)
	}
	if _, err := newPacker(context.Background(), newRecordingTxn(), packerOptions{PartSize: 0, HashAlgo: segformat.HashXXH64}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("bad part size error = %v, want %v", err, ErrInvalidOptions)
	}
	if _, err := newPacker(context.Background(), newRecordingTxn(), packerOptions{PartSize: 1, HashAlgo: segformat.HashAlgo(99)}); !errors.Is(err, segformat.ErrUnsupportedHashAlgo) {
		t.Fatalf("bad hash error = %v, want %v", err, segformat.ErrUnsupportedHashAlgo)
	}
}

func newTestPacker(t *testing.T, txn Txn, opts packerOptions) *packer {
	t.Helper()
	if opts.HashAlgo == 0 {
		opts.HashAlgo = segformat.HashXXH64
	}
	p, err := newPacker(context.Background(), txn, opts)
	if err != nil {
		t.Fatalf("newPacker() error = %v", err)
	}
	t.Cleanup(func() {
		_ = p.Abort(context.Background())
	})
	return p
}

type recordingTxn struct {
	mu sync.Mutex

	parts            []Part
	completeReceipts []PartReceipt
	completed        int
	aborted          int

	delayPart   map[int]time.Duration
	uploadDelay time.Duration
	failPart    int
	failErr     error

	activeUploads int
	maxUploads    int
}

func newRecordingTxn() *recordingTxn {
	return &recordingTxn{
		delayPart: make(map[int]time.Duration),
	}
}

func (t *recordingTxn) UploadPart(ctx context.Context, part Part) (PartReceipt, error) {
	t.beginUpload()
	defer t.endUpload()

	if d := t.delayFor(part.Number); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return PartReceipt{}, ctx.Err()
		}
	}
	if t.failFor(part.Number) {
		return PartReceipt{}, t.failure()
	}

	partCopy := Part{
		Number: part.Number,
		Bytes:  append([]byte(nil), part.Bytes...),
	}
	t.mu.Lock()
	t.parts = append(t.parts, partCopy)
	t.mu.Unlock()

	return PartReceipt{Number: part.Number, Token: fmt.Sprintf("part-%d", part.Number)}, nil
}

func (t *recordingTxn) Complete(_ context.Context, receipts []PartReceipt) (CommittedObject, error) {
	t.mu.Lock()
	t.completed++
	t.completeReceipts = append([]PartReceipt(nil), receipts...)
	size := uint64(len(t.objectBytesLocked()))
	t.mu.Unlock()

	return CommittedObject{
		URI:       "memory://segment",
		SizeBytes: size,
		Token:     "complete",
	}, nil
}

func (t *recordingTxn) Abort(_ context.Context) error {
	t.mu.Lock()
	t.aborted++
	t.mu.Unlock()
	return nil
}

func (t *recordingTxn) beginUpload() {
	t.mu.Lock()
	t.activeUploads++
	if t.activeUploads > t.maxUploads {
		t.maxUploads = t.activeUploads
	}
	t.mu.Unlock()
}

func (t *recordingTxn) endUpload() {
	t.mu.Lock()
	t.activeUploads--
	t.mu.Unlock()
}

func (t *recordingTxn) delayFor(partNumber int) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.uploadDelay + t.delayPart[partNumber]
}

func (t *recordingTxn) failFor(partNumber int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failPart == partNumber
}

func (t *recordingTxn) failure() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failErr != nil {
		return t.failErr
	}
	return errors.New("upload failed")
}

func (t *recordingTxn) partsSnapshot() []Part {
	t.mu.Lock()
	defer t.mu.Unlock()

	parts := append([]Part(nil), t.parts...)
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Number < parts[j].Number
	})
	return parts
}

func (t *recordingTxn) completeReceiptsSnapshot() []PartReceipt {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]PartReceipt(nil), t.completeReceipts...)
}

func (t *recordingTxn) objectBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.objectBytesLocked()
}

func (t *recordingTxn) objectBytesLocked() []byte {
	parts := append([]Part(nil), t.parts...)
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Number < parts[j].Number
	})
	var out []byte
	for _, part := range parts {
		out = append(out, part.Bytes...)
	}
	return out
}

func (t *recordingTxn) abortCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.aborted
}

func (t *recordingTxn) maxActiveUploads() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxUploads
}
