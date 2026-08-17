package segwriter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestPackerPollResultsAccountsForEveryAvailableResult(t *testing.T) {
	const resultCount = 128
	p := &packer{results: make(chan uploadResult, resultCount)}
	for i := 1; i <= resultCount; i++ {
		p.results <- uploadResult{receipt: PartReceipt{Number: i}}
	}

	if err := p.pollResults(); err != nil {
		t.Fatalf("pollResults() error = %v", err)
	}
	if got := p.collected; got != resultCount {
		t.Fatalf("collected = %d, want %d", got, resultCount)
	}
	if got := len(p.receipts); got != resultCount {
		t.Fatalf("receipts = %d, want %d", got, resultCount)
	}
	for i, receipt := range p.receipts {
		if want := i + 1; receipt.Number != want {
			t.Fatalf("receipt[%d].Number = %d, want %d", i, receipt.Number, want)
		}
	}
}

func TestPackerRejectsMismatchedReceiptNumber(t *testing.T) {
	txn := &shiftedReceiptTxn{recordingTxn: newRecordingTxn()}
	p := newTestPacker(t, txn, packerOptions{PartSize: 1, UploadParallelism: 1})

	if err := p.WriteBody(context.Background(), []byte("a")); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	_ = p.BodyHash()
	_, err := p.Complete(context.Background())
	if !errors.Is(err, ErrSinkContract) {
		t.Fatalf("Complete() error = %v, want %v", err, ErrSinkContract)
	}
	if got, want := txn.abortCount(), 1; got != want {
		t.Fatalf("Abort calls = %d, want %d", got, want)
	}
	if got := len(txn.completeReceiptsSnapshot()); got != 0 {
		t.Fatalf("Complete received %d receipts after contract violation", got)
	}
}

func TestPackerRejectsInvalidCommittedObject(t *testing.T) {
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
			base := newRecordingTxn()
			txn := &invalidObjectTxn{recordingTxn: base, mutate: tt.mutate}
			p := newTestPacker(t, txn, packerOptions{PartSize: 2, UploadParallelism: 2})

			if err := p.WriteBody(context.Background(), []byte("abcd")); err != nil {
				t.Fatalf("WriteBody() error = %v", err)
			}
			_ = p.BodyHash()
			_, err := p.Complete(context.Background())
			if !errors.Is(err, ErrSinkContract) {
				t.Fatalf("Complete() error = %v, want %v", err, ErrSinkContract)
			}
			if got, want := base.abortCount(), 1; got != want {
				t.Fatalf("Abort calls = %d, want %d", got, want)
			}
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("Complete() error = %v, want text %q", err, tt.contains)
			}
		})
	}
}

func TestWriterTreatsSinkContractViolationAsTerminal(t *testing.T) {
	base := newRecordingTxn()
	txn := &invalidObjectTxn{
		recordingTxn: base,
		mutate: func(obj CommittedObject) CommittedObject {
			obj.SizeBytes++
			return obj
		},
	}
	w, err := New(testWriterOptions(segformat.CodecNone), fixedTxnSink{txn: txn})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	record := Record{LSN: 10, TimestampMS: 20, Value: []byte("value")}
	if err := w.Append(context.Background(), record); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrSinkContract) {
		t.Fatalf("Close() error = %v, want %v", err, ErrSinkContract)
	}
	if err := w.Append(context.Background(), record); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Append() after contract violation = %v, want %v", err, ErrWriterAborted)
	}
	if got, want := base.abortCount(), 1; got != want {
		t.Fatalf("Abort calls = %d, want %d", got, want)
	}
}

// TestWriterPipelineStress is intentionally deterministic so it can be run
// repeatedly under the race detector as a bounded soak gate.
func TestWriterPipelineStress(t *testing.T) {
	txn := newRecordingTxn()
	for part := 1; part <= 512; part++ {
		txn.delayPart[part] = time.Duration((part*7)%5) * 50 * time.Microsecond
	}

	opts := testWriterOptions(segformat.CodecZstd)
	opts.TargetBlockSize = 512
	opts.PartSize = 127
	opts.SealParallelism = 4
	opts.BlockBufferCount = 9
	opts.UploadParallelism = 4
	opts.UploadQueueSize = 2

	w, err := New(opts, fixedTxnSink{txn: txn})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Abort(context.Background()) })

	records := makeWriterRecords(256, 50_000, 1_800_000_000_000, 73)
	for i := range records {
		records[i].Headers = []segformat.Header{{Key: []byte("kind"), Value: []byte{byte(i % 17)}}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, record := range records {
		if err := w.Append(ctx, record); err != nil {
			t.Fatalf("Append(lsn=%d) error = %v", record.LSN, err)
		}
	}
	result, err := w.Close(ctx)
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if result.Object.SizeBytes != result.Metadata.SizeBytes {
		t.Fatalf("object size = %d, metadata size = %d", result.Object.SizeBytes, result.Metadata.SizeBytes)
	}

	decoded := decodeSegmentForTest(t, txn.objectBytes())
	assertRecordsEqual(t, decoded.records, records)
	receipts := txn.completeReceiptsSnapshot()
	if len(receipts) < opts.UploadParallelism {
		t.Fatalf("receipts = %d, want at least %d", len(receipts), opts.UploadParallelism)
	}
	for i, receipt := range receipts {
		if want := i + 1; receipt.Number != want {
			t.Fatalf("receipt[%d].Number = %d, want %d", i, receipt.Number, want)
		}
	}
	if got := txn.maxActiveUploads(); got > opts.UploadParallelism {
		t.Fatalf("max active uploads = %d, limit = %d", got, opts.UploadParallelism)
	}
}

type fixedTxnSink struct {
	txn Txn
}

func (s fixedTxnSink) Begin(context.Context, Plan) (Txn, error) {
	return s.txn, nil
}

type shiftedReceiptTxn struct {
	*recordingTxn
}

func (t *shiftedReceiptTxn) UploadPart(ctx context.Context, part Part) (PartReceipt, error) {
	receipt, err := t.recordingTxn.UploadPart(ctx, part)
	if err == nil {
		receipt.Number++
	}
	return receipt, err
}

type invalidObjectTxn struct {
	*recordingTxn
	mutate func(CommittedObject) CommittedObject
}

func (t *invalidObjectTxn) Complete(ctx context.Context, receipts []PartReceipt) (CommittedObject, error) {
	obj, err := t.recordingTxn.Complete(ctx, receipts)
	if err != nil {
		return CommittedObject{}, err
	}
	return t.mutate(obj), nil
}

type gatedFailureTxn struct {
	err         error
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newGatedFailureTxn(err error) *gatedFailureTxn {
	return &gatedFailureTxn{
		err:     err,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (t *gatedFailureTxn) UploadPart(ctx context.Context, _ Part) (PartReceipt, error) {
	t.startedOnce.Do(func() { close(t.started) })
	select {
	case <-t.release:
		return PartReceipt{}, t.err
	case <-ctx.Done():
		return PartReceipt{}, ctx.Err()
	}
}

func (t *gatedFailureTxn) Complete(context.Context, []PartReceipt) (CommittedObject, error) {
	return CommittedObject{}, errors.New("unexpected complete")
}

func (t *gatedFailureTxn) Abort(context.Context) error {
	t.releaseFailure()
	return nil
}

func (t *gatedFailureTxn) waitStarted(tb testing.TB) {
	tb.Helper()
	select {
	case <-t.started:
	case <-time.After(2 * time.Second):
		tb.Fatal("timed out waiting for upload to start")
	}
}

func (t *gatedFailureTxn) releaseFailure() {
	t.releaseOnce.Do(func() { close(t.release) })
}
