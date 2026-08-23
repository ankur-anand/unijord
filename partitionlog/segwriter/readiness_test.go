package segwriter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestWriterTreatsSinkContractViolationAsTerminal(t *testing.T) {
	t.Parallel()

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
	if got := base.abortCount(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
}

// TestWriterPipelineStress is intentionally deterministic so it can be run
// repeatedly under the race detector as a bounded soak gate. Multipart
// scheduling is tested by blob/sink/stream; this test covers sealing and
// ordered byte emission into a sink transaction.
func TestWriterPipelineStress(t *testing.T) {
	t.Parallel()

	txn := newRecordingTxn()
	opts := testWriterOptions(segformat.CodecZstd)
	opts.TargetBlockSize = 512
	opts.SealParallelism = 4
	opts.BlockBufferCount = 9

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
}

type fixedTxnSink struct {
	txn Txn
}

func (s fixedTxnSink) Begin(context.Context, Plan) (Txn, error) {
	return s.txn, nil
}

type invalidObjectTxn struct {
	*recordingTxn
	mutate func(CommittedObject) CommittedObject
}

func (t *invalidObjectTxn) Commit(ctx context.Context) (CommittedObject, error) {
	obj, err := t.recordingTxn.Commit(ctx)
	if err != nil {
		return CommittedObject{}, err
	}
	return t.mutate(obj), nil
}
