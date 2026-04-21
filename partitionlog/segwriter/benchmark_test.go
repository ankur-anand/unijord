package segwriter

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

var (
	benchWriterResult Result
	benchWriterBytes  []byte
	benchWriterMeta   Metadata
)

func BenchmarkWriterCloseDiscardSink(b *testing.B) {
	for _, tc := range []struct {
		name      string
		codec     segformat.Codec
		records   int
		valueSize int
	}{
		{name: "none_4k_x_256b", codec: segformat.CodecNone, records: 4 * 1024, valueSize: 256},
		{name: "zstd_4k_x_256b", codec: segformat.CodecZstd, records: 4 * 1024, valueSize: 256},
		{name: "none_16k_x_128b", codec: segformat.CodecNone, records: 16 * 1024, valueSize: 128},
		{name: "zstd_16k_x_128b", codec: segformat.CodecZstd, records: 16 * 1024, valueSize: 128},
	} {
		b.Run(tc.name, func(b *testing.B) {
			records := makeWriterRecords(tc.records, 1_000, 1_776_263_000_000, tc.valueSize)
			opts := benchOptions(tc.codec)
			b.SetBytes(int64(tc.records * (tc.valueSize + segformat.RecordHeaderSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink := &discardSink{}
				w, err := New(opts, sink)
				if err != nil {
					b.Fatal(err)
				}
				for _, record := range records {
					if err := w.Append(context.Background(), record); err != nil {
						b.Fatal(err)
					}
				}
				result, err := w.Close(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				benchWriterResult = result
			}
		})
	}
}

func BenchmarkEncodeMemorySink(b *testing.B) {
	for _, tc := range []struct {
		name      string
		codec     segformat.Codec
		records   int
		valueSize int
	}{
		{name: "none_4k_x_256b", codec: segformat.CodecNone, records: 4 * 1024, valueSize: 256},
		{name: "zstd_4k_x_256b", codec: segformat.CodecZstd, records: 4 * 1024, valueSize: 256},
	} {
		b.Run(tc.name, func(b *testing.B) {
			records := makeWriterRecords(tc.records, 1_000, 1_776_263_000_000, tc.valueSize)
			opts := benchOptions(tc.codec)
			b.SetBytes(int64(tc.records * (tc.valueSize + segformat.RecordHeaderSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				object, meta, err := Encode(context.Background(), records, opts)
				if err != nil {
					b.Fatal(err)
				}
				benchWriterBytes = object
				benchWriterMeta = meta
			}
		})
	}
}

func benchOptions(codec segformat.Codec) Options {
	opts := DefaultOptions(7)
	opts.Codec = codec
	opts.HashAlgo = segformat.HashXXH64
	opts.TargetBlockSize = 1 << 20
	opts.PartSize = 1 << 20
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	opts.SegmentUUID = [16]byte{9, 8, 7}
	opts.WriterTag = [16]byte{6, 5, 4}
	opts.CreatedUnixMS = 1_776_263_000_000
	return opts
}

type discardSink struct{}

func (s *discardSink) Begin(context.Context, Plan) (Txn, error) {
	return &discardTxn{}, nil
}

type discardTxn struct {
	mu    sync.Mutex
	size  uint64
	parts int
}

func (t *discardTxn) UploadPart(_ context.Context, part Part) (PartReceipt, error) {
	t.mu.Lock()
	t.size += uint64(len(part.Bytes))
	t.parts++
	t.mu.Unlock()
	return PartReceipt{Number: part.Number, Token: fmt.Sprintf("discard-%d", part.Number)}, nil
}

func (t *discardTxn) Complete(_ context.Context, receipts []PartReceipt) (CommittedObject, error) {
	t.mu.Lock()
	size := t.size
	parts := t.parts
	t.mu.Unlock()
	if len(receipts) != parts {
		return CommittedObject{}, fmt.Errorf("receipts=%d parts=%d", len(receipts), parts)
	}
	return CommittedObject{
		URI:       "discard://segment",
		SizeBytes: size,
		Token:     "discard-complete",
	}, nil
}

func (t *discardTxn) Abort(context.Context) error {
	return nil
}
