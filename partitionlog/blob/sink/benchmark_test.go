package sink

import (
	"context"
	"fmt"
	"testing"

	objmultipart "github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

func BenchmarkSegmentSinkFinish(b *testing.B) {
	cases := []struct {
		name     string
		records  int
		valueLen int
	}{
		{name: "4k_x_256b", records: 4096, valueLen: 256},
		{name: "16k_x_128b", records: 16_384, valueLen: 128},
	}
	for _, tc := range cases {
		b.Run("memory_sink_"+tc.name, func(b *testing.B) {
			benchmarkSegmentSinkFinish(b, tc.records, tc.valueLen, func(uri string) segwriter.Sink {
				return segwriter.NewMemorySink(uri)
			})
		})
		b.Run("blob_sink_"+tc.name, func(b *testing.B) {
			store := objmultipart.NewMemoryStore()
			factory, err := New(store, Options{Prefix: "segments"})
			if err != nil {
				b.Fatalf("New() error = %v", err)
			}
			var n uint64
			benchmarkSegmentSinkFinish(b, tc.records, tc.valueLen, func(_ string) segwriter.Sink {
				n++
				uuid := [16]byte{
					byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
					byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
				}
				sink, err := factory.NewSegmentSink(context.Background(), plwriter.SegmentInfo{
					Partition:     1,
					BaseLSN:       n * uint64(tc.records),
					WriterEpoch:   1,
					SegmentUUID:   uuid,
					CreatedUnixMS: 1_776_263_000_000,
				})
				if err != nil {
					b.Fatalf("NewSegmentSink() error = %v", err)
				}
				return sink
			})
		})
	}
}

func benchmarkSegmentSinkFinish(b *testing.B, records int, valueLen int, newSink func(uri string) segwriter.Sink, configure ...func(*segwriter.Options)) {
	value := make([]byte, valueLen)
	for i := range value {
		value[i] = byte(i)
	}
	opts := segwriter.DefaultOptions(1)
	opts.Codec = segformat.CodecNone
	opts.HashAlgo = segformat.HashXXH64
	opts.TargetBlockSize = 1 << 20
	opts.PartSize = 1 << 20
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 2
	opts.UploadQueueSize = 2
	for _, fn := range configure {
		fn(&opts)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(int64(records * valueLen))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink := newSink(fmt.Sprintf("memory://bench/%d", i))
		writer, err := segwriter.New(opts, sink)
		if err != nil {
			b.Fatalf("segwriter.New() error = %v", err)
		}
		for j := 0; j < records; j++ {
			if err := writer.Append(ctx, segwriter.Record{
				LSN:         uint64(j),
				TimestampMS: int64(j),
				Value:       value,
			}); err != nil {
				b.Fatalf("Append(%d) error = %v", j, err)
			}
		}
		if _, err := writer.Close(ctx); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
	}
}
