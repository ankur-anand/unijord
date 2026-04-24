package writer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

var (
	benchWriterSnapshot Snapshot
	benchWriterState    State
)

func BenchmarkWriterCloseMemorySession(b *testing.B) {
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
			records := makeBenchRecords(tc.records, 1_776_263_000_000, tc.valueSize)
			b.SetBytes(int64(tc.records * (tc.valueSize + segformat.RecordHeaderSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				factory := newMemorySegmentFactory()
				opts := benchWriterOptions(factory, tc.codec)
				w, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				for _, record := range records {
					if _, err := w.Append(context.Background(), record); err != nil {
						b.Fatal(err)
					}
				}
				snapshot, err := w.Close(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				benchWriterSnapshot = snapshot
				benchWriterState = w.State()
			}
		})
	}
}

func BenchmarkWriterRollByRecordsMemorySession(b *testing.B) {
	for _, tc := range []struct {
		name           string
		codec          segformat.Codec
		records        int
		valueSize      int
		maxSegmentRecs uint32
	}{
		{name: "none_16k_x_128b_roll_1k", codec: segformat.CodecNone, records: 16 * 1024, valueSize: 128, maxSegmentRecs: 1024},
		{name: "zstd_16k_x_128b_roll_1k", codec: segformat.CodecZstd, records: 16 * 1024, valueSize: 128, maxSegmentRecs: 1024},
		{name: "none_16k_x_128b_roll_256", codec: segformat.CodecNone, records: 16 * 1024, valueSize: 128, maxSegmentRecs: 256},
		{name: "zstd_16k_x_128b_roll_256", codec: segformat.CodecZstd, records: 16 * 1024, valueSize: 128, maxSegmentRecs: 256},
	} {
		b.Run(tc.name, func(b *testing.B) {
			records := makeBenchRecords(tc.records, 1_776_263_000_000, tc.valueSize)
			b.SetBytes(int64(tc.records * (tc.valueSize + segformat.RecordHeaderSize)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				factory := newMemorySegmentFactory()
				opts := benchWriterOptions(factory, tc.codec)
				opts.Roll.MaxSegmentRecords = tc.maxSegmentRecs
				w, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				for _, record := range records {
					if _, err := w.Append(context.Background(), record); err != nil {
						b.Fatal(err)
					}
				}
				snapshot, err := w.Close(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				benchWriterSnapshot = snapshot
				benchWriterState = w.State()
			}
		})
	}
}

func benchWriterOptions(factory SinkFactory, codec segformat.Codec) Options {
	opts := DefaultOptions(factory)
	opts.Session = &benchSession{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   7,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{6, 5, 4},
			},
		},
	}
	opts.Clock = func() time.Time { return time.UnixMilli(1_776_263_000_000).UTC() }
	opts.UUIDGen = newSequenceUUIDGen()
	opts.SegmentOptions = newTestSegmentOptions(7)
	opts.SegmentOptions.Codec = codec
	opts.SegmentOptions.HashAlgo = segformat.HashXXH64
	opts.SegmentOptions.TargetBlockSize = 1 << 20
	opts.SegmentOptions.PartSize = 1 << 20
	opts.SegmentOptions.SealParallelism = 1
	opts.SegmentOptions.BlockBufferCount = 3
	opts.SegmentOptions.UploadParallelism = 2
	opts.SegmentOptions.UploadQueueSize = 2
	opts.Queue = QueuePolicy{
		MaxInflightSegments: 8,
		MaxInflightBytes:    512 << 20,
	}
	return opts
}

type benchSession struct {
	snapshot Snapshot
}

func (s *benchSession) Snapshot() Snapshot {
	return s.snapshot
}

func (s *benchSession) PublishSegment(_ context.Context, req PublishRequest) (Snapshot, error) {
	if req.ExpectedNextLSN != s.snapshot.Head.NextLSN {
		return Snapshot{}, fmt.Errorf("expected_next_lsn=%d current=%d", req.ExpectedNextLSN, s.snapshot.Head.NextLSN)
	}
	s.snapshot.Head.NextLSN = req.Segment.NextLSN()
	s.snapshot.Head.WriterEpoch = s.snapshot.Identity.Epoch
	s.snapshot.Head.SegmentCount++
	s.snapshot.Head.LastSegment = req.Segment
	s.snapshot.Head.HasLastSegment = true
	if s.snapshot.Head.OldestLSN == 0 && s.snapshot.Head.SegmentCount == 1 {
		s.snapshot.Head.OldestLSN = req.Segment.BaseLSN
	}
	return s.snapshot, nil
}

func makeBenchRecords(n int, baseTimestampMS int64, valueSize int) []Record {
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}
	records := make([]Record, n)
	for i := 0; i < n; i++ {
		payload := make([]byte, len(value))
		copy(payload, value)
		records[i] = Record{
			TimestampMS: baseTimestampMS + int64(i),
			Value:       payload,
		}
	}
	return records
}
