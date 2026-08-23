// Package lifecycletest provides provider conformance and soak helpers for
// partitionlog's physical object lifecycle.
package lifecycletest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	"github.com/ankur-anand/unijord/partitionlog/blob/lifecycle"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
)

const SoakEnvironment = "PARTITIONLOG_LIFECYCLE_SOAK"

type Store interface {
	partitionlog.Store
	NewReclaimer(opts lifecycle.Options) (*lifecycle.Reclaimer, error)
}

type Config struct {
	Partition   uint32
	RecordCount int
	RetainFrom  uint64
	DeleteDelay time.Duration
}

func Run(t testing.TB, ctx context.Context, store Store, cfg Config) {
	t.Helper()
	if cfg.Partition == 0 {
		cfg.Partition = 1
	}
	if cfg.RecordCount == 0 {
		cfg.RecordCount = 6
	}
	if cfg.RetainFrom == 0 {
		cfg.RetainFrom = uint64(cfg.RecordCount - 2)
	}
	if cfg.DeleteDelay == 0 {
		cfg.DeleteDelay = 5 * time.Millisecond
	}
	if cfg.RecordCount < 3 || cfg.RetainFrom >= uint64(cfg.RecordCount) {
		t.Fatalf("invalid lifecycle conformance config: %+v", cfg)
	}

	log, err := partitionlog.Open(partitionlog.Options{Store: store})
	if err != nil {
		t.Fatalf("partitionlog.Open() error = %v", err)
	}
	writer, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: cfg.Partition,
		WriterID:  testIdentity(cfg.Partition, 1),
		Batch:     partitionlog.BatchPolicy{MaxRecords: 1},
	})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	defer func() { _ = writer.Abort(context.Background()) }()

	for i := 0; i < cfg.RecordCount; i++ {
		result, err := writer.Append(ctx, partitionlog.Record{
			TimestampMS: int64(i),
			Value:       []byte(fmt.Sprintf("record-%d", i)),
		})
		if err != nil {
			t.Fatalf("Append(%d) error = %v (terminal cause: %v)", i, err, writer.Err())
		}
		if result.LSN != uint64(i) {
			t.Fatalf("Append(%d) LSN = %d, want %d", i, result.LSN, i)
		}
	}
	if _, err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	before, err := store.ReaderCatalog().ListSegments(ctx, catalog.ListSegmentsRequest{
		Partition: cfg.Partition,
		Limit:     cfg.RecordCount + 1,
	})
	if err != nil {
		t.Fatalf("ListSegments(before retention) error = %v", err)
	}
	if len(before.Segments) != cfg.RecordCount {
		t.Fatalf("segments before retention = %d, want %d", len(before.Segments), cfg.RecordCount)
	}

	if _, err := log.RequestRetention(ctx, partitionlog.RetentionRequest{
		Partition: cfg.Partition, PolicyVersion: 1, BeforeLSN: cfg.RetainFrom,
	}); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	retention, err := writer.ApplyRetention(ctx)
	if err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	if !retention.Applied || retention.Snapshot.Head.OldestLSN != cfg.RetainFrom {
		t.Fatalf("ApplyRetention() result = %+v", retention)
	}

	reclaimer, err := store.NewReclaimer(lifecycle.Options{
		OwnerID:           testIdentity(cfg.Partition, 2),
		DeleteDelay:       cfg.DeleteDelay,
		ListPageSize:      3,
		MaxObjectsPerRun:  256,
		MaxDeletesPerRun:  256,
		MaxDeleteBytes:    256 << 20,
		DeleteBatchSize:   3,
		DeleteConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("NewReclaimer() error = %v", err)
	}
	if _, err := reclaimer.RunPartition(ctx, cfg.Partition); err != nil {
		t.Fatalf("RunPartition(observe) error = %v", err)
	}
	wait(t, ctx, cfg.DeleteDelay+5*time.Millisecond)
	scheduler, err := lifecycle.NewScheduler(reclaimer, lifecycle.SchedulerOptions{
		MaxConcurrentPartitions: 1,
		PartitionRunTimeout:     10 * time.Second,
		ContinuationDelay:       time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}
	summary, err := scheduler.Run(ctx, []lifecycle.Task{{
		Partition: cfg.Partition,
		Operation: lifecycle.OperationReclaim,
	}})
	if err != nil {
		t.Fatalf("Scheduler.Run() error = %v", err)
	}
	if summary.Completed != 1 || summary.Failed != 0 {
		t.Fatalf("Scheduler.Run() summary = %+v", summary)
	}

	for _, segment := range before.Segments {
		_, err := store.SegmentStore().ReadAt(ctx, segment.URI, 0, 1)
		if segment.BaseLSN < cfg.RetainFrom {
			if err == nil {
				t.Fatalf("expired segment %q remains readable", segment.URI)
			}
			continue
		}
		if err != nil {
			t.Fatalf("retained segment %q ReadAt() error = %v", segment.URI, err)
		}
	}

	appended, err := writer.Append(ctx, partitionlog.Record{
		TimestampMS: int64(cfg.RecordCount),
		Value:       []byte("after-gc"),
	})
	if err != nil {
		t.Fatalf("Append(after GC) error = %v", err)
	}
	if appended.LSN != uint64(cfg.RecordCount) {
		t.Fatalf("Append(after GC) LSN = %d, want %d", appended.LSN, cfg.RecordCount)
	}
	if _, err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush(after GC) error = %v", err)
	}

	read, err := log.Reader().Partition(cfg.Partition).Read(ctx, partitionlog.ReadRequest{
		StartLSN:  cfg.RetainFrom,
		Limit:     cfg.RecordCount + 1,
		Freshness: partitionlog.FreshnessLatest,
	})
	if err != nil {
		t.Fatalf("Read(retained after GC) error = %v", err)
	}
	wantRecords := cfg.RecordCount - int(cfg.RetainFrom) + 1
	if len(read.Records) != wantRecords || read.Records[0].LSN != cfg.RetainFrom || read.Records[len(read.Records)-1].LSN != uint64(cfg.RecordCount) {
		t.Fatalf("Read(retained after GC) = %+v, want records=%d through LSN=%d", read, wantRecords, cfg.RecordCount)
	}
	if _, err := writer.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func RunSoak(t testing.TB, ctx context.Context, store Store, startPartition uint32) {
	t.Helper()
	raw := os.Getenv(SoakEnvironment)
	if raw == "" {
		t.Skipf("set %s to a duration such as 2m", SoakEnvironment)
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatalf("%s=%q is not a positive duration", SoakEnvironment, raw)
	}

	deadline := time.Now().Add(duration)
	cycles := 0
	for {
		Run(t, ctx, store, Config{Partition: startPartition + uint32(cycles)})
		cycles++
		if time.Now().After(deadline) {
			break
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("lifecycle soak context ended after %d cycles: %v", cycles, err)
		}
	}
	t.Logf("lifecycle soak completed provider cycles=%s", strconv.Itoa(cycles))
}

func wait(t testing.TB, ctx context.Context, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatalf("wait for delete delay: %v", ctx.Err())
	}
}

func testIdentity(partition uint32, kind byte) [16]byte {
	var id [16]byte
	id[0] = kind
	id[1] = byte(partition >> 24)
	id[2] = byte(partition >> 16)
	id[3] = byte(partition >> 8)
	id[4] = byte(partition)
	return id
}
