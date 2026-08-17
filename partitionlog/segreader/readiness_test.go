package segreader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestReaderConcurrentScanStress(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecZstd, segformat.HashXXH64, 160, 10_000, 50_000, 96)
	reader := openFixture(t, fixture, DefaultOptions())

	const goroutines = 16
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < 8; iteration++ {
				startAt := (worker*11 + iteration*7) % (len(fixture.records) - 12)
				records, err := reader.Read(context.Background(), fixture.ref.BaseLSN+uint64(startAt), 12)
				if err != nil {
					errs <- err
					return
				}
				if len(records) != 12 {
					errs <- fmt.Errorf("worker=%d iteration=%d records=%d want=12", worker, iteration, len(records))
					return
				}
				for i := range records {
					if records[i].LSN != fixture.records[startAt+i].LSN {
						errs <- fmt.Errorf("worker=%d iteration=%d record=%d lsn=%d want=%d", worker, iteration, i, records[i].LSN, fixture.records[startAt+i].LSN)
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestScanCancellationDoesNotFetchBlock(t *testing.T) {
	t.Parallel()

	fixture := buildSegment(t, segformat.CodecNone, segformat.HashXXH64, 24, 1, 1, 48)
	store := newCountingStore(newMemoryStore(map[string][]byte{fixture.ref.URI: fixture.object}))
	reader, err := Open(context.Background(), store, fixture.ref, DefaultOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	scanner, err := reader.Scan(context.Background(), fixture.ref.BaseLSN)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = scanner.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v, want %v", err, context.Canceled)
	}
	if got := len(store.reads()); got != 3 {
		t.Fatalf("range reads = %d, want metadata-only 3", got)
	}
}
