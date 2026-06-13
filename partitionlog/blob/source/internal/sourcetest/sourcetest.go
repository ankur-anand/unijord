package sourcetest

import (
	"bytes"
	"context"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

type Fixture struct {
	Store segreader.SegmentStore
	Key   string
	Body  []byte
}

type Config struct {
	NewFixture func(t testing.TB) Fixture
}

func Run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.NewFixture == nil {
		t.Fatal("sourcetest: nil NewFixture")
	}
	t.Run("read_ranges", func(t *testing.T) {
		runReadRanges(t, cfg.newFixture(t))
	})
	t.Run("zero_length", func(t *testing.T) {
		runZeroLength(t, cfg.newFixture(t))
	})
	t.Run("bad_inputs", func(t *testing.T) {
		runBadInputs(t, cfg.newFixture(t))
	})
	t.Run("defensive_copy", func(t *testing.T) {
		runDefensiveCopy(t, cfg.newFixture(t))
	})
}

func (c Config) newFixture(t testing.TB) Fixture {
	t.Helper()
	fixture := c.NewFixture(t)
	if fixture.Store == nil {
		t.Fatal("sourcetest: nil Store")
	}
	if fixture.Key == "" {
		t.Fatal("sourcetest: empty Key")
	}
	if len(fixture.Body) < 16 {
		t.Fatalf("sourcetest: Body length=%d, want at least 16 bytes", len(fixture.Body))
	}
	return fixture
}

func runReadRanges(t *testing.T, fixture Fixture) {
	t.Helper()
	ctx := context.Background()

	checkReadAt(t, ctx, fixture, 0, 5)
	checkReadAt(t, ctx, fixture, 6, 4)
	checkReadAt(t, ctx, fixture, uint64(len(fixture.Body)-4), 4)
}

func runZeroLength(t *testing.T, fixture Fixture) {
	t.Helper()
	got, err := fixture.Store.ReadAt(context.Background(), fixture.Key, 3, 0)
	if err != nil {
		t.Fatalf("ReadAt(zero length) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ReadAt(zero length) len = %d, want 0", len(got))
	}
}

func runBadInputs(t *testing.T, fixture Fixture) {
	t.Helper()
	ctx := context.Background()

	if _, err := fixture.Store.ReadAt(ctx, "", 0, 1); err == nil {
		t.Fatal("ReadAt(empty key) error = nil, want error")
	}
	if _, err := fixture.Store.ReadAt(ctx, fixture.Key, ^uint64(0), 2); err == nil {
		t.Fatal("ReadAt(overflow range) error = nil, want error")
	}
	if _, err := fixture.Store.ReadAt(ctx, fixture.Key, uint64(len(fixture.Body)), 1); err == nil {
		t.Fatal("ReadAt(range beyond end) error = nil, want error")
	}
	if _, err := fixture.Store.ReadAt(ctx, fixture.Key+"-missing", 0, 1); err == nil {
		t.Fatal("ReadAt(missing key) error = nil, want error")
	}
}

func runDefensiveCopy(t *testing.T, fixture Fixture) {
	t.Helper()
	ctx := context.Background()
	got, err := fixture.Store.ReadAt(ctx, fixture.Key, 1, 5)
	if err != nil {
		t.Fatalf("ReadAt(first) error = %v", err)
	}
	got[0] ^= 0xff
	again, err := fixture.Store.ReadAt(ctx, fixture.Key, 1, 5)
	if err != nil {
		t.Fatalf("ReadAt(second) error = %v", err)
	}
	want := fixture.Body[1:6]
	if !bytes.Equal(again, want) {
		t.Fatalf("ReadAt returned mutable backing storage: got %q want %q", again, want)
	}
}

func checkReadAt(t *testing.T, ctx context.Context, fixture Fixture, off uint64, n uint64) {
	t.Helper()
	got, err := fixture.Store.ReadAt(ctx, fixture.Key, off, n)
	if err != nil {
		t.Fatalf("ReadAt(offset=%d length=%d) error = %v", off, n, err)
	}
	want := fixture.Body[off : off+n]
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadAt(offset=%d length=%d) = %q, want %q", off, n, got, want)
	}
}
