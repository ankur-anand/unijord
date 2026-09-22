package blobstore

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"testing/iotest"
)

func TestValidKey(t *testing.T) {
	for _, ok := range []string{"a", "a/b.c", "tenant/runs/0bb/x.ujrn", strings.Repeat("k", MaxKeyBytes)} {
		if !ValidKey(ok) {
			t.Fatalf("rejected %q", ok)
		}
	}
	for _, bad := range []string{"", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "a\x00b", "a\nb", "a\x7fb",
		"\xff\xfe", strings.Repeat("k", MaxKeyBytes+1)} {
		if ValidKey(bad) {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestCheckRangeOverflow(t *testing.T) {
	id := RunIdentity{Key: "k", Size: 100, Token: "t"}
	for _, tc := range []struct {
		offset, length int64
		want           error
	}{
		{0, 100, nil}, {99, 1, nil}, {100, 1, ErrInvalidRequest}, {0, 101, ErrInvalidRequest}, {-1, 1, ErrInvalidRequest},
		{0, 0, ErrInvalidRequest}, {math.MaxInt64, math.MaxInt64, ErrInvalidRequest}, {1, math.MaxInt64, ErrInvalidRequest},
	} {
		if err := CheckRange(id, "k", tc.offset, tc.length); !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
			t.Fatalf("(%d,%d): %v", tc.offset, tc.length, err)
		}
	}
	if err := CheckRange(id, "other", 0, 1); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatal(err)
	}
	if err := CheckRange(RunIdentity{Key: "k", Size: 100}, "k", 0, 1); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatal(err)
	}
}

func TestListLimitNormalization(t *testing.T) {
	for in, want := range map[int]int{-1: 1000, 0: 1000, 1: 1, 1000: 1000, 1001: 1000, 1 << 30: 1000} {
		if got := (ListOptions{Limit: in}).NormalizedLimit(); got != want {
			t.Fatalf("%d: %d", in, got)
		}
	}
}

// drain reads with a deliberately awkward buffer so chunk boundaries land on
// and around the final byte.
func drain(b *CountingBody, chunk int) ([]byte, error) {
	var out []byte
	buf := make([]byte, chunk)
	for {
		n, err := b.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, err
		}
	}
}

func TestCountingBodyWithholdsFinalByteUntilEOF(t *testing.T) {
	data := []byte("0123456789")
	injected := errors.New("producer failed")
	for _, chunk := range []int{1, 3, 10, 64} {
		for name, wrap := range map[string]func(io.Reader) io.Reader{
			"plain": func(r io.Reader) io.Reader { return r }, "one-byte": iotest.OneByteReader,
			"data-err": iotest.DataErrReader, "half": iotest.HalfReader,
		} {
			exact := NewCountingBody(wrap(bytes.NewReader(data)), 10)
			got, err := drain(exact, chunk)
			if err != io.EOF || !bytes.Equal(got, data) || !exact.Complete() || exact.Consumed() != 10 {
				t.Fatalf("%s/%d exact: %q %v", name, chunk, got, err)
			}
			short := NewCountingBody(wrap(bytes.NewReader(data[:9])), 10)
			got, err = drain(short, chunk)
			if !errors.Is(err, io.ErrUnexpectedEOF) || short.Complete() || short.Consumed() >= 10 || len(got) >= 10 {
				t.Fatalf("%s/%d short: %q %v", name, chunk, got, err)
			}
			long := NewCountingBody(wrap(bytes.NewReader(append(bytes.Clone(data), 'x'))), 10)
			got, err = drain(long, chunk)
			if !errors.Is(err, ErrInvalidRequest) || long.Complete() || long.Consumed() != 9 || len(got) != 9 {
				t.Fatalf("%s/%d long: released %d bytes, %v", name, chunk, len(got), err)
			}
			late := NewCountingBody(wrap(io.MultiReader(bytes.NewReader(data), iotest.ErrReader(injected))), 10)
			got, err = drain(late, chunk)
			if !errors.Is(err, injected) || late.Complete() || len(got) != 9 || !errors.Is(late.Err(), injected) {
				t.Fatalf("%s/%d late error: released %d bytes, %v", name, chunk, len(got), err)
			}
			if _, again := late.Read(make([]byte, 4)); !errors.Is(again, injected) {
				t.Fatalf("%s/%d error not sticky: %v", name, chunk, again)
			}
		}
	}
}

type stalled struct{}

func (stalled) Read([]byte) (int, error) { return 0, nil }

func TestCountingBodyBoundsEmptyProbeReads(t *testing.T) {
	body := NewCountingBody(io.MultiReader(bytes.NewReader([]byte("ab")), stalled{}), 2)
	got, err := drain(body, 8)
	if !errors.Is(err, io.ErrNoProgress) || len(got) != 1 || body.Complete() {
		t.Fatalf("%q %v", got, err)
	}
}

func TestOutcomeZeroValuesAreUncertain(t *testing.T) {
	if (CASResult{}).Outcome != CASUnknown || (CreateResult{}).Outcome != CreateIndeterminate {
		t.Fatal("zero outcome must be the uncertain outcome")
	}
	if CASUnknown.String() != "unknown" || CreateIndeterminate.String() != "indeterminate" {
		t.Fatal("outcome names")
	}
}
