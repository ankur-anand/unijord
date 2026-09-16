package runfile

import (
	"math"
	"testing"
)

func TestCheckedArithmetic(t *testing.T) {
	if got, ok := checkedAdd(math.MaxUint64-1, 1); !ok || got != math.MaxUint64 {
		t.Fatalf("checkedAdd boundary=(%d,%v)", got, ok)
	}
	if _, ok := checkedAdd(math.MaxUint64, 1); ok {
		t.Fatal("checkedAdd accepted overflow")
	}
	if got, ok := checkedMultiply(math.MaxUint64, 1); !ok || got != math.MaxUint64 {
		t.Fatalf("checkedMultiply boundary=(%d,%v)", got, ok)
	}
	if got, ok := checkedMultiply(0, math.MaxUint64); !ok || got != 0 {
		t.Fatalf("checkedMultiply zero=(%d,%v)", got, ok)
	}
	if _, ok := checkedMultiply(math.MaxUint64, 2); ok {
		t.Fatal("checkedMultiply accepted overflow")
	}
	if got, ok := checkedCeilingDivide(math.MaxUint64, 2); !ok || got != 1<<63 {
		t.Fatalf("checkedCeilingDivide boundary=(%d,%v)", got, ok)
	}
	if got, ok := checkedCeilingDivide(0, 7); !ok || got != 0 {
		t.Fatalf("checkedCeilingDivide zero=(%d,%v)", got, ok)
	}
	if _, ok := checkedCeilingDivide(1, 0); ok {
		t.Fatal("checkedCeilingDivide accepted zero denominator")
	}
	if got, ok := checkedAlign(9, 8); !ok || got != 16 {
		t.Fatalf("checkedAlign=(%d,%v), want (16,true)", got, ok)
	}
	if got, ok := checkedAlign8(math.MaxUint64 - 7); !ok || got != math.MaxUint64-7 {
		t.Fatalf("checkedAlign8 boundary=(%d,%v)", got, ok)
	}
	if _, ok := checkedAlign8(math.MaxUint64); ok {
		t.Fatal("checkedAlign8 accepted overflow")
	}
	if _, ok := checkedAlign(1, 0); ok {
		t.Fatal("checkedAlign accepted zero alignment")
	}
}

func TestCheckedArithmeticOverflowDoesNotAllocate(t *testing.T) {
	allocations := testing.AllocsPerRun(1_000, func() {
		if _, ok := checkedAdd(math.MaxUint64, 1); ok {
			panic("checkedAdd accepted overflow")
		}
		if _, ok := checkedMultiply(math.MaxUint64, 2); ok {
			panic("checkedMultiply accepted overflow")
		}
		if _, ok := checkedAlign8(math.MaxUint64); ok {
			panic("checkedAlign8 accepted overflow")
		}
		if rangeContains(math.MaxUint64, 1, math.MaxUint64, 1) {
			panic("rangeContains accepted overflow")
		}
	})
	if allocations != 0 {
		t.Fatalf("overflow checks allocated %g times per run, want 0", allocations)
	}
}

func TestRangeContains(t *testing.T) {
	tests := []struct {
		name                     string
		outerOffset, outerLength uint64
		innerOffset, innerLength uint64
		want                     bool
	}{
		{name: "same", outerOffset: 10, outerLength: 10, innerOffset: 10, innerLength: 10, want: true},
		{name: "inside", outerOffset: 10, outerLength: 10, innerOffset: 12, innerLength: 3, want: true},
		{name: "empty at end", outerOffset: 10, outerLength: 10, innerOffset: 20, innerLength: 0, want: true},
		{name: "starts before", outerOffset: 10, outerLength: 10, innerOffset: 9, innerLength: 1},
		{name: "ends after", outerOffset: 10, outerLength: 10, innerOffset: 19, innerLength: 2},
		{name: "outer overflow", outerOffset: math.MaxUint64, outerLength: 1, innerOffset: math.MaxUint64, innerLength: 0},
		{name: "inner overflow", outerOffset: 0, outerLength: math.MaxUint64, innerOffset: math.MaxUint64, innerLength: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rangeContains(test.outerOffset, test.outerLength, test.innerOffset, test.innerLength); got != test.want {
				t.Fatalf("rangeContains()=%v, want %v", got, test.want)
			}
		})
	}
}
