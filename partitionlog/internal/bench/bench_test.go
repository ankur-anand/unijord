package bench

import (
	"testing"
	"time"
)

func TestExpectedSealedPages(t *testing.T) {
	cases := []struct {
		segments, leaf, k int
		leaves            int
		index             []int
	}{
		{0, 128, 128, 0, nil},
		{127, 128, 128, 0, nil},
		{128, 128, 128, 1, nil},
		{2_000, 128, 8, 15, []int{1}},
		{100_000, 128, 128, 781, []int{6}},
		{10_000_000, 128, 128, 78_125, []int{610, 4}},
	}
	for _, c := range cases {
		leaves, index := ExpectedSealedPages(c.segments, c.leaf, c.k)
		if leaves != c.leaves || len(index) != len(c.index) {
			t.Fatalf("ExpectedSealedPages(%d,%d,%d) = %d,%v want %d,%v", c.segments, c.leaf, c.k, leaves, index, c.leaves, c.index)
		}
		for i := range index {
			if index[i] != c.index[i] {
				t.Fatalf("ExpectedSealedPages(%d,%d,%d) = %d,%v want %d,%v", c.segments, c.leaf, c.k, leaves, index, c.leaves, c.index)
			}
		}
	}
}

func TestPageSizesMatchSpec(t *testing.T) {
	leaf, index := PageSizes(128, 128)
	if leaf != 16544 || index != 10400 {
		t.Fatalf("PageSizes(128,128) = %d,%d want 16544,10400", leaf, index)
	}
	if _, index := PageSizes(128, 8); index != 800 {
		t.Fatalf("PageSizes(128,8) index = %d want 800", index)
	}
}

func TestCompareGatesChecksAndThresholds(t *testing.T) {
	base := &Baseline{
		Result: Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"}, Metrics: []Metric{
			{Name: "lat", Kind: KindLatency, P50: 10, P99: 20, Rate: 100},
			{Name: "catalog.x.objects", Kind: KindScalar, Value: 6},
			{Name: "bytes", Kind: KindScalar, Value: 1000, Unit: "bytes"},
		}},
		Thresholds: map[string]Threshold{
			"lat":               {P50Ratio: 0.25, P99Ratio: 0.5, RateRatio: 0.15},
			"catalog.x.objects": {Exact: true},
			"bytes":             {ValueRatio: 0.10},
		},
	}
	ok := &Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"}, Metrics: []Metric{
		{Name: "lat", Kind: KindLatency, P50: 12, P99: 25, Rate: 90},
		{Name: "catalog.x.objects", Kind: KindScalar, Value: 6},
		{Name: "bytes", Kind: KindScalar, Value: 1050, Unit: "bytes"},
	}, Checks: []Check{{Name: "c", OK: true}}}
	if rep := Compare(ok, base); rep.Failed {
		t.Fatalf("within thresholds should pass: %+v", rep.Rows)
	}

	slow := *ok
	slow.Metrics = []Metric{{Name: "lat", Kind: KindLatency, P50: 13, P99: 25, Rate: 90}}
	if rep := Compare(&slow, base); !rep.Failed {
		t.Fatalf("p50 +30%% must fail a +25%% threshold")
	}

	garbage := *ok
	garbage.Metrics = []Metric{{Name: "catalog.x.objects", Kind: KindScalar, Value: 7}}
	if rep := Compare(&garbage, base); !rep.Failed {
		t.Fatalf("object count mismatch must fail an exact threshold")
	}

	failedCheck := *ok
	failedCheck.Checks = []Check{{Name: "c", OK: false, Detail: "boom"}}
	if rep := Compare(&failedCheck, base); !rep.Failed {
		t.Fatalf("a failed check must fail the comparison")
	}

	otherProvider := *ok
	otherProvider.Env.Provider = "azurite"
	if rep := Compare(&otherProvider, base); !rep.Failed || rep.Mismatched == "" {
		t.Fatalf("cross-provider comparison must be refused")
	}

	if rep := Compare(ok, nil); rep.Failed {
		t.Fatalf("no baseline: checks only, must pass")
	}
}

func TestPromoteKeepsExistingThresholds(t *testing.T) {
	r := &Result{Profile: "smoke", Metrics: []Metric{{Name: "lat", Kind: KindLatency, P50: 1}, {Name: "n.objects", Kind: KindScalar, Value: 1}}}
	existing := &Baseline{Thresholds: map[string]Threshold{"lat": {P50Ratio: 9}}}
	b := Promote(r, existing)
	if b.Thresholds["lat"].P50Ratio != 9 {
		t.Fatalf("existing threshold not preserved: %+v", b.Thresholds["lat"])
	}
	if !b.Thresholds["n.objects"].Exact {
		t.Fatalf("object counts must default to exact")
	}
}

func TestSummarize(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	s := Summarize(d)
	if s.N != 100 || s.P50 != 50*time.Millisecond || s.P99 != 99*time.Millisecond || s.Max != 100*time.Millisecond {
		t.Fatalf("Summarize = %+v", s)
	}
}

func TestCompareRateScalarsGateOnDecrease(t *testing.T) {
	base := &Baseline{
		Result:     Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"}, Metrics: []Metric{{Name: "reclaim.deletes_per_s", Kind: KindScalar, Value: 100, Unit: "del/s"}}},
		Thresholds: map[string]Threshold{"reclaim.deletes_per_s": {ValueRatio: 0.10}},
	}
	faster := &Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"}, Metrics: []Metric{{Name: "reclaim.deletes_per_s", Kind: KindScalar, Value: 150, Unit: "del/s"}}}
	if rep := Compare(faster, base); rep.Failed {
		t.Fatalf("a faster rate must not fail: %+v", rep.Rows)
	}
	slower := &Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"}, Metrics: []Metric{{Name: "reclaim.deletes_per_s", Kind: KindScalar, Value: 80, Unit: "del/s"}}}
	if rep := Compare(slower, base); !rep.Failed {
		t.Fatalf("a rate drop beyond threshold must fail")
	}
}

func TestCompareFailsWhenBaselineCoverageIsLost(t *testing.T) {
	base := &Baseline{
		Result: Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"},
			Metrics: []Metric{{Name: "lat", Kind: KindLatency, P50: 10, P99: 20}, {Name: "n.objects", Kind: KindScalar, Value: 1}},
			Checks:  []Check{{Name: "invariant", OK: true}}},
		Thresholds: map[string]Threshold{"lat": {P50Ratio: 1, P99Ratio: 1}, "n.objects": {Exact: true}},
	}
	full := &Result{Scenario: "s", Profile: "ci", Env: Env{Provider: "minio"},
		Metrics: []Metric{{Name: "lat", Kind: KindLatency, P50: 10, P99: 20}, {Name: "n.objects", Kind: KindScalar, Value: 1}, {Name: "extra", Kind: KindScalar, Value: 3}},
		Checks:  []Check{{Name: "invariant", OK: true}, {Name: "new_check", OK: true}}}
	if rep := Compare(full, base); rep.Failed {
		t.Fatalf("new metrics and checks are informational: %+v", rep.Rows)
	}

	noCheck := *full
	noCheck.Checks = []Check{{Name: "new_check", OK: true}}
	if rep := Compare(&noCheck, base); !rep.Failed {
		t.Fatalf("a baseline check missing from the result must fail")
	}

	noMetric := *full
	noMetric.Metrics = []Metric{{Name: "n.objects", Kind: KindScalar, Value: 1}}
	if rep := Compare(&noMetric, base); !rep.Failed {
		t.Fatalf("a baseline metric missing from the result must fail")
	}

	wrongKind := *full
	wrongKind.Metrics = []Metric{{Name: "lat", Kind: KindScalar, Value: 10}, {Name: "n.objects", Kind: KindScalar, Value: 1}}
	if rep := Compare(&wrongKind, base); !rep.Failed {
		t.Fatalf("a metric whose kind changed must fail")
	}

	dup := *full
	dup.Checks = append(dup.Checks, Check{Name: "invariant", OK: true})
	if rep := Compare(&dup, base); !rep.Failed {
		t.Fatalf("a duplicated check must fail")
	}
}

func TestValidatePrefix(t *testing.T) {
	for _, ok := range []string{"plbench/x", "plbench/catalog_history/smoke/1", "plbench"} {
		if err := ValidatePrefix(ok); err != nil {
			t.Fatalf("%q should be accepted: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "production", "plbench-old/x", "plbench/../prod", "x/plbench/y", "plbench//x"} {
		if err := ValidatePrefix(bad); err == nil {
			t.Fatalf("%q should be rejected", bad)
		}
	}
}
