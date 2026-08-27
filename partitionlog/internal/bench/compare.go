package bench

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// Threshold bounds how far a metric may move from its baseline before the
// comparison fails. Ratios are fractions: 0.25 means +25 %. Zero means
// "exact" for counts and "not gated" for latencies (see DefaultThreshold).
type Threshold struct {
	P50Ratio   float64 `json:"p50_ratio,omitempty"`   // max allowed increase of p50
	P99Ratio   float64 `json:"p99_ratio,omitempty"`   // max allowed increase of p99
	RateRatio  float64 `json:"rate_ratio,omitempty"`  // max allowed decrease of rate
	ValueRatio float64 `json:"value_ratio,omitempty"` // max allowed increase of a scalar (bytes)
	Exact      bool    `json:"exact,omitempty"`       // scalar must match exactly (object counts)
	Ignore     bool    `json:"ignore,omitempty"`      // informational only
}

// Baseline is a promoted Result with per-metric thresholds.
type Baseline struct {
	Result
	Thresholds map[string]Threshold `json:"thresholds"`
}

// DefaultThreshold returns the gate for a metric given the profile. Smoke is
// coarse (a 3× step is a structural regression, not noise); ci and deep are
// tight enough to track drift on a stable runner class.
func DefaultThreshold(profile string, m Metric) Threshold {
	if m.Kind == KindScalar {
		if strings.HasSuffix(m.Name, ".objects") || strings.HasSuffix(m.Name, ".gets_max") {
			return Threshold{Exact: true}
		}
		if profile == "smoke" {
			return Threshold{ValueRatio: 0.5}
		}
		return Threshold{ValueRatio: 0.10}
	}
	if profile == "smoke" {
		// Smoke samples are small (tens of calls) so p99 and rate are a single
		// stall away from a false alarm; gate p50 only and keep the rest
		// informational. The structural checks carry the smoke tier.
		return Threshold{P50Ratio: 2.0}
	}
	return Threshold{P50Ratio: 0.25, P99Ratio: 0.50, RateRatio: 0.15}
}

// BaselinePath is the checked-in location for a scenario/profile/provider.
func BaselinePath(dir, scenario, profile, provider string) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s-%s.json", scenario, profile, provider))
}

func ReadBaseline(path string) (*Baseline, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var base Baseline
	if err := json.Unmarshal(b, &base); err != nil {
		return nil, fmt.Errorf("bench: parse baseline %s: %w", path, err)
	}
	return &base, nil
}

// Promote turns a result into a baseline with default thresholds for its
// profile, preserving any thresholds already set for the same metric names.
func Promote(r *Result, existing *Baseline) *Baseline {
	base := &Baseline{Result: *r, Thresholds: map[string]Threshold{}}
	for _, m := range r.Metrics {
		if existing != nil {
			if t, ok := existing.Thresholds[m.Name]; ok {
				base.Thresholds[m.Name] = t
				continue
			}
		}
		base.Thresholds[m.Name] = DefaultThreshold(r.Profile, m)
	}
	return base
}

func (b *Baseline) WriteJSON(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	out, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

// Row is one line of a comparison.
type Row struct {
	Metric        string
	Field         string // p50, p99, rate, value, check
	Baseline      string
	Current       string
	Delta         string
	Limit         string
	Changed       bool // moved beyond the informational band (10 % or exact mismatch)
	Failed        bool
	Informational bool
}

// Report is the outcome of comparing a result to its baseline.
type Report struct {
	Rows       []Row
	Failed     bool
	Mismatched string // non-empty when params/provider differ and no comparison was made
}

// Compare gates r against base. Checks in r fail the report on their own;
// metrics are gated by the baseline's thresholds.
func Compare(r *Result, base *Baseline) Report {
	rep := Report{}
	if base != nil {
		if base.Env.Provider != r.Env.Provider || base.Scenario != r.Scenario || base.Profile != r.Profile {
			rep.Mismatched = fmt.Sprintf("baseline is %s/%s/%s, result is %s/%s/%s", base.Scenario, base.Profile, base.Env.Provider, r.Scenario, r.Profile, r.Env.Provider)
			rep.Failed = true
			return rep
		}
		if base.Params != r.Params {
			rep.Mismatched = "baseline params differ from result params; refresh the baseline or run with matching parameters"
			rep.Failed = true
			return rep
		}
	}
	seenChecks := map[string]int{}
	for _, c := range r.Checks {
		seenChecks[c.Name]++
		row := Row{Metric: c.Name, Field: "check", Current: boolMark(c.OK), Failed: !c.OK, Changed: !c.OK}
		if c.Detail != "" {
			row.Current += " " + c.Detail
		}
		if !c.OK {
			rep.Failed = true
		}
		rep.Rows = append(rep.Rows, row)
	}
	for name, n := range seenChecks {
		if n > 1 {
			rep.Failed = true
			rep.Rows = append(rep.Rows, Row{Metric: name, Field: "check", Current: fmt.Sprintf("❌ recorded %d times", n), Failed: true, Changed: true})
		}
	}
	if base != nil {
		// A check or metric that the baseline has and the result lacks is a
		// silent loss of coverage — the invariant meant to catch a regression
		// may be the thing that was deleted. Fail loudly.
		for _, bc := range base.Checks {
			if seenChecks[bc.Name] == 0 {
				rep.Failed = true
				rep.Rows = append(rep.Rows, Row{Metric: bc.Name, Field: "check", Baseline: "present", Current: "❌ missing", Failed: true, Changed: true})
			}
		}
		seenMetrics := map[string]int{}
		for _, m := range r.Metrics {
			seenMetrics[m.Name]++
		}
		for _, bm := range base.Metrics {
			switch {
			case seenMetrics[bm.Name] == 0:
				rep.Failed = true
				rep.Rows = append(rep.Rows, Row{Metric: bm.Name, Field: "metric", Baseline: "present", Current: "❌ missing", Failed: true, Changed: true})
			case seenMetrics[bm.Name] > 1:
				rep.Failed = true
				rep.Rows = append(rep.Rows, Row{Metric: bm.Name, Field: "metric", Current: fmt.Sprintf("❌ recorded %d times", seenMetrics[bm.Name]), Failed: true, Changed: true})
			default:
				cm, _ := r.Metric(bm.Name)
				if cm.Kind != bm.Kind || cm.Unit != bm.Unit {
					rep.Failed = true
					rep.Rows = append(rep.Rows, Row{Metric: bm.Name, Field: "metric", Baseline: string(bm.Kind) + " " + bm.Unit, Current: "❌ " + string(cm.Kind) + " " + cm.Unit, Failed: true, Changed: true})
				}
			}
		}
	}
	if base == nil {
		for _, m := range r.Metrics {
			rep.Rows = append(rep.Rows, metricRows(m, nil, Threshold{Ignore: true})...)
		}
		return rep
	}
	for _, m := range r.Metrics {
		bm, ok := base.Metric(m.Name)
		t, hasT := base.Thresholds[m.Name]
		if !hasT {
			t = DefaultThreshold(r.Profile, m)
		}
		var bp *Metric
		if ok {
			bp = &bm
		}
		rows := metricRows(m, bp, t)
		for _, row := range rows {
			if row.Failed {
				rep.Failed = true
			}
		}
		rep.Rows = append(rep.Rows, rows...)
	}
	return rep
}

func metricRows(m Metric, base *Metric, t Threshold) []Row {
	var rows []Row
	add := func(field string, cur, bas float64, fmtv func(float64) string, higherIsWorse bool, limit float64, exact bool) {
		row := Row{Metric: m.Name, Field: field, Current: fmtv(cur), Informational: t.Ignore || base == nil}
		if base == nil {
			row.Baseline = "—"
			rows = append(rows, row)
			return
		}
		row.Baseline = fmtv(bas)
		if exact {
			row.Limit = "exact"
			row.Changed = cur != bas
			row.Failed = row.Changed && !t.Ignore
			row.Delta = fmt.Sprintf("%+.0f", cur-bas)
			rows = append(rows, row)
			return
		}
		if bas == 0 {
			row.Delta = "n/a"
			rows = append(rows, row)
			return
		}
		delta := (cur - bas) / bas
		if !higherIsWorse {
			delta = -delta // for rates, a drop is the bad direction
		}
		row.Delta = fmt.Sprintf("%+.0f%%", (cur-bas)/bas*100)
		row.Limit = fmt.Sprintf("%+.0f%%", limit*100*signFor(higherIsWorse))
		row.Changed = math.Abs((cur-bas)/bas) >= 0.10
		row.Failed = limit > 0 && delta > limit && !t.Ignore
		rows = append(rows, row)
	}
	switch m.Kind {
	case KindLatency:
		var bp50, bp99, brate float64
		if base != nil {
			bp50, bp99, brate = base.P50, base.P99, base.Rate
		}
		add("p50", m.P50, bp50, fmtMS, true, t.P50Ratio, false)
		add("p99", m.P99, bp99, fmtMS, true, t.P99Ratio, false)
		if m.Rate > 0 {
			add("rate", m.Rate, brate, fmtRate, false, t.RateRatio, false)
		}
	case KindScalar:
		var bv float64
		if base != nil {
			bv = base.Value
		}
		unit := m.Unit
		// Throughput-shaped scalars ("del/s", "rec/s", "seg/s", …) regress when
		// they fall; everything else (bytes, counts) regresses when it rises.
		higherIsWorse := !strings.HasSuffix(unit, "/s")
		add("value", m.Value, bv, func(v float64) string { return fmtScalar(v, unit) }, higherIsWorse, t.ValueRatio, t.Exact)
	}
	return rows
}

func signFor(higherIsWorse bool) float64 {
	if higherIsWorse {
		return 1
	}
	return -1
}

func boolMark(ok bool) string {
	if ok {
		return "✅"
	}
	return "❌"
}

// Format renders a report. format is "text", "markdown", or "github"
// (markdown with a heading and a failure banner). onlyChanged hides rows that
// stayed within the informational band.
func (rep Report) Format(format string, onlyChanged bool, r *Result) string {
	var b strings.Builder
	if format == "github" {
		status := "✅ all checks pass, no metric beyond threshold"
		if rep.Failed {
			status = "❌ **regression** — see rows marked ❌"
		}
		fmt.Fprintf(&b, "### plbench %s · %s · %s\n\n%s\n\n", r.Scenario, r.Profile, r.Env.Provider, status)
	}
	if rep.Mismatched != "" {
		fmt.Fprintf(&b, "**cannot compare:** %s\n", rep.Mismatched)
		return b.String()
	}
	if format == "text" {
		fmt.Fprintf(&b, "%-42s %-6s %12s %12s %8s %8s %s\n", "metric", "field", "baseline", "current", "delta", "limit", "")
		for _, row := range rep.Rows {
			if onlyChanged && !row.Changed && !row.Failed {
				continue
			}
			mark := ""
			if row.Failed {
				mark = "FAIL"
			} else if row.Changed {
				mark = "changed"
			}
			fmt.Fprintf(&b, "%-42s %-6s %12s %12s %8s %8s %s\n", row.Metric, row.Field, row.Baseline, row.Current, row.Delta, row.Limit, mark)
		}
		return b.String()
	}
	b.WriteString("| metric | field | baseline | current | Δ | limit | |\n|---|---|---:|---:|---:|---:|---|\n")
	shown := 0
	for _, row := range rep.Rows {
		if onlyChanged && !row.Changed && !row.Failed {
			continue
		}
		mark := ""
		if row.Failed {
			mark = "❌"
		} else if row.Changed {
			mark = "⚠️"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s |\n", row.Metric, row.Field, row.Baseline, row.Current, row.Delta, row.Limit, mark)
		shown++
	}
	if onlyChanged {
		fmt.Fprintf(&b, "\n%d of %d rows shown (rows within the informational band are omitted).\n", shown, len(rep.Rows))
	}
	return b.String()
}
