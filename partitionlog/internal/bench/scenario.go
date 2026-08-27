// Package bench is the harness for partitionlog load scenarios: provider
// resolution for every supported object store, latency sampling, a
// self-describing result schema, and baseline comparison with thresholds.
//
// Scenarios live in the scenarios subpackage and register themselves; the
// plbench command runs them. Nothing here runs under `go test`; every
// scenario needs a live object store or emulator.
package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// Params is the flat set of knobs a scenario may use. Profiles pick defaults;
// flags override. Unused fields are zero and ignored by scenarios that do not
// read them. The struct must stay comparable: Compare refuses to gate a result
// against a baseline with different params.
type Params struct {
	Segments          int           `json:"segments,omitempty"`
	RecordsPerSegment int           `json:"records_per_segment,omitempty"`
	RecordBytes       int           `json:"record_bytes,omitempty"`
	LeafLimit         int           `json:"leaf_limit,omitempty"`
	IndexLimit        int           `json:"index_limit,omitempty"`
	Samples           int           `json:"samples,omitempty"`
	ReplaySegments    int           `json:"replay_segments,omitempty"`
	WriterOpens       int           `json:"writer_opens,omitempty"`
	Inflight          int           `json:"inflight,omitempty"`
	Partitions        int           `json:"partitions,omitempty"`
	Tailers           int           `json:"tailers,omitempty"`
	Duration          time.Duration `json:"duration,omitempty"`
	Retention         bool          `json:"retention,omitempty"`
	Reclaim           bool          `json:"reclaim,omitempty"`
	Scrub             bool          `json:"scrub,omitempty"`
}

// Scenario is one load test. Profiles must include "smoke"; "ci" and "deep"
// are conventional.
type Scenario interface {
	Name() string
	Description() string
	Profiles() map[string]Params
	Run(ctx context.Context, run *Run) error
}

var registry = map[string]Scenario{}

func Register(s Scenario) {
	if _, dup := registry[s.Name()]; dup {
		panic("bench: duplicate scenario " + s.Name())
	}
	registry[s.Name()] = s
}

func Lookup(name string) (Scenario, bool) {
	s, ok := registry[name]
	return s, ok
}

func Scenarios() []Scenario {
	out := make([]Scenario, 0, len(registry))
	for _, s := range registry {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Run is the per-execution context handed to a scenario: the provider, the
// resolved parameters, a unique object prefix, and the result being built.
type Run struct {
	Provider Provider
	Params   Params
	Profile  string
	Prefix   string
	Keep     bool
	Log      io.Writer
	Result   *Result
	started  time.Time
}

// NewRun prepares a run. prefix may be empty; a unique one is generated so
// concurrent runs never share objects. Reusing a prefix is how resumable
// scenarios continue a previous run.
func NewRun(scenario Scenario, profile string, p Provider, params Params, prefix string, keep bool, log io.Writer) (*Run, error) {
	if _, ok := scenario.Profiles()[profile]; !ok {
		return nil, fmt.Errorf("bench: scenario %s has no profile %q", scenario.Name(), profile)
	}
	if prefix == "" {
		prefix = fmt.Sprintf("plbench/%s/%s/%d", scenario.Name(), profile, time.Now().UnixNano())
	}
	if err := ValidatePrefix(prefix); err != nil {
		return nil, err
	}
	if log == nil {
		log = os.Stdout
	}
	now := time.Now()
	return &Run{
		Provider: p, Params: params, Profile: profile, Prefix: strings.Trim(prefix, "/"), Keep: keep, Log: log,
		Result: &Result{
			Scenario: scenario.Name(), Profile: profile, Params: params, Env: CaptureEnv(p), Prefix: prefix, StartedAt: now,
			Inventory: map[string]InventoryClass{},
		},
		started: now,
	}, nil
}

// Execute runs the scenario, stamps elapsed time, and cleans up the prefix
// unless Keep is set or the scenario failed (failed runs keep their objects
// for inspection).
func (r *Run) Execute(ctx context.Context, s Scenario) (err error) {
	defer func() {
		r.Result.ElapsedS = time.Since(r.started).Seconds()
		if r.Keep || err != nil || r.Result.Failed() {
			r.Logf("objects kept under %s", r.Prefix)
			return
		}
		// Cleanup runs even if the run context was canceled, but never for
		// longer than CleanupTimeout, and a cleanup failure fails the run:
		// a baseline must not be promoted from a run that leaked objects.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
		defer cancel()
		t0 := time.Now()
		n, cerr := r.Provider.Cleanup(cctx, r.Prefix)
		if cerr != nil {
			err = fmt.Errorf("bench: cleanup of %s failed after deleting %d objects: %w", r.Prefix, n, cerr)
			r.Logf("cleanup error: %v", cerr)
			return
		}
		r.Logf("cleanup: deleted %d objects in %s", n, time.Since(t0).Round(time.Millisecond))
	}()
	r.Section("%s · %s · %s · prefix %s", s.Name(), r.Profile, r.Provider.Name(), r.Prefix)
	return s.Run(ctx, r)
}

func (r *Run) Section(format string, args ...any) {
	fmt.Fprintf(r.Log, "\n"+format+"\n", args...)
}

func (r *Run) Logf(format string, args ...any) {
	fmt.Fprintf(r.Log, "  "+format+"\n", args...)
}

// Latency records a latency metric from a sample. elapsed may be zero when a
// rate is meaningless.
func (r *Run) Latency(name string, s Stats, elapsed time.Duration, note string) {
	m := Metric{Name: name, Kind: KindLatency, N: s.N, P50: ms(s.P50), P90: ms(s.P90), P99: ms(s.P99), Max: ms(s.Max), Mean: ms(s.Mean), Note: note}
	if elapsed > 0 && s.N > 0 {
		m.Rate = float64(s.N) / elapsed.Seconds()
	}
	r.Result.Metrics = append(r.Result.Metrics, m)
	fmt.Fprintf(r.Log, "  %-38s n=%-7d %9s  p50=%-9s p90=%-9s p99=%-9s max=%-9s %s\n",
		name, s.N, fmtRate(m.Rate), fmtMS(m.P50), fmtMS(m.P90), fmtMS(m.P99), fmtMS(m.Max), note)
}

// Scalar records a single observed value.
func (r *Run) Scalar(name string, value float64, unit, note string) {
	r.Result.Metrics = append(r.Result.Metrics, Metric{Name: name, Kind: KindScalar, Value: value, Unit: unit, Note: note})
	fmt.Fprintf(r.Log, "  %-38s %s %s\n", name, fmtScalar(value, unit), note)
}

// Check records an invariant.
func (r *Run) Check(name string, ok bool, format string, args ...any) {
	detail := fmt.Sprintf(format, args...)
	r.Result.Checks = append(r.Result.Checks, Check{Name: name, OK: ok, Detail: detail})
	fmt.Fprintf(r.Log, "  check %-32s %s %s\n", name, boolMark(ok), detail)
}

// Measure times fn n times sequentially and records the latency metric.
func (r *Run) Measure(name string, n int, fn func() error, note string) error {
	s := &Sample{}
	t0 := time.Now()
	for range n {
		var err error
		s.Time(func() { err = fn() })
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	r.Latency(name, s.Stats(), time.Since(t0), note)
	return nil
}

func (r *Run) Note(format string, args ...any) {
	r.Result.Notes = append(r.Result.Notes, fmt.Sprintf(format, args...))
}

// PrefixRoot is the only object-key root the suite will ever write to or
// delete under. Runs, resumes, and cleanup all refuse anything outside it, so
// a mistyped -prefix cannot delete unrelated data.
const PrefixRoot = "plbench/"

// CleanupTimeout bounds end-of-run object deletion.
const CleanupTimeout = 10 * time.Minute

// ValidatePrefix rejects prefixes outside PrefixRoot or containing path
// tricks.
func ValidatePrefix(prefix string) error {
	p := strings.Trim(prefix, "/")
	if !strings.HasPrefix(p+"/", PrefixRoot) || strings.Contains(p, "..") || strings.Contains(p, "//") {
		return fmt.Errorf("bench: prefix %q must be under %q", prefix, PrefixRoot)
	}
	return nil
}
