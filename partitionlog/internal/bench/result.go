package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"
)

// MetricKind distinguishes latency samples from scalar observations.
type MetricKind string

const (
	KindLatency MetricKind = "latency" // P50/P90/P99/Max/Mean in milliseconds, Rate per second
	KindScalar  MetricKind = "scalar"  // Value in Unit
)

// Metric is one named measurement. Names are stable identifiers used as
// baseline keys, so they never carry run-specific detail; that goes in Note.
type Metric struct {
	Name string     `json:"name"`
	Kind MetricKind `json:"kind"`
	N    int        `json:"n,omitempty"`
	P50  float64    `json:"p50_ms,omitempty"`
	P90  float64    `json:"p90_ms,omitempty"`
	P99  float64    `json:"p99_ms,omitempty"`
	Max  float64    `json:"max_ms,omitempty"`
	Mean float64    `json:"mean_ms,omitempty"`
	Rate float64    `json:"rate_per_s,omitempty"`

	Value float64 `json:"value,omitempty"`
	Unit  string  `json:"unit,omitempty"`

	Note string `json:"note,omitempty"`
}

// Check is a pass/fail invariant a scenario asserts. A failed check fails the
// run regardless of timings.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// InventoryClass summarizes one class of objects under the run prefix.
type InventoryClass struct {
	Objects  int `json:"objects"`
	Bytes    int `json:"bytes"`
	MaxBytes int `json:"max_bytes"`
}

// Env captures where a result was produced. Baselines are only comparable
// within the same provider and machine class.
type Env struct {
	Provider  string `json:"provider"`
	Emulator  bool   `json:"emulator"`
	GitCommit string `json:"git_commit,omitempty"`
	GitDirty  bool   `json:"git_dirty,omitempty"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPU       string `json:"cpu,omitempty"`
	Host      string `json:"host,omitempty"`
	CI        bool   `json:"ci,omitempty"`
}

// Result is the self-describing output of one scenario run.
type Result struct {
	Scenario  string                    `json:"scenario"`
	Profile   string                    `json:"profile"`
	Params    Params                    `json:"params"`
	Env       Env                       `json:"env"`
	Prefix    string                    `json:"prefix"`
	StartedAt time.Time                 `json:"started_at"`
	ElapsedS  float64                   `json:"elapsed_s"`
	Metrics   []Metric                  `json:"metrics"`
	Inventory map[string]InventoryClass `json:"inventory,omitempty"`
	Checks    []Check                   `json:"checks"`
	Notes     []string                  `json:"notes,omitempty"`
}

func (r *Result) Metric(name string) (Metric, bool) {
	for _, m := range r.Metrics {
		if m.Name == name {
			return m, true
		}
	}
	return Metric{}, false
}

func (r *Result) Failed() bool {
	for _, c := range r.Checks {
		if !c.OK {
			return true
		}
	}
	return false
}

func (r *Result) WriteJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func ReadResult(path string) (*Result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("bench: parse %s: %w", path, err)
	}
	return &r, nil
}

// CaptureEnv fills environment fields best-effort; nothing here is fatal.
func CaptureEnv(p Provider) Env {
	env := Env{Provider: p.Name(), Emulator: p.Emulator(), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH}
	if out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output(); err == nil {
		env.GitCommit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil {
		env.GitDirty = strings.TrimSpace(string(out)) != ""
	}
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			env.CPU = strings.TrimSpace(string(out))
		}
	case "linux":
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "model name") {
					if _, v, ok := strings.Cut(line, ":"); ok {
						env.CPU = strings.TrimSpace(v)
					}
					break
				}
			}
		}
	}
	env.Host, _ = os.Hostname()
	env.CI = os.Getenv("CI") != ""
	return env
}

// Markdown renders the full result as a table for job summaries and docs.
func (r *Result) Markdown() string {
	var b strings.Builder
	status := "✅"
	if r.Failed() {
		status = "❌"
	}
	fmt.Fprintf(&b, "### plbench %s · %s · %s %s\n\n", r.Scenario, r.Profile, r.Env.Provider, status)
	fmt.Fprintf(&b, "commit `%s`%s · %s · %s/%s · elapsed %.0fs\n\n", r.Env.GitCommit, dirtyMark(r.Env.GitDirty), r.Env.CPU, r.Env.OS, r.Env.Arch, r.ElapsedS)
	if len(r.Checks) > 0 {
		b.WriteString("| check | result |\n|---|---|\n")
		for _, c := range r.Checks {
			mark := "✅"
			if !c.OK {
				mark = "❌"
			}
			fmt.Fprintf(&b, "| `%s` | %s %s |\n", c.Name, mark, c.Detail)
		}
		b.WriteString("\n")
	}
	b.WriteString("| metric | n | rate | p50 | p90 | p99 | max | note |\n|---|---:|---:|---:|---:|---:|---:|---|\n")
	for _, m := range r.Metrics {
		switch m.Kind {
		case KindLatency:
			fmt.Fprintf(&b, "| `%s` | %d | %s | %s | %s | %s | %s | %s |\n", m.Name, m.N, fmtRate(m.Rate), fmtMS(m.P50), fmtMS(m.P90), fmtMS(m.P99), fmtMS(m.Max), m.Note)
		case KindScalar:
			fmt.Fprintf(&b, "| `%s` | | | %s | | | | %s |\n", m.Name, fmtScalar(m.Value, m.Unit), m.Note)
		}
	}
	if len(r.Inventory) > 0 {
		b.WriteString("\n| inventory class | objects | bytes | max bytes |\n|---|---:|---:|---:|\n")
		keys := make([]string, 0, len(r.Inventory))
		for k := range r.Inventory {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			c := r.Inventory[k]
			fmt.Fprintf(&b, "| %s | %d | %d | %d |\n", k, c.Objects, c.Bytes, c.MaxBytes)
		}
	}
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "\n> %s\n", n)
	}
	return b.String()
}

func dirtyMark(dirty bool) string {
	if dirty {
		return "+dirty"
	}
	return ""
}

func fmtMS(ms float64) string {
	switch {
	case ms == 0:
		return "—"
	case ms < 1:
		return fmt.Sprintf("%.0fµs", ms*1000)
	case ms < 1000:
		return fmt.Sprintf("%.1fms", ms)
	default:
		return fmt.Sprintf("%.2fs", ms/1000)
	}
}

func fmtRate(rate float64) string {
	switch {
	case rate == 0:
		return "—"
	case rate >= 1000:
		return fmt.Sprintf("%.0f/s", rate)
	default:
		return fmt.Sprintf("%.1f/s", rate)
	}
}

func fmtScalar(v float64, unit string) string {
	switch unit {
	case "bytes":
		switch {
		case v >= 1<<20:
			return fmt.Sprintf("%.1f MB", v/(1<<20))
		case v >= 1<<10:
			return fmt.Sprintf("%.1f KB", v/(1<<10))
		default:
			return fmt.Sprintf("%.0f B", v)
		}
	case "ms":
		return fmtMS(v)
	case "":
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2f %s", v, unit)
	}
}

// ms converts a duration to float milliseconds for the result schema.
func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
