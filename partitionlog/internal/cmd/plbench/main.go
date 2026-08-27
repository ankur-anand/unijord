// Command plbench runs partitionlog load scenarios against a live object
// store or emulator and compares results with checked-in baselines.
//
//	plbench list
//	plbench run   -scenario catalog_history -profile smoke -provider minio [-set segments=5000] [-prefix P] [-keep] [-out results/]
//	plbench compare results/<file>.json [-baseline path] [-format text|markdown|github] [-only-changed]
//	plbench baseline results/<file>.json  # promote to internal/bench/baselines/<scenario>-<profile>-<provider>.json
//	plbench cleanup  -provider minio -prefix plbench/   # delete kept or failed run objects
//
// Provider selection and credentials: see bench.OpenProvider. PLBENCH_PROVIDER
// sets the default provider; PLBENCH_BASELINES sets the baselines directory.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/internal/bench"
	_ "github.com/ankur-anand/unijord/partitionlog/internal/bench/scenarios"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "list":
		err = list()
	case "run":
		err = run(os.Args[2:])
	case "compare":
		err = compare(os.Args[2:])
	case "baseline":
		err = baseline(os.Args[2:])
	case "cleanup":
		err = cleanup(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "plbench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: plbench list | run | compare | baseline | cleanup  (see -h on each)")
}

func baselinesDir() string {
	if v := os.Getenv("PLBENCH_BASELINES"); v != "" {
		return v
	}
	return filepath.Join("partitionlog", "internal", "bench", "baselines")
}

func list() error {
	for _, s := range bench.Scenarios() {
		fmt.Printf("%-20s %s\n", s.Name(), s.Description())
		for name, p := range s.Profiles() {
			fmt.Printf("    %-8s %+v\n", name, p)
		}
	}
	fmt.Printf("\nproviders: %s\n", strings.Join(bench.ProviderNames, ", "))
	return nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario name (see plbench list)")
	profile := fs.String("profile", "smoke", "smoke | ci | deep")
	provider := fs.String("provider", bench.Getenv("PLBENCH_PROVIDER", "minio"), strings.Join(bench.ProviderNames, " | "))
	prefix := fs.String("prefix", "", "object prefix (default unique; reuse to resume)")
	keep := fs.Bool("keep", false, "do not delete objects at the end")
	out := fs.String("out", "results", "directory for result JSON")
	timeout := fs.Duration("timeout", 3*time.Hour, "overall timeout")
	var sets multiFlag
	fs.Var(&sets, "set", "override a param: -set segments=5000 -set samples=50 (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, ok := bench.Lookup(*scenario)
	if !ok {
		return fmt.Errorf("unknown scenario %q (see plbench list)", *scenario)
	}
	params, ok := s.Profiles()[*profile]
	if !ok {
		return fmt.Errorf("scenario %s has no profile %q", s.Name(), *profile)
	}
	for _, kv := range sets {
		if err := applyOverride(&params, kv); err != nil {
			return err
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, *timeout)
	defer cancelTimeout()

	p, err := bench.OpenProvider(ctx, *provider)
	if err != nil {
		return err
	}
	defer p.Close()
	r, err := bench.NewRun(s, *profile, p, params, *prefix, *keep, os.Stdout)
	if err != nil {
		return err
	}
	runErr := r.Execute(ctx, s)
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	path := filepath.Join(*out, fmt.Sprintf("%s-%s-%s-%s.json", s.Name(), *profile, p.Name(), r.Result.StartedAt.UTC().Format("20060102T150405Z")))
	if err := r.Result.WriteJSON(path); err != nil {
		return err
	}
	fmt.Printf("\nwrote %s\n", path)
	if runErr != nil {
		return runErr
	}
	if r.Result.Failed() {
		return fmt.Errorf("%d check(s) failed", countFailed(r.Result))
	}
	return nil
}

func countFailed(r *bench.Result) int {
	n := 0
	for _, c := range r.Checks {
		if !c.OK {
			n++
		}
	}
	return n
}

// splitArgs moves positional arguments after the flags so "plbench compare
// result.json -format github" and "plbench compare -format github result.json"
// both work.
func splitArgs(args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if !strings.Contains(a, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") && !isBoolFlag(a) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	return append(flags, positional...)
}

func isBoolFlag(a string) bool {
	switch strings.TrimLeft(a, "-") {
	case "only-changed", "require-baseline", "keep":
		return true
	}
	return false
}

func compare(args []string) error {
	args = splitArgs(args)
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	baselinePath := fs.String("baseline", "", "baseline file (default: <baselines>/<scenario>-<profile>-<provider>.json)")
	format := fs.String("format", "text", "text | markdown | github")
	onlyChanged := fs.Bool("only-changed", false, "show only rows that moved or failed")
	requireBaseline := fs.Bool("require-baseline", false, "fail when no baseline exists")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: plbench compare <result.json>")
	}
	r, err := bench.ReadResult(fs.Arg(0))
	if err != nil {
		return err
	}
	path := *baselinePath
	if path == "" {
		path = bench.BaselinePath(baselinesDir(), r.Scenario, r.Profile, r.Env.Provider)
	}
	var base *bench.Baseline
	if b, err := bench.ReadBaseline(path); err == nil {
		base = b
	} else if !os.IsNotExist(err) {
		return err
	} else if *requireBaseline {
		return fmt.Errorf("no baseline at %s", path)
	} else {
		fmt.Fprintf(os.Stderr, "plbench: no baseline at %s; reporting checks only\n", path)
	}
	rep := bench.Compare(r, base)
	fmt.Print(rep.Format(*format, *onlyChanged, r))
	if rep.Failed {
		return fmt.Errorf("comparison failed")
	}
	return nil
}

func baseline(args []string) error {
	args = splitArgs(args)
	fs := flag.NewFlagSet("baseline", flag.ExitOnError)
	dir := fs.String("dir", baselinesDir(), "baselines directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: plbench baseline <result.json>")
	}
	r, err := bench.ReadResult(fs.Arg(0))
	if err != nil {
		return err
	}
	if r.Failed() {
		return fmt.Errorf("refusing to promote a result with failed checks")
	}
	path := bench.BaselinePath(*dir, r.Scenario, r.Profile, r.Env.Provider)
	var existing *bench.Baseline
	if b, err := bench.ReadBaseline(path); err == nil {
		existing = b
	}
	base := bench.Promote(r, existing)
	if err := base.WriteJSON(path); err != nil {
		return err
	}
	fmt.Printf("wrote %s (%d metrics, %d thresholds)\n", path, len(base.Metrics), len(base.Thresholds))
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func applyOverride(p *bench.Params, kv string) error {
	key, value, ok := strings.Cut(kv, "=")
	if !ok {
		return fmt.Errorf("bad -set %q, want key=value", kv)
	}
	parseInt := func() (int, error) { return strconv.Atoi(value) }
	parseBool := func() (bool, error) { return strconv.ParseBool(value) }
	var err error
	switch strings.ToLower(key) {
	case "segments":
		p.Segments, err = parseInt()
	case "records", "records_per_segment":
		p.RecordsPerSegment, err = parseInt()
	case "record_bytes":
		p.RecordBytes, err = parseInt()
	case "leaf", "leaf_limit":
		p.LeafLimit, err = parseInt()
	case "k", "index_limit":
		p.IndexLimit, err = parseInt()
	case "samples":
		p.Samples, err = parseInt()
	case "replay", "replay_segments":
		p.ReplaySegments, err = parseInt()
	case "writer_opens":
		p.WriterOpens, err = parseInt()
	case "inflight":
		p.Inflight, err = parseInt()
	case "partitions":
		p.Partitions, err = parseInt()
	case "tailers":
		p.Tailers, err = parseInt()
	case "duration":
		p.Duration, err = time.ParseDuration(value)
	case "retention":
		p.Retention, err = parseBool()
	case "reclaim":
		p.Reclaim, err = parseBool()
	case "scrub":
		p.Scrub, err = parseBool()
	default:
		return fmt.Errorf("unknown param %q", key)
	}
	if err != nil {
		return fmt.Errorf("bad value for %s: %w", key, err)
	}
	return nil
}

func cleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	provider := fs.String("provider", bench.Getenv("PLBENCH_PROVIDER", "minio"), strings.Join(bench.ProviderNames, " | "))
	prefix := fs.String("prefix", "plbench/", "object prefix to delete (must start with plbench/)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := bench.ValidatePrefix(*prefix); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	p, err := bench.OpenProvider(ctx, *provider)
	if err != nil {
		return err
	}
	defer p.Close()
	t0 := time.Now()
	n, err := p.Cleanup(ctx, *prefix)
	fmt.Printf("deleted %d objects under %s on %s in %s\n", n, *prefix, p.Name(), time.Since(t0).Round(time.Millisecond))
	return err
}
