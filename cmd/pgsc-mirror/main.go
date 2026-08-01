package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/carbocation/pgsc_mirror/internal/app"
	"github.com/carbocation/pgsc_mirror/internal/config"
	"github.com/carbocation/pgsc_mirror/internal/planner"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

var commands = map[string]bool{"probe": true, "plan": true, "reconcile": true, "update": true, "pull": true, "verify": true, "status": true, "rebuild-state": true, "gc": true}

type stringList []string

func (v *stringList) String() string { return strings.Join(*v, ",") }
func (v *stringList) Set(value string) error {
	*v = append(*v, value)
	return nil
}

type options struct {
	config   string
	json     bool
	dryRun   bool
	full     bool
	sample   int
	apply    bool
	pgsIDs   stringList
	maxSize  string
	gcsSmoke bool
	report   string
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	command, flagArgs, err := splitCommand(args)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		usage(stderr)
		return 2
	}
	if command == "version" {
		fmt.Fprintf(stdout, "pgsc-mirror %s (commit %s, built %s)\n", version, commit, buildDate)
		return 0
	}
	fs := flag.NewFlagSet("pgsc-mirror "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.config, "config", "", "path to TOML configuration")
	fs.BoolVar(&o.json, "json", false, "emit structured JSON")
	fs.BoolVar(&o.dryRun, "dry-run", false, "report changes without writing")
	fs.BoolVar(&o.full, "full", false, "verify every available blob")
	fs.IntVar(&o.sample, "sample", 0, "number of blobs to verify when not using --full")
	fs.BoolVar(&o.apply, "apply", false, "apply garbage collection (default is dry-run)")
	fs.Var(&o.pgsIDs, "pgs-id", "PGS score ID to check (repeatable; probe only)")
	fs.StringVar(&o.maxSize, "max-size", "100MiB", "maximum compressed scoring-file size (probe only)")
	fs.BoolVar(&o.gcsSmoke, "gcs-smoke-test", false, "exercise conditional GCS object operations after upstream checks pass")
	fs.StringVar(&o.report, "report", "", "write the probe JSON report to a file or directory")
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "error: unexpected arguments:", strings.Join(fs.Args(), " "))
		return 2
	}
	var maxBytes int64
	if command == "probe" {
		maxBytes, err = parseByteSize(o.maxSize)
		if err != nil {
			fmt.Fprintln(stderr, "error: --max-size:", err)
			return 2
		}
	}
	cfg, err := config.Load(o.config)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 2
	}
	mutating := ((command == "reconcile" || command == "update" || command == "pull" || command == "rebuild-state") && !o.dryRun) || command == "status"
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	a, err := app.New(ctx, cfg, mutating)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer a.Close()
	var result any
	switch command {
	case "probe":
		var report app.ProbeReport
		report, err = a.Probe(ctx, app.ProbeOptions{PGSIDs: []string(o.pgsIDs), MaxBytes: maxBytes, GCSSmoke: o.gcsSmoke})
		if o.report != "" {
			reportPath, pathErr := resolveReportPath(o.report, report.RunID)
			if pathErr == nil {
				report.ReportPath = reportPath
				pathErr = writeJSONReport(reportPath, report)
			}
			if pathErr != nil {
				err = errors.Join(err, fmt.Errorf("write probe report: %w", pathErr))
			}
		}
		result = report
	case "plan":
		result, err = a.Plan(ctx)
	case "reconcile":
		result, err = a.Reconcile(ctx, o.dryRun)
	case "update":
		result, err = a.Update(ctx, o.dryRun)
	case "pull":
		result, err = a.Pull(ctx, o.dryRun)
	case "verify":
		result, err = a.Verify(ctx, o.full, o.sample)
	case "status":
		result, err = a.Status(ctx)
	case "rebuild-state":
		result, err = a.RebuildState(ctx, o.dryRun)
	case "gc":
		result, err = a.GC(ctx, o.apply)
	}
	if o.json {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if encodeErr := enc.Encode(result); encodeErr != nil && err == nil {
			err = encodeErr
		}
	} else {
		printHuman(stdout, result)
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func splitCommand(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("a command is required")
	}
	for i, arg := range args {
		if arg == "version" || commands[arg] {
			return arg, append(append([]string{}, args[:i]...), args[i+1:]...), nil
		}
	}
	return "", nil, fmt.Errorf("unknown command %q", args[0])
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: pgsc-mirror [global flags] <command> [flags]")
	fmt.Fprintln(w, "commands: probe, plan, reconcile, update, pull, verify, status, rebuild-state, gc, version")
}

func printHuman(w io.Writer, v any) {
	switch r := v.(type) {
	case app.ProbeReport:
		fmt.Fprintf(w, "probe %s: %s (%d score checks, %d ms)\n", r.RunID, strings.ToUpper(r.Status), len(r.Scores), r.DurationMS)
		for _, score := range r.Scores {
			fmt.Fprintf(w, "%s\t%s", score.PGSID, score.Status)
			if score.Status == "verified" {
				fmt.Fprintf(w, "\t%d bytes\t%s\t%d attempt(s)", score.DownloadedSize, score.ObservedMD5, score.DownloadAttempts)
			}
			if score.Error != "" {
				fmt.Fprintf(w, "\t%s", score.Error)
			}
			fmt.Fprintln(w)
		}
		if r.GCS != nil {
			fmt.Fprintf(w, "GCS smoke test: %s", r.GCS.Status)
			if r.GCS.Key != "" {
				fmt.Fprintf(w, " (%s/%s)", r.GCS.Target, r.GCS.Key)
			}
			if r.GCS.Error != "" {
				fmt.Fprintf(w, ": %s", r.GCS.Error)
			}
			fmt.Fprintln(w)
		}
		if r.ReportPath != "" {
			fmt.Fprintln(w, "JSON report:", r.ReportPath)
		}
	case app.PlanReport:
		fmt.Fprintf(w, "previous release: %s\nscore IDs inspected: %d of %d\n", valueOr(r.PreviousRelease, "none"), r.ScoreCount, r.TotalScoreCount)
		if r.Truncated {
			fmt.Fprintln(w, "warning: development sidecar limit is active; withdrawals are intentionally omitted")
		}
		if r.MetadataChanged {
			fmt.Fprintln(w, "metadata snapshot changed")
		}
		if r.ScoreListChanged {
			fmt.Fprintln(w, "score-list snapshot changed")
		}
		if r.HeaderInspections > 0 {
			fmt.Fprintf(w, "scoring headers needing inspection: %d\n", r.HeaderInspections)
		}
		keys := make([]string, 0, len(r.Counts))
		for k := range r.Counts {
			keys = append(keys, string(k))
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%-10s %d\n", k, r.Counts[planner.Kind(k)])
		}
		for _, c := range r.Changes {
			if c.Kind != planner.Unchanged {
				fmt.Fprintf(w, "%s\t%s\n", c.Kind, c.PGSID)
			}
		}
	case app.RunReport:
		fmt.Fprintf(w, "%s: %s\n", r.Command, r.Message)
		if r.ReleaseID != "" {
			fmt.Fprintln(w, "release:", r.ReleaseID)
		}
		if r.Plan != nil {
			printHuman(w, *r.Plan)
		}
	case app.VerifyReport:
		for _, t := range r.Targets {
			fmt.Fprintf(w, "%s: release %s, checked %d", t.Target, t.ReleaseID, t.Checked)
			if len(t.Failures) == 0 {
				fmt.Fprintln(w, ", OK")
			} else {
				fmt.Fprintf(w, ", %d failure(s)\n", len(t.Failures))
				for _, f := range t.Failures {
					fmt.Fprintln(w, "  "+f)
				}
			}
		}
	case app.StatusReport:
		fmt.Fprintf(w, "state: latest=%s available=%d withdrawn=%d failed_runs=%d\n", valueOr(r.State.LatestRelease, "none"), r.State.Available, r.State.Withdrawn, r.State.FailedRuns)
		for _, t := range r.Targets {
			if t.Error != "" {
				fmt.Fprintf(w, "%s: %s\n", t.Target, t.Error)
			} else {
				fmt.Fprintf(w, "%s: release %s (%d entries)\n", t.Target, t.ReleaseID, t.Entries)
				if len(t.HeaderSchemas) > 0 {
					for _, schema := range t.HeaderSchemas {
						fingerprint := schema.SchemaSHA256
						if len(fingerprint) > 12 {
							fingerprint = fingerprint[:12]
						}
						fmt.Fprintf(w, "  header %-40s %s %d\n", schema.Type, fingerprint, schema.Count)
					}
				}
				if t.HeaderAnomalies > 0 || t.UninspectedHeaders > 0 {
					fmt.Fprintf(w, "  header anomalies=%d uninspected=%d\n", t.HeaderAnomalies, t.UninspectedHeaders)
				}
			}
		}
	case app.GCReport:
		mode := "DRY RUN"
		if !r.DryRun {
			mode = "APPLIED"
		}
		fmt.Fprintf(w, "gc %s: %d object(s)\n", mode, len(r.Items))
		for _, item := range r.Items {
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", item.Target, item.Size, item.Key, item.Reason)
		}
	default:
		b, _ := json.MarshalIndent(v, "", "  ")
		fmt.Fprintln(w, string(b))
	}
}

func parseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("value is empty")
	}
	upper := strings.ToUpper(raw)
	multipliers := []struct {
		suffix string
		value  uint64
	}{
		{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	}
	var number string
	var multiplier uint64
	for _, candidate := range multipliers {
		if strings.HasSuffix(upper, candidate.suffix) {
			number = strings.TrimSpace(raw[:len(raw)-len(candidate.suffix)])
			multiplier = candidate.value
			break
		}
	}
	if multiplier == 0 {
		number = raw
		multiplier = 1
	}
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid positive byte size %q", raw)
	}
	if n > uint64(math.MaxInt64)/multiplier {
		return 0, fmt.Errorf("byte size %q overflows int64", raw)
	}
	return int64(n * multiplier), nil
}

func resolveReportPath(raw, runID string) (string, error) {
	if runID == "" {
		return "", errors.New("probe did not assign a run ID")
	}
	clean := filepath.Clean(raw)
	if info, err := os.Stat(clean); err == nil {
		if info.IsDir() {
			return filepath.Join(clean, runID+".json"), nil
		}
		return clean, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if strings.HasSuffix(raw, string(os.PathSeparator)) {
		return filepath.Join(clean, runID+".json"), nil
	}
	return clean, nil
}

func writeJSONReport(path string, report app.ProbeReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".pgsc-probe-report-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
