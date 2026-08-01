package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/pgsc-mirror/pgsc-mirror/internal/app"
	"github.com/pgsc-mirror/pgsc-mirror/internal/config"
	"github.com/pgsc-mirror/pgsc-mirror/internal/planner"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

var commands = map[string]bool{"plan": true, "reconcile": true, "update": true, "pull": true, "verify": true, "status": true, "rebuild-state": true, "gc": true}

type options struct {
	config string
	json   bool
	dryRun bool
	full   bool
	sample int
	apply  bool
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
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "error: unexpected arguments:", strings.Join(fs.Args(), " "))
		return 2
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
	fmt.Fprintln(w, "commands: plan, reconcile, update, pull, verify, status, rebuild-state, gc, version")
}

func printHuman(w io.Writer, v any) {
	switch r := v.(type) {
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

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
