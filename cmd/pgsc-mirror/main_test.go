package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/app"
)

func TestParseByteSize(t *testing.T) {
	for raw, want := range map[string]int64{
		"1": 1, "1B": 1, "2KiB": 2 << 10, "100MiB": 100 << 20, "3GB": 3_000_000_000,
	} {
		got, err := parseByteSize(raw)
		if err != nil || got != want {
			t.Fatalf("parseByteSize(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"", "0", "-1MiB", "1.5MiB", "many"} {
		if _, err := parseByteSize(raw); err == nil {
			t.Fatalf("parseByteSize(%q) succeeded", raw)
		}
	}
}

func TestSplitCommandDefaultsToRun(t *testing.T) {
	command, args, err := splitCommand([]string{"--config", "mirror.toml"})
	if err != nil || command != "run" || strings.Join(args, " ") != "--config mirror.toml" {
		t.Fatalf("command=%q args=%v err=%v", command, args, err)
	}
	command, args, err = splitCommand(nil)
	if err != nil || command != "run" || len(args) != 0 {
		t.Fatalf("empty args: command=%q args=%v err=%v", command, args, err)
	}
	command, args, err = splitCommand([]string{"--config", "run"})
	if err != nil || command != "run" || strings.Join(args, " ") != "--config run" {
		t.Fatalf("command-like config path: command=%q args=%v err=%v", command, args, err)
	}
}

func TestServiceReporterJSONLines(t *testing.T) {
	var out strings.Builder
	report := serviceReporter(&out, true)
	event := app.ServiceEvent{At: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC), Operation: "update", Status: app.ServiceSucceeded}
	if err := report(event); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 1 || !strings.Contains(out.String(), `"operation":"update"`) {
		t.Fatalf("not JSONL: %q", out.String())
	}
}

func TestStringListPreservesRepeatedValues(t *testing.T) {
	var ids stringList
	if err := ids.Set("PGS000002"); err != nil {
		t.Fatal(err)
	}
	if err := ids.Set("PGS000001"); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "PGS000002" || ids[1] != "PGS000001" {
		t.Fatalf("unexpected IDs: %v", ids)
	}
}

func TestWriteJSONReportInDirectory(t *testing.T) {
	dir := t.TempDir()
	path, err := resolveReportPath(dir, "probe-run")
	if err != nil {
		t.Fatal(err)
	}
	report := app.ProbeReport{RunID: "probe-run", Status: "passed", ReportPath: path}
	if err := writeJSONReport(path, report); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "probe-run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got app.ProbeReport
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != report.RunID || got.ReportPath != path {
		t.Fatalf("unexpected report: %+v", got)
	}
}
