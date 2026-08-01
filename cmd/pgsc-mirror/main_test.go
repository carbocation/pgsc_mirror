package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pgsc-mirror/pgsc-mirror/internal/app"
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
