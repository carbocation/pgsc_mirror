package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadResolvesPathsAndContact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	text := `genome_build = "GRCh38"
[identity]
user_agent = "test/1"
contact = "ops@example.org"
[targets]
local = true
gcs = false
[local]
root = "data"
[transfer]
concurrency = 2
request_timeout = "1m"
max_attempts = 2
initial_backoff = "1s"
max_backoff = "2s"
lease_duration = "1h"
`
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Local.Root != filepath.Join(dir, "data") {
		t.Fatalf("root=%q", c.Local.Root)
	}
	if !strings.Contains(c.Identity.UserAgent, "ops@example.org") {
		t.Fatalf("contact missing from %q", c.Identity.UserAgent)
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.toml")
	if err := os.WriteFile(path, []byte("genome_build=\"GRCh38\"\nunknown=true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown key was accepted")
	}
}
