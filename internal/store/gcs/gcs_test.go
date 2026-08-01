package gcs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupStagingRemovesOnlyOwnedPartialFiles(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "pgsc-gcs-put-123.part")
	keep := filepath.Join(dir, "operator-note.txt")
	for _, name := range []string{stale, keep} {
		if err := os.WriteFile(name, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := &Store{stagingDir: dir}
	removed, err := s.CleanupStaging()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want 1", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}
