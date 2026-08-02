package app

import (
	"strings"
	"testing"

	"github.com/carbocation/pgsc_mirror/internal/model"
)

func TestRequireCurrentScoreLayout(t *testing.T) {
	pointer := model.Pointer{ReleaseID: "current", ScoreLayoutVersion: model.ScoreLayoutVersion}
	entry := model.Entry{
		ReleaseID:   pointer.ReleaseID,
		PGSID:       "PGS000001",
		GenomeBuild: "GRCh38",
		ScoreKey:    model.ScoreKey("PGS000001", "GRCh38"),
	}
	if err := requireCurrentScoreLayout(pointer, []model.Entry{entry}); err != nil {
		t.Fatalf("current score layout was rejected: %v", err)
	}

	entry.ReleaseID = "another-release"
	if err := requireCurrentScoreLayout(pointer, []model.Entry{entry}); err == nil || !strings.Contains(err.Error(), "belongs to release") {
		t.Fatalf("entry from another release was accepted: %v", err)
	}
	entry.ReleaseID = pointer.ReleaseID

	pointer.ScoreLayoutVersion = 0
	if err := requireCurrentScoreLayout(pointer, []model.Entry{entry}); err == nil || !strings.Contains(err.Error(), "unsupported score layout version") {
		t.Fatalf("missing score layout version was accepted: %v", err)
	}

	pointer.ScoreLayoutVersion = model.ScoreLayoutVersion
	entry.ScoreKey = "elsewhere/PGS000001.txt.gz"
	if err := requireCurrentScoreLayout(pointer, []model.Entry{entry}); err == nil || !strings.Contains(err.Error(), "has score_key") {
		t.Fatalf("noncanonical score key was accepted: %v", err)
	}

	entry.ScoreKey = model.ScoreKey(entry.PGSID, entry.GenomeBuild)
	pointer.ManifestTSVKey = model.ManifestTSVKey(pointer.ReleaseID)
	if err := requireCurrentScoreLayout(pointer, []model.Entry{entry}); err == nil || !strings.Contains(err.Error(), "incomplete manifest TSV identity") {
		t.Fatalf("incomplete manifest TSV identity was accepted: %v", err)
	}
}
