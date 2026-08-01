package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pgsc-mirror/pgsc-mirror/internal/model"
)

func TestRecordAndRebuild(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	e := model.Entry{ReleaseID: "r1", PGSID: "PGS000001", GenomeBuild: "GRCh38", SourceMD5: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BlobKey: model.BlobKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Status: model.StatusReady, FirstSeenAt: now, LastSeenAt: now}
	p := model.Pointer{ReleaseID: "r1", ManifestKey: model.ManifestKey("r1"), ManifestSHA256: "abc", PublishedAt: now, EntryCount: 1}
	if err := db.RecordRelease(ctx, p, []model.Entry{e}, true); err != nil {
		t.Fatal(err)
	}
	s, err := db.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Available != 1 || s.LatestRelease != "r1" {
		t.Fatalf("bad summary %+v", s)
	}
	if err := db.Rebuild(ctx, []RebuildRelease{{Pointer: p, Entries: []model.Entry{e}}}); err != nil {
		t.Fatal(err)
	}
	s, err = db.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Entries != 1 {
		t.Fatalf("bad rebuilt summary %+v", s)
	}
}
