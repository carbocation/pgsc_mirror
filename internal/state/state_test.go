package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
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

func TestInventoryCheckpointResumeExpiryAndConsume(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	first, err := db.BeginInventory(ctx, "snapshot-a", time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PutInventorySidecar(ctx, first, "PGS000001", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https://example/one", now); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.BeginInventory(ctx, "snapshot-a", time.Hour, now.Add(30*time.Minute))
	if err != nil || resumed != first {
		t.Fatalf("checkpoint was not resumed: first=%d resumed=%d err=%v", first, resumed, err)
	}
	got, ok, err := db.InventorySidecar(ctx, resumed, "PGS000001")
	if err != nil || !ok || got.SourceURL != "https://example/one" {
		t.Fatalf("cached sidecar missing: %+v ok=%v err=%v", got, ok, err)
	}
	expired, err := db.BeginInventory(ctx, "snapshot-a", time.Hour, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.InventorySidecar(ctx, expired, "PGS000001"); err != nil || ok {
		t.Fatalf("expired checkpoint retained its sidecar: ok=%v err=%v", ok, err)
	}
	if err := db.ConsumeInventory(ctx, expired); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.InventorySidecar(ctx, expired, "PGS000001"); err != nil || ok {
		t.Fatalf("consumed checkpoint retained sidecars: ok=%v err=%v", ok, err)
	}
}

func TestOpenMarksAbandonedRunInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginRun(context.Background(), "reconcile"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	summary, err := db.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.LastRunStatus != "interrupted" {
		t.Fatalf("abandoned run status is %q", summary.LastRunStatus)
	}
}

func TestLastSuccessfulReconciliation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, ok, err := db.LastSuccessfulReconciliation(ctx); err != nil || ok {
		t.Fatalf("unexpected initial result: ok=%v err=%v", ok, err)
	}
	check, err := db.BeginRun(ctx, "update-check")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishRun(ctx, check, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.LastSuccessfulReconciliation(ctx); err != nil || ok {
		t.Fatalf("cheap update counted as reconciliation: ok=%v err=%v", ok, err)
	}
	full, err := db.BeginRun(ctx, "reconcile")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishRun(ctx, full, nil); err != nil {
		t.Fatal(err)
	}
	stamp, ok, err := db.LastSuccessfulReconciliation(ctx)
	if err != nil || !ok || stamp.IsZero() {
		t.Fatalf("successful reconciliation missing: stamp=%s ok=%v err=%v", stamp, ok, err)
	}
}

func TestRestoreReconciliationProvidesPortableScheduleAndSentinels(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	stamp := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	sentinels := []Sentinel{
		{Name: "score_list", ETag: `"list-1"`, ContentSHA256: "score-sha", ObservedAt: stamp},
		{Name: "metadata", ETag: `"metadata-1"`, ContentSHA256: "metadata-sha", ObservedAt: stamp},
	}
	if err := db.RestoreReconciliation(ctx, stamp, sentinels); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.LastSuccessfulReconciliation(ctx)
	if err != nil || !ok || !got.Equal(stamp) {
		t.Fatalf("restored reconciliation=%s ok=%v err=%v, want %s", got, ok, err, stamp)
	}
	for _, want := range sentinels {
		got, ok, err := db.Sentinel(ctx, want.Name)
		if err != nil || !ok || got.ETag != want.ETag || got.ContentSHA256 != want.ContentSHA256 || !got.ObservedAt.Equal(stamp) {
			t.Fatalf("restored sentinel %s: got=%+v ok=%v err=%v", want.Name, got, ok, err)
		}
	}

	newer := stamp.Add(time.Hour)
	if err := db.RestoreReconciliation(ctx, newer, []Sentinel{{Name: "score_list", ETag: `"new"`, ContentSHA256: "new", ObservedAt: newer}}); err != nil {
		t.Fatal(err)
	}
	if err := db.RestoreReconciliation(ctx, stamp, []Sentinel{{Name: "score_list", ETag: `"old"`, ContentSHA256: "old", ObservedAt: stamp}}); err != nil {
		t.Fatal(err)
	}
	gotSentinel, _, err := db.Sentinel(ctx, "score_list")
	if err != nil || gotSentinel.ETag != `"new"` || gotSentinel.ContentSHA256 != "new" {
		t.Fatalf("older restore replaced newer state: got=%+v err=%v", gotSentinel, err)
	}
}
