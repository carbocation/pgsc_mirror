package app

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
)

func TestMaintenanceCheckpointCASKeepsNewestAudit(t *testing.T) {
	st, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base := maintenanceCheckpoint{
		FormatVersion:            maintenanceCheckpointVersion,
		GenomeBuild:              "GRCh38",
		ObservedReleaseID:        "release-1",
		LastFullReconciliationAt: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		ScoreList:                maintenanceDocument{ETag: `"list-1"`, ContentSHA256: "score-sha"},
		Metadata:                 maintenanceDocument{ETag: `"metadata-1"`, ContentSHA256: "metadata-sha"},
	}
	newer := base
	newer.ObservedReleaseID = "release-2"
	newer.LastFullReconciliationAt = base.LastFullReconciliationAt.Add(time.Minute)
	newer.ScoreList.ETag = `"list-2"`
	newer.Metadata.ContentSHA256 = "metadata-newer-snapshot"

	if err := putMaintenanceCheckpoint(ctx, st, base); err != nil {
		t.Fatal(err)
	}
	if err := putMaintenanceCheckpoint(ctx, st, newer); err != nil {
		t.Fatal(err)
	}
	if err := putMaintenanceCheckpoint(ctx, st, base); err != nil {
		t.Fatal(err)
	}
	got, _, err := readMaintenanceCheckpoint(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if got != newer {
		t.Fatalf("older checkpoint replaced newer checkpoint: got=%+v want=%+v", got, newer)
	}
}

func TestMaintenanceCheckpointAuditRepairsInconsistentNewerDocument(t *testing.T) {
	st, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	want := maintenanceCheckpoint{
		FormatVersion:            maintenanceCheckpointVersion,
		GenomeBuild:              "GRCh38",
		ObservedReleaseID:        "release-current",
		LastFullReconciliationAt: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		ScoreList:                maintenanceDocument{ContentSHA256: "score-current"},
		Metadata:                 maintenanceDocument{ContentSHA256: "metadata-current"},
	}
	if err := putMaintenanceCheckpoint(ctx, st, want); err != nil {
		t.Fatal(err)
	}
	invalid := want
	invalid.LastFullReconciliationAt = want.LastFullReconciliationAt.Add(time.Hour)
	invalid.Metadata.ContentSHA256 = "wrong-snapshot"
	b, err := encodeMaintenanceCheckpoint(invalid)
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Stat(ctx, model.MaintenanceKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, model.MaintenanceKey, bytes.NewReader(b), store.PutOptions{GenerationMatch: &info.Generation, ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}
	if err := putMaintenanceCheckpointMode(ctx, st, want, true); err != nil {
		t.Fatal(err)
	}
	got, _, err := readMaintenanceCheckpoint(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("full audit did not repair inconsistent checkpoint: got=%+v want=%+v", got, want)
	}
}

func TestMaintenanceCheckpointValidationPinsSnapshotContent(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	pointer := model.Pointer{
		ReleaseID:       "annotation-successor",
		GenomeBuild:     "GRCh38",
		ScoreListSHA256: "score-sha",
		MetadataSHA256:  "metadata-sha",
	}
	checkpoint := maintenanceCheckpoint{
		FormatVersion:            maintenanceCheckpointVersion,
		GenomeBuild:              "GRCh38",
		ObservedReleaseID:        "earlier-full-reconciliation",
		LastFullReconciliationAt: now.Add(-time.Hour),
		ScoreList:                maintenanceDocument{ContentSHA256: "score-sha"},
		Metadata:                 maintenanceDocument{ContentSHA256: "metadata-sha"},
	}
	if err := validateMaintenanceCheckpoint(checkpoint, pointer, "GRCh38", now); err != nil {
		t.Fatalf("annotation-only successor invalidated compatible checkpoint: %v", err)
	}
	checkpoint.Metadata.ContentSHA256 = "different"
	if err := validateMaintenanceCheckpoint(checkpoint, pointer, "GRCh38", now); err == nil {
		t.Fatal("checkpoint with mismatched snapshot was accepted")
	}
	checkpoint.Metadata.ContentSHA256 = "metadata-sha"
	checkpoint.LastFullReconciliationAt = now.Add(maintenanceFutureTolerance + time.Second)
	if err := validateMaintenanceCheckpoint(checkpoint, pointer, "GRCh38", now); err == nil {
		t.Fatal("checkpoint from the future was accepted")
	}
}
