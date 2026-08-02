package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
)

func seedContentAddressedRelease(t *testing.T, a *App, now time.Time) (model.Pointer, model.Entry, []byte) {
	t.Helper()
	ctx := context.Background()
	data := gzipFixture(harmonizedHeaderFixture)
	sum := md5hex(data)
	releaseID := "20260801T120000Z-000000000000"
	entry := model.Entry{
		ReleaseID:   releaseID,
		PGSID:       "PGS000001",
		GenomeBuild: "GRCh38",
		SourceURL:   "https://upstream.invalid/PGS000001_hmPOS_GRCh38.txt.gz",
		SourceMD5:   sum,
		SizeBytes:   int64(len(data)),
		ScoreKey:    model.LegacyBlobKey(sum),
		FirstSeenAt: now,
		LastSeenAt:  now,
		Status:      model.StatusReady,
	}
	if _, err := a.targets[0].Put(ctx, entry.ScoreKey, bytes.NewReader(data), store.PutOptions{DoesNotExist: true, ContentType: "application/gzip"}); err != nil {
		t.Fatal(err)
	}
	manifestBytes, manifestSHA, err := manifest.Encode([]model.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	scoreList := []byte("PGS000001\n")
	metadata := []byte("pgs_id,license\nPGS000001,CC0\n")
	scoreSum := sha256.Sum256(scoreList)
	metadataSum := sha256.Sum256(metadata)
	pointer := model.Pointer{
		ReleaseID:       releaseID,
		ManifestKey:     model.ManifestKey(releaseID),
		ManifestSHA256:  manifestSHA,
		ScoreListSHA256: hex.EncodeToString(scoreSum[:]),
		MetadataSHA256:  hex.EncodeToString(metadataSum[:]),
		PublishedAt:     now,
		EntryCount:      1,
		GenomeBuild:     "GRCh38",
	}
	if err := a.publish(ctx, pointer, scoreList, metadata, manifestBytes); err != nil {
		t.Fatal(err)
	}
	if err := a.State.RecordRelease(ctx, pointer, []model.Entry{entry}, true); err != nil {
		t.Fatal(err)
	}
	return pointer, entry, data
}

func TestScoreLayoutMigrationPublishesFlatNamedReleaseAndIsIdempotent(t *testing.T) {
	cfg := integrationConfig(t.TempDir(), "http://upstream.invalid")
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	legacyPointer, legacyEntry, data := seedContentAddressedRelease(t, a, now)
	a.now = func() time.Time { return now.Add(time.Minute) }

	report, err := a.migrateScoreLayout(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Changed || report.Migrated != 1 || report.ReleaseID == legacyPointer.ReleaseID {
		t.Fatalf("unexpected migration report %+v", report)
	}
	pointer, entries, err := a.latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantKey := model.ScoreKey("PGS000001", "GRCh38")
	if pointer.ScoreLayoutVersion != model.ScoreLayoutVersion || entries[0].ScoreKey != wantKey {
		t.Fatalf("migration did not publish current layout: pointer=%+v entry=%+v", pointer, entries[0])
	}
	r, _, err := a.targets[0].Open(context.Background(), wantKey)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("flat score differs: err=%v", err)
	}
	if _, err := a.targets[0].Stat(context.Background(), legacyEntry.ScoreKey); err != nil {
		t.Fatalf("legacy object was removed during additive migration: %v", err)
	}

	second, err := a.migrateScoreLayout(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed || second.ReleaseID != pointer.ReleaseID {
		t.Fatalf("idempotent migration changed release: %+v", second)
	}
}

func TestScoreLayoutMigrationFailureLeavesPointerUnchanged(t *testing.T) {
	cfg := integrationConfig(t.TempDir(), "http://upstream.invalid")
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	legacyPointer, _, _ := seedContentAddressedRelease(t, a, now)
	original := a.targets[0].Store
	a.targets[0].Store = &failingStore{Store: original, failSuffix: "PGS000001_hmPOS_GRCh38.txt.gz"}
	if _, err := a.migrateScoreLayout(context.Background()); err == nil {
		t.Fatal("injected score copy failure succeeded")
	}
	a.targets[0].Store = original
	after, _, err := a.readPointer(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReleaseID != legacyPointer.ReleaseID {
		t.Fatalf("LATEST advanced across failed migration: %s -> %s", legacyPointer.ReleaseID, after.ReleaseID)
	}
}
