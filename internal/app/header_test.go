package app

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/config"
	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

const harmonizedHeaderFixture = "###PGS CATALOG SCORING FILE\n#format_version=2.0\n#pgs_id=PGS000001\n##HARMONIZATION DETAILS\n#HmPOS_build=GRCh38\nrsID\teffect_allele\tother_allele\teffect_weight\thm_source\thm_rsID\thm_chr\thm_pos\nrs1\tA\tG\t0.25\tENSEMBL\trs1\t1\t42\n"

func TestEnsureScoreAnnotatesHeaderFromStoredObject(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	ls, err := localstore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()

	data := gzipFixture(harmonizedHeaderFixture)
	sum := md5hex(data)
	key := model.ScoreKey("PGS000001", "GRCh38")
	if _, err := ls.Put(ctx, key, bytes.NewReader(data), store.PutOptions{DoesNotExist: true}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.State.WorkDir = t.TempDir()
	a := &App{Config: cfg, targets: []target{{kind: "local", Store: ls}}}
	entry := model.Entry{PGSID: "PGS000001", GenomeBuild: "GRCh38", SourceMD5: sum, ScoreKey: key, Status: model.StatusReady}
	if err := a.ensureScore(ctx, &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Header == nil || entry.Header.Type != scoreheader.TypeHarmonizedV2 || entry.Header.Status != scoreheader.StatusRecognized {
		t.Fatalf("header was not annotated: %+v", entry.Header)
	}
	if entry.SizeBytes != int64(len(data)) {
		t.Fatalf("size=%d, want %d", entry.SizeBytes, len(data))
	}
}

func TestUpdateAnnotatesOutdatedManifestHeader(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	root := t.TempDir()
	cfg := integrationConfig(root, srv.URL)
	ctx := context.Background()
	a, err := New(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	data := gzipFixture(harmonizedHeaderFixture)
	sum := md5hex(data)
	sourceID := "20260801T100000Z-000000000000"
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	entry := model.Entry{
		ReleaseID:     sourceID,
		PGSID:         "PGS000001",
		PGSName:       "name-PGS000001",
		TraitReported: "reported-PGS000001",
		TraitMapped:   "mapped-PGS000001",
		TraitEFO:      "EFO_000001",
		ReleaseDate:   "2020-01-02",
		GenomeBuild:   "GRCh38",
		SourceURL:     srv.URL + "/root/scores/PGS000001/ScoringFiles/Harmonized/PGS000001_hmPOS_GRCh38.txt.gz",
		SourceMD5:     sum,
		SizeBytes:     int64(len(data)),
		ScoreKey:      model.ScoreKey("PGS000001", "GRCh38"),
		FirstSeenAt:   now,
		LastSeenAt:    now,
		Status:        model.StatusReady,
		License:       "CC0",
	}
	if _, err := a.targets[0].Put(ctx, entry.ScoreKey, bytes.NewReader(data), store.PutOptions{DoesNotExist: true}); err != nil {
		t.Fatal(err)
	}
	manifestBytes, manifestSHA, err := manifest.Encode([]model.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	pointer := model.Pointer{ReleaseID: sourceID, ManifestKey: model.ManifestKey(sourceID), ManifestSHA256: manifestSHA, PublishedAt: now, EntryCount: 1, GenomeBuild: "GRCh38", CatalogMetadataVersion: model.CatalogMetadataVersion, ScoreLayoutVersion: model.ScoreLayoutVersion}
	pointer, manifestTSV, err := attachManifestTSV(pointer, []model.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	scoreList := []byte("PGS000001\n")
	metadata := []byte("pgs_id,release_date,license,note\nPGS000001,2020-01-02,CC0,\n")
	if err := a.publish(ctx, pointer, scoreList, metadata, manifestBytes, manifestTSV); err != nil {
		t.Fatal(err)
	}
	if err := a.State.RecordRelease(ctx, pointer, []model.Entry{entry}); err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return now.Add(time.Minute) }
	report, err := a.Update(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Changed || report.ReleaseID == sourceID {
		t.Fatalf("outdated header was not annotated: %+v", report)
	}
	_, entries, err := a.latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Header == nil || entries[0].Header.Type != scoreheader.TypeHarmonizedV2 {
		t.Fatalf("annotated manifest lacks header observation: %+v", entries)
	}
}

func TestHeaderInspectionsNeeded(t *testing.T) {
	current := &scoreheader.Inspection{InspectorVersion: scoreheader.InspectorVersion}
	stale := &scoreheader.Inspection{InspectorVersion: scoreheader.InspectorVersion + 1}
	entries := []model.Entry{
		{Status: model.StatusReady},
		{Status: model.StatusReady, Header: stale},
		{Status: model.StatusReady, Header: current},
		{Status: model.StatusGone},
	}
	if got := headerInspectionsNeeded(entries); got != 2 {
		t.Fatalf("got %d inspections, want 2", got)
	}
}

func TestObservedHeaderInspectorVersionRequiresCompleteCoverage(t *testing.T) {
	v1 := &scoreheader.Inspection{InspectorVersion: 1}
	v2 := &scoreheader.Inspection{InspectorVersion: 2}
	if got := observedHeaderInspectorVersion([]model.Entry{{Status: model.StatusReady, Header: v2}, {Status: model.StatusReady, Header: v1}}); got != 1 {
		t.Fatalf("minimum complete version=%d, want 1", got)
	}
	if got := observedHeaderInspectorVersion([]model.Entry{{Status: model.StatusReady, Header: v2}, {Status: model.StatusReady}}); got != 0 {
		t.Fatalf("incomplete coverage version=%d, want 0", got)
	}
	if got := observedHeaderInspectorVersion([]model.Entry{{Status: model.StatusGone}}); got != 0 {
		t.Fatalf("withdrawn-only version=%d, want 0", got)
	}
}
