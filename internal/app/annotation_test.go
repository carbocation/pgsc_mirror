package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/state"
	"github.com/carbocation/pgsc_mirror/internal/store"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

type annotationFixture struct {
	PGSID string
	Data  []byte
}

func seedLegacyRelease(t *testing.T, a *App, now time.Time, fixtures ...annotationFixture) model.Pointer {
	t.Helper()
	ctx := context.Background()
	releaseID := "20260801T100000Z-000000000000"
	entries := make([]model.Entry, 0, len(fixtures))
	for _, fixture := range fixtures {
		sum := md5hex(fixture.Data)
		entry := model.Entry{
			ReleaseID:   releaseID,
			PGSID:       fixture.PGSID,
			GenomeBuild: "GRCh38",
			SourceURL:   "https://upstream.invalid/" + fixture.PGSID + ".txt.gz",
			SourceMD5:   sum,
			SizeBytes:   int64(len(fixture.Data)),
			ScoreKey:    model.ScoreKey(fixture.PGSID, "GRCh38"),
			FirstSeenAt: now,
			LastSeenAt:  now,
			Status:      model.StatusReady,
			License:     "test",
		}
		if err := putImmutable(ctx, a.targets[0].Store, entry.ScoreKey, fixture.Data, store.PutOptions{DoesNotExist: true}); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	manifestBytes, manifestSHA, err := manifest.Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	scoreList := make([]byte, 0)
	metadata := []byte("pgs_id,license,note\n")
	for _, fixture := range fixtures {
		scoreList = append(scoreList, fixture.PGSID...)
		scoreList = append(scoreList, '\n')
		metadata = append(metadata, []byte(fmt.Sprintf("%s,test,\n", fixture.PGSID))...)
	}
	scoreSum := sha256.Sum256(scoreList)
	metadataSum := sha256.Sum256(metadata)
	pointer := model.Pointer{
		ReleaseID:          releaseID,
		ManifestKey:        model.ManifestKey(releaseID),
		ManifestSHA256:     manifestSHA,
		ScoreListSHA256:    hex.EncodeToString(scoreSum[:]),
		MetadataSHA256:     hex.EncodeToString(metadataSum[:]),
		PublishedAt:        now,
		EntryCount:         len(entries),
		GenomeBuild:        "GRCh38",
		ScoreLayoutVersion: model.ScoreLayoutVersion,
	}
	if err := a.publish(ctx, pointer, scoreList, metadata, manifestBytes); err != nil {
		t.Fatal(err)
	}
	if a.State != nil {
		if err := a.State.RecordRelease(ctx, pointer, entries, a.Config.Targets.Local); err != nil {
			t.Fatal(err)
		}
	}
	return pointer
}

func TestAnnotateUsesOnlyStoredObjectsAndThenNoOps(t *testing.T) {
	var upstreamRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		http.Error(w, "upstream must not be contacted", http.StatusInternalServerError)
	}))
	defer srv.Close()
	cfg := integrationConfig(t.TempDir(), srv.URL)
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	legacy := seedLegacyRelease(t, a, now,
		annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)},
		annotationFixture{PGSID: "PGS000002", Data: gzipFixture("mystery_a\tmystery_b\nvalue_a\tvalue_b\n")},
		annotationFixture{PGSID: "PGS000003", Data: []byte("not a gzip stream")},
	)
	secondary, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a.targets = append(a.targets, target{kind: "local", Store: secondary})
	a.now = func() time.Time { return now.Add(time.Minute) }

	var progress []AnnotationProgress
	report, err := a.AnnotateWithProgress(context.Background(), false, func(update AnnotationProgress) {
		progress = append(progress, update)
	})
	if err != nil {
		t.Fatal(err)
	}
	if upstreamRequests.Load() != 0 {
		t.Fatalf("annotation made %d upstream request(s)", upstreamRequests.Load())
	}
	if !report.UpstreamIndependent || !report.Changed || !report.RepairedTargets || report.SourceReleaseID != legacy.ReleaseID || report.ReleaseID == legacy.ReleaseID {
		t.Fatalf("unexpected annotation report: %+v", report)
	}
	if report.Available != 3 || report.Inspected != 3 || report.Updated != 3 || report.Recognized != 1 || report.Unrecognized != 1 || report.Unreadable != 1 || report.Failed != 0 {
		t.Fatalf("unexpected annotation counts: %+v", report)
	}
	if len(report.Anomalies) != 2 || report.Anomalies[0].PGSID != "PGS000002" || report.Anomalies[0].Observation.Status != scoreheader.StatusUnrecognized || report.Anomalies[1].PGSID != "PGS000003" || report.Anomalies[1].Observation.Status != scoreheader.StatusUnreadable {
		t.Fatalf("unexpected anomaly details: %+v", report.Anomalies)
	}
	if len(progress) < 2 || progress[0].Available != 3 || progress[0].Processed != 0 {
		t.Fatalf("missing initial progress: %+v", progress)
	}
	lastProgress := progress[len(progress)-1]
	if lastProgress.Processed != 3 || lastProgress.Inspected != 3 || lastProgress.Updated != 3 || lastProgress.Unrecognized != 1 || lastProgress.Unreadable != 1 || lastProgress.Anomalies != 2 {
		t.Fatalf("unexpected final progress: %+v", lastProgress)
	}
	pointer, entries, err := a.latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pointer.HeaderInspectorVersion != scoreheader.InspectorVersion || len(entries) != 3 {
		t.Fatalf("annotated release is incomplete: pointer=%+v entries=%+v", pointer, entries)
	}
	for _, entry := range entries {
		if entry.Header == nil || !entry.Header.Current() {
			t.Fatalf("%s lacks a current annotation: %+v", entry.PGSID, entry.Header)
		}
	}
	secondaryPointer, _, err := a.readPointer(context.Background(), secondary)
	if err != nil {
		t.Fatal(err)
	}
	secondaryEntries, _, err := a.readManifest(context.Background(), secondary, secondaryPointer)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryPointer.ReleaseID != pointer.ReleaseID || len(secondaryEntries) != len(entries) {
		t.Fatalf("lagging target did not converge: pointer=%+v entries=%d", secondaryPointer, len(secondaryEntries))
	}

	second, err := a.Annotate(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed || second.ReleaseID != pointer.ReleaseID || second.Inspected != 0 || second.Unchanged != 3 || len(second.Anomalies) != 2 {
		t.Fatalf("current annotation pass was not a no-op: %+v", second)
	}
	if upstreamRequests.Load() != 0 {
		t.Fatalf("no-op annotation made %d upstream request(s)", upstreamRequests.Load())
	}
}

func TestRecordAnnotationObservationIncludesRecognizedWarningsAndErrors(t *testing.T) {
	report := AnnotationReport{}
	recordAnnotationObservation(&report, "PGS000001", scoreheader.Inspection{
		Status:   scoreheader.StatusRecognized,
		Warnings: []string{"synthetic warning"},
	})
	recordAnnotationObservation(&report, "PGS000002", scoreheader.Inspection{
		Status: scoreheader.StatusRecognized,
		Error:  "synthetic error",
	})
	recordAnnotationObservation(&report, "PGS000003", scoreheader.Inspection{
		Status: scoreheader.StatusRecognized,
	})

	if report.Recognized != 3 {
		t.Fatalf("recognized=%d, want 3", report.Recognized)
	}
	if len(report.Anomalies) != 2 || report.Anomalies[0].PGSID != "PGS000001" || report.Anomalies[1].PGSID != "PGS000002" {
		t.Fatalf("unexpected anomaly details: %+v", report.Anomalies)
	}
	reportAnnotationProgress(func(progress AnnotationProgress) {
		if progress.Anomalies != 2 {
			t.Fatalf("progress anomalies=%d, want 2", progress.Anomalies)
		}
	}, report)
}

func TestAnnotateDryRunDoesNotPublishOrLease(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	legacy := seedLegacyRelease(t, a, now, annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)})
	a.now = func() time.Time { return now.Add(time.Minute) }

	report, err := a.Annotate(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || !report.Changed || report.Updated != 1 || report.ReleaseID != "" {
		t.Fatalf("unexpected dry-run report: %+v", report)
	}
	pointer, entries, err := a.latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pointer.ReleaseID != legacy.ReleaseID || entries[0].Header != nil {
		t.Fatalf("dry run changed the published release: pointer=%+v entries=%+v", pointer, entries)
	}
	if _, err := a.targets[0].Stat(context.Background(), model.LeaseKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("dry run created a lease: %v", err)
	}
}

type openFailureStore struct {
	store.Store
	key string
}

func (s *openFailureStore) Open(ctx context.Context, key string) (io.ReadCloser, store.ObjectInfo, error) {
	if key == s.key {
		return nil, store.ObjectInfo{}, errors.New("injected stored-object read failure")
	}
	return s.Store.Open(ctx, key)
}

func TestAnnotateStoredObjectFailureDoesNotAdvancePointer(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	legacy := seedLegacyRelease(t, a, now,
		annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)},
		annotationFixture{PGSID: "PGS000002", Data: gzipFixture(strings.Replace(harmonizedHeaderFixture, "PGS000001", "PGS000002", 1))},
	)
	_, entries, err := a.latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a.targets[0].Store = &openFailureStore{Store: a.targets[0].Store, key: entries[0].ScoreKey}

	report, err := a.Annotate(context.Background(), false)
	if err == nil || report.Available != 2 || report.Inspected != 2 || report.Updated != 1 || report.Failed != 1 || len(report.Failures) != 1 {
		t.Fatalf("stored-object failure was not reported: report=%+v err=%v", report, err)
	}
	pointer, _, err := a.readPointer(context.Background(), a.targets[0].Store)
	if err != nil {
		t.Fatal(err)
	}
	if pointer.ReleaseID != legacy.ReleaseID {
		t.Fatalf("LATEST advanced across annotation failure: %s -> %s", legacy.ReleaseID, pointer.ReleaseID)
	}
	if _, err := a.targets[0].Stat(context.Background(), model.LeaseKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed annotation retained its lease: %v", err)
	}
}

func replaceLatestPointer(t *testing.T, a *App, pointer model.Pointer) {
	t.Helper()
	ctx := context.Background()
	_, info, err := a.readPointer(ctx, a.targets[0].Store)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := manifest.PointerJSON(pointer)
	if err != nil {
		t.Fatal(err)
	}
	gen := info.Generation
	if _, err := a.targets[0].Put(ctx, model.LatestKey, bytes.NewReader(payload), store.PutOptions{GenerationMatch: &gen}); err != nil {
		t.Fatal(err)
	}
}

func TestAnnotateRejectsNewerAnnotationVersionWithoutUpstreamAccess(t *testing.T) {
	var upstreamRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		http.Error(w, "upstream must not be contacted", http.StatusInternalServerError)
	}))
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	pointer := seedLegacyRelease(t, a, now, annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)})
	pointer.HeaderInspectorVersion = scoreheader.InspectorVersion + 1
	replaceLatestPointer(t, a, pointer)

	if _, err := a.Annotate(context.Background(), false); err == nil {
		t.Fatal("annotation accepted a newer published inspector version")
	}
	if _, err := a.Update(context.Background(), false); err == nil {
		t.Fatal("update accepted a newer published inspector version")
	}
	if _, err := a.Reconcile(context.Background(), false); err == nil {
		t.Fatal("reconciliation accepted a newer published inspector version")
	}
	if upstreamRequests.Load() != 0 {
		t.Fatalf("version rejection made %d upstream request(s)", upstreamRequests.Load())
	}
}

func TestAnnotateRejectsCorruptStoredSnapshotWithoutAdvancingPointer(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	pointer := seedLegacyRelease(t, a, now, annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)})
	pointer.ScoreListSHA256 = strings.Repeat("0", 64)
	replaceLatestPointer(t, a, pointer)

	if _, err := a.Annotate(context.Background(), false); err == nil {
		t.Fatal("annotation accepted a stored snapshot with the wrong SHA-256")
	}
	after, _, err := a.readPointer(context.Background(), a.targets[0].Store)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReleaseID != pointer.ReleaseID || after.ScoreListSHA256 != pointer.ScoreListSHA256 {
		t.Fatalf("LATEST advanced after stored snapshot failure: before=%+v after=%+v", pointer, after)
	}
}

func TestAnnotatePublicationFailureDoesNotAdvancePointer(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	legacy := seedLegacyRelease(t, a, now, annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)})
	a.now = func() time.Time { return now.Add(time.Minute) }
	a.targets[0].Store = &failingStore{Store: a.targets[0].Store, failSuffix: "/manifest.jsonl.gz"}

	if _, err := a.Annotate(context.Background(), false); err == nil {
		t.Fatal("injected annotation publication failure succeeded")
	}
	pointer, _, err := a.readPointer(context.Background(), a.targets[0].Store)
	if err != nil {
		t.Fatal(err)
	}
	if pointer.ReleaseID != legacy.ReleaseID {
		t.Fatalf("LATEST advanced across failed annotation publication: %s -> %s", legacy.ReleaseID, pointer.ReleaseID)
	}
}

type pathCounter struct {
	handler http.Handler
	mu      sync.Mutex
	paths   map[string]int
}

func (c *pathCounter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.paths[r.URL.Path]++
	c.mu.Unlock()
	c.handler.ServeHTTP(w, r)
}

func (c *pathCounter) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths[path]
}

func TestUpdateAnnotatesBeforeLightweightUpstreamCheck(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	counter := &pathCounter{handler: up, paths: make(map[string]int)}
	srv := httptest.NewServer(counter)
	defer srv.Close()
	a, err := New(context.Background(), integrationConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	legacy := seedLegacyRelease(t, a, now, annotationFixture{PGSID: "PGS000001", Data: gzipFixture(harmonizedHeaderFixture)})
	if err := a.State.PutSentinel(context.Background(), state.Sentinel{Name: "score_list", ETag: `"list-1"`, ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := a.State.PutSentinel(context.Background(), state.Sentinel{Name: "metadata", ETag: `"metadata-1"`, ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return now.Add(time.Minute) }

	report, err := a.Update(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Changed || report.ReleaseID == legacy.ReleaseID {
		t.Fatalf("update did not publish annotations: %+v", report)
	}
	if counter.count("/root/pgs_scores_list.txt") != 1 || counter.count("/root/metadata/pgs_all_metadata_scores.csv") != 1 {
		t.Fatalf("lightweight sentinels were not checked exactly once: %+v", counter.paths)
	}
	sidecar := "/root/scores/PGS000001/ScoringFiles/Harmonized/PGS000001_hmPOS_GRCh38.txt.gz.md5"
	score := "/root/scores/PGS000001/ScoringFiles/Harmonized/PGS000001_hmPOS_GRCh38.txt.gz"
	if counter.count(sidecar) != 0 || counter.count(score) != 0 {
		t.Fatalf("annotation-triggered update accessed scoring upstream: %+v", counter.paths)
	}
	pointer, entries, err := a.latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pointer.HeaderInspectorVersion != scoreheader.InspectorVersion || entries[0].Header == nil {
		t.Fatalf("updated release lacks annotations: pointer=%+v entries=%+v", pointer, entries)
	}
}
