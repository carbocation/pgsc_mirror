package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pgsc-mirror/pgsc-mirror/internal/config"
	"github.com/pgsc-mirror/pgsc-mirror/internal/model"
	"github.com/pgsc-mirror/pgsc-mirror/internal/store"
	localstore "github.com/pgsc-mirror/pgsc-mirror/internal/store/local"
	"github.com/pgsc-mirror/pgsc-mirror/internal/transfer"
)

type syntheticUpstream struct {
	mu       sync.RWMutex
	scores   map[string][]byte
	licenses map[string]string
	note     string
	version  int
}

type sidecarGate struct {
	handler http.Handler
	mu      sync.Mutex
	blocked map[string]bool
	calls   map[string]int
}

func newSidecarGate(handler http.Handler) *sidecarGate {
	return &sidecarGate{handler: handler, blocked: make(map[string]bool), calls: make(map[string]int)}
}

func (g *sidecarGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, ".md5") {
		var id string
		for _, part := range strings.Split(r.URL.Path, "/") {
			if strings.HasPrefix(part, "PGS") && len(part) == 9 {
				id = part
				break
			}
		}
		g.mu.Lock()
		g.calls[id]++
		blocked := g.blocked[id]
		g.mu.Unlock()
		if blocked {
			http.NotFound(w, r)
			return
		}
	}
	g.handler.ServeHTTP(w, r)
}

func (g *sidecarGate) block(id string, blocked bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.blocked[id] = blocked
}

func (g *sidecarGate) count(id string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[id]
}

func newSyntheticUpstream() *syntheticUpstream {
	return &syntheticUpstream{scores: map[string][]byte{}, licenses: map[string]string{}}
}

func gzipFixture(text string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Header.ModTime = time.Unix(0, 0)
	_, _ = w.Write([]byte(text))
	_ = w.Close()
	return b.Bytes()
}

func md5hex(b []byte) string { sum := md5.Sum(b); return hex.EncodeToString(sum[:]) }

func (u *syntheticUpstream) set(id, stringBody, license string, bump bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.scores[id] = gzipFixture(stringBody)
	u.licenses[id] = license
	if bump {
		u.version++
	}
}
func (u *syntheticUpstream) remove(id string, bump bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.scores, id)
	delete(u.licenses, id)
	if bump {
		u.version++
	}
}

func (u *syntheticUpstream) setNote(note string, bump bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.note = note
	if bump {
		u.version++
	}
}

func (u *syntheticUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	ids := make([]string, 0, len(u.scores))
	for id := range u.scores {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	switch r.URL.Path {
	case "/root/pgs_scores_list.txt":
		etag := fmt.Sprintf("\"list-%d\"", u.version)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		io.WriteString(w, strings.Join(ids, "\n")+"\n")
		return
	case "/root/metadata/pgs_all_metadata_scores.csv":
		etag := fmt.Sprintf("\"metadata-%d\"", u.version)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		io.WriteString(w, "pgs_id,license,note\n")
		for _, id := range ids {
			fmt.Fprintf(w, "%s,%s,%s\n", id, u.licenses[id], u.note)
		}
		return
	}
	for id, data := range u.scores {
		base := "/root/scores/" + id + "/ScoringFiles/Harmonized/" + id + "_hmPOS_GRCh38.txt.gz"
		if r.URL.Path == base+".md5" {
			fmt.Fprintf(w, "%s  %s\n", md5hex(data), filepath.Base(base))
			return
		}
		if r.URL.Path == base {
			w.Header().Set("ETag", "\""+md5hex(data)+"\"")
			w.Header().Set("Last-Modified", "Fri, 31 Jul 2026 12:00:00 GMT")
			w.Write(data)
			return
		}
	}
	http.NotFound(w, r)
}

func integrationConfig(root, base string) config.Config {
	c := config.Defaults()
	c.Upstream.BaseURLs = []string{base + "/root"}
	c.Local.Root = root
	c.State.Path = filepath.Join(root, ".pgsc-mirror", "state.db")
	c.Transfer.Concurrency = 2
	c.Transfer.InitialBackoff = config.Duration{Duration: time.Millisecond}
	c.Transfer.MaxBackoff = config.Duration{Duration: time.Millisecond}
	c.Transfer.RequestTimeout = config.Duration{Duration: 5 * time.Second}
	c.Identity.UserAgent = "pgsc-mirror-test"
	return c
}

type failingStore struct {
	store.Store
	failSuffix string
	failed     bool
}

func (s *failingStore) Put(ctx context.Context, key string, r io.Reader, o store.PutOptions) (store.ObjectInfo, error) {
	if !s.failed && strings.HasSuffix(key, s.failSuffix) {
		s.failed = true
		return store.ObjectInfo{}, errors.New("injected publication crash")
	}
	return s.Store.Put(ctx, key, r, o)
}

func TestReconcileLifecycleAndRecovery(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "first-v1", "CC BY 4.0", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	root := t.TempDir()
	cfg := integrationConfig(root, srv.URL)
	ctx := context.Background()
	a, err := New(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return clock }
	r, err := a.Reconcile(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Changed || r.ReleaseID == "" {
		t.Fatalf("unexpected initial report %+v", r)
	}
	initial := r.ReleaseID
	v, err := a.Verify(ctx, true, 0)
	if err != nil {
		t.Fatalf("verify: %v (%+v)", err, v)
	}

	// Cheap update sees unchanged sentinels. An in-place file revision is caught by full reconciliation.
	u, err := a.Update(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if u.Changed {
		t.Fatalf("unchanged update reported change: %+v", u)
	}
	// Raw metadata snapshots are versioned even when entry fields are unchanged.
	up.setNote("curation update", true)
	clock = clock.Add(time.Minute)
	if r, err = a.Reconcile(ctx, false); err != nil || !r.Changed {
		t.Fatalf("metadata-only release: report=%+v err=%v", r, err)
	}
	up.set("PGS000001", "first-v2", "CC BY 4.0", false)
	clock = clock.Add(time.Minute)
	u, err = a.Update(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if u.Changed {
		t.Fatal("sentinel-only update unexpectedly caught in-place revision")
	}
	r, err = a.Reconcile(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Changed || r.ReleaseID == initial {
		t.Fatalf("revision not published: %+v", r)
	}

	up.set("PGS000002", "second-v1", "custom", true)
	clock = clock.Add(time.Minute)
	if r, err = a.Reconcile(ctx, false); err != nil || !r.Changed {
		t.Fatalf("addition: report=%+v err=%v", r, err)
	}
	up.remove("PGS000002", true)
	clock = clock.Add(time.Minute)
	if r, err = a.Reconcile(ctx, false); err != nil || !r.Changed {
		t.Fatalf("withdrawal: report=%+v err=%v", r, err)
	}
	p, entries, err := a.latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = p
	foundGone := false
	for _, e := range entries {
		if e.PGSID == "PGS000002" && e.Status == model.StatusGone {
			foundGone = true
		}
	}
	if !foundGone {
		t.Fatal("withdrawn score was not retained in manifest")
	}
	up.set("PGS000002", "second-v1", "custom", true)
	clock = clock.Add(time.Minute)
	if r, err = a.Reconcile(ctx, false); err != nil || !r.Changed {
		t.Fatalf("restoration: report=%+v err=%v", r, err)
	}

	// A crash while publishing the immutable manifest leaves the old pointer in place.
	before, _, err := a.readPointer(ctx, a.targets[0].Store)
	if err != nil {
		t.Fatal(err)
	}
	up.set("PGS000001", "first-v3", "CC BY 4.0", true)
	clock = clock.Add(time.Minute)
	original := a.targets[0].Store
	failManifest := &failingStore{Store: original, failSuffix: "/manifest.jsonl.gz"}
	a.targets[0].Store = failManifest
	if _, err := a.Reconcile(ctx, false); err == nil {
		t.Fatal("injected manifest failure succeeded")
	}
	a.targets[0].Store = original
	after, _, err := a.readPointer(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReleaseID != before.ReleaseID {
		t.Fatalf("LATEST advanced across failed manifest: %s -> %s", before.ReleaseID, after.ReleaseID)
	}

	// A crash after immutable publication but before LATEST also preserves the old pointer.
	failLatest := &failingStore{Store: original, failSuffix: model.LatestKey}
	a.targets[0].Store = failLatest
	clock = clock.Add(time.Minute)
	if _, err := a.Reconcile(ctx, false); err == nil {
		t.Fatal("injected pointer failure succeeded")
	}
	a.targets[0].Store = original
	after, _, err = a.readPointer(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	if after.ReleaseID != before.ReleaseID {
		t.Fatalf("LATEST advanced across failed pointer: %s -> %s", before.ReleaseID, after.ReleaseID)
	}
	objects, err := original.List(ctx, "releases")
	if err != nil {
		t.Fatal(err)
	}
	orphanManifest := false
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, "/manifest.jsonl.gz") && !strings.Contains(obj.Key, before.ReleaseID) {
			orphanManifest = true
		}
	}
	if !orphanManifest {
		t.Fatal("expected immutable manifest to survive pointer failure")
	}
	clock = clock.Add(time.Minute)
	if r, err = a.Reconcile(ctx, false); err != nil || !r.Changed {
		t.Fatalf("retry after publication crash: report=%+v err=%v", r, err)
	}
	if _, err := a.Verify(ctx, true, 0); err != nil {
		t.Fatal(err)
	}

	// Pull the published release through the storage abstraction into a fresh local target.
	pullDest, err := localstore.New(filepath.Join(t.TempDir(), "pulled"))
	if err != nil {
		t.Fatal(err)
	}
	puller := &App{Config: cfg, targets: []target{{kind: "gcs", Store: original}, {kind: "local", Store: pullDest}}, now: a.now}
	pulled, err := puller.Pull(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if pulled.ReleaseID == "" {
		t.Fatalf("bad pull report %+v", pulled)
	}
	if _, _, err := puller.readPointer(ctx, pullDest); err != nil {
		t.Fatal(err)
	}

	// GC is a true dry run by default, then applies only after explicit approval.
	a.Config.Retention.KeepReleases = 1
	a.Config.Retention.MissingGrace = config.Duration{}
	a.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	gcPlan, err := a.GC(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !gcPlan.DryRun || len(gcPlan.Items) == 0 {
		t.Fatalf("unexpected GC plan %+v", gcPlan)
	}
	if _, _, err := a.readPointer(ctx, original); err != nil {
		t.Fatalf("dry-run mutated latest: %v", err)
	}
	gcApplied, err := a.GC(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if gcApplied.DryRun {
		t.Fatal("applied GC reported dry-run")
	}

	// Delete the disposable SQLite index and rebuild it only from immutable manifests.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(cfg.State.Path + suffix)
	}
	a2, err := New(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	rebuilt, err := a2.RebuildState(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !rebuilt.Changed {
		t.Fatalf("unexpected rebuild report %+v", rebuilt)
	}
	status, err := a2.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.State.Available != 2 || status.State.LatestRelease == "" {
		t.Fatalf("bad rebuilt status %+v", status)
	}
}

func TestReconcileResumesSidecarCheckpoint(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "first", "one", true)
	up.set("PGS000002", "second", "two", true)
	gate := newSidecarGate(up)
	gate.block("PGS000002", true)
	srv := httptest.NewServer(gate)
	defer srv.Close()
	cfg := integrationConfig(t.TempDir(), srv.URL)
	cfg.Transfer.Concurrency = 1
	cfg.Transfer.MaxAttempts = 1
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Reconcile(context.Background(), false); err == nil {
		t.Fatal("blocked sidecar unexpectedly reconciled")
	}
	gate.block("PGS000002", false)
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := gate.count("PGS000001"); got != 1 {
		t.Fatalf("successful sidecar was refetched after interruption: %d calls", got)
	}
	if got := gate.count("PGS000002"); got != 2 {
		t.Fatalf("failed sidecar call count is %d, want 2", got)
	}
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if gate.count("PGS000001") != 2 || gate.count("PGS000002") != 3 {
		t.Fatalf("completed checkpoint was reused by a later audit: one=%d two=%d", gate.count("PGS000001"), gate.count("PGS000002"))
	}
}

func TestReconcileAutomaticallyCatchesUpLostState(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "first", "one", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	root := t.TempDir()
	cfg := integrationConfig(root, srv.URL)
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(cfg.State.Path + suffix)
	}
	a, err = New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	second, err := a.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed || second.ReleaseID != first.ReleaseID {
		t.Fatalf("state catch-up republished the release: first=%+v second=%+v", first, second)
	}
	summary, err := a.State.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.LatestRelease != first.ReleaseID || summary.Available != 1 {
		t.Fatalf("state did not catch up from canonical release: %+v", summary)
	}
}

func TestLeaseRenewsAndReleases(t *testing.T) {
	ls, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults().WithRuntimeDefaults()
	cfg.Transfer.LeaseDuration = config.Duration{Duration: 300 * time.Millisecond}
	a := &App{Config: cfg, targets: []target{{kind: "local", Store: ls}}, now: time.Now}
	lease, err := a.acquireLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstGeneration := lease.generation
	_ = a.startLeaseRenewal(context.Background(), lease)
	deadline := time.After(time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	renewed := false
	for !renewed {
		select {
		case <-deadline:
			t.Fatal("lease did not renew")
		case <-ticker.C:
			lease.mu.Lock()
			renewed = lease.generation != firstGeneration
			lease.mu.Unlock()
		}
	}
	if err := lease.stopRenewal(); err != nil {
		t.Fatal(err)
	}
	if err := a.releaseLease(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := ls.Stat(context.Background(), model.LeaseKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("lease remains after release: %v", err)
	}
}

func TestReconcileReusesVerifiedDownloadAfterUploadFailure(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "durable score", "one", true)
	var scoreGets atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, ".txt.gz") {
			scoreGets.Add(1)
		}
		up.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()
	cfg := integrationConfig(t.TempDir(), srv.URL)
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	original := a.targets[0].Store
	a.targets[0].Store = &failingStore{Store: original, failSuffix: ".txt.gz"}
	if _, err := a.Reconcile(context.Background(), false); err == nil {
		t.Fatal("injected blob upload failure succeeded")
	}
	partials, err := filepath.Glob(filepath.Join(a.Config.State.WorkDir, "partials", "*.part"))
	if err != nil || len(partials) != 1 {
		t.Fatalf("verified partial was not retained: files=%v err=%v", partials, err)
	}
	a.targets[0].Store = original
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := scoreGets.Load(); got != 1 {
		t.Fatalf("score body was fetched %d times, want 1", got)
	}
	if _, err := os.Stat(partials[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stored partial was not removed: %v", err)
	}
}

func TestReconcileRepairsLaggingSecondaryPointer(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "first", "one", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	cfg := integrationConfig(t.TempDir(), srv.URL)
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	primary := a.targets[0].Store
	secondary, err := localstore.New(filepath.Join(t.TempDir(), "secondary"))
	if err != nil {
		t.Fatal(err)
	}
	a.targets = []target{{kind: "gcs", Store: primary}, {kind: "local", Store: secondary}}
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	up.set("PGS000001", "second", "one", true)
	a.targets[1].Store = &failingStore{Store: secondary, failSuffix: model.LatestKey}
	if _, err := a.Reconcile(context.Background(), false); err == nil {
		t.Fatal("injected secondary pointer failure succeeded")
	}
	primaryPointer, _, err := a.readPointer(context.Background(), primary)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPointer, _, err := a.readPointer(context.Background(), secondary)
	if err != nil {
		t.Fatal(err)
	}
	if primaryPointer.ReleaseID == secondaryPointer.ReleaseID {
		t.Fatal("secondary pointer unexpectedly advanced during injected failure")
	}
	a.targets[1].Store = secondary
	repaired, err := a.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !repaired.Changed || !strings.Contains(repaired.Message, "repaired lagging targets") {
		t.Fatalf("repair was not reported: %+v", repaired)
	}
	secondaryPointer, _, err = a.readPointer(context.Background(), secondary)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryPointer.ReleaseID != primaryPointer.ReleaseID {
		t.Fatalf("secondary remains stale: primary=%s secondary=%s", primaryPointer.ReleaseID, secondaryPointer.ReleaseID)
	}
}

func TestReconcileEnforcesConfiguredScoringFileLimit(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "larger than a tiny configured cap", "one", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	cfg := integrationConfig(t.TempDir(), srv.URL)
	cfg.Transfer.MaxFileSize = config.ByteSize{Bytes: 8}
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Reconcile(context.Background(), false); !errors.Is(err, transfer.ErrSizeLimit) {
		t.Fatalf("got %v, want scoring-file size-limit failure", err)
	}
	partials, err := filepath.Glob(filepath.Join(a.Config.State.WorkDir, "partials", "*.part"))
	if err != nil || len(partials) != 0 {
		t.Fatalf("oversized partial was retained: files=%v err=%v", partials, err)
	}
}

func TestPrunePartialsBoundsRetainedRestartFiles(t *testing.T) {
	cfg := config.Defaults()
	cfg.State.WorkDir = t.TempDir()
	cfg.Transfer.FileConcurrency = 2
	a := &App{Config: cfg}
	now := time.Now()
	entries := []model.Entry{
		{PGSID: "PGS000001", SourceMD5: strings.Repeat("1", 32), Status: model.StatusReady},
		{PGSID: "PGS000002", SourceMD5: strings.Repeat("2", 32), Status: model.StatusReady},
		{PGSID: "PGS000003", SourceMD5: strings.Repeat("3", 32), Status: model.StatusReady},
	}
	for i := range entries {
		part := a.partialPath(&entries[i])
		if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(part, []byte(entries[i].PGSID), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(part+".json", []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := now.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(part, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	obsolete := filepath.Join(cfg.State.WorkDir, "partials", "PGS999999-"+strings.Repeat("f", 32)+".part")
	if err := os.WriteFile(obsolete, []byte("obsolete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.prunePartials(entries); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(a.partialPath(&entries[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest excess partial remains: %v", err)
	}
	for i := 1; i < len(entries); i++ {
		if _, err := os.Stat(a.partialPath(&entries[i])); err != nil {
			t.Fatalf("recent partial %d was removed: %v", i, err)
		}
	}
	if _, err := os.Stat(obsolete); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("obsolete checksum partial remains: %v", err)
	}
	order := a.blobJobOrder(entries)
	if order[0] != 1 || order[1] != 2 {
		t.Fatalf("retained partials were not prioritized: %v", order)
	}
}

func TestPlanIsReadOnlyAndCanLimitSidecars(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", "one", "a", true)
	up.set("PGS000002", "two", "b", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	base := t.TempDir()
	root := filepath.Join(base, "does-not-exist")
	cfg := integrationConfig(root, srv.URL)
	cfg.Transfer.SidecarLimit = 1
	a, err := New(context.Background(), cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	report, err := a.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Truncated || report.ScoreCount != 1 || report.TotalScoreCount != 2 {
		t.Fatalf("unexpected plan %+v", report)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plan created local root: %v", err)
	}
}
