// Package app coordinates catalog discovery, transfer, publication, and recovery.
package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/catalog"
	"github.com/carbocation/pgsc_mirror/internal/config"
	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/planner"
	"github.com/carbocation/pgsc_mirror/internal/state"
	"github.com/carbocation/pgsc_mirror/internal/store"
	gcsstore "github.com/carbocation/pgsc_mirror/internal/store/gcs"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
	"github.com/carbocation/pgsc_mirror/internal/transfer"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

type target struct {
	kind string
	store.Store
}

type App struct {
	Config  config.Config
	Catalog *catalog.Client
	HTTP    *transfer.HTTPClient
	State   *state.DB
	targets []target
	now     func() time.Time
}

type PlanReport struct {
	PreviousRelease    string               `json:"previous_release,omitempty"`
	ScoreCount         int                  `json:"score_count"`
	TotalScoreCount    int                  `json:"total_score_count"`
	Truncated          bool                 `json:"truncated"`
	ScoreListChanged   bool                 `json:"score_list_changed"`
	MetadataChanged    bool                 `json:"metadata_changed"`
	ScoreLayoutChanged bool                 `json:"score_layout_changed"`
	ScoreKeyMigrations int                  `json:"score_key_migrations_needed"`
	HeaderInspections  int                  `json:"header_inspections_needed"`
	Changes            []planner.Change     `json:"changes"`
	Counts             map[planner.Kind]int `json:"counts"`
}

type RunReport struct {
	Command   string      `json:"command"`
	ReleaseID string      `json:"release_id,omitempty"`
	Changed   bool        `json:"changed"`
	Message   string      `json:"message"`
	Plan      *PlanReport `json:"plan,omitempty"`
}

type VerifyTarget struct {
	Target    string   `json:"target"`
	ReleaseID string   `json:"release_id"`
	Checked   int      `json:"checked"`
	Available int      `json:"available"`
	Sampled   bool     `json:"sampled"`
	Failures  []string `json:"failures,omitempty"`
}

type VerifyReport struct {
	Targets []VerifyTarget `json:"targets"`
}

type StatusReport struct {
	State   state.Summary  `json:"state"`
	Targets []TargetStatus `json:"targets"`
}

type TargetStatus struct {
	Target             string         `json:"target"`
	ReleaseID          string         `json:"release_id,omitempty"`
	PublishedAt        *time.Time     `json:"published_at,omitempty"`
	Entries            int            `json:"entries,omitempty"`
	HeaderTypes        map[string]int `json:"header_types,omitempty"`
	HeaderSchemas      []HeaderSchema `json:"header_schemas,omitempty"`
	HeaderAnomalies    int            `json:"header_anomalies,omitempty"`
	UninspectedHeaders int            `json:"uninspected_headers,omitempty"`
	Error              string         `json:"error,omitempty"`
}

type HeaderSchema struct {
	Type         string `json:"type"`
	SchemaSHA256 string `json:"schema_sha256"`
	Count        int    `json:"count"`
}

type GCItem struct {
	Target string `json:"target"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
	Size   int64  `json:"size"`
}
type GCReport struct {
	DryRun bool     `json:"dry_run"`
	Items  []GCItem `json:"items"`
}

func New(ctx context.Context, cfg config.Config, writable bool) (*App, error) {
	cfg = cfg.WithRuntimeDefaults()
	if writable {
		if err := cfg.Prepare(); err != nil {
			return nil, err
		}
	}
	httpClient := transfer.NewHTTPClient(cfg.Transfer.RequestTimeout.Duration, transfer.Policy{Attempts: cfg.Transfer.MaxAttempts, InitialBackoff: cfg.Transfer.InitialBackoff.Duration, MaxBackoff: cfg.Transfer.MaxBackoff.Duration}, cfg.Identity.UserAgent)
	a := &App{Config: cfg, HTTP: httpClient, now: time.Now}
	a.Catalog = catalog.New(cfg.Upstream.BaseURLs, cfg.Upstream.ScoreList, cfg.Upstream.MetadataCSV, httpClient)
	// GCS is authoritative when enabled, so publish its pointer before local pointers.
	if cfg.Targets.GCS {
		gs, err := gcsstore.New(ctx, cfg.GCS.Bucket, cfg.GCS.Prefix, cfg.GCS.BillingProject, filepath.Join(cfg.State.WorkDir, "gcs-staging"))
		if err != nil {
			return nil, err
		}
		a.targets = append(a.targets, target{kind: "gcs", Store: gs})
	}
	if cfg.Targets.Local {
		ls, err := localstore.New(cfg.Local.Root)
		if err != nil {
			a.Close()
			return nil, err
		}
		a.targets = append(a.targets, target{kind: "local", Store: ls})
	}
	if writable {
		db, err := state.Open(cfg.State.Path)
		if err != nil {
			a.Close()
			return nil, fmt.Errorf("open state: %w", err)
		}
		a.State = db
	}
	return a, nil
}

func (a *App) Close() error {
	var errs []error
	if a.State != nil {
		errs = append(errs, a.State.Close())
	}
	for _, t := range a.targets {
		errs = append(errs, t.Close())
	}
	return errors.Join(errs...)
}

func (a *App) target(kind string) (target, bool) {
	for _, t := range a.targets {
		if t.kind == kind {
			return t, true
		}
	}
	return target{}, false
}

func (a *App) readPointer(ctx context.Context, st store.Store) (model.Pointer, store.ObjectInfo, error) {
	r, info, err := st.Open(ctx, model.LatestKey)
	if err != nil {
		return model.Pointer{}, store.ObjectInfo{}, err
	}
	defer r.Close()
	var p model.Pointer
	if err := json.NewDecoder(io.LimitReader(r, 1<<20)).Decode(&p); err != nil {
		return p, info, fmt.Errorf("decode %s in %s: %w", model.LatestKey, st.Name(), err)
	}
	return p, info, nil
}

func (a *App) readManifest(ctx context.Context, st store.Store, p model.Pointer) ([]model.Entry, []byte, error) {
	r, _, err := st.Open(ctx, p.ManifestKey)
	if err != nil {
		return nil, nil, err
	}
	b, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); p.ManifestSHA256 != "" && got != p.ManifestSHA256 {
		return nil, nil, fmt.Errorf("manifest SHA-256 mismatch: got %s, want %s", got, p.ManifestSHA256)
	}
	entries, err := manifest.Decode(bytes.NewReader(b))
	return entries, b, err
}

func (a *App) latest(ctx context.Context) (model.Pointer, []model.Entry, error) {
	if len(a.targets) == 0 {
		return model.Pointer{}, nil, errors.New("no configured targets")
	}
	p, _, err := a.readPointer(ctx, a.targets[0].Store)
	if errors.Is(err, store.ErrNotFound) {
		return model.Pointer{}, nil, nil
	}
	if err != nil {
		return model.Pointer{}, nil, err
	}
	entries, _, err := a.readManifest(ctx, a.targets[0].Store, p)
	return p, entries, err
}

type inventory struct {
	ids                   []string
	totalIDs              int
	truncated             bool
	checkpointID          int64
	metadata              []byte
	entries               []model.Entry
	scoreDoc, metadataDoc catalog.Document
}

func (a *App) inventory(ctx context.Context, previous []model.Entry, allowLimit, resume bool) (inventory, error) {
	scoreDoc, ids, err := a.Catalog.ScoreList(ctx, "", "")
	if err != nil {
		return inventory{}, fmt.Errorf("fetch score list: %w", err)
	}
	metadataDoc, licenses, err := a.Catalog.Metadata(ctx, "", "")
	if err != nil {
		return inventory{}, fmt.Errorf("fetch metadata: %w", err)
	}
	totalIDs := len(ids)
	truncated := false
	if a.Config.Transfer.SidecarLimit > 0 && len(ids) > a.Config.Transfer.SidecarLimit {
		if !allowLimit {
			return inventory{}, fmt.Errorf("inventory has %d scores but transfer.sidecar_limit=%d; refusing a partial publication", len(ids), a.Config.Transfer.SidecarLimit)
		}
		ids = ids[:a.Config.Transfer.SidecarLimit]
		truncated = true
	}
	var checkpointID int64
	if resume && a.State != nil {
		checkpointID, err = a.State.BeginInventory(ctx, inventoryFingerprint(a.Config.GenomeBuild, scoreDoc.Body, metadataDoc.Body), a.Config.State.CheckpointMaxAge.Duration, a.now())
		if err != nil {
			return inventory{}, fmt.Errorf("open inventory checkpoint: %w", err)
		}
	}
	old := make(map[string]model.Entry, len(previous))
	for _, e := range previous {
		old[e.PGSID] = e
	}
	now := a.now().UTC()
	entries := make([]model.Entry, len(ids))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	errCh := make(chan error, 1)
	workers := a.Config.Transfer.Concurrency
	if workers > len(ids) {
		workers = len(ids)
	}
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				id := ids[i]
				var sum, sourceURL string
				var err error
				if checkpointID != 0 {
					cached, ok, cacheErr := a.State.InventorySidecar(ctx, checkpointID, id)
					if cacheErr != nil {
						err = cacheErr
					} else if ok {
						sum, sourceURL = cached.SourceMD5, cached.SourceURL
					}
				}
				if err == nil && sum == "" {
					sum, sourceURL, err = a.Catalog.Checksum(ctx, id, a.Config.GenomeBuild)
					if err == nil && checkpointID != 0 {
						err = a.State.PutInventorySidecar(ctx, checkpointID, id, sum, sourceURL, a.now())
					}
				}
				if err != nil {
					select {
					case errCh <- fmt.Errorf("checksum %s: %w", id, err):
						cancel()
					default:
					}
					return
				}
				first := now
				e := model.Entry{PGSID: id, GenomeBuild: a.Config.GenomeBuild, SourceURL: sourceURL, SourceMD5: sum, ScoreKey: model.ScoreKey(id, a.Config.GenomeBuild), FirstSeenAt: first, LastSeenAt: now, Status: model.StatusReady, License: licenses[id]}
				if prior, ok := old[id]; ok {
					e.FirstSeenAt = prior.FirstSeenAt
					if prior.SourceMD5 == sum {
						e.SizeBytes = prior.SizeBytes
						e.UpstreamETag = prior.UpstreamETag
						e.UpstreamLastModified = prior.UpstreamLastModified
						e.GCSGeneration = prior.GCSGeneration
						if prior.Header.Current() {
							e.Header = prior.Header
						}
					}
				}
				entries[i] = e
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := range ids {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		return inventory{}, err
	default:
	}
	active := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		active[id] = struct{}{}
	}
	if !truncated {
		for _, prior := range previous {
			if _, ok := active[prior.PGSID]; !ok {
				prior.Status = model.StatusGone
				entries = append(entries, prior)
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].PGSID < entries[j].PGSID })
	return inventory{ids: ids, totalIDs: totalIDs, truncated: truncated, checkpointID: checkpointID, metadata: metadataDoc.Body, entries: entries, scoreDoc: scoreDoc, metadataDoc: metadataDoc}, nil
}

func inventoryFingerprint(genomeBuild string, scoreList, metadata []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(genomeBuild))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(scoreList)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(metadata)
	return hex.EncodeToString(h.Sum(nil))
}

func (a *App) Plan(ctx context.Context) (PlanReport, error) {
	p, previous, err := a.latest(ctx)
	if err != nil {
		return PlanReport{}, err
	}
	inv, err := a.inventory(ctx, previous, true, false)
	if err != nil {
		return PlanReport{}, err
	}
	changes := planner.Plan(previous, inv.entries)
	metadataChanged, err := a.snapshotChanged(ctx, p, model.MetadataKey(p.ReleaseID), inv.metadata)
	if err != nil {
		return PlanReport{}, err
	}
	scoreListChanged, err := a.snapshotChanged(ctx, p, model.ScoreListKey(p.ReleaseID), inv.scoreDoc.Body)
	if err != nil {
		return PlanReport{}, err
	}
	keyMigrations := scoreLayoutMigrationsNeeded(previous)
	report := PlanReport{PreviousRelease: p.ReleaseID, ScoreCount: len(inv.ids), TotalScoreCount: inv.totalIDs, Truncated: inv.truncated, ScoreListChanged: scoreListChanged, MetadataChanged: metadataChanged, ScoreLayoutChanged: p.ReleaseID != "" && (p.ScoreLayoutVersion != model.ScoreLayoutVersion || keyMigrations > 0), ScoreKeyMigrations: keyMigrations, HeaderInspections: headerInspectionsNeeded(inv.entries), Changes: changes, Counts: map[planner.Kind]int{}}
	for _, c := range changes {
		report.Counts[c.Kind]++
	}
	return report, nil
}

func (a *App) Reconcile(ctx context.Context, dryRun bool) (report RunReport, runErr error) {
	report.Command = "reconcile"
	if dryRun {
		p, err := a.Plan(ctx)
		report.Plan = &p
		report.Changed = planner.HasChanges(p.Changes) || p.MetadataChanged || p.ScoreListChanged || p.ScoreLayoutChanged || p.HeaderInspections > 0
		report.Message = "dry run; no objects written"
		return report, err
	}
	return a.reconcile(ctx, "reconcile")
}

func (a *App) reconcile(ctx context.Context, command string) (report RunReport, runErr error) {
	report.Command = command
	if a.State == nil {
		return report, errors.New("writable state is not open")
	}
	runID, err := a.State.BeginRun(ctx, command)
	if err != nil {
		return report, err
	}
	defer func() { _ = a.State.FinishRun(context.Background(), runID, runErr) }()
	lease, err := a.acquireLease(ctx)
	if err != nil {
		return report, err
	}
	ctx = a.startLeaseRenewal(ctx, lease)
	defer func() {
		renewErr := lease.stopRenewal()
		releaseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		releaseErr := a.releaseLease(releaseCtx, lease)
		cancel()
		if renewErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("synchronization lease renewal: %w", renewErr))
		}
		if releaseErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("release synchronization lease: %w", releaseErr))
		}
	}()
	if err := a.cleanupStaging(); err != nil {
		return report, fmt.Errorf("clean provider staging: %w", err)
	}
	p, previous, err := a.latest(ctx)
	if err != nil {
		return report, err
	}
	if err := requireSupportedAnnotations(p, previous); err != nil {
		return report, err
	}
	layout, err := a.migrateCurrentScoreLayout(ctx)
	if err != nil {
		return report, fmt.Errorf("migrate score layout: %w", err)
	}
	if layout.Changed {
		p, previous, err = a.latest(ctx)
		if err != nil {
			return report, err
		}
	}
	repairedTargets := layout.RepairedTargets
	repaired, err := a.convergeTargets(ctx, p, previous)
	if err != nil {
		return report, err
	}
	repairedTargets = repairedTargets || repaired
	if err := a.catchUpState(ctx, p, previous); err != nil {
		return report, err
	}
	inv, err := a.inventory(ctx, previous, false, true)
	if err != nil {
		return report, err
	}
	if _, err := a.prunePartials(inv.entries); err != nil {
		return report, fmt.Errorf("prune scoring-file scratch: %w", err)
	}
	changes := planner.Plan(previous, inv.entries)
	headerInspections := headerInspectionsNeeded(inv.entries)
	metadataChanged, err := a.snapshotChanged(ctx, p, model.MetadataKey(p.ReleaseID), inv.metadata)
	if err != nil {
		return report, err
	}
	scoreListChanged, err := a.snapshotChanged(ctx, p, model.ScoreListKey(p.ReleaseID), inv.scoreDoc.Body)
	if err != nil {
		return report, err
	}
	if !planner.HasChanges(changes) && !metadataChanged && !scoreListChanged && headerInspections == 0 {
		if err := a.recordSentinels(ctx, inv); err != nil {
			return report, err
		}
		if err := a.publishMaintenanceCheckpoint(ctx, p, inv); err != nil {
			return report, err
		}
		if err := a.State.ConsumeInventory(ctx, inv.checkpointID); err != nil {
			return report, err
		}
		report.ReleaseID = p.ReleaseID
		report.Changed = repairedTargets || layout.Changed
		if layout.Changed {
			report.Message = fmt.Sprintf("migrated %d scoring files to the flat scores/ layout; upstream checksums and metadata match the latest release", layout.Migrated)
		} else if repairedTargets {
			report.Message = "repaired lagging targets; upstream checksums and metadata match the latest release"
		} else {
			report.Message = "upstream checksums and metadata match the latest release"
		}
		return report, nil
	}
	if err := a.ensureScores(ctx, inv.entries); err != nil {
		return report, err
	}
	now := a.now().UTC()
	releaseID, err := manifest.ReleaseID(now, inv.entries, inv.scoreDoc.Body, inv.metadata)
	if err != nil {
		return report, err
	}
	for i := range inv.entries {
		inv.entries[i].ReleaseID = releaseID
	}
	manifestBytes, manifestSHA, err := manifest.Encode(inv.entries)
	if err != nil {
		return report, err
	}
	scoreSum := sha256.Sum256(inv.scoreDoc.Body)
	metadataSum := sha256.Sum256(inv.metadata)
	pointer := model.Pointer{ReleaseID: releaseID, ManifestKey: model.ManifestKey(releaseID), ManifestSHA256: manifestSHA, ScoreListSHA256: hex.EncodeToString(scoreSum[:]), MetadataSHA256: hex.EncodeToString(metadataSum[:]), PublishedAt: now, EntryCount: len(inv.entries), GenomeBuild: a.Config.GenomeBuild, HeaderInspectorVersion: scoreheader.InspectorVersion, ScoreLayoutVersion: model.ScoreLayoutVersion}
	if err := a.publish(ctx, pointer, inv.scoreDoc.Body, inv.metadata, manifestBytes); err != nil {
		return report, err
	}
	if err := a.State.RecordRelease(ctx, pointer, inv.entries, a.Config.Targets.Local); err != nil {
		return report, err
	}
	if err := a.recordSentinels(ctx, inv); err != nil {
		return report, err
	}
	if err := a.publishMaintenanceCheckpoint(ctx, pointer, inv); err != nil {
		return report, err
	}
	if err := a.State.ConsumeInventory(ctx, inv.checkpointID); err != nil {
		return report, err
	}
	report.ReleaseID = releaseID
	report.Changed = true
	report.Message = "published complete immutable release"
	return report, nil
}

func (a *App) catchUpState(ctx context.Context, p model.Pointer, entries []model.Entry) error {
	if a.State == nil || p.ReleaseID == "" {
		return nil
	}
	current, err := a.State.LatestRelease(ctx)
	if err != nil {
		return fmt.Errorf("read operational state release: %w", err)
	}
	if current == p.ReleaseID {
		return nil
	}
	if err := a.State.RecordRelease(ctx, p, entries, a.Config.Targets.Local); err != nil {
		return fmt.Errorf("catch up operational state from canonical release %s: %w", p.ReleaseID, err)
	}
	return nil
}

func (a *App) snapshotChanged(ctx context.Context, p model.Pointer, key string, current []byte) (bool, error) {
	if p.ReleaseID == "" {
		return len(current) > 0, nil
	}
	r, _, err := a.targets[0].Open(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	existing, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return false, err
	}
	return !bytes.Equal(existing, current), nil
}

func (a *App) recordSentinels(ctx context.Context, inv inventory) error {
	for name, d := range map[string]catalog.Document{"score_list": inv.scoreDoc, "metadata": inv.metadataDoc} {
		sum := sha256.Sum256(d.Body)
		if err := a.State.PutSentinel(ctx, state.Sentinel{Name: name, ETag: d.ETag, LastModified: d.LastModified, ContentSHA256: hex.EncodeToString(sum[:]), ObservedAt: a.now().UTC()}); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) Update(ctx context.Context, dryRun bool) (report RunReport, runErr error) {
	if dryRun {
		p, err := a.Plan(ctx)
		return RunReport{Command: "update", Changed: planner.HasChanges(p.Changes) || p.MetadataChanged || p.ScoreListChanged || p.ScoreLayoutChanged || p.HeaderInspections > 0, Message: "dry run; no objects written", Plan: &p}, err
	}
	if a.State == nil {
		return RunReport{}, errors.New("writable state is not open")
	}
	if _, err := a.restoreMaintenanceCheckpoint(ctx); err != nil {
		return report, fmt.Errorf("restore shared maintenance state: %w", err)
	}
	layout, err := a.migrateScoreLayout(ctx)
	if err != nil {
		return report, fmt.Errorf("migrate score layout: %w", err)
	}
	repairedTargets, err := a.repairLaggingTargets(ctx)
	if err != nil {
		return report, fmt.Errorf("repair lagging targets: %w", err)
	}
	checkRun, err := a.State.BeginRun(ctx, "update-check")
	if err != nil {
		return report, err
	}
	finishCheck := true
	defer func() {
		if finishCheck {
			_ = a.State.FinishRun(context.Background(), checkRun, runErr)
		}
	}()
	pointer, _, err := a.readPointer(ctx, a.targets[0].Store)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return RunReport{}, err
	}
	var annotation AnnotationReport
	if err == nil {
		if err := requireSupportedAnnotations(pointer, nil); err != nil {
			return report, err
		}
		if pointer.HeaderInspectorVersion < scoreheader.InspectorVersion {
			annotation, err = a.Annotate(ctx, false)
			if err != nil {
				return report, fmt.Errorf("refresh stored-object annotations: %w", err)
			}
		}
	}
	scoreS, scoreOK, err := a.State.Sentinel(ctx, "score_list")
	if err != nil {
		return RunReport{}, err
	}
	metaS, metaOK, err := a.State.Sentinel(ctx, "metadata")
	if err != nil {
		return RunReport{}, err
	}
	scoreDoc, _, err := a.Catalog.ScoreList(ctx, scoreS.ETag, scoreS.LastModified)
	if err != nil {
		return RunReport{}, err
	}
	metaDoc, _, err := a.Catalog.Metadata(ctx, metaS.ETag, metaS.LastModified)
	if err != nil {
		return RunReport{}, err
	}
	if scoreOK && metaOK && scoreDoc.NotModified && metaDoc.NotModified {
		if annotation.Changed {
			message := "published stored-object annotations; upstream sentinels are unchanged"
			if layout.Changed {
				message = fmt.Sprintf("migrated %d scoring files to the flat scores/ layout and published stored-object annotations; upstream sentinels are unchanged", layout.Migrated)
			} else if repairedTargets {
				message = "published stored-object annotations and repaired lagging targets; upstream sentinels are unchanged"
			}
			return RunReport{Command: "update", ReleaseID: annotation.ReleaseID, Changed: true, Message: message}, nil
		}
		if layout.Changed {
			return RunReport{Command: "update", ReleaseID: layout.ReleaseID, Changed: true, Message: fmt.Sprintf("migrated %d scoring files to the flat scores/ layout; upstream sentinels are unchanged", layout.Migrated)}, nil
		}
		if repairedTargets {
			return RunReport{Command: "update", ReleaseID: pointer.ReleaseID, Changed: true, Message: "repaired lagging targets; upstream sentinels are unchanged"}, nil
		}
		return RunReport{Command: "update", Changed: false, Message: "upstream sentinels are unchanged"}, nil
	}
	if err := a.State.FinishRun(ctx, checkRun, nil); err != nil {
		return report, err
	}
	finishCheck = false
	reconciled, err := a.reconcile(ctx, "update")
	if err == nil && annotation.Changed {
		reconciled.Changed = true
		if reconciled.ReleaseID == "" {
			reconciled.ReleaseID = annotation.ReleaseID
		}
		reconciled.Message = "published stored-object annotations; " + reconciled.Message
	}
	if err == nil && layout.Changed {
		reconciled.Changed = true
		if reconciled.ReleaseID == "" {
			reconciled.ReleaseID = layout.ReleaseID
		}
		reconciled.Message = fmt.Sprintf("migrated %d scoring files to the flat scores/ layout; %s", layout.Migrated, reconciled.Message)
	}
	return reconciled, err
}

func headerInspectionsNeeded(entries []model.Entry) int {
	count := 0
	for i := range entries {
		if entries[i].Status == model.StatusReady && !entries[i].Header.Current() {
			count++
		}
	}
	return count
}

func requireSupportedAnnotations(pointer model.Pointer, entries []model.Entry) error {
	if pointer.HeaderInspectorVersion > scoreheader.InspectorVersion {
		return fmt.Errorf("published header inspector version %d is newer than this binary's version %d; upgrade pgsc-mirror before publishing", pointer.HeaderInspectorVersion, scoreheader.InspectorVersion)
	}
	for i := range entries {
		if entries[i].Status == model.StatusReady && entries[i].Header != nil && entries[i].Header.InspectorVersion > scoreheader.InspectorVersion {
			return fmt.Errorf("%s has header inspector version %d, newer than this binary's version %d; upgrade pgsc-mirror before publishing", entries[i].PGSID, entries[i].Header.InspectorVersion, scoreheader.InspectorVersion)
		}
	}
	return nil
}

type heldLease struct {
	target     target
	owner      string
	acquiredAt time.Time
	duration   time.Duration
	mu         sync.Mutex
	generation int64
	cancel     context.CancelFunc
	done       chan struct{}
	renewErr   error
}

func (a *App) acquireLease(ctx context.Context) (*heldLease, error) {
	if len(a.targets) == 0 {
		return nil, errors.New("no target for lease")
	}
	t := a.targets[0]
	host, _ := os.Hostname()
	now := a.now().UTC()
	l := model.Lease{Owner: fmt.Sprintf("%s:%d", host, os.Getpid()), AcquiredAt: now, ExpiresAt: now.Add(a.Config.Transfer.LeaseDuration.Duration)}
	b, _ := json.Marshal(l)
	for attempt := 0; attempt < 2; attempt++ {
		info, err := t.Put(ctx, model.LeaseKey, bytes.NewReader(b), store.PutOptions{DoesNotExist: true, ContentType: "application/json"})
		if err == nil {
			return &heldLease{target: t, owner: l.Owner, acquiredAt: l.AcquiredAt, duration: a.Config.Transfer.LeaseDuration.Duration, generation: info.Generation}, nil
		}
		if !errors.Is(err, store.ErrPrecondition) {
			return nil, err
		}
		r, oldInfo, openErr := t.Open(ctx, model.LeaseKey)
		if openErr != nil {
			return nil, openErr
		}
		var old model.Lease
		decodeErr := json.NewDecoder(io.LimitReader(r, 1<<20)).Decode(&old)
		r.Close()
		if decodeErr != nil || old.ExpiresAt.After(now) {
			return nil, fmt.Errorf("synchronization lease is held by %s until %s", old.Owner, old.ExpiresAt.Format(time.RFC3339))
		}
		gen := oldInfo.Generation
		if err := t.Delete(ctx, model.LeaseKey, store.DeleteOptions{GenerationMatch: &gen}); err != nil {
			return nil, fmt.Errorf("remove expired lease: %w", err)
		}
	}
	return nil, errors.New("could not acquire synchronization lease")
}

func (a *App) startLeaseRenewal(parent context.Context, l *heldLease) context.Context {
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.done = make(chan struct{})
	interval := l.duration / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		defer close(l.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.renewLease(ctx, l); err != nil {
					if ctx.Err() != nil {
						return
					}
					l.mu.Lock()
					l.renewErr = err
					l.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return ctx
}

func (a *App) renewLease(ctx context.Context, l *heldLease) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := a.now().UTC()
	payload, _ := json.Marshal(model.Lease{Owner: l.owner, AcquiredAt: l.acquiredAt, ExpiresAt: now.Add(l.duration)})
	gen := l.generation
	info, err := l.target.Put(ctx, model.LeaseKey, bytes.NewReader(payload), store.PutOptions{GenerationMatch: &gen, ContentType: "application/json"})
	if err != nil {
		return err
	}
	l.generation = info.Generation
	return nil
}

func (l *heldLease) stopRenewal() error {
	if l.cancel != nil {
		l.cancel()
	}
	if l.done != nil {
		<-l.done
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.renewErr
}

func (a *App) releaseLease(ctx context.Context, l *heldLease) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	gen := l.generation
	err := l.target.Delete(ctx, model.LeaseKey, store.DeleteOptions{GenerationMatch: &gen})
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrPrecondition) {
		return nil
	}
	return err
}

func (a *App) cleanupStaging() error {
	for _, t := range a.targets {
		if cleaner, ok := t.Store.(store.StagingCleaner); ok {
			if _, err := cleaner.CleanupStaging(); err != nil {
				return fmt.Errorf("%s: %w", t.Name(), err)
			}
		}
	}
	return nil
}
