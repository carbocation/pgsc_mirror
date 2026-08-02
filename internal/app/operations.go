package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/state"
	"github.com/carbocation/pgsc_mirror/internal/store"
)

// Verify performs an operator-requested integrity audit. A zero sample checks
// every scoring object; a positive sample deliberately limits the audit.
func (a *App) Verify(ctx context.Context, sample int) (VerifyReport, error) {
	var report VerifyReport
	var failed bool
	for _, t := range a.targets {
		result := VerifyTarget{Target: t.Name()}
		p, _, err := a.readPointer(ctx, t.Store)
		if err != nil {
			result.Failures = append(result.Failures, err.Error())
			report.Targets = append(report.Targets, result)
			failed = true
			continue
		}
		result.ReleaseID = p.ReleaseID
		entries, _, err := a.readManifest(ctx, t.Store, p)
		if err != nil {
			result.Failures = append(result.Failures, err.Error())
			report.Targets = append(report.Targets, result)
			failed = true
			continue
		}
		if p.CatalogMetadataVersion != model.CatalogMetadataVersion {
			result.Failures = append(result.Failures, fmt.Sprintf("catalog metadata version is %d, want %d", p.CatalogMetadataVersion, model.CatalogMetadataVersion))
			failed = true
		}
		if err := verifySnapshot(ctx, t.Store, p.ManifestTSVKey, p.ManifestTSVSHA256); err != nil {
			result.Failures = append(result.Failures, "release manifest TSV: "+err.Error())
			failed = true
		}
		if err := verifySnapshot(ctx, t.Store, model.LatestManifestTSVKey, p.ManifestTSVSHA256); err != nil {
			result.Failures = append(result.Failures, "latest manifest TSV: "+err.Error())
			failed = true
		}
		if err := verifySnapshot(ctx, t.Store, model.ScoreListKey(p.ReleaseID), p.ScoreListSHA256); err != nil {
			result.Failures = append(result.Failures, "score list: "+err.Error())
			failed = true
		}
		if err := verifySnapshot(ctx, t.Store, model.MetadataKey(p.ReleaseID), p.MetadataSHA256); err != nil {
			result.Failures = append(result.Failures, "metadata: "+err.Error())
			failed = true
		}
		var active []model.Entry
		for _, e := range entries {
			if e.Status == model.StatusReady {
				active = append(active, e)
			}
		}
		result.Available = len(active)
		if sample > 0 && sample < len(active) {
			active = evenlySpacedSample(active, sample)
			result.Sampled = true
		}
		for _, e := range active {
			result.Checked++
			if err := verifyObject(ctx, t.Store, e); err != nil {
				result.Failures = append(result.Failures, e.PGSID+": "+err.Error())
				failed = true
			}
		}
		report.Targets = append(report.Targets, result)
	}
	if failed {
		return report, errors.New("verification failed")
	}
	return report, nil
}

func evenlySpacedSample(entries []model.Entry, count int) []model.Entry {
	if count <= 0 || count >= len(entries) {
		return entries
	}
	sample := make([]model.Entry, count)
	for i := range sample {
		sample[i] = entries[i*len(entries)/count]
	}
	return sample
}

func verifySnapshot(ctx context.Context, st store.Store, key, expected string) error {
	if expected == "" {
		return nil
	}
	r, _, err := st.Open(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != expected {
		return fmt.Errorf("SHA-256 is %s, want %s", got, expected)
	}
	return nil
}

func verifyObject(ctx context.Context, st store.Store, e model.Entry) error {
	r, info, err := st.Open(ctx, e.ScoreKey)
	if err != nil {
		return err
	}
	defer r.Close()
	h := md5.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if n != e.SizeBytes {
		return fmt.Errorf("size is %d, manifest says %d", n, e.SizeBytes)
	}
	if info.Size != e.SizeBytes {
		return fmt.Errorf("stored size is %d, manifest says %d", info.Size, e.SizeBytes)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != e.SourceMD5 {
		return fmt.Errorf("MD5 is %s, want %s", got, e.SourceMD5)
	}
	return nil
}

func (a *App) Pull(ctx context.Context, dryRun bool) (RunReport, error) {
	source, ok := a.target("gcs")
	if !ok {
		return RunReport{}, errors.New("pull requires the GCS target")
	}
	dest, ok := a.target("local")
	if !ok {
		return RunReport{}, errors.New("pull requires the local target")
	}
	p, _, err := a.readPointer(ctx, source.Store)
	if err != nil {
		return RunReport{}, err
	}
	entries, manifestBytes, err := a.readManifest(ctx, source.Store, p)
	if err != nil {
		return RunReport{}, err
	}
	if p.CatalogMetadataVersion != model.CatalogMetadataVersion {
		return RunReport{}, errors.New("source release does not satisfy the required catalog phenotype metadata contract")
	}
	manifestTSV, err := readStoredSnapshot(ctx, source.Store, p.ManifestTSVKey, p.ManifestTSVSHA256)
	if err != nil {
		return RunReport{}, fmt.Errorf("source release manifest TSV: %w", err)
	}
	if dryRun {
		return RunReport{Command: "pull", ReleaseID: p.ReleaseID, Changed: true, Message: fmt.Sprintf("dry run; would make %d manifest entries available locally", len(entries))}, nil
	}
	for _, e := range entries {
		if e.Status != model.StatusReady {
			continue
		}
		if err := verifyObject(ctx, dest.Store, e); err == nil {
			continue
		}
		if info, statErr := dest.Stat(ctx, e.ScoreKey); statErr == nil {
			gen := info.Generation
			if delErr := dest.Delete(ctx, e.ScoreKey, store.DeleteOptions{GenerationMatch: &gen}); delErr != nil {
				return RunReport{}, fmt.Errorf("remove corrupt local %s: %w", e.ScoreKey, delErr)
			}
		} else if !errors.Is(statErr, store.ErrNotFound) {
			return RunReport{}, statErr
		}
		r, _, err := source.Open(ctx, e.ScoreKey)
		if err != nil {
			return RunReport{}, err
		}
		info, err := dest.Put(ctx, e.ScoreKey, r, store.PutOptions{DoesNotExist: true, ContentType: "application/gzip"})
		r.Close()
		if err != nil {
			return RunReport{}, err
		}
		if got := hex.EncodeToString(info.MD5); got != e.SourceMD5 {
			gen := info.Generation
			_ = dest.Delete(ctx, e.ScoreKey, store.DeleteOptions{GenerationMatch: &gen})
			return RunReport{}, fmt.Errorf("source object %s has MD5 %s, want %s", e.ScoreKey, got, e.SourceMD5)
		}
	}
	metaReader, _, err := source.Open(ctx, model.MetadataKey(p.ReleaseID))
	if err != nil {
		return RunReport{}, err
	}
	metadata, err := io.ReadAll(metaReader)
	metaReader.Close()
	if err != nil {
		return RunReport{}, err
	}
	if err := putImmutable(ctx, dest.Store, model.MetadataKey(p.ReleaseID), metadata, store.PutOptions{DoesNotExist: true, ContentType: "text/csv"}); err != nil {
		return RunReport{}, err
	}
	scoreReader, _, err := source.Open(ctx, model.ScoreListKey(p.ReleaseID))
	if err != nil {
		return RunReport{}, err
	}
	scoreList, err := io.ReadAll(scoreReader)
	scoreReader.Close()
	if err != nil {
		return RunReport{}, err
	}
	if err := putImmutable(ctx, dest.Store, model.ScoreListKey(p.ReleaseID), scoreList, store.PutOptions{DoesNotExist: true, ContentType: "text/plain"}); err != nil {
		return RunReport{}, err
	}
	if err := putImmutable(ctx, dest.Store, p.ManifestKey, manifestBytes, store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"format": "jsonl"}}); err != nil {
		return RunReport{}, err
	}
	if err := putImmutable(ctx, dest.Store, p.ManifestTSVKey, manifestTSV, store.PutOptions{DoesNotExist: true, ContentType: "text/tab-separated-values; charset=utf-8", Metadata: map[string]string{"format": "tsv", "release-id": p.ReleaseID, "sha256": p.ManifestTSVSHA256}}); err != nil {
		return RunReport{}, err
	}
	pointerBytes, _ := manifest.PointerJSON(p)
	_, oldInfo, readErr := a.readPointer(ctx, dest.Store)
	opts := store.PutOptions{ContentType: "application/json"}
	if errors.Is(readErr, store.ErrNotFound) {
		opts.DoesNotExist = true
	} else if readErr != nil {
		return RunReport{}, readErr
	} else {
		gen := oldInfo.Generation
		opts.GenerationMatch = &gen
	}
	if _, err := dest.Put(ctx, model.LatestKey, bytes.NewReader(pointerBytes), opts); err != nil {
		return RunReport{}, err
	}
	if _, err := putReplaceable(ctx, dest.Store, model.LatestManifestTSVKey, manifestTSV, store.PutOptions{ContentType: "text/tab-separated-values; charset=utf-8", Metadata: map[string]string{"format": "tsv", "release-id": p.ReleaseID, "sha256": p.ManifestTSVSHA256}}); err != nil {
		return RunReport{}, err
	}
	if a.State != nil {
		if err := a.State.RecordRelease(ctx, p, entries); err != nil {
			return RunReport{}, err
		}
	}
	return RunReport{Command: "pull", ReleaseID: p.ReleaseID, Changed: true, Message: "local target now pins the published GCS release"}, nil
}

func (a *App) Status(ctx context.Context) (StatusReport, error) {
	var report StatusReport
	if a.State != nil {
		s, err := a.State.Summary(ctx)
		if err != nil {
			return report, err
		}
		report.State = s
	}
	for _, t := range a.targets {
		ts := TargetStatus{Target: t.Name()}
		p, _, err := a.readPointer(ctx, t.Store)
		if errors.Is(err, store.ErrNotFound) {
			ts.Error = "no completed release"
		} else if err != nil {
			ts.Error = err.Error()
		} else {
			ts.ReleaseID = p.ReleaseID
			ts.PublishedAt = &p.PublishedAt
			ts.Entries = p.EntryCount
			entries, _, manifestErr := a.readManifest(ctx, t.Store, p)
			if manifestErr != nil {
				ts.Error = "read release manifest: " + manifestErr.Error()
			} else {
				ts.HeaderTypes = make(map[string]int)
				schemas := make(map[string]*HeaderSchema)
				for i := range entries {
					if entries[i].Status != model.StatusReady {
						continue
					}
					h := entries[i].Header
					if !h.Current() {
						ts.UninspectedHeaders++
						continue
					}
					ts.HeaderTypes[h.Type]++
					key := h.Type + "\x00" + h.SchemaSHA256
					schema := schemas[key]
					if schema == nil {
						schema = &HeaderSchema{Type: h.Type, SchemaSHA256: h.SchemaSHA256}
						schemas[key] = schema
					}
					schema.Count++
					if h.Status != "recognized" || h.Error != "" || len(h.Warnings) > 0 {
						ts.HeaderAnomalies++
					}
				}
				for _, schema := range schemas {
					ts.HeaderSchemas = append(ts.HeaderSchemas, *schema)
				}
				sort.Slice(ts.HeaderSchemas, func(i, j int) bool {
					if ts.HeaderSchemas[i].Type != ts.HeaderSchemas[j].Type {
						return ts.HeaderSchemas[i].Type < ts.HeaderSchemas[j].Type
					}
					return ts.HeaderSchemas[i].SchemaSHA256 < ts.HeaderSchemas[j].SchemaSHA256
				})
			}
		}
		report.Targets = append(report.Targets, ts)
	}
	return report, nil
}

func (a *App) RebuildState(ctx context.Context, dryRun bool) (RunReport, error) {
	if len(a.targets) == 0 {
		return RunReport{}, errors.New("no canonical target")
	}
	t := a.targets[0]
	latest, _, err := a.readPointer(ctx, t.Store)
	if err != nil {
		return RunReport{}, err
	}
	_, _, err = a.readManifest(ctx, t.Store, latest)
	if err != nil {
		return RunReport{}, err
	}
	if latest.CatalogMetadataVersion != model.CatalogMetadataVersion {
		return RunReport{}, errors.New("current release does not satisfy the required catalog phenotype metadata contract")
	}
	objects, err := t.List(ctx, "releases")
	if err != nil {
		return RunReport{}, err
	}
	var releases []state.RebuildRelease
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, "/manifest.jsonl.gz") {
			continue
		}
		r, _, err := t.Open(ctx, obj.Key)
		if err != nil {
			return RunReport{}, err
		}
		b, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			return RunReport{}, err
		}
		entries, err := manifest.Decode(bytes.NewReader(b))
		if err != nil {
			return RunReport{}, fmt.Errorf("%s: %w", obj.Key, err)
		}
		parts := strings.Split(obj.Key, "/")
		if len(parts) < 3 {
			return RunReport{}, fmt.Errorf("invalid manifest key %s", obj.Key)
		}
		id := parts[1]
		published, _ := time.Parse("20060102T150405Z", strings.Split(id, "-")[0])
		if id == latest.ReleaseID {
			published = latest.PublishedAt
		}
		sum := sha256.Sum256(b)
		scoreSHA, err := objectSHA256(ctx, t.Store, model.ScoreListKey(id))
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return RunReport{}, err
		}
		metadataSHA, err := objectSHA256(ctx, t.Store, model.MetadataKey(id))
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return RunReport{}, err
		}
		p := model.Pointer{ReleaseID: id, ManifestKey: obj.Key, ManifestSHA256: hex.EncodeToString(sum[:]), ScoreListSHA256: scoreSHA, MetadataSHA256: metadataSHA, PublishedAt: published, EntryCount: len(entries), GenomeBuild: a.Config.GenomeBuild, HeaderInspectorVersion: observedHeaderInspectorVersion(entries), CatalogMetadataVersion: observedCatalogMetadataVersion(entries), ScoreLayoutVersion: model.ScoreLayoutVersion}
		manifestTSVKey := model.ManifestTSVKey(id)
		manifestTSVSHA, err := objectSHA256(ctx, t.Store, manifestTSVKey)
		if errors.Is(err, store.ErrNotFound) {
			if id == latest.ReleaseID {
				return RunReport{}, errors.New("current release manifest TSV is missing")
			}
			continue
		}
		if err != nil {
			return RunReport{}, err
		}
		p.ManifestTSVKey = manifestTSVKey
		p.ManifestTSVSHA256 = manifestTSVSHA
		if err := requireCurrentScoreLayout(p, entries); err != nil {
			continue
		}
		if id == latest.ReleaseID && (p.ManifestTSVKey != latest.ManifestTSVKey || p.ManifestTSVSHA256 != latest.ManifestTSVSHA256) {
			return RunReport{}, fmt.Errorf("%s manifest TSV SHA-256 is %s, want %s", id, manifestTSVSHA, latest.ManifestTSVSHA256)
		}
		releases = append(releases, state.RebuildRelease{Pointer: p, Entries: entries})
	}
	sort.Slice(releases, func(i, j int) bool {
		if releases[i].Pointer.ReleaseID == latest.ReleaseID {
			return false
		}
		if releases[j].Pointer.ReleaseID == latest.ReleaseID {
			return true
		}
		return releases[i].Pointer.ReleaseID < releases[j].Pointer.ReleaseID
	})
	if dryRun {
		return RunReport{Command: "rebuild-state", Message: fmt.Sprintf("dry run; found %d immutable releases", len(releases))}, nil
	}
	if a.State == nil {
		return RunReport{}, errors.New("writable state is not open")
	}
	if err := a.State.Rebuild(ctx, releases); err != nil {
		return RunReport{}, err
	}
	return RunReport{Command: "rebuild-state", Changed: true, Message: fmt.Sprintf("rebuilt SQLite from %d immutable releases", len(releases))}, nil
}

func observedHeaderInspectorVersion(entries []model.Entry) int {
	version := 0
	found := false
	for i := range entries {
		if entries[i].Status != model.StatusReady {
			continue
		}
		if entries[i].Header == nil {
			return 0
		}
		if !found || entries[i].Header.InspectorVersion < version {
			version = entries[i].Header.InspectorVersion
			found = true
		}
	}
	return version
}

func observedCatalogMetadataVersion(entries []model.Entry) int {
	found := false
	phenotypesPresent := true
	releaseDatesPresent := true
	for i := range entries {
		if entries[i].Status != model.StatusReady {
			continue
		}
		found = true
		if entries[i].PGSName == "" && entries[i].TraitReported == "" && entries[i].TraitMapped == "" && entries[i].TraitEFO == "" {
			phenotypesPresent = false
		}
		if entries[i].ReleaseDate == "" {
			releaseDatesPresent = false
		}
	}
	if !found || !phenotypesPresent {
		return 0
	}
	if releaseDatesPresent {
		return model.CatalogMetadataVersion
	}
	return 1
}

func objectSHA256(ctx context.Context, st store.Store, key string) (string, error) {
	r, _, err := st.Open(ctx, key)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (a *App) GC(ctx context.Context, apply bool) (GCReport, error) {
	report := GCReport{DryRun: !apply}
	cutoff := a.now().Add(-a.Config.Retention.MissingGrace.Duration)
	for _, t := range a.targets {
		latest, _, err := a.readPointer(ctx, t.Store)
		if err != nil {
			return report, err
		}
		objects, err := t.List(ctx, "releases")
		if err != nil {
			return report, err
		}
		type rel struct {
			id       string
			manifest store.ObjectInfo
			all      []store.ObjectInfo
			entries  []model.Entry
		}
		byID := map[string]*rel{}
		for _, obj := range objects {
			parts := strings.Split(obj.Key, "/")
			if len(parts) < 3 {
				continue
			}
			r := byID[parts[1]]
			if r == nil {
				r = &rel{id: parts[1]}
				byID[r.id] = r
			}
			r.all = append(r.all, obj)
			if strings.HasSuffix(obj.Key, "/manifest.jsonl.gz") {
				r.manifest = obj
			}
		}
		var rels []*rel
		for _, r := range byID {
			if r.manifest.Key != "" {
				rels = append(rels, r)
			}
		}
		sort.Slice(rels, func(i, j int) bool { return rels[i].id > rels[j].id })
		keep := map[string]bool{latest.ReleaseID: true}
		for i, r := range rels {
			if i < a.Config.Retention.KeepReleases {
				keep[r.id] = true
			}
		}
		for _, r := range rels {
			if keep[r.id] || r.manifest.LastModified.After(cutoff) {
				continue
			}
			for _, obj := range r.all {
				report.Items = append(report.Items, GCItem{Target: t.Name(), Key: obj.Key, Reason: "release retention expired", Size: obj.Size})
			}
		}
		refs := map[string]bool{}
		for _, r := range rels {
			if !keep[r.id] && r.manifest.LastModified.Before(cutoff) {
				continue
			}
			rd, _, err := t.Open(ctx, r.manifest.Key)
			if err != nil {
				return report, err
			}
			entries, err := manifest.Decode(rd)
			rd.Close()
			if err != nil {
				return report, err
			}
			for _, e := range entries {
				refs[e.ScoreKey] = true
			}
		}
		scores, err := t.List(ctx, "scores")
		if err != nil {
			return report, err
		}
		for _, obj := range scores {
			if !refs[obj.Key] && obj.LastModified.Before(cutoff) {
				report.Items = append(report.Items, GCItem{Target: t.Name(), Key: obj.Key, Reason: "unreferenced scoring object past grace period", Size: obj.Size})
			}
		}
		if apply {
			for _, item := range report.Items {
				if item.Target != t.Name() {
					continue
				}
				if err := t.Delete(ctx, item.Key, store.DeleteOptions{}); err != nil && !errors.Is(err, store.ErrNotFound) {
					return report, err
				}
			}
		}
	}
	return report, nil
}
