package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

func (a *App) ensureScores(ctx context.Context, entries []model.Entry) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workers := a.Config.Transfer.FileConcurrency
	if workers > len(entries) {
		workers = len(entries)
	}
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if entries[i].Status != model.StatusReady {
					continue
				}
				if err := a.ensureScore(ctx, &entries[i]); err != nil {
					select {
					case errCh <- fmt.Errorf("%s: %w", entries[i].PGSID, err):
						cancel()
					default:
						{
						}
					}
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, i := range a.scoreJobOrder(entries) {
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
		return err
	default:
		return nil
	}
}

func (a *App) scoreJobOrder(entries []model.Entry) []int {
	withPartial := make([]int, 0, len(entries))
	withoutPartial := make([]int, 0, len(entries))
	for i := range entries {
		if _, err := os.Stat(a.partialPath(&entries[i])); err == nil {
			withPartial = append(withPartial, i)
		} else {
			withoutPartial = append(withoutPartial, i)
		}
	}
	return append(withPartial, withoutPartial...)
}

type scoreDestination struct {
	target target
	info   store.ObjectInfo
	exists bool
}

func (a *App) ensureScore(ctx context.Context, e *model.Entry) error {
	destinations := make([]scoreDestination, 0)
	var source target
	var sourceInfo store.ObjectInfo
	for _, t := range a.targets {
		info, err := t.Stat(ctx, e.ScoreKey)
		if err == nil {
			matches, err := scoreObjectMatches(ctx, t.Store, info, *e)
			if err != nil {
				return err
			}
			if matches {
				if source.Store == nil {
					source = t
					sourceInfo = info
				}
				if t.kind == "gcs" {
					e.GCSGeneration = info.Generation
				}
				if e.SizeBytes == 0 {
					e.SizeBytes = info.Size
				}
				continue
			}
			destinations = append(destinations, scoreDestination{target: t, info: info, exists: true})
			continue
		}
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		destinations = append(destinations, scoreDestination{target: t})
	}
	if len(destinations) == 0 {
		if !e.Header.Current() {
			if err := inspectStoredHeader(ctx, source.Store, e); err != nil {
				return err
			}
		}
		_ = removePartial(a.partialPath(e))
		return nil
	}
	if source.Store != nil {
		if !e.Header.Current() {
			if err := inspectStoredHeader(ctx, source.Store, e); err != nil {
				return err
			}
		}
		for _, destination := range destinations {
			r, _, err := source.Open(ctx, e.ScoreKey)
			if err != nil {
				return err
			}
			info, putErr := destination.target.Put(ctx, e.ScoreKey, r, scorePutOptions(*e, destination))
			closeErr := r.Close()
			err = errors.Join(putErr, closeErr)
			if errors.Is(err, store.ErrPrecondition) {
				info, err = destination.target.Stat(ctx, e.ScoreKey)
				if err == nil {
					var matches bool
					matches, err = scoreObjectMatches(ctx, destination.target.Store, info, *e)
					if err == nil && !matches {
						err = errors.New("concurrent scoring-file write has unexpected content")
					}
				}
			}
			if err != nil {
				return err
			}
			if destination.target.kind == "gcs" {
				e.GCSGeneration = info.Generation
			}
			if e.SizeBytes == 0 {
				e.SizeBytes = sourceInfo.Size
			}
		}
		_ = removePartial(a.partialPath(e))
		return nil
	}

	part := a.partialPath(e)
	result, err := a.HTTP.DownloadBounded(ctx, a.Catalog.URLs(catalogScorePath(e)), part, e.SourceMD5, a.Config.Transfer.MaxFileSize.Bytes)
	if err != nil {
		if a.State != nil {
			if fi, statErr := os.Stat(part); statErr == nil {
				_ = a.State.RecordTransfer(context.Background(), e.PGSID, e.SourceMD5, part, fi.Size(), 0, "failed", err)
			} else {
				_ = a.State.RecordTransfer(context.Background(), e.PGSID, e.SourceMD5, part, 0, 0, "failed", err)
			}
		}
		return err
	}
	if a.State != nil {
		_ = a.State.RecordTransfer(ctx, e.PGSID, e.SourceMD5, part, result.Size, result.Attempts, "verified", nil)
	}
	e.SizeBytes = result.Size
	if result.SourceURL != "" {
		e.SourceURL = result.SourceURL
	}
	e.UpstreamETag = result.ETag
	e.UpstreamLastModified = result.LastModified
	if !e.Header.Current() {
		if err := inspectHeaderFile(result.Path, e); err != nil {
			return err
		}
	}
	for _, destination := range destinations {
		info, err := putFile(ctx, destination.target.Store, e.ScoreKey, result.Path, scorePutOptions(*e, destination))
		if errors.Is(err, store.ErrPrecondition) {
			info, err = destination.target.Stat(ctx, e.ScoreKey)
			if err == nil {
				var matches bool
				matches, err = scoreObjectMatches(ctx, destination.target.Store, info, *e)
				if err == nil && !matches {
					err = errors.New("concurrent scoring-file write has unexpected content")
				}
			}
		}
		if err != nil {
			return err
		}
		if destination.target.kind == "gcs" {
			e.GCSGeneration = info.Generation
		}
	}
	_ = removePartial(result.Path)
	if a.State != nil {
		_ = a.State.RecordTransfer(ctx, e.PGSID, e.SourceMD5, "", result.Size, result.Attempts, "stored", nil)
	}
	return nil
}

func scorePutOptions(e model.Entry, destination scoreDestination) store.PutOptions {
	opts := store.PutOptions{ContentType: "application/gzip", Metadata: map[string]string{"source-md5": e.SourceMD5, "pgs-id": e.PGSID}}
	if destination.exists {
		generation := destination.info.Generation
		opts.GenerationMatch = &generation
	} else {
		opts.DoesNotExist = true
	}
	return opts
}

func scoreObjectMatches(ctx context.Context, st store.Store, info store.ObjectInfo, e model.Entry) (bool, error) {
	if e.SizeBytes > 0 && info.Size != e.SizeBytes {
		return false, nil
	}
	if len(info.MD5) > 0 {
		return strings.EqualFold(hex.EncodeToString(info.MD5), e.SourceMD5), nil
	}
	r, _, err := st.Open(ctx, e.ScoreKey)
	if err != nil {
		return false, err
	}
	h := md5.New()
	n, readErr := io.Copy(h, r)
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	if e.SizeBytes > 0 && n != e.SizeBytes {
		return false, nil
	}
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), e.SourceMD5), nil
}

func inspectStoredHeader(ctx context.Context, st store.Store, e *model.Entry) error {
	r, _, err := st.Open(ctx, e.ScoreKey)
	if err != nil {
		return fmt.Errorf("open stored score for header inspection: %w", err)
	}
	inspection := scoreheader.InspectGzip(r)
	if err := r.Close(); err != nil {
		return fmt.Errorf("close stored score after header inspection: %w", err)
	}
	e.Header = &inspection
	return nil
}

func inspectHeaderFile(path string, e *model.Entry) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open verified download for header inspection: %w", err)
	}
	inspection := scoreheader.InspectGzip(f)
	if err := f.Close(); err != nil {
		return fmt.Errorf("close verified download after header inspection: %w", err)
	}
	e.Header = &inspection
	return nil
}

func (a *App) partialPath(e *model.Entry) string {
	return filepath.Join(a.Config.State.WorkDir, "partials", e.PGSID+"-"+e.SourceMD5+".part")
}

func removePartial(part string) error {
	var errs []error
	for _, name := range []string{part, part + ".json", part + ".json.tmp"} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// prunePartials prevents resumable files from accumulating across repeated
// interruptions or checksum revisions. The newest file-concurrency partials
// for the current inventory are kept and scheduled first; everything else is
// safe to refetch if needed.
func (a *App) prunePartials(entries []model.Entry) (int, error) {
	dir := filepath.Join(a.Config.State.WorkDir, "partials")
	files, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	expected := make(map[string]struct{}, len(entries))
	for i := range entries {
		if entries[i].Status == model.StatusReady {
			expected[filepath.Base(a.partialPath(&entries[i]))] = struct{}{}
		}
	}
	type candidate struct {
		name    string
		modTime int64
	}
	var candidates []candidate
	removed := 0
	for _, file := range files {
		name := file.Name()
		full := filepath.Join(dir, name)
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(name, ".part.json.tmp") {
			if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
				return removed, err
			}
			removed++
			continue
		}
		if !strings.HasSuffix(name, ".part") {
			continue
		}
		if _, ok := expected[name]; !ok {
			if err := removePartial(full); err != nil {
				return removed, err
			}
			removed++
			continue
		}
		info, err := file.Info()
		if err != nil {
			return removed, err
		}
		if info.Size() > a.Config.Transfer.MaxFileSize.Bytes {
			if err := removePartial(full); err != nil {
				return removed, err
			}
			removed++
			continue
		}
		candidates = append(candidates, candidate{name: full, modTime: info.ModTime().UnixNano()})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].modTime > candidates[j].modTime })
	keep := a.Config.Transfer.FileConcurrency
	for i := keep; i < len(candidates); i++ {
		if err := removePartial(candidates[i].name); err != nil {
			return removed, err
		}
		removed++
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".part.json") {
			continue
		}
		part := strings.TrimSuffix(filepath.Join(dir, file.Name()), ".json")
		if _, err := os.Stat(part); errors.Is(err, os.ErrNotExist) {
			if err := os.Remove(filepath.Join(dir, file.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}

func putFile(ctx context.Context, st store.Store, key, filename string, opts store.PutOptions) (store.ObjectInfo, error) {
	if fileStore, ok := st.(store.FilePutter); ok {
		return fileStore.PutFile(ctx, key, filename, opts)
	}
	f, err := os.Open(filename)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	defer f.Close()
	return st.Put(ctx, key, f, opts)
}

func catalogScorePath(e *model.Entry) string {
	// Source URLs may point at a fallback host, but the path is provider-neutral.
	return "scores/" + e.PGSID + "/ScoringFiles/Harmonized/" + e.PGSID + "_hmPOS_" + e.GenomeBuild + ".txt.gz"
}

// convergeTargets repairs a publication interrupted after the authoritative
// target advanced but before a later target's pointer did. Immutable objects
// are copied first; the lagging pointer is advanced last with compare-and-swap.
func (a *App) convergeTargets(ctx context.Context, p model.Pointer, entries []model.Entry) (bool, error) {
	if p.ReleaseID == "" || len(a.targets) < 2 {
		return false, nil
	}
	source := a.targets[0]
	repaired := false
	for _, dest := range a.targets[1:] {
		current, currentInfo, err := a.readPointer(ctx, dest.Store)
		if err == nil && current.ReleaseID == p.ReleaseID && current.ManifestTSVKey == p.ManifestTSVKey && current.ManifestTSVSHA256 == p.ManifestTSVSHA256 {
			continue
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return repaired, fmt.Errorf("read pointer from %s: %w", dest.Name(), err)
		}
		if err := syncReleaseToTarget(ctx, source.Store, dest.Store, p, entries); err != nil {
			return repaired, fmt.Errorf("repair %s from %s: %w", dest.Name(), source.Name(), err)
		}
		pointerBytes, err := manifest.PointerJSON(p)
		if err != nil {
			return repaired, err
		}
		opts := store.PutOptions{ContentType: "application/json"}
		if current.ReleaseID == "" {
			opts.DoesNotExist = true
		} else {
			gen := currentInfo.Generation
			opts.GenerationMatch = &gen
		}
		if _, err := dest.Put(ctx, model.LatestKey, bytes.NewReader(pointerBytes), opts); err != nil {
			return repaired, fmt.Errorf("advance repaired pointer in %s: %w", dest.Name(), err)
		}
		repaired = true
	}
	return repaired, nil
}

// repairLaggingTargets performs provider-to-provider catch-up without reading
// the upstream catalog. It is used by lightweight startup after local state was
// restored from the shared maintenance checkpoint.
func (a *App) repairLaggingTargets(ctx context.Context) (repaired bool, runErr error) {
	pointer, entries, err := a.latest(ctx)
	if err != nil || pointer.ReleaseID == "" {
		return false, err
	}
	needsRepair, err := a.manifestTSVPublicationNeedsRepair(ctx, pointer, entries)
	if err != nil {
		return false, err
	}
	if !needsRepair && len(a.targets) > 1 {
		for _, target := range a.targets[1:] {
			current, _, err := a.readPointer(ctx, target.Store)
			if errors.Is(err, store.ErrNotFound) || (err == nil && (current.ReleaseID != pointer.ReleaseID || current.ManifestTSVKey != pointer.ManifestTSVKey || current.ManifestTSVSHA256 != pointer.ManifestTSVSHA256)) {
				needsRepair = true
				break
			}
			if err != nil {
				return false, fmt.Errorf("read pointer from %s: %w", target.Name(), err)
			}
		}
	}
	if !needsRepair {
		return false, nil
	}
	lease, err := a.acquireLease(ctx)
	if err != nil {
		return false, err
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
	// Re-pin after acquiring the lease so a publication completed between the
	// initial check and lease acquisition cannot be repaired backward.
	pointer, entries, err = a.latest(ctx)
	if err != nil {
		return false, err
	}
	repaired, err = a.convergeTargets(ctx, pointer, entries)
	if err != nil {
		return repaired, err
	}
	pointer, manifestTSVRepaired, err := a.ensureManifestTSV(ctx, pointer, entries)
	repaired = repaired || manifestTSVRepaired
	if err != nil {
		return repaired, err
	}
	if a.State != nil && a.Config.Targets.Local {
		if err := a.State.RecordRelease(ctx, pointer, entries); err != nil {
			return repaired, err
		}
	}
	return repaired, nil
}

func (a *App) manifestTSVPublicationNeedsRepair(ctx context.Context, pointer model.Pointer, entries []model.Entry) (bool, error) {
	expected, _, err := attachManifestTSV(pointer, entries)
	if err != nil {
		return false, err
	}
	if pointer.ManifestTSVKey != expected.ManifestTSVKey || pointer.ManifestTSVSHA256 != expected.ManifestTSVSHA256 {
		return true, nil
	}
	for _, target := range a.targets {
		for _, key := range []string{expected.ManifestTSVKey, model.LatestManifestTSVKey} {
			got, err := objectSHA256(ctx, target.Store, key)
			if errors.Is(err, store.ErrNotFound) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			if got != expected.ManifestTSVSHA256 {
				return true, nil
			}
		}
	}
	return false, nil
}

func syncReleaseToTarget(ctx context.Context, source, dest store.Store, p model.Pointer, entries []model.Entry) error {
	for _, e := range entries {
		if e.Status != model.StatusReady {
			continue
		}
		opts := store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"source-md5": e.SourceMD5, "pgs-id": e.PGSID}}
		if err := copyMissingObject(ctx, source, dest, e.ScoreKey, e.SizeBytes, e.SourceMD5, opts); err != nil {
			return fmt.Errorf("copy %s: %w", e.ScoreKey, err)
		}
	}
	objects := []struct {
		key, contentType string
		metadata         map[string]string
	}{
		{key: model.ScoreListKey(p.ReleaseID), contentType: "text/plain"},
		{key: model.MetadataKey(p.ReleaseID), contentType: "text/csv"},
		{key: p.ManifestKey, contentType: "application/gzip", metadata: map[string]string{"format": "jsonl"}},
	}
	if p.ManifestTSVKey != "" {
		objects = append(objects, struct {
			key, contentType string
			metadata         map[string]string
		}{key: p.ManifestTSVKey, contentType: "text/tab-separated-values; charset=utf-8", metadata: map[string]string{"format": "tsv", "release-id": p.ReleaseID}})
	}
	for _, object := range objects {
		opts := store.PutOptions{DoesNotExist: true, ContentType: object.contentType, Metadata: object.metadata}
		if err := copyMissingObject(ctx, source, dest, object.key, 0, "", opts); err != nil {
			return fmt.Errorf("copy %s: %w", object.key, err)
		}
	}
	return nil
}

func copyMissingObject(ctx context.Context, source, dest store.Store, key string, expectedSize int64, expectedMD5 string, opts store.PutOptions) error {
	info, err := dest.Stat(ctx, key)
	if err == nil {
		matches := expectedSize <= 0 || info.Size == expectedSize
		if matches && expectedMD5 != "" {
			matches, err = scoreObjectMatches(ctx, dest, info, model.Entry{ScoreKey: key, SourceMD5: expectedMD5, SizeBytes: expectedSize})
			if err != nil {
				return err
			}
		}
		if matches {
			return nil
		}
		generation := info.Generation
		if expectedMD5 != "" {
			opts.DoesNotExist = false
			opts.GenerationMatch = &generation
		} else {
			if err := dest.Delete(ctx, key, store.DeleteOptions{GenerationMatch: &generation}); err != nil {
				return fmt.Errorf("replace wrong-size object: %w", err)
			}
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	r, sourceInfo, err := source.Open(ctx, key)
	if err != nil {
		return err
	}
	if expectedSize > 0 && sourceInfo.Size != expectedSize {
		r.Close()
		return fmt.Errorf("authoritative size is %d, manifest says %d", sourceInfo.Size, expectedSize)
	}
	got, putErr := dest.Put(ctx, key, r, opts)
	closeErr := r.Close()
	if errors.Is(putErr, store.ErrPrecondition) {
		got, putErr = dest.Stat(ctx, key)
		if putErr == nil && expectedMD5 != "" {
			var matches bool
			matches, putErr = scoreObjectMatches(ctx, dest, got, model.Entry{ScoreKey: key, SourceMD5: expectedMD5, SizeBytes: expectedSize})
			if putErr == nil && !matches {
				putErr = errors.New("concurrent target repair has unexpected scoring-file content")
			}
		}
	}
	if err := errors.Join(putErr, closeErr); err != nil {
		return err
	}
	if expectedSize > 0 && got.Size != expectedSize {
		return fmt.Errorf("copied size is %d, want %d", got.Size, expectedSize)
	}
	if expectedMD5 != "" {
		matches, err := scoreObjectMatches(ctx, dest, got, model.Entry{ScoreKey: key, SourceMD5: expectedMD5, SizeBytes: expectedSize})
		if err != nil {
			return err
		}
		if !matches {
			generation := got.Generation
			_ = dest.Delete(ctx, key, store.DeleteOptions{GenerationMatch: &generation})
			return fmt.Errorf("copied scoring object does not match MD5 %s", expectedMD5)
		}
	}
	return nil
}

func attachManifestTSV(pointer model.Pointer, entries []model.Entry) (model.Pointer, []byte, error) {
	data, sum, err := manifest.EncodeTSV(entries)
	if err != nil {
		return model.Pointer{}, nil, err
	}
	wantKey := model.ManifestTSVKey(pointer.ReleaseID)
	if pointer.ManifestTSVKey != "" && pointer.ManifestTSVKey != wantKey {
		return model.Pointer{}, nil, fmt.Errorf("manifest TSV key is %q, want %q", pointer.ManifestTSVKey, wantKey)
	}
	if pointer.ManifestTSVSHA256 != "" && pointer.ManifestTSVSHA256 != sum {
		return model.Pointer{}, nil, fmt.Errorf("manifest TSV SHA-256 is %s, want %s", pointer.ManifestTSVSHA256, sum)
	}
	pointer.ManifestTSVKey = wantKey
	pointer.ManifestTSVSHA256 = sum
	return pointer, data, nil
}

func (a *App) ensureManifestTSV(ctx context.Context, pointer model.Pointer, entries []model.Entry) (model.Pointer, bool, error) {
	original := pointer
	pointer, data, err := attachManifestTSV(pointer, entries)
	if err != nil {
		return model.Pointer{}, false, err
	}
	changed := original.ManifestTSVKey != pointer.ManifestTSVKey || original.ManifestTSVSHA256 != pointer.ManifestTSVSHA256
	for _, target := range a.targets {
		if err := putImmutable(ctx, target.Store, pointer.ManifestTSVKey, data, store.PutOptions{
			DoesNotExist: true,
			ContentType:  "text/tab-separated-values; charset=utf-8",
			Metadata:     map[string]string{"format": "tsv", "release-id": pointer.ReleaseID, "sha256": pointer.ManifestTSVSHA256},
		}); err != nil {
			return model.Pointer{}, changed, fmt.Errorf("publish release manifest TSV to %s: %w", target.Name(), err)
		}
	}
	if changed {
		pointerBytes, err := manifest.PointerJSON(pointer)
		if err != nil {
			return model.Pointer{}, changed, err
		}
		for _, target := range a.targets {
			current, info, err := a.readPointer(ctx, target.Store)
			if err != nil {
				return model.Pointer{}, changed, err
			}
			if current.ReleaseID != pointer.ReleaseID {
				return model.Pointer{}, changed, fmt.Errorf("cannot attach manifest TSV to %s at release %s; current release is %s", target.Name(), pointer.ReleaseID, current.ReleaseID)
			}
			generation := info.Generation
			if _, err := target.Put(ctx, model.LatestKey, bytes.NewReader(pointerBytes), store.PutOptions{ContentType: "application/json", GenerationMatch: &generation}); err != nil {
				return model.Pointer{}, changed, fmt.Errorf("attach manifest TSV to LATEST in %s: %w", target.Name(), err)
			}
		}
	}
	for _, target := range a.targets {
		updated, err := putReplaceable(ctx, target.Store, model.LatestManifestTSVKey, data, store.PutOptions{
			ContentType: "text/tab-separated-values; charset=utf-8",
			Metadata:    map[string]string{"format": "tsv", "release-id": pointer.ReleaseID, "sha256": pointer.ManifestTSVSHA256},
		})
		changed = changed || updated
		if err != nil {
			return model.Pointer{}, changed, fmt.Errorf("publish latest manifest TSV to %s: %w", target.Name(), err)
		}
	}
	return pointer, changed, nil
}

func (a *App) publish(ctx context.Context, p model.Pointer, scoreList, metadata, manifestBytes, manifestTSV []byte) error {
	for _, t := range a.targets {
		if err := putImmutable(ctx, t.Store, model.ScoreListKey(p.ReleaseID), scoreList, store.PutOptions{DoesNotExist: true, ContentType: "text/plain"}); err != nil {
			return fmt.Errorf("publish score list to %s: %w", t.Name(), err)
		}
		if err := putImmutable(ctx, t.Store, model.MetadataKey(p.ReleaseID), metadata, store.PutOptions{DoesNotExist: true, ContentType: "text/csv"}); err != nil {
			return fmt.Errorf("publish metadata to %s: %w", t.Name(), err)
		}
		if err := putImmutable(ctx, t.Store, p.ManifestKey, manifestBytes, store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"format": "jsonl"}}); err != nil {
			return fmt.Errorf("publish manifest to %s: %w", t.Name(), err)
		}
		if err := putImmutable(ctx, t.Store, p.ManifestTSVKey, manifestTSV, store.PutOptions{DoesNotExist: true, ContentType: "text/tab-separated-values; charset=utf-8", Metadata: map[string]string{"format": "tsv", "release-id": p.ReleaseID, "sha256": p.ManifestTSVSHA256}}); err != nil {
			return fmt.Errorf("publish manifest TSV to %s: %w", t.Name(), err)
		}
	}
	pointerBytes, err := manifest.PointerJSON(p)
	if err != nil {
		return err
	}
	for _, t := range a.targets {
		_, info, readErr := a.readPointer(ctx, t.Store)
		opts := store.PutOptions{ContentType: "application/json"}
		if errors.Is(readErr, store.ErrNotFound) {
			opts.DoesNotExist = true
		} else if readErr != nil {
			return readErr
		} else {
			gen := info.Generation
			opts.GenerationMatch = &gen
		}
		if _, err := t.Put(ctx, model.LatestKey, bytes.NewReader(pointerBytes), opts); err != nil {
			return fmt.Errorf("advance LATEST in %s: %w", t.Name(), err)
		}
		if _, err := putReplaceable(ctx, t.Store, model.LatestManifestTSVKey, manifestTSV, store.PutOptions{ContentType: "text/tab-separated-values; charset=utf-8", Metadata: map[string]string{"format": "tsv", "release-id": p.ReleaseID, "sha256": p.ManifestTSVSHA256}}); err != nil {
			return fmt.Errorf("advance latest manifest TSV in %s: %w", t.Name(), err)
		}
	}
	return nil
}

func putReplaceable(ctx context.Context, st store.Store, key string, data []byte, opts store.PutOptions) (bool, error) {
	for attempt := 0; attempt < 4; attempt++ {
		r, info, err := st.Open(ctx, key)
		if err == nil {
			existing, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return false, err
			}
			if bytes.Equal(existing, data) {
				return false, nil
			}
			generation := info.Generation
			opts.GenerationMatch = &generation
			opts.DoesNotExist = false
		} else if errors.Is(err, store.ErrNotFound) {
			opts.DoesNotExist = true
			opts.GenerationMatch = nil
		} else {
			return false, err
		}
		if _, err := st.Put(ctx, key, bytes.NewReader(data), opts); errors.Is(err, store.ErrPrecondition) {
			continue
		} else if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, errors.New("replaceable object changed repeatedly")
}

func putImmutable(ctx context.Context, st store.Store, key string, data []byte, opts store.PutOptions) error {
	_, err := st.Put(ctx, key, bytes.NewReader(data), opts)
	if !errors.Is(err, store.ErrPrecondition) {
		return err
	}
	r, _, err := st.Open(ctx, key)
	if err != nil {
		return err
	}
	existing, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("immutable object %s already exists with different content", key)
	}
	return nil
}
