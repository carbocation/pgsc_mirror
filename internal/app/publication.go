package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

func (a *App) ensureBlobs(ctx context.Context, entries []model.Entry) error {
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
				if err := a.ensureBlob(ctx, &entries[i]); err != nil {
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
		for _, i := range a.blobJobOrder(entries) {
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

func (a *App) blobJobOrder(entries []model.Entry) []int {
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

func (a *App) ensureBlob(ctx context.Context, e *model.Entry) error {
	missing := make([]target, 0)
	var source target
	var sourceInfo store.ObjectInfo
	for _, t := range a.targets {
		info, err := t.Stat(ctx, e.BlobKey)
		if err == nil {
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
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		missing = append(missing, t)
	}
	if len(missing) == 0 {
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
		for _, dst := range missing {
			r, _, err := source.Open(ctx, e.BlobKey)
			if err != nil {
				return err
			}
			info, err := dst.Put(ctx, e.BlobKey, r, store.PutOptions{DoesNotExist: true, ContentType: "application/gzip"})
			r.Close()
			if errors.Is(err, store.ErrPrecondition) {
				info, err = dst.Stat(ctx, e.BlobKey)
			}
			if err != nil {
				return err
			}
			if dst.kind == "gcs" {
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
	for _, dst := range missing {
		opts := store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"source-md5": e.SourceMD5, "pgs-id": e.PGSID}}
		info, err := putFile(ctx, dst.Store, e.BlobKey, result.Path, opts)
		if errors.Is(err, store.ErrPrecondition) {
			info, err = dst.Stat(ctx, e.BlobKey)
		}
		if err != nil {
			return err
		}
		if dst.kind == "gcs" {
			e.GCSGeneration = info.Generation
		}
	}
	_ = removePartial(result.Path)
	if a.State != nil {
		_ = a.State.RecordTransfer(ctx, e.PGSID, e.SourceMD5, "", result.Size, result.Attempts, "stored", nil)
	}
	return nil
}

func inspectStoredHeader(ctx context.Context, st store.Store, e *model.Entry) error {
	r, _, err := st.Open(ctx, e.BlobKey)
	if err != nil {
		return fmt.Errorf("open stored blob for header inspection: %w", err)
	}
	inspection := scoreheader.InspectGzip(r)
	if err := r.Close(); err != nil {
		return fmt.Errorf("close stored blob after header inspection: %w", err)
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
		if err == nil && current.ReleaseID == p.ReleaseID {
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

func syncReleaseToTarget(ctx context.Context, source, dest store.Store, p model.Pointer, entries []model.Entry) error {
	for _, e := range entries {
		if e.Status != model.StatusReady {
			continue
		}
		opts := store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"source-md5": e.SourceMD5, "pgs-id": e.PGSID}}
		if err := copyMissingObject(ctx, source, dest, e.BlobKey, e.SizeBytes, e.SourceMD5, opts); err != nil {
			return fmt.Errorf("copy %s: %w", e.BlobKey, err)
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
		if expectedSize <= 0 || info.Size == expectedSize {
			return nil
		}
		gen := info.Generation
		if err := dest.Delete(ctx, key, store.DeleteOptions{GenerationMatch: &gen}); err != nil {
			return fmt.Errorf("replace wrong-size object: %w", err)
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
	}
	if err := errors.Join(putErr, closeErr); err != nil {
		return err
	}
	if expectedSize > 0 && got.Size != expectedSize {
		return fmt.Errorf("copied size is %d, want %d", got.Size, expectedSize)
	}
	if expectedMD5 != "" && len(got.MD5) > 0 && !strings.EqualFold(hex.EncodeToString(got.MD5), expectedMD5) {
		gen := got.Generation
		_ = dest.Delete(ctx, key, store.DeleteOptions{GenerationMatch: &gen})
		return fmt.Errorf("copied MD5 is %s, want %s", hex.EncodeToString(got.MD5), expectedMD5)
	}
	return nil
}

func (a *App) publish(ctx context.Context, p model.Pointer, scoreList, metadata, manifestBytes []byte) error {
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
	}
	return nil
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
