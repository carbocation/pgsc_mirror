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
	"strings"
	"sync"

	"github.com/pgsc-mirror/pgsc-mirror/internal/manifest"
	"github.com/pgsc-mirror/pgsc-mirror/internal/model"
	"github.com/pgsc-mirror/pgsc-mirror/internal/store"
)

func (a *App) ensureBlobs(ctx context.Context, entries []model.Entry) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workers := a.Config.Transfer.Concurrency
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
		for i := range entries {
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
		return nil
	}
	if source.Store != nil {
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
		return nil
	}
	partDir := filepath.Join(a.Config.State.WorkDir, "partials")
	part := filepath.Join(partDir, e.PGSID+"-"+e.SourceMD5+".part")
	result, err := a.HTTP.Download(ctx, a.Catalog.URLs(catalogScorePath(e)), part, e.SourceMD5)
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
	_ = os.Remove(result.Path)
	_ = os.Remove(result.Path + ".json")
	if a.State != nil {
		_ = a.State.RecordTransfer(ctx, e.PGSID, e.SourceMD5, "", result.Size, result.Attempts, "stored", nil)
	}
	return nil
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
