package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	partDir := filepath.Join(filepath.Dir(a.Config.State.Path), "partials")
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
	e.SourceURL = result.SourceURL
	e.UpstreamETag = result.ETag
	e.UpstreamLastModified = result.LastModified
	for _, dst := range missing {
		f, err := os.Open(result.Path)
		if err != nil {
			return err
		}
		info, err := dst.Put(ctx, e.BlobKey, f, store.PutOptions{DoesNotExist: true, ContentType: "application/gzip", Metadata: map[string]string{"source-md5": e.SourceMD5, "pgs-id": e.PGSID}})
		f.Close()
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
	if a.State != nil {
		_ = a.State.RecordTransfer(ctx, e.PGSID, e.SourceMD5, "", result.Size, result.Attempts, "stored", nil)
	}
	return nil
}

func catalogScorePath(e *model.Entry) string {
	// Source URLs may point at a fallback host, but the path is provider-neutral.
	return "scores/" + e.PGSID + "/ScoringFiles/Harmonized/" + e.PGSID + "_hmPOS_" + e.GenomeBuild + ".txt.gz"
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
