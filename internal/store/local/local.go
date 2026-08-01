// Package local implements atomic filesystem object storage.
package local

import (
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
	"time"

	"github.com/pgsc-mirror/pgsc-mirror/internal/store"
)

type Store struct{ root string }

func New(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &Store{root: abs}, nil
}

func (s *Store) Name() string { return "local:" + s.root }
func (s *Store) Close() error { return nil }

func (s *Store) path(key string) (string, error) {
	key = filepath.ToSlash(strings.TrimPrefix(key, "/"))
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	p := filepath.Join(s.root, clean)
	rel, err := filepath.Rel(s.root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("object key escapes root: %q", key)
	}
	return p, nil
}

func info(key string, fi os.FileInfo) store.ObjectInfo {
	return store.ObjectInfo{Key: key, Size: fi.Size(), Generation: fi.ModTime().UnixNano(), LastModified: fi.ModTime()}
}

func (s *Store) Open(_ context.Context, key string) (io.ReadCloser, store.ObjectInfo, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, store.ObjectInfo{}, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, store.ObjectInfo{}, store.ErrNotFound
	}
	if err != nil {
		return nil, store.ObjectInfo{}, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, store.ObjectInfo{}, err
	}
	return f, info(key, fi), nil
}

func (s *Store) Stat(_ context.Context, key string) (store.ObjectInfo, error) {
	p, err := s.path(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	fi, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return store.ObjectInfo{}, store.ErrNotFound
	}
	if err != nil {
		return store.ObjectInfo{}, err
	}
	return info(key, fi), nil
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader, opts store.PutOptions) (store.ObjectInfo, error) {
	p, err := s.path(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return store.ObjectInfo{}, err
	}
	var unlock func()
	var matchedGeneration int64
	if opts.GenerationMatch != nil {
		unlock, err = acquireObjectLock(ctx, p+".pgsc-lock")
		if err != nil {
			return store.ObjectInfo{}, err
		}
		defer unlock()
		cur, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) || (err == nil && cur.ModTime().UnixNano() != *opts.GenerationMatch) {
			return store.ObjectInfo{}, store.ErrPrecondition
		}
		if err != nil {
			return store.ObjectInfo{}, err
		}
		matchedGeneration = cur.ModTime().UnixNano()
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".pgsc-put-*.part")
	if err != nil {
		return store.ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	remove := true
	defer func() {
		tmp.Close()
		if remove {
			_ = os.Remove(tmpName)
		}
	}()
	h := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), r); err != nil {
		return store.ObjectInfo{}, err
	}
	if err := tmp.Sync(); err != nil {
		return store.ObjectInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return store.ObjectInfo{}, err
	}
	if opts.GenerationMatch != nil {
		// A temp file can receive the same filesystem timestamp as the object it
		// replaces. Force the next generation forward by at least one second so
		// compare-and-swap remains reliable even on coarse timestamp filesystems.
		next := time.Now().UnixNano()
		if minimum := matchedGeneration + int64(time.Second); next < minimum {
			next = minimum
		}
		atime := time.Now()
		if err := os.Chtimes(tmpName, atime, time.Unix(0, next)); err != nil {
			return store.ObjectInfo{}, err
		}
	}
	if opts.DoesNotExist {
		if err := os.Link(tmpName, p); err != nil {
			if errors.Is(err, os.ErrExist) {
				return store.ObjectInfo{}, store.ErrPrecondition
			}
			return store.ObjectInfo{}, err
		}
		_ = os.Remove(tmpName)
		remove = false
	} else {
		if opts.GenerationMatch != nil {
			cur, err := os.Stat(p)
			if err != nil || cur.ModTime().UnixNano() != *opts.GenerationMatch {
				return store.ObjectInfo{}, store.ErrPrecondition
			}
		}
		if err := os.Rename(tmpName, p); err != nil {
			return store.ObjectInfo{}, err
		}
		remove = false
	}
	fi, err := os.Stat(p)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	got := info(key, fi)
	got.MD5 = h.Sum(nil)
	got.ETag = hex.EncodeToString(got.MD5)
	return got, nil
}

func (s *Store) Delete(ctx context.Context, key string, opts store.DeleteOptions) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if opts.GenerationMatch != nil {
		unlock, err := acquireObjectLock(ctx, p+".pgsc-lock")
		if err != nil {
			return err
		}
		defer unlock()
		fi, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) || (err == nil && fi.ModTime().UnixNano() != *opts.GenerationMatch) {
			return store.ErrPrecondition
		}
		if err != nil {
			return err
		}
	}
	if err := os.Remove(p); errors.Is(err, os.ErrNotExist) {
		return store.ErrNotFound
	} else {
		return err
	}
}

func acquireObjectLock(ctx context.Context, name string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
			_ = f.Close()
			return func() { _ = os.Remove(name) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if fi, statErr := os.Stat(name); statErr == nil && time.Since(fi.ModTime()) > time.Hour {
			_ = os.Remove(name)
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Store) List(_ context.Context, prefix string) ([]store.ObjectInfo, error) {
	base, err := s.path(prefix)
	if err != nil {
		return nil, err
	}
	var out []store.ObjectInfo
	err = filepath.Walk(base, func(path string, fi os.FileInfo, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && path == base {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(fi.Name(), ".pgsc-put-") || strings.HasSuffix(fi.Name(), ".part") || strings.HasSuffix(fi.Name(), ".pgsc-lock") {
			return nil
		}
		out = append(out, info(key, fi))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
