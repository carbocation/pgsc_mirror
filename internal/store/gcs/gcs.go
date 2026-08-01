// Package gcs implements object storage backed by Google Cloud Storage.
package gcs

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pgsc-mirror/pgsc-mirror/internal/store"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type Store struct {
	client *storage.Client
	bucket *storage.BucketHandle
	name   string
	prefix string
}

func New(ctx context.Context, bucket, prefix, billingProject string) (*Store, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	b := client.Bucket(bucket)
	if billingProject != "" {
		b = b.UserProject(billingProject)
	}
	return &Store{client: client, bucket: b, name: "gcs://" + bucket + "/" + strings.Trim(prefix, "/"), prefix: strings.Trim(prefix, "/")}, nil
}

func (s *Store) Name() string { return strings.TrimSuffix(s.name, "/") }
func (s *Store) Close() error { return s.client.Close() }

func (s *Store) object(key string) (*storage.ObjectHandle, string, error) {
	if strings.HasPrefix(key, "/") {
		return nil, "", fmt.Errorf("invalid object key %q", key)
	}
	key = path.Clean(key)
	if key == "." || key == "" || key == ".." || strings.HasPrefix(key, "../") {
		return nil, "", fmt.Errorf("invalid object key %q", key)
	}
	full := key
	if s.prefix != "" {
		full = s.prefix + "/" + key
	}
	return s.bucket.Object(full), full, nil
}

func objectInfo(key string, a *storage.ObjectAttrs) store.ObjectInfo {
	return store.ObjectInfo{
		Key: key, Size: a.Size, ETag: a.Etag, MD5: a.MD5,
		Generation: a.Generation, LastModified: a.Updated,
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return store.ErrNotFound
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 404:
			return store.ErrNotFound
		case 409, 412:
			return store.ErrPrecondition
		}
	}
	return err
}

func (s *Store) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	obj, _, err := s.object(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	a, err := obj.Attrs(ctx)
	if err != nil {
		return store.ObjectInfo{}, mapError(err)
	}
	return objectInfo(key, a), nil
}

func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, store.ObjectInfo, error) {
	obj, _, err := s.object(key)
	if err != nil {
		return nil, store.ObjectInfo{}, err
	}
	a, err := obj.Attrs(ctx)
	if err != nil {
		return nil, store.ObjectInfo{}, mapError(err)
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, store.ObjectInfo{}, mapError(err)
	}
	return r, objectInfo(key, a), nil
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader, opts store.PutOptions) (store.ObjectInfo, error) {
	obj, _, err := s.object(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	cond := storage.Conditions{}
	if opts.DoesNotExist {
		cond.DoesNotExist = true
	}
	if opts.GenerationMatch != nil {
		cond.GenerationMatch = *opts.GenerationMatch
	}
	if opts.DoesNotExist || opts.GenerationMatch != nil {
		obj = obj.If(cond)
	}

	// GCS can validate the upload only if CRC32C is known before streaming. Spool
	// once to disk so large blobs are never buffered in memory.
	tmp, err := os.CreateTemp("", "pgsc-gcs-put-*.part")
	if err != nil {
		return store.ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	md := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmp, crc, md), r); err != nil {
		tmp.Close()
		return store.ObjectInfo{}, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return store.ObjectInfo{}, err
	}
	w := obj.NewWriter(ctx)
	w.ContentType = opts.ContentType
	w.ContentEncoding = opts.ContentEncoding
	w.Metadata = opts.Metadata
	w.CRC32C = crc.Sum32()
	w.SendCRC32C = true
	if _, err := io.Copy(w, tmp); err != nil {
		_ = w.CloseWithError(err)
		tmp.Close()
		return store.ObjectInfo{}, mapError(err)
	}
	tmp.Close()
	if err := w.Close(); err != nil {
		return store.ObjectInfo{}, mapError(err)
	}
	a := w.Attrs()
	got := objectInfo(key, a)
	got.MD5 = md.Sum(nil)
	return got, nil
}

func (s *Store) Delete(ctx context.Context, key string, opts store.DeleteOptions) error {
	obj, _, err := s.object(key)
	if err != nil {
		return err
	}
	if opts.GenerationMatch != nil {
		obj = obj.If(storage.Conditions{GenerationMatch: *opts.GenerationMatch})
	}
	return mapError(obj.Delete(ctx))
}

func (s *Store) List(ctx context.Context, prefix string) ([]store.ObjectInfo, error) {
	fullPrefix := strings.TrimPrefix(prefix, "/")
	if s.prefix != "" {
		fullPrefix = s.prefix + "/" + fullPrefix
	}
	it := s.bucket.Objects(ctx, &storage.Query{Prefix: fullPrefix})
	var out []store.ObjectInfo
	for {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, mapError(err)
		}
		key := a.Name
		if s.prefix != "" {
			key = strings.TrimPrefix(key, s.prefix+"/")
		}
		out = append(out, objectInfo(key, a))
	}
	return out, nil
}
