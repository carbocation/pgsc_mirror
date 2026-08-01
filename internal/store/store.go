// Package store defines provider-neutral immutable object storage operations.
package store

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNotFound     = errors.New("object not found")
	ErrPrecondition = errors.New("object precondition failed")
)

type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	MD5          []byte
	Generation   int64
	LastModified time.Time
}

type PutOptions struct {
	DoesNotExist    bool
	GenerationMatch *int64
	ContentType     string
	ContentEncoding string
	Metadata        map[string]string
}

type DeleteOptions struct {
	GenerationMatch *int64
}

type Store interface {
	Name() string
	Open(context.Context, string) (io.ReadCloser, ObjectInfo, error)
	Stat(context.Context, string) (ObjectInfo, error)
	Put(context.Context, string, io.Reader, PutOptions) (ObjectInfo, error)
	Delete(context.Context, string, DeleteOptions) error
	List(context.Context, string) ([]ObjectInfo, error)
	Close() error
}
