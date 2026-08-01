package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/pgsc-mirror/pgsc-mirror/internal/store"
)

func TestIntegrationConditionalRoundTrip(t *testing.T) {
	bucket := os.Getenv("PGSC_MIRROR_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("PGSC_MIRROR_GCS_TEST_BUCKET is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("pgsc-mirror-integration/%d", time.Now().UnixNano())
	s, err := New(ctx, bucket, prefix, os.Getenv("PGSC_MIRROR_GCS_BILLING_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	key := "objects/test"
	info, err := s.Put(ctx, key, bytes.NewBufferString("first"), store.PutOptions{DoesNotExist: true, ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gen := info.Generation
		_ = s.Delete(context.Background(), key, store.DeleteOptions{GenerationMatch: &gen})
	})
	if _, err := s.Put(ctx, key, bytes.NewBufferString("collision"), store.PutOptions{DoesNotExist: true}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("got %v, want precondition", err)
	}
	r, gotInfo, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "first" || gotInfo.Generation != info.Generation {
		t.Fatalf("bad roundtrip content=%q info=%+v", b, gotInfo)
	}
	gen := info.Generation
	if _, err := s.Put(ctx, key, bytes.NewBufferString("second"), store.PutOptions{GenerationMatch: &gen}); err != nil {
		t.Fatal(err)
	}
}
