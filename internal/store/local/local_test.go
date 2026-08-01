package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/carbocation/pgsc_mirror/internal/store"
)

func TestConditionalWrites(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := s.Put(ctx, "a/object", bytes.NewBufferString("one"), store.PutOptions{DoesNotExist: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "a/object", bytes.NewBufferString("two"), store.PutOptions{DoesNotExist: true}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("got %v, want precondition", err)
	}
	bad := first.Generation + 1
	if _, err := s.Put(ctx, "a/object", bytes.NewBufferString("two"), store.PutOptions{GenerationMatch: &bad}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("got %v, want precondition", err)
	}
	gen := first.Generation
	if _, err := s.Put(ctx, "a/object", bytes.NewBufferString("two"), store.PutOptions{GenerationMatch: &gen}); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Open(ctx, "a/object")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "two" {
		t.Fatalf("got %q", b)
	}
	if _, err := s.Stat(ctx, "../escape"); err == nil {
		t.Fatal("path traversal was accepted")
	}
}

func TestCompetingGenerationUpdates(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := s.Put(ctx, "LATEST.json", bytes.NewBufferString("old"), store.PutOptions{DoesNotExist: true})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, body := range []string{"new-a", "new-b"} {
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			<-start
			gen := first.Generation
			_, err := s.Put(ctx, "LATEST.json", bytes.NewBufferString(body), store.PutOptions{GenerationMatch: &gen})
			results <- err
		}(body)
	}
	close(start)
	wg.Wait()
	close(results)
	success, precondition := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, store.ErrPrecondition) {
			precondition++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || precondition != 1 {
		t.Fatalf("success=%d precondition=%d", success, precondition)
	}
}

func TestCleanupStagingRemovesOnlyAtomicWritePartials(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.root, "blobs", "md5")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, ".pgsc-put-123.part")
	keep := filepath.Join(dir, "content.part")
	for _, name := range []string{stale, keep} {
		if err := os.WriteFile(name, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := s.CleanupStaging()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want 1", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}
