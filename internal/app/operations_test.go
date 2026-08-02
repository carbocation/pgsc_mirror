package app

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
)

func TestEvenlySpacedSampleDoesNotOnlyCheckManifestPrefix(t *testing.T) {
	entries := []model.Entry{
		{PGSID: "PGS000001"},
		{PGSID: "PGS000002"},
		{PGSID: "PGS000003"},
		{PGSID: "PGS000004"},
		{PGSID: "PGS000005"},
		{PGSID: "PGS000006"},
	}
	got := evenlySpacedSample(entries, 3)
	want := []string{"PGS000001", "PGS000003", "PGS000005"}
	for i := range want {
		if got[i].PGSID != want[i] {
			t.Fatalf("sample[%d]=%s, want %s", i, got[i].PGSID, want[i])
		}
	}
	if got := evenlySpacedSample(entries, 0); len(got) != len(entries) {
		t.Fatalf("zero sample returned %d entries, want full set of %d", len(got), len(entries))
	}
}

func TestCopyMissingObjectReplacesSameSizeStaleScore(t *testing.T) {
	ctx := context.Background()
	source, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	destination, err := localstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := model.ScoreKey("PGS000001", "GRCh38")
	want := []byte("current")
	stale := []byte("outdate")
	if _, err := source.Put(ctx, key, bytes.NewReader(want), store.PutOptions{DoesNotExist: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Put(ctx, key, bytes.NewReader(stale), store.PutOptions{DoesNotExist: true}); err != nil {
		t.Fatal(err)
	}
	opts := store.PutOptions{DoesNotExist: true, ContentType: "application/gzip"}
	if err := copyMissingObject(ctx, source, destination, key, int64(len(want)), md5hex(want), opts); err != nil {
		t.Fatal(err)
	}
	r, _, err := destination.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("destination was not replaced: got=%q err=%v", got, err)
	}
}
