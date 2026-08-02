package app

import (
	"testing"

	"github.com/carbocation/pgsc_mirror/internal/model"
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
