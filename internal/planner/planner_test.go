package planner

import (
	"testing"

	"github.com/carbocation/pgsc_mirror/internal/model"
)

func TestPlanTransitions(t *testing.T) {
	old := []model.Entry{
		{PGSID: "PGS000001", SourceMD5: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: model.StatusReady, License: "a"},
		{PGSID: "PGS000002", SourceMD5: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Status: model.StatusGone},
		{PGSID: "PGS000003", SourceMD5: "cccccccccccccccccccccccccccccccc", Status: model.StatusReady},
	}
	desired := []model.Entry{
		{PGSID: "PGS000001", SourceMD5: "dddddddddddddddddddddddddddddddd", Status: model.StatusReady, License: "a"},
		{PGSID: "PGS000002", SourceMD5: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Status: model.StatusReady},
		{PGSID: "PGS000004", SourceMD5: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Status: model.StatusReady},
	}
	changes := Plan(old, desired)
	want := map[string]Kind{"PGS000001": Revise, "PGS000002": Restore, "PGS000003": Withdraw, "PGS000004": Add}
	for _, c := range changes {
		if want[c.PGSID] != c.Kind {
			t.Errorf("%s: got %s, want %s", c.PGSID, c.Kind, want[c.PGSID])
		}
	}
}

func TestPlanTreatsReleaseDateChangeAsMetadata(t *testing.T) {
	old := []model.Entry{{PGSID: "PGS000001", SourceMD5: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: model.StatusReady, ReleaseDate: "2019-10-14"}}
	desired := []model.Entry{{PGSID: "PGS000001", SourceMD5: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: model.StatusReady, ReleaseDate: "2020-01-02"}}
	changes := Plan(old, desired)
	if len(changes) != 1 || changes[0].Kind != Metadata {
		t.Fatalf("release date change was not metadata: %+v", changes)
	}
}
