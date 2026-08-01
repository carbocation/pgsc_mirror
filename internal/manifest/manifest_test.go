package manifest

import (
	"bytes"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

func TestEncodeDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	entries := []model.Entry{
		{ReleaseID: "r", PGSID: "PGS000002", GenomeBuild: "GRCh38", SourceMD5: "22222222222222222222222222222222", FirstSeenAt: now, LastSeenAt: now, Status: model.StatusReady},
		{ReleaseID: "r", PGSID: "PGS000001", GenomeBuild: "GRCh38", SourceMD5: "11111111111111111111111111111111", FirstSeenAt: now, LastSeenAt: now, Status: model.StatusReady},
	}
	a, sumA, err := Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	b, sumB, err := Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) || sumA != sumB {
		t.Fatal("manifest encoding is not deterministic")
	}
	decoded, err := Decode(bytes.NewReader(a))
	if err != nil {
		t.Fatal(err)
	}
	if decoded[0].PGSID != "PGS000001" || decoded[1].PGSID != "PGS000002" {
		t.Fatalf("manifest is not sorted: %v", decoded)
	}
	idA, _ := ReleaseID(now, entries)
	idB, _ := ReleaseID(now, append([]model.Entry(nil), entries...))
	if idA != idB {
		t.Fatalf("release IDs differ: %s != %s", idA, idB)
	}
	idC, _ := ReleaseID(now, entries, []byte("metadata-a"))
	idD, _ := ReleaseID(now, entries, []byte("metadata-b"))
	if idC == idD {
		t.Fatal("snapshot content did not affect release ID")
	}
	withHeader := append([]model.Entry(nil), entries...)
	withHeader[0].Header = &scoreheader.Inspection{InspectorVersion: scoreheader.InspectorVersion, Status: scoreheader.StatusRecognized, Type: scoreheader.TypeHarmonizedV2, SchemaSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	idE, _ := ReleaseID(now, withHeader)
	if idE == idA {
		t.Fatal("header observation did not affect release ID")
	}
	encoded, _, err := Encode(withHeader)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip[1].Header == nil || roundTrip[1].Header.Type != scoreheader.TypeHarmonizedV2 {
		t.Fatalf("header observation was not preserved: %+v", roundTrip)
	}
}
