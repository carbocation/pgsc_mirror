package manifest

import (
	"bytes"
	"encoding/csv"
	"reflect"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

func TestEncodeDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	entries := []model.Entry{
		{ReleaseID: "r", PGSID: "PGS000002", GenomeBuild: "GRCh38", SourceMD5: "22222222222222222222222222222222", ScoreKey: model.ScoreKey("PGS000002", "GRCh38"), FirstSeenAt: now, LastSeenAt: now, Status: model.StatusReady},
		{ReleaseID: "r", PGSID: "PGS000001", GenomeBuild: "GRCh38", SourceMD5: "11111111111111111111111111111111", ScoreKey: model.ScoreKey("PGS000001", "GRCh38"), FirstSeenAt: now, LastSeenAt: now, Status: model.StatusReady},
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

func TestEncodeTSVIsDeterministicSortedAndComplete(t *testing.T) {
	now := time.Date(2026, 7, 31, 12, 0, 0, 123, time.UTC)
	entries := []model.Entry{
		{ReleaseID: "release", PGSID: "PGS000002", GenomeBuild: "GRCh38", Status: model.StatusGone, ScoreKey: model.ScoreKey("PGS000002", "GRCh38"), SourceMD5: "22222222222222222222222222222222", FirstSeenAt: now, LastSeenAt: now},
		{ReleaseID: "release", PGSID: "PGS000001", GenomeBuild: "GRCh38", Status: model.StatusReady, ScoreKey: model.ScoreKey("PGS000001", "GRCh38"), SourceMD5: "11111111111111111111111111111111", FirstSeenAt: now, LastSeenAt: now, Header: &scoreheader.Inspection{InspectorVersion: scoreheader.InspectorVersion, Status: scoreheader.StatusRecognized, Type: scoreheader.TypeHarmonizedV2, Columns: []string{"effect_allele", "hm_chr"}}},
	}
	first, firstSHA, err := EncodeTSV(entries)
	if err != nil {
		t.Fatal(err)
	}
	second, secondSHA, err := EncodeTSV(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || firstSHA != secondSHA {
		t.Fatal("TSV encoding is not deterministic")
	}
	r := csv.NewReader(bytes.NewReader(first))
	r.Comma = '\t'
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || !reflect.DeepEqual(records[0], TSVColumns) {
		t.Fatalf("unexpected TSV shape: rows=%d header=%v", len(records), records[0])
	}
	if records[1][0] != "release" || records[1][1] != "PGS000001" || records[2][1] != "PGS000002" {
		t.Fatalf("TSV rows are not release-pinned and sorted: %v", records)
	}
	if records[1][20] != `["effect_allele","hm_chr"]` {
		t.Fatalf("header columns were not preserved as JSON: %q", records[1][20])
	}
}
