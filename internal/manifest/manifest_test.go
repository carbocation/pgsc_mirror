package manifest

import (
	"bytes"
	"compress/gzip"
	"io"
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

func TestDecodeAcceptsLegacyBlobKeyAndEncodeUsesScoreKey(t *testing.T) {
	legacy := []byte("{\"release_id\":\"r\",\"pgs_id\":\"PGS000001\",\"genome_build\":\"GRCh38\",\"source_url\":\"https://example.invalid/PGS000001.txt.gz\",\"source_md5\":\"11111111111111111111111111111111\",\"size_bytes\":1,\"blob_key\":\"blobs/md5/11/11111111111111111111111111111111.txt.gz\",\"first_seen_at\":\"2026-08-01T00:00:00Z\",\"last_seen_at\":\"2026-08-01T00:00:00Z\",\"status\":\"available\"}\n")
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(legacy); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := Decode(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got := entries[0].ScoreKey; got != "blobs/md5/11/11111111111111111111111111111111.txt.gz" {
		t.Fatalf("legacy key decoded as %q", got)
	}
	entries[0].ScoreKey = model.ScoreKey(entries[0].PGSID, entries[0].GenomeBuild)
	encoded, _, err := Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := gzip.NewReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("blob_key")) || !bytes.Contains(body, []byte(`"score_key":"scores/PGS000001_hmPOS_GRCh38.txt.gz"`)) {
		t.Fatalf("unexpected current manifest %s", body)
	}
}
