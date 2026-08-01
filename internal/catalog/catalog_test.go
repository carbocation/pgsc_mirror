package catalog

import (
	"strings"
	"testing"
)

func TestParseScoreList(t *testing.T) {
	got, err := ParseScoreList(strings.NewReader("# comment\r\nPGS000002\r\nPGS000001\nPGS000002\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"PGS000001", "PGS000002"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, err := ParseScoreList(strings.NewReader("not-an-id\n")); err == nil {
		t.Fatal("invalid ID was accepted")
	}
}

func TestParseMD5(t *testing.T) {
	sum, err := ParseMD5(strings.NewReader("D41D8CD98F00B204E9800998ECF8427E  file.gz\n"))
	if err != nil {
		t.Fatal(err)
	}
	if sum != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Fatalf("unexpected checksum %q", sum)
	}
	if _, err := ParseMD5(strings.NewReader("sha256-nope")); err == nil {
		t.Fatal("invalid checksum was accepted")
	}
}

func TestParseLicenses(t *testing.T) {
	csv := "pgs_id,license,other\nPGS000001,CC BY 4.0,x\nPGS000002,custom,y\n"
	got, err := ParseLicenses(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	if got["PGS000001"] != "CC BY 4.0" || got["PGS000002"] != "custom" {
		t.Fatalf("unexpected licenses: %v", got)
	}
}

func TestParseLicensesFromPublishedMetadataHeader(t *testing.T) {
	csv := "Polygenic Score (PGS) ID,PGS Name,License/Terms of Use\nPGS000001,example,EBI terms\n"
	got, err := ParseLicenses(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	if got["PGS000001"] != "EBI terms" {
		t.Fatalf("unexpected licenses: %v", got)
	}
}
