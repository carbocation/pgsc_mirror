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

func TestParseMetadata(t *testing.T) {
	csv := "pgs_id,pgs_name,trait_reported,trait_mapped,trait_efo,release_date,license,other\nPGS000001,PRS77_BC,Breast cancer,breast carcinoma,MONDO_0004989,2019-10-14,CC BY 4.0,x\nPGS000002,example,Example trait,example phenotype,EFO_0000001,2020-01-02,custom,y\n"
	got, err := ParseMetadata(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	first := got["PGS000001"]
	if first.PGSName != "PRS77_BC" || first.TraitReported != "Breast cancer" || first.TraitMapped != "breast carcinoma" || first.TraitEFO != "MONDO_0004989" || first.ReleaseDate != "2019-10-14" || first.License != "CC BY 4.0" || got["PGS000002"].License != "custom" {
		t.Fatalf("unexpected metadata: %+v", got)
	}
}

func TestParseMetadataFromPublishedHeader(t *testing.T) {
	csv := "Polygenic Score (PGS) ID,PGS Name,Reported Trait,Mapped Trait(s) (EFO label),Mapped Trait(s) (EFO ID),Release Date,License/Terms of Use\nPGS000001,PRS77_BC,Breast cancer,breast carcinoma,MONDO_0004989,2019-10-14,EBI terms\n"
	got, err := ParseMetadata(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	first := got["PGS000001"]
	if first.PGSName != "PRS77_BC" || first.TraitReported != "Breast cancer" || first.TraitMapped != "breast carcinoma" || first.TraitEFO != "MONDO_0004989" || first.ReleaseDate != "2019-10-14" || first.License != "EBI terms" {
		t.Fatalf("unexpected metadata: %+v", got)
	}
}

func TestParseMetadataRequiresReleaseDateColumn(t *testing.T) {
	_, err := ParseMetadata(strings.NewReader("pgs_id,license\nPGS000001,CC0\n"))
	if err == nil || !strings.Contains(err.Error(), "release date") {
		t.Fatalf("missing release date column was accepted: %v", err)
	}
}

func TestParseMetadataRejectsInvalidReleaseDate(t *testing.T) {
	_, err := ParseMetadata(strings.NewReader("pgs_id,release_date\nPGS000001,October 2019\n"))
	if err == nil || !strings.Contains(err.Error(), "invalid release date") {
		t.Fatalf("invalid release date was accepted: %v", err)
	}
}
