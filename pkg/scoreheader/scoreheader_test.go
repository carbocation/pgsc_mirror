package scoreheader

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
	"time"
)

func gzipped(t *testing.T, body string) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	w.Header.ModTime = time.Unix(0, 0)
	if _, err := w.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestInspectHarmonizedV2(t *testing.T) {
	body := "###PGS CATALOG SCORING FILE\n#format_version=2.0\n#pgs_id=PGS000001\n##HARMONIZATION DETAILS\n#HmPOS_build=GRCh38\nrsID\teffect_allele\teffect_weight\thm_source\thm_rsID\thm_chr\thm_pos\nrs1\tA\t0.2\tENSEMBL\trs1\t1\t42\n"
	got := InspectGzip(bytes.NewReader(gzipped(t, body)))
	if got.Status != StatusRecognized || got.Type != TypeHarmonizedV2 {
		t.Fatalf("unexpected classification: %+v", got)
	}
	if got.FormatVersion != "2.0" || got.Delimiter != "tab" || got.CommentLines != 5 {
		t.Fatalf("unexpected header details: %+v", got)
	}
	if strings.Join(got.Columns, ",") != "rsID,effect_allele,effect_weight,hm_source,hm_rsID,hm_chr,hm_pos" {
		t.Fatalf("columns were not preserved: %#v", got.Columns)
	}
	if strings.Join(got.MetadataKeys, ",") != "format_version,pgs_id,HmPOS_build" {
		t.Fatalf("metadata keys were not preserved: %#v", got.MetadataKeys)
	}
	if strings.Join(got.Sections, "|") != "PGS CATALOG SCORING FILE|HARMONIZATION DETAILS" {
		t.Fatalf("section headings were not preserved: %#v", got.Sections)
	}
	if len(got.SchemaSHA256) != 64 || len(got.Warnings) != 0 {
		t.Fatalf("unexpected fingerprint or warnings: %+v", got)
	}
	again := InspectGzip(bytes.NewReader(gzipped(t, body)))
	if got.SchemaSHA256 != again.SchemaSHA256 {
		t.Fatal("schema fingerprint is not deterministic")
	}
}

func TestFingerprintDistinguishesColumnVariants(t *testing.T) {
	a := InspectGzip(bytes.NewReader(gzipped(t, "#format_version=2.0\neffect_allele\teffect_weight\thm_chr\thm_pos\n")))
	b := InspectGzip(bytes.NewReader(gzipped(t, "#format_version=2.0\neffect_allele\tother_allele\teffect_weight\thm_chr\thm_pos\n")))
	if a.Type != b.Type || a.SchemaSHA256 == b.SchemaSHA256 {
		t.Fatalf("column variants not distinguished: a=%+v b=%+v", a, b)
	}
}

func TestInspectHarmonizedDosageWeightsV2(t *testing.T) {
	body := `###PGS CATALOG SCORING FILE - see https://www.pgscatalog.org/downloads/#dl_ftp_scoring for additional information
#format_version=2.0
##POLYGENIC SCORE (PGS) INFORMATION
#pgs_id=PGS004255
#pgs_name=example
#trait_reported=example
#trait_mapped=example
#trait_efo=EFO_0000000
#genome_build=GRCh38
#variants_number=1
#weight_type=non-additive
##SOURCE INFORMATION
#pgp_id=PGP000000
#citation=example
##HARMONIZATION DETAILS
#HmPOS_build=GRCh38
#HmPOS_date=2026-08-01
#HmPOS_match_chr=True
#HmPOS_match_pos=True
chr_name	chr_position	effect_allele	other_allele	dosage_0_weight	dosage_1_weight	dosage_2_weight	hm_source	hm_rsID	hm_chr	hm_pos	hm_inferOtherAllele
1	42	A	G	0	0.5	1	ENSEMBL	rs1	1	42	False
`
	got := InspectGzip(bytes.NewReader(gzipped(t, body)))
	if got.Status != StatusRecognized || got.Type != TypeHarmonizedV2 || len(got.Warnings) != 0 {
		t.Fatalf("valid dosage-weight header was not recognized: %+v", got)
	}
	if got.SchemaSHA256 != "7dcf9f6fea7c61f54a71072327ab7a9b4f8efbb56da51507788fb6623332a59c" {
		t.Fatalf("live dosage schema fingerprint changed: %s", got.SchemaSHA256)
	}
}

func TestRejectsIncompleteDosageWeightSet(t *testing.T) {
	body := "#format_version=2.0\neffect_allele\tdosage_0_weight\tdosage_1_weight\thm_chr\thm_pos\n"
	got := InspectGzip(bytes.NewReader(gzipped(t, body)))
	if got.Status != StatusUnrecognized || got.Type != TypeUnknown {
		t.Fatalf("incomplete dosage-weight header was recognized: %+v", got)
	}
	warnings := strings.Join(got.Warnings, "\n")
	if !strings.Contains(warnings, "dosage-specific weights require dosage_0_weight, dosage_1_weight, and dosage_2_weight") {
		t.Fatalf("incomplete dosage warning missing: %+v", got.Warnings)
	}
}

func TestClassifiesUnversionedAndFormattedHeaders(t *testing.T) {
	harmonized := InspectGzip(bytes.NewReader(gzipped(t, "effect_allele\teffect_weight\thm_source\thm_chr\thm_pos\n")))
	if harmonized.Type != TypeHarmonizedUnversioned {
		t.Fatalf("got %+v", harmonized)
	}
	formatted := InspectGzip(bytes.NewReader(gzipped(t, "#format_version=2.0\neffect_allele\teffect_weight\n")))
	if formatted.Type != TypeFormattedV2 || len(formatted.Warnings) == 0 {
		t.Fatalf("got %+v", formatted)
	}
}

func TestRecordsUnknownAndUnreadableHeaders(t *testing.T) {
	unknown := InspectGzip(bytes.NewReader(gzipped(t, "foo,bar\n1,2\n")))
	if unknown.Status != StatusUnrecognized || unknown.Type != TypeUnknown || unknown.Delimiter != "comma" {
		t.Fatalf("got %+v", unknown)
	}
	bad := InspectGzip(strings.NewReader("not gzip"))
	if bad.Status != StatusUnreadable || bad.Error == "" || bad.Type != TypeUnknown {
		t.Fatalf("got %+v", bad)
	}
}
