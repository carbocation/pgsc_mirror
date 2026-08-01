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
