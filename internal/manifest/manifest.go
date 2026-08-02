// Package manifest produces deterministic compressed release manifests.
package manifest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

func sorted(entries []model.Entry) []model.Entry {
	out := append([]model.Entry(nil), entries...)
	sort.Slice(out, func(i, j int) bool { return out[i].PGSID < out[j].PGSID })
	return out
}

// ReleaseID combines a UTC timestamp with a digest of the desired logical state.
func ReleaseID(now time.Time, entries []model.Entry, snapshots ...[]byte) (string, error) {
	type seed struct {
		PGSID, PGSName, TraitReported, TraitMapped, TraitEFO         string
		GenomeBuild, SourceURL, SourceMD5, ScoreKey, Status, License string
		SizeBytes                                                    int64
		Header                                                       *scoreheader.Inspection
	}
	ss := make([]seed, 0, len(entries))
	for _, e := range sorted(entries) {
		ss = append(ss, seed{e.PGSID, e.PGSName, e.TraitReported, e.TraitMapped, e.TraitEFO, e.GenomeBuild, e.SourceURL, e.SourceMD5, e.ScoreKey, e.Status, e.License, e.SizeBytes, e.Header})
	}
	b, err := json.Marshal(ss)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write(b)
	for _, snapshot := range snapshots {
		sum := sha256.Sum256(snapshot)
		_, _ = h.Write(sum[:])
	}
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(h.Sum(nil)[:6]), nil
}

func Encode(entries []model.Entry) ([]byte, string, error) {
	var out bytes.Buffer
	gz, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		return nil, "", err
	}
	gz.Header.ModTime = time.Unix(0, 0).UTC()
	gz.Header.OS = 255
	enc := json.NewEncoder(gz)
	enc.SetEscapeHTML(false)
	for _, e := range sorted(entries) {
		if err := enc.Encode(e); err != nil {
			return nil, "", err
		}
	}
	if err := gz.Close(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(out.Bytes())
	return out.Bytes(), hex.EncodeToString(sum[:]), nil
}

var TSVColumns = []string{
	"release_id",
	"pgs_id",
	"pgs_name",
	"trait_reported",
	"trait_mapped",
	"trait_efo",
	"genome_build",
	"status",
	"score_key",
	"gcs_generation",
	"source_md5",
	"size_bytes",
	"source_url",
	"upstream_etag",
	"upstream_last_modified",
	"first_seen_at",
	"last_seen_at",
	"license",
	"header_inspector_version",
	"header_status",
	"header_type",
	"header_format_version",
	"header_delimiter",
	"header_schema_sha256",
	"header_columns_json",
	"header_metadata_keys_json",
	"header_sections_json",
	"header_comment_lines",
	"header_warnings_json",
	"header_error",
}

// EncodeTSV renders the complete manifest into a deterministic, database-ready
// tab-separated table. Nested header slices remain lossless JSON columns.
func EncodeTSV(entries []model.Entry) ([]byte, string, error) {
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	w.Comma = '\t'
	if err := w.Write(TSVColumns); err != nil {
		return nil, "", err
	}
	for _, e := range sorted(entries) {
		record, err := tsvRecord(e)
		if err != nil {
			return nil, "", err
		}
		if err := w.Write(record); err != nil {
			return nil, "", err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(out.Bytes())
	return out.Bytes(), hex.EncodeToString(sum[:]), nil
}

func tsvRecord(e model.Entry) ([]string, error) {
	record := []string{
		e.ReleaseID,
		e.PGSID,
		e.PGSName,
		e.TraitReported,
		e.TraitMapped,
		e.TraitEFO,
		e.GenomeBuild,
		e.Status,
		e.ScoreKey,
		strconv.FormatInt(e.GCSGeneration, 10),
		e.SourceMD5,
		strconv.FormatInt(e.SizeBytes, 10),
		e.SourceURL,
		e.UpstreamETag,
		e.UpstreamLastModified,
		e.FirstSeenAt.UTC().Format(time.RFC3339Nano),
		e.LastSeenAt.UTC().Format(time.RFC3339Nano),
		e.License,
	}
	if e.Header == nil {
		return append(record, "", "", "", "", "", "", "", "", "", "", "", ""), nil
	}
	columns, err := json.Marshal(e.Header.Columns)
	if err != nil {
		return nil, err
	}
	metadataKeys, err := json.Marshal(e.Header.MetadataKeys)
	if err != nil {
		return nil, err
	}
	sections, err := json.Marshal(e.Header.Sections)
	if err != nil {
		return nil, err
	}
	warnings, err := json.Marshal(e.Header.Warnings)
	if err != nil {
		return nil, err
	}
	return append(record,
		strconv.Itoa(e.Header.InspectorVersion),
		e.Header.Status,
		e.Header.Type,
		e.Header.FormatVersion,
		e.Header.Delimiter,
		e.Header.SchemaSHA256,
		string(columns),
		string(metadataKeys),
		string(sections),
		strconv.Itoa(e.Header.CommentLines),
		string(warnings),
		e.Header.Error,
	), nil
}

func Decode(r io.Reader) ([]model.Entry, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open manifest gzip: %w", err)
	}
	defer gz.Close()
	s := bufio.NewScanner(gz)
	s.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var entries []model.Entry
	line := 0
	for s.Scan() {
		line++
		var e model.Entry
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("manifest line %d: %w", line, err)
		}
		entries = append(entries, e)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func PointerJSON(p model.Pointer) ([]byte, error) {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
