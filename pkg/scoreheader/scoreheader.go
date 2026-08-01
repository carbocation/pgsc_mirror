// Package scoreheader inspects PGS Catalog scoring-file headers without
// changing or fully decompressing the upstream file.
package scoreheader

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	// InspectorVersion changes when classification or fingerprint semantics
	// change. Mirrors can use it to decide whether stored observations need to
	// be refreshed from their immutable blobs.
	InspectorVersion = 2

	StatusRecognized   = "recognized"
	StatusUnrecognized = "unrecognized"
	StatusUnreadable   = "unreadable"

	TypeHarmonizedV2          = "pgs-catalog-harmonized-v2"
	TypeHarmonizedVersioned   = "pgs-catalog-harmonized-versioned"
	TypeHarmonizedUnversioned = "pgs-catalog-harmonized-unversioned"
	TypeFormattedV2           = "pgs-catalog-formatted-v2"
	TypeFormatted             = "pgs-catalog-formatted"
	TypeUnknown               = "unknown"

	maxHeaderBytes = 2 << 20
	maxHeaderLines = 10_000
)

// Inspection is derived metadata about the decompressed header. Values from
// metadata fields are deliberately excluded: the fingerprint describes the
// schema, not score-specific content.
type Inspection struct {
	InspectorVersion int      `json:"inspector_version"`
	Status           string   `json:"status"`
	Type             string   `json:"type"`
	FormatVersion    string   `json:"format_version,omitempty"`
	Delimiter        string   `json:"delimiter,omitempty"`
	SchemaSHA256     string   `json:"schema_sha256,omitempty"`
	Columns          []string `json:"columns,omitempty"`
	MetadataKeys     []string `json:"metadata_keys,omitempty"`
	Sections         []string `json:"sections,omitempty"`
	CommentLines     int      `json:"comment_lines"`
	Warnings         []string `json:"warnings,omitempty"`
	Error            string   `json:"error,omitempty"`
}

// Current reports whether an observation was produced with the current
// inspector semantics.
func (i *Inspection) Current() bool {
	return i != nil && i.InspectorVersion == InspectorVersion
}

// InspectGzip reads only far enough into a gzip stream to find the scoring
// table's column header. Upstream format anomalies are represented in the
// returned value instead of being returned as operational errors.
func InspectGzip(r io.Reader) Inspection {
	result := Inspection{InspectorVersion: InspectorVersion, Status: StatusUnreadable, Type: TypeUnknown}
	gz, err := gzip.NewReader(r)
	if err != nil {
		result.Error = "open gzip: " + err.Error()
		return result
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 64*1024), maxHeaderBytes)
	consumed := 0
	lines := 0
	for scanner.Scan() {
		lines++
		consumed += len(scanner.Bytes()) + 1
		if lines > maxHeaderLines || consumed > maxHeaderBytes {
			result.Error = fmt.Sprintf("header exceeds inspection limit (%d bytes or %d lines)", maxHeaderBytes, maxHeaderLines)
			return result
		}
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if lines == 1 {
			line = strings.TrimPrefix(line, "\ufeff")
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			result.CommentLines++
			hashCount := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
			body := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			if key, value, ok := strings.Cut(body, "="); ok {
				key = strings.TrimSpace(key)
				if key != "" {
					result.MetadataKeys = append(result.MetadataKeys, key)
					if strings.EqualFold(key, "format_version") {
						result.FormatVersion = strings.TrimSpace(value)
					}
				}
			} else if hashCount >= 2 && body != "" {
				result.Sections = append(result.Sections, body)
			}
			continue
		}

		result.Delimiter, result.Columns = splitColumns(line)
		if len(result.Columns) == 0 {
			result.Error = "column header is empty"
			return result
		}
		result.Type, result.Warnings = classify(result.FormatVersion, result.Delimiter, result.Columns)
		result.SchemaSHA256 = fingerprint(result)
		if result.Type == TypeUnknown {
			result.Status = StatusUnrecognized
		} else {
			result.Status = StatusRecognized
		}
		return result
	}
	if err := scanner.Err(); err != nil {
		result.Error = "read decompressed header: " + err.Error()
	} else {
		result.Error = "scoring file has no column header"
	}
	return result
}

func splitColumns(line string) (string, []string) {
	var delimiter string
	var fields []string
	switch {
	case strings.Contains(line, "\t"):
		delimiter, fields = "tab", strings.Split(line, "\t")
	case strings.Contains(line, ","):
		delimiter, fields = "comma", strings.Split(line, ",")
	default:
		delimiter, fields = "whitespace", strings.Fields(line)
	}
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	return delimiter, fields
}

func classify(formatVersion, delimiter string, columns []string) (string, []string) {
	seen := make(map[string]bool, len(columns))
	var warnings []string
	for _, column := range columns {
		seen[column] = true
		if column == "" {
			warnings = append(warnings, "column header contains an empty name")
		}
	}
	if delimiter != "tab" {
		warnings = append(warnings, "column header is not tab-delimited")
	}
	effectAllele := seen["effect_allele"]
	effectWeight := seen["effect_weight"]
	dosageWeightCount := 0
	for _, column := range []string{"dosage_0_weight", "dosage_1_weight", "dosage_2_weight"} {
		if seen[column] {
			dosageWeightCount++
		}
	}
	dosageWeights := dosageWeightCount == 3
	core := effectAllele && (effectWeight || dosageWeights)
	harmonized := core && seen["hm_chr"] && seen["hm_pos"]
	if !effectAllele {
		warnings = append(warnings, "required effect_allele column was not found")
	}
	if !effectWeight && !dosageWeights {
		warnings = append(warnings, "neither effect_weight nor a complete dosage-specific weight set was found")
	}
	if dosageWeightCount > 0 && !dosageWeights {
		warnings = append(warnings, "dosage-specific weights require dosage_0_weight, dosage_1_weight, and dosage_2_weight")
	}
	if core && !harmonized {
		warnings = append(warnings, "harmonized hm_chr/hm_pos columns were not both found")
	}
	version2 := formatVersion == "2" || strings.HasPrefix(formatVersion, "2.")
	if formatVersion == "" {
		warnings = append(warnings, "format_version metadata field was not found")
	} else if !version2 {
		warnings = append(warnings, "format_version is not version 2")
	}
	if harmonized && !seen["hm_source"] {
		warnings = append(warnings, "harmonized hm_source column was not found")
	}
	switch {
	case harmonized && version2:
		return TypeHarmonizedV2, warnings
	case harmonized && formatVersion != "":
		return TypeHarmonizedVersioned, warnings
	case harmonized:
		return TypeHarmonizedUnversioned, warnings
	case core && version2:
		return TypeFormattedV2, warnings
	case core:
		return TypeFormatted, warnings
	default:
		return TypeUnknown, warnings
	}
}

func fingerprint(i Inspection) string {
	seed := struct {
		FormatVersion string   `json:"format_version"`
		Delimiter     string   `json:"delimiter"`
		MetadataKeys  []string `json:"metadata_keys"`
		Sections      []string `json:"sections"`
		Columns       []string `json:"columns"`
	}{i.FormatVersion, i.Delimiter, i.MetadataKeys, i.Sections, i.Columns}
	b, _ := json.Marshal(seed)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
