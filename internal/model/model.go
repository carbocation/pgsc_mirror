// Package model contains canonical release and object data structures.
package model

import (
	"path"
	"time"

	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

const (
	LatestKey            = "LATEST.json"
	LatestManifestTSVKey = "LATEST.manifest.tsv"
	LeaseKey             = "leases/reconcile.json"
	MaintenanceKey       = "operations/maintenance.json"
	StatusReady          = "available"
	StatusGone           = "withdrawn"
	// CatalogMetadataVersion identifies the descriptive PGS name, phenotype,
	// and catalog release-date fields materialized in each manifest entry.
	CatalogMetadataVersion = 2
	// ScoreLayoutVersion identifies the flat, source-named scoring-file layout.
	ScoreLayoutVersion = 1
)

type Entry struct {
	ReleaseID            string                  `json:"release_id"`
	PGSID                string                  `json:"pgs_id"`
	PGSName              string                  `json:"pgs_name,omitempty"`
	TraitReported        string                  `json:"trait_reported,omitempty"`
	TraitMapped          string                  `json:"trait_mapped,omitempty"`
	TraitEFO             string                  `json:"trait_efo,omitempty"`
	ReleaseDate          string                  `json:"release_date"`
	GenomeBuild          string                  `json:"genome_build"`
	SourceURL            string                  `json:"source_url"`
	SourceMD5            string                  `json:"source_md5"`
	SizeBytes            int64                   `json:"size_bytes"`
	ScoreKey             string                  `json:"score_key"`
	UpstreamETag         string                  `json:"upstream_etag,omitempty"`
	UpstreamLastModified string                  `json:"upstream_last_modified,omitempty"`
	FirstSeenAt          time.Time               `json:"first_seen_at"`
	LastSeenAt           time.Time               `json:"last_seen_at"`
	Status               string                  `json:"status"`
	License              string                  `json:"license,omitempty"`
	GCSGeneration        int64                   `json:"gcs_generation,omitempty"`
	Header               *scoreheader.Inspection `json:"header,omitempty"`
}

type Pointer struct {
	ReleaseID              string    `json:"release_id"`
	ManifestKey            string    `json:"manifest_key"`
	ManifestSHA256         string    `json:"manifest_sha256"`
	ManifestTSVKey         string    `json:"manifest_tsv_key,omitempty"`
	ManifestTSVSHA256      string    `json:"manifest_tsv_sha256,omitempty"`
	ScoreListSHA256        string    `json:"score_list_sha256,omitempty"`
	MetadataSHA256         string    `json:"metadata_sha256,omitempty"`
	PublishedAt            time.Time `json:"published_at"`
	EntryCount             int       `json:"entry_count"`
	GenomeBuild            string    `json:"genome_build"`
	HeaderInspectorVersion int       `json:"header_inspector_version,omitempty"`
	CatalogMetadataVersion int       `json:"catalog_metadata_version,omitempty"`
	ScoreLayoutVersion     int       `json:"score_layout_version,omitempty"`
}

type Lease struct {
	Owner      string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// ScoreKey is the simple public scoring-file namespace. The filename is the
// original PGS Catalog basename; the scores/ prefix keeps data separate from
// release and operational objects.
func ScoreKey(pgsID, genomeBuild string) string {
	return path.Join("scores", pgsID+"_hmPOS_"+genomeBuild+".txt.gz")
}

func ManifestKey(releaseID string) string {
	return "releases/" + releaseID + "/manifest.jsonl.gz"
}

func ManifestTSVKey(releaseID string) string {
	return "releases/" + releaseID + "/manifest.tsv"
}

func MetadataKey(releaseID string) string {
	return "releases/" + releaseID + "/metadata/pgs_all_metadata_scores.csv"
}

func ScoreListKey(releaseID string) string {
	return "releases/" + releaseID + "/metadata/pgs_scores_list.txt"
}
