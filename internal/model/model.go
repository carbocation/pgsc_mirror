// Package model contains canonical release and object data structures.
package model

import (
	"time"

	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

const (
	LatestKey      = "LATEST.json"
	LeaseKey       = "leases/reconcile.json"
	MaintenanceKey = "operations/maintenance.json"
	StatusReady    = "available"
	StatusGone     = "withdrawn"
)

type Entry struct {
	ReleaseID            string                  `json:"release_id"`
	PGSID                string                  `json:"pgs_id"`
	GenomeBuild          string                  `json:"genome_build"`
	SourceURL            string                  `json:"source_url"`
	SourceMD5            string                  `json:"source_md5"`
	SizeBytes            int64                   `json:"size_bytes"`
	BlobKey              string                  `json:"blob_key"`
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
	ScoreListSHA256        string    `json:"score_list_sha256,omitempty"`
	MetadataSHA256         string    `json:"metadata_sha256,omitempty"`
	PublishedAt            time.Time `json:"published_at"`
	EntryCount             int       `json:"entry_count"`
	GenomeBuild            string    `json:"genome_build"`
	HeaderInspectorVersion int       `json:"header_inspector_version,omitempty"`
}

type Lease struct {
	Owner      string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func BlobKey(md5sum string) string {
	prefix := "00"
	if len(md5sum) >= 2 {
		prefix = md5sum[:2]
	}
	return "blobs/md5/" + prefix + "/" + md5sum + ".txt.gz"
}

func ManifestKey(releaseID string) string {
	return "releases/" + releaseID + "/manifest.jsonl.gz"
}

func MetadataKey(releaseID string) string {
	return "releases/" + releaseID + "/metadata/pgs_all_metadata_scores.csv"
}

func ScoreListKey(releaseID string) string {
	return "releases/" + releaseID + "/metadata/pgs_scores_list.txt"
}
