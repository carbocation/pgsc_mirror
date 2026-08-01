package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/catalog"
	"github.com/carbocation/pgsc_mirror/internal/store"
	"github.com/carbocation/pgsc_mirror/internal/transfer"
)

var ErrProbeFailed = errors.New("probe failed")

type ProbeOptions struct {
	PGSIDs   []string
	MaxBytes int64
	GCSSmoke bool
}

type ProbeReport struct {
	RunID             string          `json:"run_id"`
	StartedAt         time.Time       `json:"started_at"`
	FinishedAt        time.Time       `json:"finished_at"`
	DurationMS        int64           `json:"duration_ms"`
	Status            string          `json:"status"`
	Error             string          `json:"error,omitempty"`
	ReportPath        string          `json:"report_path,omitempty"`
	GenomeBuild       string          `json:"genome_build"`
	MaximumBytes      int64           `json:"maximum_bytes"`
	ScoreListURL      string          `json:"score_list_url,omitempty"`
	ScoreListETag     string          `json:"score_list_etag,omitempty"`
	ScoreListModified string          `json:"score_list_last_modified,omitempty"`
	ScoreListMS       int64           `json:"score_list_duration_ms"`
	Scores            []ProbeScore    `json:"scores"`
	GCS               *GCSSmokeReport `json:"gcs_smoke_test,omitempty"`
}

type ProbeScore struct {
	PGSID              string `json:"pgs_id"`
	Current            bool   `json:"current"`
	Status             string `json:"status"`
	Error              string `json:"error,omitempty"`
	URL                string `json:"url,omitempty"`
	MD5SidecarURL      string `json:"md5_sidecar_url,omitempty"`
	ExpectedMD5        string `json:"expected_md5,omitempty"`
	ObservedMD5        string `json:"observed_md5,omitempty"`
	SizeKnown          bool   `json:"size_known"`
	AdvertisedSize     int64  `json:"advertised_size_bytes,omitempty"`
	DownloadedSize     int64  `json:"downloaded_size_bytes,omitempty"`
	ETag               string `json:"etag,omitempty"`
	LastModified       string `json:"last_modified,omitempty"`
	HeadSupported      bool   `json:"head_supported"`
	HeadStatusCode     int    `json:"head_status_code,omitempty"`
	DownloadAttempts   int    `json:"download_attempts,omitempty"`
	ChecksumDurationMS int64  `json:"checksum_duration_ms"`
	HeadDurationMS     int64  `json:"head_duration_ms"`
	DownloadDurationMS int64  `json:"download_duration_ms"`
	DurationMS         int64  `json:"duration_ms"`
}

type GCSSmokeReport struct {
	Status                  string `json:"status"`
	Error                   string `json:"error,omitempty"`
	Target                  string `json:"target,omitempty"`
	Key                     string `json:"key,omitempty"`
	PayloadSHA256           string `json:"payload_sha256,omitempty"`
	CreateGeneration        int64  `json:"create_generation,omitempty"`
	ReadGeneration          int64  `json:"read_generation,omitempty"`
	ReplacementGeneration   int64  `json:"replacement_generation,omitempty"`
	CreateSucceeded         bool   `json:"create_succeeded"`
	ReadChecksumVerified    bool   `json:"read_checksum_verified"`
	ReplaceSucceeded        bool   `json:"replace_succeeded"`
	StaleGenerationRejected bool   `json:"stale_generation_rejected"`
	DeleteSucceeded         bool   `json:"delete_succeeded"`
	AbsenceConfirmed        bool   `json:"absence_confirmed"`
	CleanupAttempted        bool   `json:"cleanup_attempted"`
	CleanupSucceeded        bool   `json:"cleanup_succeeded"`
	DeletionNote            string `json:"deletion_note,omitempty"`
	DurationMS              int64  `json:"duration_ms"`
}

func (a *App) Probe(ctx context.Context, opts ProbeOptions) (ProbeReport, error) {
	started := a.now().UTC()
	report := ProbeReport{
		RunID:        probeRunID(started),
		StartedAt:    started,
		Status:       "failed",
		GenomeBuild:  a.Config.GenomeBuild,
		MaximumBytes: opts.MaxBytes,
		Scores:       make([]ProbeScore, 0, len(opts.PGSIDs)),
	}
	finish := func() {
		report.FinishedAt = a.now().UTC()
		report.DurationMS = elapsedMillis(report.StartedAt, report.FinishedAt)
	}

	if err := validateProbeOptions(opts); err != nil {
		report.Error = err.Error()
		finish()
		return report, err
	}

	listStarted := a.now()
	doc, currentIDs, err := a.Catalog.ScoreList(ctx, "", "")
	report.ScoreListMS = elapsedMillis(listStarted, a.now())
	report.ScoreListURL = doc.URL
	report.ScoreListETag = doc.ETag
	report.ScoreListModified = doc.LastModified
	if err != nil {
		report.Error = fmt.Sprintf("fetch score list: %v", err)
		if opts.GCSSmoke {
			report.GCS = &GCSSmokeReport{Status: "skipped", Error: "score-list check failed"}
		}
		finish()
		return report, fmt.Errorf("fetch score list: %w", err)
	}
	current := make(map[string]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		current[id] = struct{}{}
	}

	tempDir, err := os.MkdirTemp("", "pgsc-mirror-probe-")
	if err != nil {
		report.Error = fmt.Sprintf("create probe workspace: %v", err)
		finish()
		return report, fmt.Errorf("create probe workspace: %w", err)
	}
	defer os.RemoveAll(tempDir)

	failed := 0
	for i, id := range opts.PGSIDs {
		scoreStarted := a.now()
		score := ProbeScore{PGSID: id, Status: "failed"}
		if _, ok := current[id]; !ok {
			score.Status = "missing"
			score.Error = "requested ID is absent from the current score list"
			score.DurationMS = elapsedMillis(scoreStarted, a.now())
			report.Scores = append(report.Scores, score)
			failed++
			continue
		}
		score.Current = true

		checksumStarted := a.now()
		expected, scoreURL, checksumErr := a.Catalog.Checksum(ctx, id, a.Config.GenomeBuild)
		score.ChecksumDurationMS = elapsedMillis(checksumStarted, a.now())
		score.URL = scoreURL
		if scoreURL != "" {
			score.MD5SidecarURL = scoreURL + ".md5"
		}
		score.ExpectedMD5 = expected

		headStarted := a.now()
		object, headErr := a.Catalog.InspectScore(ctx, id, a.Config.GenomeBuild)
		score.HeadDurationMS = elapsedMillis(headStarted, a.now())
		if object.URL != "" {
			score.URL = object.URL
		}
		score.SizeKnown = object.SizeKnown
		score.AdvertisedSize = object.Size
		score.ETag = object.ETag
		score.LastModified = object.LastModified
		score.HeadSupported = object.HeadSupported
		score.HeadStatusCode = object.StatusCode

		var problems []error
		if checksumErr != nil {
			problems = append(problems, fmt.Errorf("fetch MD5 sidecar: %w", checksumErr))
		}
		if headErr != nil {
			problems = append(problems, fmt.Errorf("inspect scoring file: %w", headErr))
		}
		if object.SizeKnown && object.Size > opts.MaxBytes {
			score.Status = "oversized"
			problems = append(problems, fmt.Errorf("advertised size %d exceeds limit %d", object.Size, opts.MaxBytes))
		}

		if len(problems) == 0 {
			partPath := filepath.Join(tempDir, fmt.Sprintf("%02d-%s.txt.gz", i, id))
			downloadStarted := a.now()
			got, downloadErr := a.HTTP.DownloadBounded(ctx, a.Catalog.URLs(catalog.ScorePath(id, a.Config.GenomeBuild)), partPath, expected, opts.MaxBytes)
			score.DownloadDurationMS = elapsedMillis(downloadStarted, a.now())
			_ = os.Remove(partPath)
			_ = os.Remove(partPath + ".json")
			if downloadErr != nil {
				if errors.Is(downloadErr, transfer.ErrSizeLimit) {
					score.Status = "oversized"
				} else if errors.Is(downloadErr, transfer.ErrChecksumMismatch) {
					score.Status = "checksum_mismatch"
				}
				problems = append(problems, fmt.Errorf("download scoring file: %w", downloadErr))
			} else {
				score.Status = "verified"
				score.URL = got.SourceURL
				score.ObservedMD5 = got.MD5
				score.DownloadedSize = got.Size
				score.DownloadAttempts = got.Attempts
				if got.ETag != "" {
					score.ETag = got.ETag
				}
				if got.LastModified != "" {
					score.LastModified = got.LastModified
				}
			}
		}

		if len(problems) > 0 {
			failed++
			score.Error = errors.Join(problems...).Error()
		}
		score.DurationMS = elapsedMillis(scoreStarted, a.now())
		report.Scores = append(report.Scores, score)
	}

	if failed > 0 {
		report.Error = fmt.Sprintf("%d of %d score checks failed", failed, len(opts.PGSIDs))
		if opts.GCSSmoke {
			report.GCS = &GCSSmokeReport{Status: "skipped", Error: fmt.Sprintf("%d upstream score check(s) failed", failed)}
		}
		finish()
		return report, fmt.Errorf("%w: %d of %d score checks failed", ErrProbeFailed, failed, len(opts.PGSIDs))
	}

	if opts.GCSSmoke {
		gcsTarget, ok := a.target("gcs")
		if !ok {
			report.Error = "GCS target is not configured"
			report.GCS = &GCSSmokeReport{Status: "failed", Error: "GCS target is not configured"}
			finish()
			return report, fmt.Errorf("%w: GCS target is not configured", ErrProbeFailed)
		}
		gcsReport, smokeErr := runStoreSmokeTest(ctx, gcsTarget.Store, report.RunID, a.now)
		report.GCS = &gcsReport
		if smokeErr != nil {
			report.Error = "GCS smoke test: " + smokeErr.Error()
			finish()
			return report, fmt.Errorf("%w: GCS smoke test: %v", ErrProbeFailed, smokeErr)
		}
	}

	report.Status = "passed"
	finish()
	return report, nil
}

func validateProbeOptions(opts ProbeOptions) error {
	if len(opts.PGSIDs) == 0 {
		return errors.New("at least one --pgs-id is required")
	}
	if len(opts.PGSIDs) > 20 {
		return errors.New("at most 20 --pgs-id values are allowed")
	}
	if opts.MaxBytes <= 0 {
		return errors.New("--max-size must be positive")
	}
	seen := make(map[string]struct{}, len(opts.PGSIDs))
	for _, id := range opts.PGSIDs {
		if !catalog.ValidScoreID(id) {
			return fmt.Errorf("invalid PGS ID %q", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate PGS ID %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func probeRunID(now time.Time) string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return now.UTC().Format("20060102T150405.000000000Z")
	}
	return now.UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(suffix[:])
}

func elapsedMillis(start, end time.Time) int64 {
	d := end.Sub(start)
	if d < 0 {
		return 0
	}
	return d.Milliseconds()
}

func runStoreSmokeTest(ctx context.Context, st store.Store, runID string, now func() time.Time) (report GCSSmokeReport, retErr error) {
	started := now()
	report = GCSSmokeReport{
		Status: "failed",
		Target: st.Name(),
		Key:    "probes/" + runID + ".json",
	}
	var liveGeneration int64
	defer func() {
		if liveGeneration != 0 {
			report.CleanupAttempted = true
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			cleanupErr := st.Delete(cleanupCtx, report.Key, store.DeleteOptions{GenerationMatch: &liveGeneration})
			cancel()
			if cleanupErr == nil || errors.Is(cleanupErr, store.ErrNotFound) {
				report.CleanupSucceeded = true
			} else {
				retErr = errors.Join(retErr, fmt.Errorf("cleanup probe object: %w", cleanupErr))
			}
		}
		report.DurationMS = elapsedMillis(started, now())
		if retErr != nil {
			report.Error = retErr.Error()
		}
	}()

	createdPayload := []byte(fmt.Sprintf("{\"run_id\":%q,\"phase\":\"created\"}\n", runID))
	created, err := st.Put(ctx, report.Key, bytes.NewReader(createdPayload), store.PutOptions{DoesNotExist: true, ContentType: "application/json"})
	if err != nil {
		return report, fmt.Errorf("create object: %w", err)
	}
	liveGeneration = created.Generation
	report.CreateGeneration = created.Generation
	report.CreateSucceeded = true

	r, readInfo, err := st.Open(ctx, report.Key)
	if err != nil {
		return report, fmt.Errorf("read object: %w", err)
	}
	readBody, readErr := io.ReadAll(io.LimitReader(r, 1<<20))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return report, fmt.Errorf("read object: %w", err)
	}
	report.ReadGeneration = readInfo.Generation
	readSHA := sha256.Sum256(readBody)
	report.PayloadSHA256 = hex.EncodeToString(readSHA[:])
	wantMD5 := md5.Sum(createdPayload)
	if !bytes.Equal(readBody, createdPayload) || (len(readInfo.MD5) > 0 && !bytes.Equal(readInfo.MD5, wantMD5[:])) {
		return report, errors.New("read-back checksum mismatch")
	}
	report.ReadChecksumVerified = true

	replacementPayload := []byte(fmt.Sprintf("{\"run_id\":%q,\"phase\":\"replaced\"}\n", runID))
	createGeneration := created.Generation
	replaced, err := st.Put(ctx, report.Key, bytes.NewReader(replacementPayload), store.PutOptions{GenerationMatch: &createGeneration, ContentType: "application/json"})
	if err != nil {
		return report, fmt.Errorf("replace object: %w", err)
	}
	liveGeneration = replaced.Generation
	report.ReplacementGeneration = replaced.Generation
	report.ReplaceSucceeded = true

	unexpected, staleErr := st.Put(ctx, report.Key, bytes.NewReader(createdPayload), store.PutOptions{GenerationMatch: &createGeneration, ContentType: "application/json"})
	if staleErr == nil {
		liveGeneration = unexpected.Generation
		return report, errors.New("stale generation write unexpectedly succeeded")
	}
	if !errors.Is(staleErr, store.ErrPrecondition) {
		return report, fmt.Errorf("stale generation write returned %w", staleErr)
	}
	report.StaleGenerationRejected = true

	if err := st.Delete(ctx, report.Key, store.DeleteOptions{GenerationMatch: &liveGeneration}); err != nil {
		return report, fmt.Errorf("delete object: %w", err)
	}
	liveGeneration = 0
	report.DeleteSucceeded = true
	report.DeletionNote = "the deleted generation may remain recoverable under the bucket's existing soft-delete policy"
	if _, err := st.Stat(ctx, report.Key); !errors.Is(err, store.ErrNotFound) {
		if err == nil {
			return report, errors.New("object remains live after delete")
		}
		return report, fmt.Errorf("confirm deletion: %w", err)
	}
	report.AbsenceConfirmed = true
	report.Status = "passed"
	return report, nil
}
