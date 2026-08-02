package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	"github.com/carbocation/pgsc_mirror/pkg/scoreheader"
)

// AnnotationFailure identifies a stored object that could not be inspected.
type AnnotationFailure struct {
	PGSID string `json:"pgs_id"`
	Error string `json:"error"`
}

// AnnotationAnomaly preserves the complete observation for a score whose
// stored header was unrecognized, unreadable, or otherwise warning-bearing.
type AnnotationAnomaly struct {
	PGSID       string                 `json:"pgs_id"`
	Observation scoreheader.Inspection `json:"observation"`
}

// AnnotationProgress is a point-in-time view of a stored-object annotation
// pass. Processed includes both objects read during this pass and current
// observations reused from the pinned manifest.
type AnnotationProgress struct {
	Available    int `json:"available"`
	Processed    int `json:"processed"`
	Inspected    int `json:"inspected"`
	Updated      int `json:"updated"`
	Unchanged    int `json:"unchanged"`
	Recognized   int `json:"recognized"`
	Unrecognized int `json:"unrecognized"`
	Unreadable   int `json:"unreadable"`
	Anomalies    int `json:"anomalies"`
	Failed       int `json:"failed"`
}

// AnnotationProgressReporter receives monotonically increasing progress from
// the result-collection goroutine. It should return quickly.
type AnnotationProgressReporter func(AnnotationProgress)

// AnnotationReport describes one upstream-independent annotation pass.
type AnnotationReport struct {
	Command             string              `json:"command"`
	SourceReleaseID     string              `json:"source_release_id,omitempty"`
	ReleaseID           string              `json:"release_id,omitempty"`
	DryRun              bool                `json:"dry_run"`
	Changed             bool                `json:"changed"`
	UpstreamIndependent bool                `json:"upstream_independent"`
	RepairedTargets     bool                `json:"repaired_targets,omitempty"`
	Available           int                 `json:"available"`
	Inspected           int                 `json:"inspected"`
	Updated             int                 `json:"updated"`
	Unchanged           int                 `json:"unchanged"`
	Recognized          int                 `json:"recognized"`
	Unrecognized        int                 `json:"unrecognized"`
	Unreadable          int                 `json:"unreadable"`
	Failed              int                 `json:"failed"`
	Anomalies           []AnnotationAnomaly `json:"anomalies,omitempty"`
	Failures            []AnnotationFailure `json:"failures,omitempty"`
	Message             string              `json:"message"`
}

// Annotate refreshes versioned descriptive annotations using only the current
// manifest release and its stored scoring files. It deliberately does not use the
// catalog or HTTP transfer clients, which makes upstream independence an
// architectural property rather than a request option.
func (a *App) Annotate(ctx context.Context, dryRun bool) (report AnnotationReport, runErr error) {
	return a.annotate(ctx, dryRun, nil)
}

// AnnotateWithProgress is Annotate with point-in-time progress reporting. The
// reporter affects output only and does not change publication semantics.
func (a *App) AnnotateWithProgress(ctx context.Context, dryRun bool, progress AnnotationProgressReporter) (report AnnotationReport, runErr error) {
	return a.annotate(ctx, dryRun, progress)
}

func (a *App) annotate(ctx context.Context, dryRun bool, progress AnnotationProgressReporter) (report AnnotationReport, runErr error) {
	report = AnnotationReport{Command: "annotate", DryRun: dryRun, UpstreamIndependent: true}
	if len(a.targets) == 0 {
		return report, errors.New("no configured targets")
	}
	if dryRun {
		return a.annotateCurrent(ctx, report, false, progress)
	}
	if a.State == nil {
		return report, errors.New("writable state is not open")
	}
	runID, err := a.State.BeginRun(ctx, "annotate")
	if err != nil {
		return report, err
	}
	defer func() { _ = a.State.FinishRun(context.Background(), runID, runErr) }()

	lease, err := a.acquireLease(ctx)
	if err != nil {
		return report, err
	}
	ctx = a.startLeaseRenewal(ctx, lease)
	defer func() {
		renewErr := lease.stopRenewal()
		releaseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		releaseErr := a.releaseLease(releaseCtx, lease)
		cancel()
		if renewErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("annotation lease renewal: %w", renewErr))
		}
		if releaseErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("release annotation lease: %w", releaseErr))
		}
	}()
	if err := a.cleanupStaging(); err != nil {
		return report, fmt.Errorf("clean provider staging: %w", err)
	}
	return a.annotateCurrent(ctx, report, true, progress)
}

func (a *App) annotateCurrent(ctx context.Context, report AnnotationReport, writable bool, progress AnnotationProgressReporter) (AnnotationReport, error) {
	pointer, entries, err := a.latest(ctx)
	if err != nil {
		return report, err
	}
	if pointer.ReleaseID == "" {
		return report, errors.New("mirror has no published release to annotate")
	}
	report.SourceReleaseID = pointer.ReleaseID
	if err := requireSupportedAnnotations(pointer, entries); err != nil {
		return report, err
	}

	if writable {
		repaired, err := a.convergeTargets(ctx, pointer, entries)
		if err != nil {
			return report, err
		}
		report.RepairedTargets = repaired
		pointer, repaired, err = a.ensureManifestTSV(ctx, pointer, entries)
		if err != nil {
			return report, err
		}
		report.RepairedTargets = report.RepairedTargets || repaired
		if err := a.catchUpState(ctx, pointer, entries); err != nil {
			return report, err
		}
	}

	report, err = annotateStoredHeaders(ctx, a.targets[0].Store, entries, a.Config.Transfer.FileConcurrency, report, progress)
	if err != nil {
		return report, err
	}
	if report.Failed > 0 {
		report.Message = fmt.Sprintf("stored-object annotation failed for %d scoring file(s); no release published", report.Failed)
		return report, fmt.Errorf("annotation failed for %d scoring file(s)", report.Failed)
	}

	needsPublication := report.Updated > 0 || (report.Available > 0 && pointer.HeaderInspectorVersion < scoreheader.InspectorVersion)
	report.Changed = needsPublication || report.RepairedTargets
	if !needsPublication {
		report.ReleaseID = pointer.ReleaseID
		if report.RepairedTargets {
			report.Message = "repaired lagging targets; stored-object annotations are current; no upstream access"
		} else {
			report.Message = "stored-object annotations are current; no upstream access"
		}
		return report, nil
	}

	scoreList, err := readStoredSnapshot(ctx, a.targets[0].Store, model.ScoreListKey(pointer.ReleaseID), pointer.ScoreListSHA256)
	if err != nil {
		return report, fmt.Errorf("read stored score list: %w", err)
	}
	metadata, err := readStoredSnapshot(ctx, a.targets[0].Store, model.MetadataKey(pointer.ReleaseID), pointer.MetadataSHA256)
	if err != nil {
		return report, fmt.Errorf("read stored metadata: %w", err)
	}
	now := a.now().UTC()
	releaseID, err := manifest.ReleaseID(now, entries, scoreList, metadata)
	if err != nil {
		return report, err
	}
	for i := range entries {
		entries[i].ReleaseID = releaseID
	}
	manifestBytes, manifestSHA, err := manifest.Encode(entries)
	if err != nil {
		return report, err
	}
	scoreSum := sha256.Sum256(scoreList)
	metadataSum := sha256.Sum256(metadata)
	annotated := model.Pointer{
		ReleaseID:              releaseID,
		ManifestKey:            model.ManifestKey(releaseID),
		ManifestSHA256:         manifestSHA,
		ScoreListSHA256:        hex.EncodeToString(scoreSum[:]),
		MetadataSHA256:         hex.EncodeToString(metadataSum[:]),
		PublishedAt:            now,
		EntryCount:             len(entries),
		GenomeBuild:            pointer.GenomeBuild,
		HeaderInspectorVersion: observedHeaderInspectorVersion(entries),
		ScoreLayoutVersion:     pointer.ScoreLayoutVersion,
	}
	annotated, manifestTSV, err := attachManifestTSV(annotated, entries)
	if err != nil {
		return report, err
	}
	if report.DryRun {
		report.Message = fmt.Sprintf("would publish stored-object annotations for %d scoring file(s); no upstream access", report.Updated)
		return report, nil
	}
	if err := a.publish(ctx, annotated, scoreList, metadata, manifestBytes, manifestTSV); err != nil {
		return report, err
	}
	report.ReleaseID = releaseID
	if err := a.State.RecordRelease(ctx, annotated, entries); err != nil {
		return report, err
	}
	report.Message = fmt.Sprintf("published stored-object annotations for %d scoring file(s); no upstream access", report.Updated)
	return report, nil
}

type annotationResult struct {
	index      int
	inspection scoreheader.Inspection
	err        error
}

func annotateStoredHeaders(ctx context.Context, source store.Store, entries []model.Entry, workers int, report AnnotationReport, progress AnnotationProgressReporter) (AnnotationReport, error) {
	var pending []int
	for i := range entries {
		if entries[i].Status != model.StatusReady {
			continue
		}
		report.Available++
		if entries[i].Header.Current() {
			report.Unchanged++
			recordAnnotationObservation(&report, entries[i].PGSID, *entries[i].Header)
			continue
		}
		pending = append(pending, i)
	}
	reportAnnotationProgress(progress, report)
	if len(pending) == 0 {
		sortAnnotationDetails(&report)
		return report, nil
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(pending) {
		workers = len(pending)
	}
	jobs := make(chan int)
	results := make(chan annotationResult)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					result := annotationResult{index: index}
					r, _, err := source.Open(ctx, entries[index].ScoreKey)
					if err == nil {
						result.inspection = scoreheader.InspectGzip(r)
						err = r.Close()
					}
					result.err = err
					select {
					case results <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, index := range pending {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	for result := range results {
		report.Inspected++
		if result.err != nil {
			report.Failed++
			report.Failures = append(report.Failures, AnnotationFailure{PGSID: entries[result.index].PGSID, Error: result.err.Error()})
			reportAnnotationProgress(progress, report)
			continue
		}
		entries[result.index].Header = &result.inspection
		report.Updated++
		recordAnnotationObservation(&report, entries[result.index].PGSID, result.inspection)
		reportAnnotationProgress(progress, report)
	}
	sortAnnotationDetails(&report)
	if err := ctx.Err(); err != nil {
		return report, err
	}
	return report, nil
}

func reportAnnotationProgress(progress AnnotationProgressReporter, report AnnotationReport) {
	if progress == nil {
		return
	}
	progress(AnnotationProgress{
		Available:    report.Available,
		Processed:    report.Unchanged + report.Inspected,
		Inspected:    report.Inspected,
		Updated:      report.Updated,
		Unchanged:    report.Unchanged,
		Recognized:   report.Recognized,
		Unrecognized: report.Unrecognized,
		Unreadable:   report.Unreadable,
		Anomalies:    len(report.Anomalies),
		Failed:       report.Failed,
	})
}

func recordAnnotationObservation(report *AnnotationReport, pgsID string, observation scoreheader.Inspection) {
	countAnnotationStatus(report, observation.Status)
	if observation.Status != scoreheader.StatusRecognized || observation.Error != "" || len(observation.Warnings) > 0 {
		report.Anomalies = append(report.Anomalies, AnnotationAnomaly{PGSID: pgsID, Observation: observation})
	}
}

func sortAnnotationDetails(report *AnnotationReport) {
	sort.Slice(report.Anomalies, func(i, j int) bool { return report.Anomalies[i].PGSID < report.Anomalies[j].PGSID })
	sort.Slice(report.Failures, func(i, j int) bool { return report.Failures[i].PGSID < report.Failures[j].PGSID })
}

func countAnnotationStatus(report *AnnotationReport, status string) {
	switch status {
	case scoreheader.StatusRecognized:
		report.Recognized++
	case scoreheader.StatusUnrecognized:
		report.Unrecognized++
	case scoreheader.StatusUnreadable:
		report.Unreadable++
	}
}

func readStoredSnapshot(ctx context.Context, st store.Store, key, expectedSHA256 string) ([]byte, error) {
	r, _, err := st.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if expectedSHA256 != "" {
		sum := sha256.Sum256(b)
		if observed := hex.EncodeToString(sum[:]); observed != expectedSHA256 {
			return nil, fmt.Errorf("SHA-256 is %s, want %s", observed, expectedSHA256)
		}
	}
	return b, nil
}
