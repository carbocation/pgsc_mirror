package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/manifest"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
)

type scoreLayoutMigration struct {
	SourceReleaseID string
	ReleaseID       string
	Migrated        int
	Changed         bool
	RepairedTargets bool
}

// migrateScoreLayout upgrades a legacy content-addressed release to the flat
// public scores/ namespace using only already mirrored objects.
func (a *App) migrateScoreLayout(ctx context.Context) (report scoreLayoutMigration, runErr error) {
	if a.State == nil {
		return report, errors.New("writable state is not open")
	}
	pointer, entries, err := a.latest(ctx)
	if err != nil {
		return report, err
	}
	if pointer.ReleaseID == "" {
		return report, nil
	}
	report.SourceReleaseID = pointer.ReleaseID
	report.ReleaseID = pointer.ReleaseID
	if pointer.ScoreLayoutVersion > model.ScoreLayoutVersion {
		return report, fmt.Errorf("published score layout version %d is newer than this binary's version %d; upgrade pgsc-mirror before publishing", pointer.ScoreLayoutVersion, model.ScoreLayoutVersion)
	}
	if scoreLayoutMigrationsNeeded(entries) == 0 && pointer.ScoreLayoutVersion == model.ScoreLayoutVersion {
		return report, nil
	}
	runID, err := a.State.BeginRun(ctx, "migrate-score-layout")
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
			runErr = errors.Join(runErr, fmt.Errorf("score-layout lease renewal: %w", renewErr))
		}
		if releaseErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("release score-layout lease: %w", releaseErr))
		}
	}()
	if err := a.cleanupStaging(); err != nil {
		return report, fmt.Errorf("clean provider staging: %w", err)
	}
	return a.migrateCurrentScoreLayout(ctx)
}

func (a *App) migrateCurrentScoreLayout(ctx context.Context) (scoreLayoutMigration, error) {
	var report scoreLayoutMigration
	pointer, entries, err := a.latest(ctx)
	if err != nil {
		return report, err
	}
	if pointer.ReleaseID == "" {
		return report, nil
	}
	report.SourceReleaseID = pointer.ReleaseID
	report.ReleaseID = pointer.ReleaseID
	if pointer.ScoreLayoutVersion > model.ScoreLayoutVersion {
		return report, fmt.Errorf("published score layout version %d is newer than this binary's version %d; upgrade pgsc-mirror before publishing", pointer.ScoreLayoutVersion, model.ScoreLayoutVersion)
	}
	if err := requireSupportedAnnotations(pointer, entries); err != nil {
		return report, err
	}
	needed := scoreLayoutMigrationsNeeded(entries)
	if needed == 0 && pointer.ScoreLayoutVersion == model.ScoreLayoutVersion {
		return report, nil
	}

	repaired, err := a.convergeTargets(ctx, pointer, entries)
	if err != nil {
		return report, err
	}
	report.RepairedTargets = repaired
	if err := a.catchUpState(ctx, pointer, entries); err != nil {
		return report, err
	}
	if needed > 0 {
		if err := a.migrateStoredScores(ctx, entries); err != nil {
			return report, err
		}
	}
	report.Migrated = needed

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
	migrated := model.Pointer{
		ReleaseID:              releaseID,
		ManifestKey:            model.ManifestKey(releaseID),
		ManifestSHA256:         manifestSHA,
		ScoreListSHA256:        hex.EncodeToString(scoreSum[:]),
		MetadataSHA256:         hex.EncodeToString(metadataSum[:]),
		PublishedAt:            now,
		EntryCount:             len(entries),
		GenomeBuild:            pointer.GenomeBuild,
		HeaderInspectorVersion: pointer.HeaderInspectorVersion,
		ScoreLayoutVersion:     model.ScoreLayoutVersion,
	}
	if err := a.publish(ctx, migrated, scoreList, metadata, manifestBytes); err != nil {
		return report, err
	}
	if err := a.State.RecordRelease(ctx, migrated, entries, a.Config.Targets.Local); err != nil {
		return report, err
	}
	if err := a.advanceMaintenanceRelease(ctx, migrated); err != nil {
		return report, err
	}
	report.ReleaseID = releaseID
	report.Changed = true
	return report, nil
}

func scoreLayoutMigrationsNeeded(entries []model.Entry) int {
	count := 0
	for i := range entries {
		if entries[i].ScoreKey != model.ScoreKey(entries[i].PGSID, entries[i].GenomeBuild) {
			count++
		}
	}
	return count
}

type scoreMigrationResult struct {
	index         int
	gcsGeneration int64
	err           error
}

func (a *App) migrateStoredScores(ctx context.Context, entries []model.Entry) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan scoreMigrationResult)
	workers := a.Config.Transfer.Concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(entries) {
		workers = len(entries)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				generation, err := a.migrateStoredScore(ctx, entries[index])
				select {
				case results <- scoreMigrationResult{index: index, gcsGeneration: generation, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := range entries {
			if entries[i].ScoreKey == model.ScoreKey(entries[i].PGSID, entries[i].GenomeBuild) {
				continue
			}
			select {
			case jobs <- i:
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
		if result.err != nil {
			cancel()
			return fmt.Errorf("migrate %s: %w", entries[result.index].PGSID, result.err)
		}
		entries[result.index].ScoreKey = model.ScoreKey(entries[result.index].PGSID, entries[result.index].GenomeBuild)
		if result.gcsGeneration != 0 {
			entries[result.index].GCSGeneration = result.gcsGeneration
		}
	}
	return ctx.Err()
}

func (a *App) migrateStoredScore(ctx context.Context, entry model.Entry) (int64, error) {
	destinationKey := model.ScoreKey(entry.PGSID, entry.GenomeBuild)
	var gcsGeneration int64
	for _, target := range a.targets {
		generation, err := copyStoredScore(ctx, target, entry, destinationKey)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", target.Name(), err)
		}
		if target.kind == "gcs" {
			gcsGeneration = generation
		}
	}
	return gcsGeneration, nil
}

func copyStoredScore(ctx context.Context, target target, entry model.Entry, destinationKey string) (int64, error) {
	sourceInfo, err := target.Stat(ctx, entry.ScoreKey)
	if err != nil {
		return 0, fmt.Errorf("stat source %s: %w", entry.ScoreKey, err)
	}
	matches, err := scoreObjectMatches(ctx, target.Store, sourceInfo, entry)
	if err != nil {
		return 0, err
	}
	if !matches {
		return 0, fmt.Errorf("source %s does not match manifest MD5 and size", entry.ScoreKey)
	}

	destinationEntry := entry
	destinationEntry.ScoreKey = destinationKey
	destinationInfo, statErr := target.Stat(ctx, destinationKey)
	destination := scoreDestination{target: target}
	if statErr == nil {
		matches, err := scoreObjectMatches(ctx, target.Store, destinationInfo, destinationEntry)
		if err != nil {
			return 0, err
		}
		if matches {
			return destinationInfo.Generation, nil
		}
		destination.info = destinationInfo
		destination.exists = true
	} else if !errors.Is(statErr, store.ErrNotFound) {
		return 0, statErr
	}

	opts := scorePutOptions(destinationEntry, destination)
	var copied store.ObjectInfo
	if copier, ok := target.Store.(store.Copier); ok {
		copied, err = copier.Copy(ctx, entry.ScoreKey, destinationKey, opts)
	} else {
		var r io.ReadCloser
		r, _, err = target.Open(ctx, entry.ScoreKey)
		if err == nil {
			var putErr error
			copied, putErr = target.Put(ctx, destinationKey, r, opts)
			err = errors.Join(putErr, r.Close())
		}
	}
	if errors.Is(err, store.ErrPrecondition) {
		copied, err = target.Stat(ctx, destinationKey)
		if err == nil {
			var current bool
			current, err = scoreObjectMatches(ctx, target.Store, copied, destinationEntry)
			if err == nil && !current {
				err = errors.New("concurrent score-layout write has unexpected content")
			}
		}
	}
	if err != nil {
		return 0, err
	}
	if copied.Size != sourceInfo.Size {
		return 0, fmt.Errorf("copied size is %d, want %d", copied.Size, sourceInfo.Size)
	}
	if len(copied.MD5) > 0 && !strings.EqualFold(hex.EncodeToString(copied.MD5), entry.SourceMD5) {
		return 0, fmt.Errorf("copied MD5 is %s, want %s", hex.EncodeToString(copied.MD5), entry.SourceMD5)
	}
	return copied.Generation, nil
}

func (a *App) advanceMaintenanceRelease(ctx context.Context, pointer model.Pointer) error {
	checkpoint, _, err := readMaintenanceCheckpoint(ctx, a.targets[0].Store)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, errInvalidMaintenanceCheckpoint) {
		return nil
	}
	if err != nil {
		return err
	}
	if checkpoint.ScoreList.ContentSHA256 != pointer.ScoreListSHA256 || checkpoint.Metadata.ContentSHA256 != pointer.MetadataSHA256 {
		return nil
	}
	checkpoint.FormatVersion = maintenanceCheckpointVersion
	checkpoint.ObservedReleaseID = pointer.ReleaseID
	return a.publishMaintenanceCheckpointDocument(ctx, checkpoint, false)
}
