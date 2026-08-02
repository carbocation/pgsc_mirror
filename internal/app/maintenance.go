package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/catalog"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/state"
	"github.com/carbocation/pgsc_mirror/internal/store"
)

const (
	// Version 2 prevents pre-flat-layout binaries from publishing the retired
	// content-addressed layout after a mirror has migrated to score_key.
	maintenanceCheckpointVersion = 2
	maintenanceCheckpointMaxSize = 1 << 20
	maintenanceFutureTolerance   = 5 * time.Minute
)

var errInvalidMaintenanceCheckpoint = errors.New("invalid maintenance checkpoint")

type maintenanceDocument struct {
	ETag          string `json:"etag,omitempty"`
	LastModified  string `json:"last_modified,omitempty"`
	ContentSHA256 string `json:"content_sha256"`
}

// maintenanceCheckpoint is portable operational state. It is deliberately
// separate from immutable releases: losing it can cause an extra audit but
// cannot make an incomplete or unverified release current.
type maintenanceCheckpoint struct {
	FormatVersion            int                 `json:"format_version"`
	GenomeBuild              string              `json:"genome_build"`
	ObservedReleaseID        string              `json:"observed_release_id"`
	LastFullReconciliationAt time.Time           `json:"last_full_reconciliation_at"`
	ScoreList                maintenanceDocument `json:"score_list"`
	Metadata                 maintenanceDocument `json:"metadata"`
}

func checkpointFromInventory(p model.Pointer, inv inventory, completedAt time.Time) maintenanceCheckpoint {
	return maintenanceCheckpoint{
		FormatVersion:            maintenanceCheckpointVersion,
		GenomeBuild:              p.GenomeBuild,
		ObservedReleaseID:        p.ReleaseID,
		LastFullReconciliationAt: completedAt.UTC(),
		ScoreList:                checkpointDocument(inv.scoreDoc),
		Metadata:                 checkpointDocument(inv.metadataDoc),
	}
}

func checkpointDocument(document catalog.Document) maintenanceDocument {
	sum := sha256.Sum256(document.Body)
	return maintenanceDocument{
		ETag:          document.ETag,
		LastModified:  document.LastModified,
		ContentSHA256: hex.EncodeToString(sum[:]),
	}
}

func encodeMaintenanceCheckpoint(checkpoint maintenanceCheckpoint) ([]byte, error) {
	b, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func readMaintenanceCheckpoint(ctx context.Context, st store.Store) (maintenanceCheckpoint, store.ObjectInfo, error) {
	r, info, err := st.Open(ctx, model.MaintenanceKey)
	if err != nil {
		return maintenanceCheckpoint{}, store.ObjectInfo{}, err
	}
	b, readErr := io.ReadAll(io.LimitReader(r, maintenanceCheckpointMaxSize+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return maintenanceCheckpoint{}, store.ObjectInfo{}, err
	}
	if len(b) > maintenanceCheckpointMaxSize {
		return maintenanceCheckpoint{}, info, fmt.Errorf("%w: exceeds %d bytes", errInvalidMaintenanceCheckpoint, maintenanceCheckpointMaxSize)
	}
	var checkpoint maintenanceCheckpoint
	if err := json.Unmarshal(b, &checkpoint); err != nil {
		return maintenanceCheckpoint{}, info, fmt.Errorf("%w: decode JSON: %v", errInvalidMaintenanceCheckpoint, err)
	}
	return checkpoint, info, nil
}

func validateMaintenanceCheckpoint(checkpoint maintenanceCheckpoint, pointer model.Pointer, genomeBuild string, now time.Time) error {
	if checkpoint.FormatVersion > maintenanceCheckpointVersion {
		return fmt.Errorf("maintenance checkpoint format %d is newer than supported format %d; upgrade pgsc-mirror", checkpoint.FormatVersion, maintenanceCheckpointVersion)
	}
	if checkpoint.FormatVersion != maintenanceCheckpointVersion {
		return fmt.Errorf("unsupported maintenance checkpoint format %d", checkpoint.FormatVersion)
	}
	if checkpoint.GenomeBuild != genomeBuild || pointer.GenomeBuild != genomeBuild {
		return fmt.Errorf("maintenance checkpoint genome build %q does not match %q", checkpoint.GenomeBuild, genomeBuild)
	}
	if checkpoint.LastFullReconciliationAt.IsZero() {
		return errors.New("maintenance checkpoint has no full reconciliation time")
	}
	if checkpoint.LastFullReconciliationAt.After(now.UTC().Add(maintenanceFutureTolerance)) {
		return errors.New("maintenance checkpoint full reconciliation time is in the future")
	}
	if checkpoint.ScoreList.ContentSHA256 == "" || checkpoint.Metadata.ContentSHA256 == "" {
		return errors.New("maintenance checkpoint is missing content hashes")
	}
	if checkpoint.ScoreList.ContentSHA256 != pointer.ScoreListSHA256 || checkpoint.Metadata.ContentSHA256 != pointer.MetadataSHA256 {
		return errors.New("maintenance checkpoint does not describe the current release snapshots")
	}
	return nil
}

func (a *App) publishMaintenanceCheckpoint(ctx context.Context, pointer model.Pointer, inv inventory) error {
	checkpoint := checkpointFromInventory(pointer, inv, a.now().UTC())
	return a.publishMaintenanceCheckpointDocument(ctx, checkpoint, true)
}

func (a *App) publishMaintenanceCheckpointDocument(ctx context.Context, checkpoint maintenanceCheckpoint, repairInvalidNewer bool) error {
	for _, target := range a.targets {
		if err := putMaintenanceCheckpointMode(ctx, target.Store, checkpoint, repairInvalidNewer); err != nil {
			return fmt.Errorf("publish maintenance checkpoint to %s: %w", target.Name(), err)
		}
	}
	return nil
}

func (a *App) publishLocalMaintenanceCheckpoint(ctx context.Context, pointer model.Pointer, completedAt time.Time) (bool, error) {
	score, scoreOK, err := a.State.Sentinel(ctx, "score_list")
	if err != nil {
		return false, err
	}
	metadata, metadataOK, err := a.State.Sentinel(ctx, "metadata")
	if err != nil {
		return false, err
	}
	if !scoreOK || !metadataOK || score.ContentSHA256 == "" || metadata.ContentSHA256 == "" || score.ContentSHA256 != pointer.ScoreListSHA256 || metadata.ContentSHA256 != pointer.MetadataSHA256 {
		return false, nil
	}
	checkpoint := maintenanceCheckpoint{
		FormatVersion:            maintenanceCheckpointVersion,
		GenomeBuild:              pointer.GenomeBuild,
		ObservedReleaseID:        pointer.ReleaseID,
		LastFullReconciliationAt: completedAt.UTC(),
		ScoreList:                maintenanceDocument{ETag: score.ETag, LastModified: score.LastModified, ContentSHA256: score.ContentSHA256},
		Metadata:                 maintenanceDocument{ETag: metadata.ETag, LastModified: metadata.LastModified, ContentSHA256: metadata.ContentSHA256},
	}
	if err := validateMaintenanceCheckpoint(checkpoint, pointer, a.Config.GenomeBuild, a.now()); err != nil {
		return false, nil
	}
	if err := a.publishMaintenanceCheckpointDocument(ctx, checkpoint, false); err != nil {
		return false, err
	}
	return true, nil
}

func (a *App) backfillMaintenanceFromLocal(ctx context.Context, available bool, completedAt time.Time) (bool, error) {
	if !available {
		return false, nil
	}
	pointer, _, err := a.readPointer(ctx, a.targets[0].Store)
	if err != nil {
		return false, err
	}
	_, err = a.publishLocalMaintenanceCheckpoint(ctx, pointer, completedAt)
	return false, err
}

func putMaintenanceCheckpoint(ctx context.Context, st store.Store, checkpoint maintenanceCheckpoint) error {
	return putMaintenanceCheckpointMode(ctx, st, checkpoint, false)
}

func putMaintenanceCheckpointMode(ctx context.Context, st store.Store, checkpoint maintenanceCheckpoint, repairInvalidNewer bool) error {
	b, err := encodeMaintenanceCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 4; attempt++ {
		current, info, readErr := readMaintenanceCheckpoint(ctx, st)
		opts := store.PutOptions{ContentType: "application/json"}
		switch {
		case errors.Is(readErr, store.ErrNotFound):
			opts.DoesNotExist = true
		case errors.Is(readErr, errInvalidMaintenanceCheckpoint):
			generation := info.Generation
			opts.GenerationMatch = &generation
		case readErr != nil:
			return readErr
		case current.FormatVersion > maintenanceCheckpointVersion:
			return fmt.Errorf("maintenance checkpoint format %d is newer than supported format %d; upgrade pgsc-mirror", current.FormatVersion, maintenanceCheckpointVersion)
		case current == checkpoint:
			return nil
		case current.LastFullReconciliationAt.After(checkpoint.LastFullReconciliationAt):
			currentLooksInvalid := current.LastFullReconciliationAt.After(checkpoint.LastFullReconciliationAt.Add(maintenanceFutureTolerance)) ||
				current.GenomeBuild != checkpoint.GenomeBuild ||
				current.ScoreList.ContentSHA256 != checkpoint.ScoreList.ContentSHA256 ||
				current.Metadata.ContentSHA256 != checkpoint.Metadata.ContentSHA256
			if !repairInvalidNewer || !currentLooksInvalid {
				return nil
			}
			generation := info.Generation
			opts.GenerationMatch = &generation
		default:
			generation := info.Generation
			opts.GenerationMatch = &generation
		}
		if _, err := st.Put(ctx, model.MaintenanceKey, bytes.NewReader(b), opts); errors.Is(err, store.ErrPrecondition) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return errors.New("maintenance checkpoint changed repeatedly during publication")
}

// restoreMaintenanceCheckpoint reconstructs disposable local scheduling,
// sentinels, and current-release state from the canonical mirror. An absent or
// stale checkpoint safely falls back to a full reconciliation.
func (a *App) restoreMaintenanceCheckpoint(ctx context.Context) (bool, error) {
	if a.State == nil || len(a.targets) == 0 {
		return false, nil
	}
	localReconciliation, localOK, err := a.State.LastSuccessfulReconciliation(ctx)
	if err != nil {
		return false, err
	}
	checkpoint, _, err := readMaintenanceCheckpoint(ctx, a.targets[0].Store)
	if errors.Is(err, store.ErrNotFound) {
		return a.backfillMaintenanceFromLocal(ctx, localOK, localReconciliation)
	}
	if errors.Is(err, errInvalidMaintenanceCheckpoint) {
		return a.backfillMaintenanceFromLocal(ctx, localOK, localReconciliation)
	}
	if err != nil {
		return false, fmt.Errorf("read canonical maintenance checkpoint: %w", err)
	}
	if checkpoint.FormatVersion > maintenanceCheckpointVersion {
		return false, fmt.Errorf("maintenance checkpoint format %d is newer than supported format %d; upgrade pgsc-mirror", checkpoint.FormatVersion, maintenanceCheckpointVersion)
	}
	if checkpoint.FormatVersion != maintenanceCheckpointVersion || checkpoint.GenomeBuild != a.Config.GenomeBuild || checkpoint.LastFullReconciliationAt.IsZero() || checkpoint.LastFullReconciliationAt.After(a.now().UTC().Add(maintenanceFutureTolerance)) {
		return a.backfillMaintenanceFromLocal(ctx, localOK, localReconciliation)
	}
	if localOK && !checkpoint.LastFullReconciliationAt.After(localReconciliation) {
		if localReconciliation.After(checkpoint.LastFullReconciliationAt) {
			return a.backfillMaintenanceFromLocal(ctx, true, localReconciliation)
		}
		return false, nil
	}
	pointer, _, err := a.readPointer(ctx, a.targets[0].Store)
	if errors.Is(err, store.ErrNotFound) || (err == nil && pointer.ReleaseID == "") {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read canonical pointer for maintenance restore: %w", err)
	}
	if err := validateMaintenanceCheckpoint(checkpoint, pointer, a.Config.GenomeBuild, a.now()); err != nil {
		return a.backfillMaintenanceFromLocal(ctx, localOK, localReconciliation)
	}
	entries, _, err := a.readManifest(ctx, a.targets[0].Store, pointer)
	if err != nil {
		return false, fmt.Errorf("read canonical manifest for maintenance restore: %w", err)
	}
	localAvailable := a.Config.Targets.Local && !a.Config.Targets.GCS
	if err := a.State.RecordRelease(ctx, pointer, entries, localAvailable); err != nil {
		return false, fmt.Errorf("restore current release from canonical mirror: %w", err)
	}
	sentinels := []state.Sentinel{
		checkpointSentinel("score_list", checkpoint.ScoreList, checkpoint.LastFullReconciliationAt),
		checkpointSentinel("metadata", checkpoint.Metadata, checkpoint.LastFullReconciliationAt),
	}
	if err := a.State.RestoreReconciliation(ctx, checkpoint.LastFullReconciliationAt, sentinels); err != nil {
		return false, fmt.Errorf("restore maintenance state from canonical mirror: %w", err)
	}
	return true, nil
}

func checkpointSentinel(name string, document maintenanceDocument, observedAt time.Time) state.Sentinel {
	return state.Sentinel{
		Name:          name,
		ETag:          document.ETag,
		LastModified:  document.LastModified,
		ContentSHA256: document.ContentSHA256,
		ObservedAt:    observedAt,
	}
}
