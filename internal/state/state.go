// Package state maintains the disposable SQLite operational index.
package state

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/model"
	_ "modernc.org/sqlite"
)

type DB struct {
	db *sql.DB
	mu sync.Mutex
}

type Sentinel struct {
	Name, ETag, LastModified, ContentSHA256 string
	ObservedAt                              time.Time
}

type Summary struct {
	LatestRelease   string     `json:"latest_release"`
	Entries         int        `json:"entries"`
	Available       int        `json:"available"`
	Withdrawn       int        `json:"withdrawn"`
	FailedRuns      int        `json:"failed_runs"`
	LastRunCommand  string     `json:"last_run_command,omitempty"`
	LastRunStatus   string     `json:"last_run_status,omitempty"`
	LastRunFinished *time.Time `json:"last_run_finished,omitempty"`
}

type RebuildRelease struct {
	Pointer model.Pointer
	Entries []model.Entry
}

type InventorySidecar struct {
	SourceMD5 string
	SourceURL string
}

func Open(path string) (*DB, error) {
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	sqldb.SetMaxOpenConns(1)
	d := &DB{db: sqldb}
	if err := d.migrate(context.Background()); err != nil {
		sqldb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error { return d.db.Close() }

func (d *DB) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS runs (
 id INTEGER PRIMARY KEY, command TEXT NOT NULL, started_at TEXT NOT NULL,
 finished_at TEXT, status TEXT NOT NULL, error TEXT
);
CREATE TABLE IF NOT EXISTS sentinels (
 name TEXT PRIMARY KEY, etag TEXT NOT NULL DEFAULT '', last_modified TEXT NOT NULL DEFAULT '',
 content_sha256 TEXT NOT NULL DEFAULT '', observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS releases (
 release_id TEXT PRIMARY KEY, manifest_key TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
 published_at TEXT NOT NULL, entry_count INTEGER NOT NULL, complete INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
 key TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scores (
 pgs_id TEXT PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(release_id),
 genome_build TEXT NOT NULL, source_url TEXT NOT NULL, source_md5 TEXT NOT NULL,
 size_bytes INTEGER NOT NULL, blob_key TEXT NOT NULL, upstream_etag TEXT NOT NULL DEFAULT '',
 upstream_last_modified TEXT NOT NULL DEFAULT '', first_seen_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL, status TEXT NOT NULL, license TEXT NOT NULL DEFAULT '',
 gcs_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS scores_status ON scores(status);
CREATE TABLE IF NOT EXISTS objects (
 blob_key TEXT PRIMARY KEY, source_md5 TEXT NOT NULL, size_bytes INTEGER NOT NULL,
 local_available INTEGER NOT NULL DEFAULT 0, gcs_generation INTEGER NOT NULL DEFAULT 0,
 verified_at TEXT
);
CREATE TABLE IF NOT EXISTS transfers (
 pgs_id TEXT PRIMARY KEY, source_md5 TEXT NOT NULL, part_path TEXT NOT NULL DEFAULT '',
 bytes_downloaded INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory_checkpoints (
 id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL, started_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS inventory_checkpoints_fingerprint ON inventory_checkpoints(fingerprint);
CREATE TABLE IF NOT EXISTS inventory_sidecars (
 checkpoint_id INTEGER NOT NULL REFERENCES inventory_checkpoints(id) ON DELETE CASCADE,
 pgs_id TEXT NOT NULL, source_md5 TEXT NOT NULL, source_url TEXT NOT NULL,
 checked_at TEXT NOT NULL, PRIMARY KEY(checkpoint_id,pgs_id)
);`
	if _, err := d.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	_, err := d.db.ExecContext(ctx, `UPDATE runs SET finished_at=?,status='interrupted',error='process ended before run completion' WHERE status='running'`, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// BeginInventory resumes a recent checkpoint for the same immutable input
// fingerprint, or starts a new checkpoint. Old checkpoints are discarded so a
// later full reconciliation still refetches every checksum sidecar.
func (d *DB) BeginInventory(ctx context.Context, fingerprint string, maxAge time.Duration, now time.Time) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	cutoff := now.Add(-maxAge).UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `DELETE FROM inventory_checkpoints WHERE fingerprint<>? OR updated_at<?`, fingerprint, cutoff); err != nil {
		return 0, err
	}
	var id int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM inventory_checkpoints WHERE fingerprint=? ORDER BY id DESC LIMIT 1`, fingerprint).Scan(&id)
	stamp := now.UTC().Format(time.RFC3339Nano)
	if err == sql.ErrNoRows {
		result, insertErr := tx.ExecContext(ctx, `INSERT INTO inventory_checkpoints(fingerprint,started_at,updated_at) VALUES(?,?,?)`, fingerprint, stamp, stamp)
		if insertErr != nil {
			return 0, insertErr
		}
		id, err = result.LastInsertId()
	} else if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE inventory_checkpoints SET updated_at=? WHERE id=?`, stamp, id)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func (d *DB) InventorySidecar(ctx context.Context, checkpointID int64, pgsID string) (InventorySidecar, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var result InventorySidecar
	err := d.db.QueryRowContext(ctx, `SELECT source_md5,source_url FROM inventory_sidecars WHERE checkpoint_id=? AND pgs_id=?`, checkpointID, pgsID).Scan(&result.SourceMD5, &result.SourceURL)
	if err == sql.ErrNoRows {
		return InventorySidecar{}, false, nil
	}
	return result, err == nil, err
}

func (d *DB) PutInventorySidecar(ctx context.Context, checkpointID int64, pgsID, sourceMD5, sourceURL string, now time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO inventory_sidecars(checkpoint_id,pgs_id,source_md5,source_url,checked_at) VALUES(?,?,?,?,?) ON CONFLICT(checkpoint_id,pgs_id) DO UPDATE SET source_md5=excluded.source_md5,source_url=excluded.source_url,checked_at=excluded.checked_at`, checkpointID, pgsID, sourceMD5, sourceURL, stamp); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory_checkpoints SET updated_at=? WHERE id=?`, stamp, checkpointID); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) ConsumeInventory(ctx context.Context, checkpointID int64) error {
	if checkpointID == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.ExecContext(ctx, `DELETE FROM inventory_checkpoints WHERE id=?`, checkpointID)
	return err
}

func (d *DB) LatestRelease(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var releaseID string
	err := d.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key='latest_release'`).Scan(&releaseID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return releaseID, err
}

// LastSuccessfulReconciliation returns the completion time of the latest run
// that performed a full checksum-sidecar reconciliation. Cheap unchanged
// update checks are recorded as update-check and intentionally do not count.
func (d *DB) LastSuccessfulReconciliation(ctx context.Context) (time.Time, bool, error) {
	return d.lastSuccessfulRun(ctx, "command IN ('reconcile','update')")
}

// LastSuccessfulVerification returns the completion time of the latest
// successful verification performed by the continuous service.
func (d *DB) LastSuccessfulVerification(ctx context.Context) (time.Time, bool, error) {
	return d.lastSuccessfulRun(ctx, "command='verify'")
}

func (d *DB) lastSuccessfulRun(ctx context.Context, commandPredicate string) (time.Time, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var finished string
	query := `SELECT finished_at FROM runs WHERE ` + commandPredicate + ` AND status='success' AND finished_at IS NOT NULL ORDER BY id DESC LIMIT 1`
	err := d.db.QueryRowContext(ctx, query).Scan(&finished)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	stamp, err := time.Parse(time.RFC3339Nano, finished)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse last successful run time: %w", err)
	}
	return stamp, true, nil
}

func (d *DB) RecordTransfer(ctx context.Context, pgsID, sourceMD5, partPath string, bytesDownloaded int64, attempts int, status string, transferErr error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	message := ""
	if transferErr != nil {
		message = transferErr.Error()
	}
	_, err := d.db.ExecContext(ctx, `INSERT INTO transfers(pgs_id,source_md5,part_path,bytes_downloaded,attempts,status,error,updated_at) VALUES(?,?,?,?,?,?,?,?)
 ON CONFLICT(pgs_id) DO UPDATE SET source_md5=excluded.source_md5,part_path=excluded.part_path,bytes_downloaded=excluded.bytes_downloaded,attempts=excluded.attempts,status=excluded.status,error=excluded.error,updated_at=excluded.updated_at`,
		pgsID, sourceMD5, partPath, bytesDownloaded, attempts, status, message, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (d *DB) BeginRun(ctx context.Context, command string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, err := d.db.ExecContext(ctx, `INSERT INTO runs(command,started_at,status) VALUES(?,?,?)`, command, time.Now().UTC().Format(time.RFC3339Nano), "running")
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func (d *DB) FinishRun(ctx context.Context, id int64, runErr error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	status, message := "success", ""
	if runErr != nil {
		status, message = "failed", runErr.Error()
	}
	_, err := d.db.ExecContext(ctx, `UPDATE runs SET finished_at=?,status=?,error=? WHERE id=?`, time.Now().UTC().Format(time.RFC3339Nano), status, message, id)
	return err
}

func (d *DB) Sentinel(ctx context.Context, name string) (Sentinel, bool, error) {
	var s Sentinel
	var observed string
	err := d.db.QueryRowContext(ctx, `SELECT name,etag,last_modified,content_sha256,observed_at FROM sentinels WHERE name=?`, name).Scan(&s.Name, &s.ETag, &s.LastModified, &s.ContentSHA256, &observed)
	if err == sql.ErrNoRows {
		return Sentinel{}, false, nil
	}
	if err != nil {
		return Sentinel{}, false, err
	}
	s.ObservedAt, _ = time.Parse(time.RFC3339Nano, observed)
	return s, true, nil
}

func (d *DB) PutSentinel(ctx context.Context, s Sentinel) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.ExecContext(ctx, `INSERT INTO sentinels(name,etag,last_modified,content_sha256,observed_at) VALUES(?,?,?,?,?)
 ON CONFLICT(name) DO UPDATE SET etag=excluded.etag,last_modified=excluded.last_modified,content_sha256=excluded.content_sha256,observed_at=excluded.observed_at`,
		s.Name, s.ETag, s.LastModified, s.ContentSHA256, s.ObservedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (d *DB) RecordRelease(ctx context.Context, p model.Pointer, entries []model.Entry, localAvailable bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO releases(release_id,manifest_key,manifest_sha256,published_at,entry_count,complete) VALUES(?,?,?,?,?,1)`, p.ReleaseID, p.ManifestKey, p.ManifestSHA256, p.PublishedAt.UTC().Format(time.RFC3339Nano), p.EntryCount); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO settings(key,value) VALUES('latest_release',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, p.ReleaseID); err != nil {
		return err
	}
	for _, e := range entries {
		_, err = tx.ExecContext(ctx, `INSERT INTO scores(pgs_id,release_id,genome_build,source_url,source_md5,size_bytes,blob_key,upstream_etag,upstream_last_modified,first_seen_at,last_seen_at,status,license,gcs_generation)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pgs_id) DO UPDATE SET release_id=excluded.release_id,genome_build=excluded.genome_build,source_url=excluded.source_url,source_md5=excluded.source_md5,size_bytes=excluded.size_bytes,blob_key=excluded.blob_key,upstream_etag=excluded.upstream_etag,upstream_last_modified=excluded.upstream_last_modified,first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,status=excluded.status,license=excluded.license,gcs_generation=excluded.gcs_generation`,
			e.PGSID, e.ReleaseID, e.GenomeBuild, e.SourceURL, e.SourceMD5, e.SizeBytes, e.BlobKey, e.UpstreamETag, e.UpstreamLastModified, e.FirstSeenAt.UTC().Format(time.RFC3339Nano), e.LastSeenAt.UTC().Format(time.RFC3339Nano), e.Status, e.License, e.GCSGeneration)
		if err != nil {
			return err
		}
		local := 0
		if localAvailable {
			local = 1
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO objects(blob_key,source_md5,size_bytes,local_available,gcs_generation,verified_at) VALUES(?,?,?,?,?,?) ON CONFLICT(blob_key) DO UPDATE SET local_available=max(local_available,excluded.local_available),gcs_generation=max(gcs_generation,excluded.gcs_generation),verified_at=excluded.verified_at`, e.BlobKey, e.SourceMD5, e.SizeBytes, local, e.GCSGeneration, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) Summary(ctx context.Context) (Summary, error) {
	var s Summary
	_ = d.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key='latest_release'`).Scan(&s.LatestRelease)
	if s.LatestRelease == "" {
		_ = d.db.QueryRowContext(ctx, `SELECT release_id FROM releases WHERE complete=1 ORDER BY published_at DESC LIMIT 1`).Scan(&s.LatestRelease)
	}
	if err := d.db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(status='available'),0),coalesce(sum(status='withdrawn'),0) FROM scores`).Scan(&s.Entries, &s.Available, &s.Withdrawn); err != nil {
		return s, err
	}
	if err := d.db.QueryRowContext(ctx, `SELECT count(*) FROM runs WHERE status IN ('failed','interrupted')`).Scan(&s.FailedRuns); err != nil {
		return s, err
	}
	var finished sql.NullString
	_ = d.db.QueryRowContext(ctx, `SELECT command,status,finished_at FROM runs ORDER BY id DESC LIMIT 1`).Scan(&s.LastRunCommand, &s.LastRunStatus, &finished)
	if finished.Valid {
		if t, err := time.Parse(time.RFC3339Nano, finished.String); err == nil {
			s.LastRunFinished = &t
		}
	}
	return s, nil
}

func (d *DB) Rebuild(ctx context.Context, releases []RebuildRelease) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM scores; DELETE FROM objects; DELETE FROM transfers; DELETE FROM releases; DELETE FROM settings;`); err != nil {
		return err
	}
	for _, rel := range releases {
		p := rel.Pointer
		if _, err = tx.ExecContext(ctx, `INSERT INTO releases(release_id,manifest_key,manifest_sha256,published_at,entry_count,complete) VALUES(?,?,?,?,?,1)`, p.ReleaseID, p.ManifestKey, p.ManifestSHA256, p.PublishedAt.UTC().Format(time.RFC3339Nano), p.EntryCount); err != nil {
			return err
		}
		for _, e := range rel.Entries {
			if _, err = tx.ExecContext(ctx, `INSERT INTO scores(pgs_id,release_id,genome_build,source_url,source_md5,size_bytes,blob_key,upstream_etag,upstream_last_modified,first_seen_at,last_seen_at,status,license,gcs_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pgs_id) DO UPDATE SET release_id=excluded.release_id,genome_build=excluded.genome_build,source_url=excluded.source_url,source_md5=excluded.source_md5,size_bytes=excluded.size_bytes,blob_key=excluded.blob_key,upstream_etag=excluded.upstream_etag,upstream_last_modified=excluded.upstream_last_modified,first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,status=excluded.status,license=excluded.license,gcs_generation=excluded.gcs_generation`, e.PGSID, e.ReleaseID, e.GenomeBuild, e.SourceURL, e.SourceMD5, e.SizeBytes, e.BlobKey, e.UpstreamETag, e.UpstreamLastModified, e.FirstSeenAt.UTC().Format(time.RFC3339Nano), e.LastSeenAt.UTC().Format(time.RFC3339Nano), e.Status, e.License, e.GCSGeneration); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO objects(blob_key,source_md5,size_bytes,gcs_generation) VALUES(?,?,?,?)`, e.BlobKey, e.SourceMD5, e.SizeBytes, e.GCSGeneration); err != nil {
				return err
			}
		}
	}
	if len(releases) > 0 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO settings(key,value) VALUES('latest_release',?)`, releases[len(releases)-1].Pointer.ReleaseID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rebuild: %w", err)
	}
	return nil
}
