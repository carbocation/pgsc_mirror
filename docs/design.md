# Mirror design and data integrity

This document describes the preservation and recovery mechanisms behind the basic operating interface. Ordinary operators only need a TOML file and either the one-shot `reconcile` command or the default long-running mode documented in the project README.

## Object layout

The preservation-critical layout is:

```text
blobs/md5/{prefix}/{md5}.txt.gz
releases/{release_id}/manifest.jsonl.gz
releases/{release_id}/metadata/pgs_scores_list.txt
releases/{release_id}/metadata/pgs_all_metadata_scores.csv
LATEST.json
```

Blobs, metadata snapshots, and manifests are immutable. `LATEST.json` is the only mutable publication object.

## Atomic publication

A reconciliation takes a renewable provider-backed lease, verifies the MD5 of each compressed scoring file, uploads missing blobs with create-only preconditions, writes the release snapshot and manifest, and finally advances `LATEST.json` with compare-and-swap.

Any failed expected object leaves the prior release current. If a multi-target run stops after the authoritative pointer advances, the next reconciliation repairs lagging targets before reporting the mirror current.

## Interruption and recovery

SQLite is a disposable operational index, not the canonical mirror. It uses foreign keys, WAL, a busy timeout, and serialized writes and is not intended for GCS FUSE.

Successful checksum-sidecar requests are checkpointed during an interrupted inventory. Verified scoring-file partials remain in `state.work_dir` until every configured target has stored them. Both can be reused after restart when the state database and work directory are on persistent disk.

`rebuild-state` reconstructs SQLite from immutable manifests. Ordinary reconciliation also catches the index up if publication completed before an abrupt shutdown. If the operational state is lost entirely, the mirror remains valid; rebuilding or reconciling may repeat work but does not require trusting an incomplete release.

The continuous service records successful complete reconciliations and verifications in SQLite. On startup it performs an immediate catch-up and runs either task immediately when its configured interval became overdue during downtime.

## Bounded local scratch

For a GCS-only mirror, scoring files are not retained wholesale on local disk. Each worker downloads one compressed file into `state.work_dir`, verifies it, uploads it from that durable file, and removes it before taking more work.

`transfer.file_concurrency` bounds the number of current scoring-file partials. `transfer.max_file_size` is enforced from `Content-Length` when available and again while streaming. On restart, current partials are handled first; obsolete checksum revisions and excess old partials are pruned.

With the example defaults, scoring-file scratch is bounded to roughly two 10-GiB files, plus small operational files and at most one metadata or upload-staging object. When `[targets].local` is enabled, `[local].root` intentionally contains the complete persistent mirror and must be sized accordingly.

## Header inspection

Every new or revised verified scoring file is inspected through its gzip stream before publication. Its manifest entry records the observed format version, delimiter, exact ordered columns, metadata-field names, section headings, a stable schema fingerprint, and a human-readable header type.

Inspection stops at the table header and is bounded to 2 MiB or 10,000 decompressed header lines. A checksum-valid upstream anomaly is preserved byte-for-byte and marked `unrecognized` or `unreadable`; it is not silently normalized or dropped.

Unchanged blobs reuse their versioned observation. A complete reconciliation can backfill an older manifest that lacks a current header observation by reading only the stored blobs' headers and publishing a new immutable release. `LATEST.json` records the completed inspector version so a lightweight update can trigger that one-time backfill when necessary.
