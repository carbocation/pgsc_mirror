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

## Multiple processes and writer coordination

When GCS is enabled it is the authoritative target and is ordered first. Reconciliation acquires `leases/reconcile.json` under the configured GCS prefix with a does-not-exist precondition. The holder renews that object with generation-match writes and deletes the current generation when finished. A contender does not enter reconciliation while an unexpired lease is present.

The lease coordinates publication work, not the complete lifetime of the service. Separate processes retain separate SQLite scheduling history and can both perform read-only sentinel checks and verification. After the lease holder finishes, a contender with stale local state may acquire the lease and repeat an otherwise unnecessary upstream audit. For this reason, multiple active services sharing a mirror are publication-safe but are not the recommended high-availability arrangement. Use one active service and a stopped standby when avoiding duplicate upstream traffic matters.

After a hard failure, a contender can conditionally remove an expired lease generation and acquire a new lease. If a former holder attempts to renew a stale generation, renewal fails and its reconciliation context is canceled. Immutable create-only objects and compare-and-swap updates to `LATEST.json` remain the final publication safeguards.

## Bounded local scratch

For a GCS-only mirror, scoring files are not retained wholesale on local disk. Each worker downloads one compressed file into `state.work_dir`, verifies it, uploads it from that durable file, and removes it before taking more work.

`transfer.file_concurrency` bounds the number of current scoring-file partials. `transfer.max_file_size` is enforced from `Content-Length` when available and again while streaming. On restart, current partials are handled first; obsolete checksum revisions and excess old partials are pruned.

With the example defaults, scoring-file scratch is bounded to roughly two 10-GiB files, plus small operational files and at most one metadata or upload-staging object. When `[targets].local` is enabled, `[local].root` intentionally contains the complete persistent mirror and must be sized accordingly.

## Header inspection

Every new or revised verified scoring file is inspected through its gzip stream before publication. Its manifest entry records the observed format version, delimiter, exact ordered columns, metadata-field names, section headings, a stable schema fingerprint, and a human-readable header type.

Inspection stops at the table header and is bounded to 2 MiB or 10,000 decompressed header lines. A checksum-valid upstream anomaly is preserved byte-for-byte and marked `unrecognized` or `unreadable`; it is not silently normalized or dropped.

Unchanged blobs reuse their versioned observation. `LATEST.json` records the completed inspector version so the service can detect older observations.

## Stored-object annotation

`annotate` refreshes versioned descriptive metadata exclusively from objects already owned by the mirror. It pins the current immutable release, reads its stored blobs and metadata snapshots, and publishes a new immutable manifest and pointer only after every required inspection succeeds. It never calls the PGS Catalog inventory, metadata, sidecar, or scoring-file endpoints.

Header inspection is the first annotation implemented through this path. An older release with absent or stale header observations can therefore be upgraded without a complete upstream reconciliation. The long-lived service performs this refresh automatically before its normal lightweight update check when the current pointer advertises an older inspector version. A newer binary may publish an annotation-only successor whose raw blob MD5s and stored upstream snapshots are identical to its predecessor.

Dry-run mode performs the stored reads and reports the result but acquires no lease and writes no objects. A real run uses the same renewable publication lease as reconciliation, repairs lagging configured targets first, and advances pointers with compare-and-swap. Any unreadable gzip is recorded as an explicit descriptive result; an operational read or publication failure leaves the prior release current. A binary refuses to publish over annotation data produced by a newer inspector version.

This mechanism is deliberately limited to reproducible, mirror-level descriptions of preserved objects. It does not normalize variants, rewrite scoring files, liftover coordinates, or create analytical derivatives; those belong in a downstream compiler.
