# Mirror design and data integrity

This document describes the preservation and recovery mechanisms behind the basic operating interface. Ordinary operators only need a TOML file and either the one-shot `reconcile` command or the default long-running mode documented in the project README.

## Object layout

The public and preservation-critical layout is:

```text
scores/{pgs_id}_hmPOS_GRCh38.txt.gz
releases/{release_id}/manifest.jsonl.gz
releases/{release_id}/manifest.tsv
releases/{release_id}/metadata/pgs_scores_list.txt
releases/{release_id}/metadata/pgs_all_metadata_scores.csv
LATEST.json
LATEST.manifest.tsv
```

The flat `scores/` namespace is the ordinary consumer interface. Object names are stable and preserve the PGS Catalog basenames. New scores create new names; revisions conditionally replace an existing name and therefore create a new GCS generation. Metadata snapshots and manifests are immutable. `LATEST.json` is the mutable atomic-snapshot pointer.

Each manifest entry records the PGS name, reported and mapped traits, mapped EFO/MONDO identifiers, `score_key`, source MD5, size, and GCS generation. The descriptive fields are joined by PGS ID from the immutable catalog metadata snapshot stored with the release; the complete upstream CSV remains available for development, publication, ancestry, and other catalog fields. Every release has equivalent immutable JSONL and TSV manifests; `LATEST.json` pins both keys and checksums. A consumer that only needs new or revised scoring files can list `scores/` and checkpoint `(name, generation)`. A consumer that needs an exact release reads `LATEST.json` and then the referenced immutable manifest. Historical GCS retrieval requires Object Versioning so recorded older generations remain available; a local-only target represents the current named view. The program accepts only this layout and does not translate manifests or objects from another namespace.

`LATEST.manifest.tsv` is an uncompressed convenience copy of the current release TSV. It is replaced after `LATEST.json` advances, so an interrupted publication can leave it briefly pointing at the preceding release but never at an uncommitted future release. Every data row begins with `release_id`; exact-snapshot consumers compare that value with `LATEST.json` or use the immutable `manifest_tsv_key` directly. The next mutating run repairs a missing or lagging convenience copy.

The mirror also carries one replaceable operational object:

```text
operations/maintenance.json
```

This versioned checkpoint records the last completed checksum-sidecar audit plus the upstream score-list and metadata validators and content hashes. It is advisory rather than preservation-critical: deleting or corrupting it cannot expose an incomplete release, but causes the next process with no newer local schedule to perform a conservative full audit.

## Atomic publication

A reconciliation takes a renewable provider-backed lease, verifies the MD5 of each compressed scoring file, creates a missing score or conditionally replaces a revised score generation, writes the release snapshots and manifests, advances `LATEST.json` with compare-and-swap, and then replaces its convenience TSV copy.

Any failed expected object leaves the prior release current. If a multi-target run stops after the authoritative pointer advances, the next reconciliation repairs lagging targets before reporting the mirror current.

## Interruption and recovery

SQLite is a disposable operational index, not the canonical mirror. It uses foreign keys, WAL, a busy timeout, and serialized writes and is not intended for GCS FUSE. The database has an explicit schema version; an incompatible operational schema is reset and reconstructed from the canonical target rather than translated indefinitely.

Every successful full reconciliation conditionally updates `operations/maintenance.json`, including audits that find no release change. An existing SQLite database can backfill an absent checkpoint without upstream access, but only after its successful-audit sentinels match the current release snapshot hashes. On an empty or older local database, the service validates the shared checkpoint's genome build and snapshot hashes against the current `LATEST.json`, then reconstructs the current release, upstream sentinels, and audit schedule from the canonical target. A current checkpoint leads to the ordinary conditional score-list and metadata check, not one checksum-sidecar request per score. The audit is skipped only when upstream confirms both documents are unmodified; a `200` response is conservatively treated as a change signal even when its body matches the stored snapshot, because scoring-file revisions need not alter those documents. Missing, malformed, future-dated, snapshot-inconsistent, or overdue state also falls back to a full audit. A newer checkpoint format stops an older binary rather than letting it overwrite semantics it does not understand.

Successful checksum-sidecar requests are checkpointed during an interrupted inventory. Verified scoring-file partials remain in `state.work_dir` until every configured target has stored them. Both can be reused after restart when the state database and work directory are on persistent disk.

`rebuild-state` reconstructs SQLite from immutable manifests. Ordinary reconciliation also catches the index up if publication completed before an abrupt shutdown. If the operational state is lost entirely, the mirror remains valid; rebuilding or reconciling may repeat work but does not require trusting an incomplete release.

The continuous service records successful complete reconciliations in SQLite. On startup it performs an immediate catch-up and runs a full reconciliation when its configured interval became overdue during downtime. Scoring-file verification is an explicit diagnostic operation, not scheduled maintenance: ingestion already validates the upstream MD5, GCS uploads include a server-validated CRC32C, and routine reads would duplicate the storage provider's integrity work.

## Multiple processes and writer coordination

When GCS is enabled it is the authoritative target and is ordered first. Reconciliation acquires `leases/reconcile.json` under the configured GCS prefix with a does-not-exist precondition. The holder renews that object with generation-match writes and deletes the current generation when finished. A contender does not enter reconciliation while an unexpired lease is present.

The lease coordinates publication work, not the complete lifetime of the service. Separate processes can both perform read-only sentinel checks. Full-audit scheduling, however, is portable: a contender rereads the shared maintenance checkpoint before scheduled work and adopts a newer completed audit instead of repeating it after the lease holder finishes. Multiple active services are therefore publication-safe and sidecar-audit-aware, but still waste lightweight checks. One active service and a stopped standby remain the recommended arrangement.

After a hard failure, a contender can conditionally remove an expired lease generation and acquire a new lease. If a former holder attempts to renew a stale generation, renewal fails and its reconciliation context is canceled. Conditional score-generation writes, immutable release objects, and compare-and-swap updates to `LATEST.json` remain the final publication safeguards.

## Bounded local scratch

For a GCS-only mirror, scoring files are not retained wholesale on local disk. Each worker downloads one compressed file into `state.work_dir`, verifies it, uploads it under its flat source filename, and removes it before taking more work.

`transfer.file_concurrency` bounds the number of current scoring-file partials. `transfer.max_file_size` is enforced from `Content-Length` when available and again while streaming. On restart, current partials are handled first; obsolete checksum revisions and excess old partials are pruned.

With the example defaults, scoring-file scratch is bounded to roughly two 10-GiB files, plus small operational files and at most one metadata or upload-staging object. When `[targets].local` is enabled, `[local].root` intentionally contains the complete persistent mirror and must be sized accordingly.

## Header inspection

Every new or revised verified scoring file is inspected through its gzip stream before publication. Its manifest entry records the observed format version, delimiter, exact ordered columns, metadata-field names, section headings, a stable schema fingerprint, and a human-readable header type.

The classifier accepts both the usual single `effect_weight` representation and the complete `dosage_0_weight`/`dosage_1_weight`/`dosage_2_weight` representation documented for non-additive PGS Catalog scores. These share the same broad formatted or harmonized header type; their exact ordered schemas remain distinguishable by `schema_sha256`.

Inspection stops at the table header and is bounded to 2 MiB or 10,000 decompressed header lines. A checksum-valid upstream anomaly is preserved byte-for-byte and marked `unrecognized` or `unreadable`; it is not silently normalized or dropped.

Unchanged scoring files reuse their versioned observation. `LATEST.json` records the completed inspector version so the service can detect older observations.

## Stored-object annotation

`annotate` refreshes versioned descriptive metadata exclusively from objects already owned by the mirror. It pins the current manifest release, reads outdated scoring-file headers only when necessary, joins catalog phenotype fields from the stored metadata snapshot, and publishes a new immutable manifest and pointer only after every required annotation succeeds. It never calls the PGS Catalog inventory, metadata, sidecar, or scoring-file endpoints.

Header inspection is the first annotation implemented through this path. An older release with absent or stale header observations can therefore be upgraded without a complete upstream reconciliation. The long-lived service performs this refresh automatically before its normal lightweight update check when the current pointer advertises an older inspector version. A newer binary may publish an annotation-only successor whose raw score MD5s and stored upstream snapshots are identical to its predecessor.

Dry-run mode performs the stored reads and reports the result but acquires no lease and writes no objects. Interactive annotation commands report periodic progress on stderr and include complete observations for every unrecognized, unreadable, or warning-bearing header in the final human or JSON report. A real run uses the same renewable publication lease as reconciliation, repairs lagging configured targets first, and advances pointers with compare-and-swap. Any unreadable gzip is recorded as an explicit descriptive result; an operational read or publication failure leaves the prior release current. A binary refuses to publish over annotation data produced by a newer inspector version.

This mechanism is deliberately limited to reproducible, mirror-level descriptions of preserved objects. It does not normalize variants, rewrite scoring files, liftover coordinates, or create analytical derivatives; those belong in a downstream compiler.
