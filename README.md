# pgsc-mirror

`pgsc-mirror` is a public, provider-neutral Go application for maintaining verified local and/or Google Cloud Storage mirrors of PGS Catalog scoring files harmonized to GRCh38. It preserves byte-identical upstream gzip files, publishes deterministic immutable release manifests, and advances a small `LATEST.json` pointer only after every expected object is present.

This project is independent and is not affiliated with or endorsed by EMBL-EBI, the PGS Catalog, or its funders. PGS Catalog score licenses vary. The mirror records license metadata but does not replace the upstream license terms; operators and downstream users remain responsible for complying with them.

## Safety model

The preservation-critical layout is:

```text
blobs/md5/{prefix}/{md5}.txt.gz
releases/{release_id}/manifest.jsonl.gz
releases/{release_id}/metadata/pgs_scores_list.txt
releases/{release_id}/metadata/pgs_all_metadata_scores.csv
LATEST.json
```

Blobs, metadata snapshots, and manifests are immutable. `LATEST.json` is the only mutable publication object. A reconciliation takes a renewable provider-backed lease, verifies the MD5 of each compressed scoring file, uploads all missing blobs with create-only preconditions, writes the release snapshot and manifest, and finally advances `LATEST.json` with compare-and-swap. Any failed expected object leaves the prior release current. If a multi-target run stops after the authoritative pointer advances, the next reconciliation repairs lagging targets before reporting the mirror current.

SQLite is only a disposable operational index. It uses foreign keys, WAL, a busy timeout, and serialized writes; it is never intended for GCS FUSE. Successful checksum sidecars are checkpointed there during an interrupted inventory, while verified scoring-file partials remain in the configured work directory until every target has stored them. Both are reused after restart. `rebuild-state` reconstructs SQLite from immutable manifests, and ordinary reconciliation automatically catches the index up if publication completed before an abrupt shutdown.

For a GCS-only mirror, scoring files are never cloned wholesale to local disk. Each file worker downloads one compressed file into `state.work_dir`, verifies it, uploads it directly from that durable file, and removes it before taking more work. `transfer.file_concurrency` bounds the number of retained current partials and `transfer.max_file_size` is enforced both from `Content-Length` and while streaming. On restart, current partials are handled first; obsolete checksum revisions and excess older partials are pruned. With the example defaults, scoring-file scratch is therefore bounded to roughly two 10-GiB files, plus small operational files and at most one metadata/upload staging object. Enabling `targets.local` is different by design: `local.root` then contains a complete persistent mirror and must be sized accordingly.

## Build

Go 1.25 or newer is required. The release binary is static and never uses CGo:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags="-s -w" \
  -o pgsc-mirror.linux ./cmd/pgsc-mirror
```

`make linux` produces the same `.linux`-suffixed artifact. The provided multi-stage `Dockerfile` produces a non-root minimal image with CA certificates.

## Configure

Copy [`config.example.toml`](config.example.toml) and change deployment-specific values. No credentials belong in this file. GCS uses Application Default Credentials and supports requester-pays billing projects.

```bash
cp config.example.toml config.toml
./pgsc-mirror.linux --config config.toml plan
```

Important settings include:

- `upstream.base_urls`: ordered HTTPS/HTTP sources. Requests fall back across this list with bounded exponential backoff, jitter, and `Retry-After` support. Version 1 accepts HTTP(S) URLs; the configured HTTP endpoint is the fallback for intermittent HTTPS issues.
- `identity.user_agent` and `identity.contact`: identify the operator responsibly to the public upstream.
- `transfer.concurrency`: bounded checksum-sidecar concurrency; the default is four.
- `transfer.file_concurrency`: concurrent scoring-file download/upload pipelines; the default is two.
- `transfer.max_file_size`: hard compressed-file scratch limit per pipeline. An oversized or misleading response fails reconciliation without advancing the release.
- `transfer.lease_duration`: takeover delay after a hard stop. The lease is renewed during active work, so this can stay short without limiting run duration.
- `transfer.sidecar_limit`: a development-only plan cap. Mutating commands refuse to publish if it would truncate the upstream inventory.
- `targets.local` / `targets.gcs`: enable either or both stores. With both enabled, GCS is authoritative and its pointer advances first.
- `state.work_dir`: durable local scratch for resumable scoring downloads and provider upload staging. Keep it on persistent disk; it defaults beside `state.path`.
- `state.checkpoint_max_age`: maximum age for resuming an interrupted checksum inventory. A completed reconciliation consumes its checkpoint, so the next full audit still refetches every sidecar.
- `retention.missing_grace`: withdrawn IDs and unreferenced data remain protected until this period expires.

All institution-specific paths, projects, buckets, prefixes, billing projects, contacts, and identities live in TOML configuration.

## Commands

| Command | Behavior |
|---|---|
| `probe` | Checks only explicitly named IDs with HEAD, a streaming size cap, and compressed-byte MD5 verification; optionally runs a self-cleaning GCS conditional-write smoke test. |
| `plan` | Read-only upstream inventory and proposed changes. |
| `reconcile` | Full checksum-sidecar audit and repair. Detects in-place score revisions. |
| `update` | Cheap conditional score-list/metadata check, then reconciliation only when a sentinel changes. |
| `pull` | Pins the local target to the completed release currently published in GCS. |
| `verify` | Verifies manifest SHA-256 and a deterministic sample of blob size/MD5; use `--full` for all blobs. |
| `status` | Reports target pointers, score counts, withdrawals, failed runs, and the last operational run. |
| `rebuild-state` | Reconstructs SQLite exclusively from immutable release manifests. |
| `gc` | Conservative retention report. It is a dry run unless `--apply` is explicit. |

Every command supports `--config`; reports support `--json`. Mutating commands support `--dry-run`. `gc` deliberately uses the separate `--apply` switch.

`probe` is the bounded preflight command. Supply one or more repeatable `--pgs-id` flags and a positive `--max-size` such as `100MiB`. It fetches the score list once, never fetches the bulk metadata CSV, and removes every temporary scoring file after checking the compressed bytes. A known or streamed size above the cap, a missing ID, an incomplete response, or an MD5 mismatch makes the probe fail after it has reported all requested IDs. `--gcs-smoke-test` runs only after every upstream check succeeds; it creates, reads, conditionally replaces, tests a stale generation, and deletes one `probes/<run-id>.json` object. Use `--report` with a JSON filename or an existing directory.

`update` cannot detect a scoring-file revision when neither sentinel changes. Schedule a periodic full `reconcile` independently (for example, weekly) in addition to more frequent `update` runs. Scheduling stays outside the binary: cron, systemd timers, Cloud Run Jobs, Kubernetes Jobs, and equivalent one-shot systems all work.

## Withdrawals and garbage collection

An ID absent from the current score list is marked `withdrawn`, never immediately deleted. Old manifests and content remain immutable and readable. `gc` protects the current release, the configured number of recent releases, all releases inside the grace period, and every blob referenced by any protected release. Review its output before using `--apply`.

## Development and tests

The default test suite uses only tiny `httptest` gzip fixtures; it never contacts PGS Catalog or GCS. It covers parsing, deterministic manifests, additions, in-place revisions, withdrawals, restorations, checksum failures, interrupted/resumed transfers, reuse of verified downloads after upload failure, inventory checkpoints, renewable leases, cross-target pointer recovery, provider staging cleanup, retry behavior, local conditional updates, and SQLite loss/reconstruction.

```bash
go test ./...
go test -race ./...
```

The race detector may use the platform C toolchain internally; production binaries and application dependencies remain CGo-free. Optional GCS integration tests run only when `PGSC_MIRROR_GCS_TEST_BUCKET` is set. During initial development no complete PGS Catalog inventory or production mirror run is performed.

## Releases

GoReleaser configuration builds reproducible Linux, macOS, and Windows archives for amd64 and arm64, emits SHA-256 checksums, and generates archive SBOMs with Syft. The application itself is licensed under the permissive MIT License.
