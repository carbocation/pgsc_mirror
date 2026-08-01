# pgsc-mirror

`pgsc-mirror` maintains a verified local and/or Google Cloud Storage mirror of PGS Catalog scoring files harmonized to GRCh38. The mirrored gzip files remain byte-identical to upstream.

## Quick start

Start with the example configuration:

```bash
cp config.example.toml config.toml
```

Edit `config.toml` to select a local mirror, GCS mirror, or both. For real synchronization, make sure `transfer.sidecar_limit = 0`.

### Perform the first synchronization and exit

```bash
./pgsc-mirror.linux --config config.toml reconcile
```

This downloads and verifies the current scores, publishes a complete mirror release, and exits. If it is interrupted, run the same command again.

### Keep the mirror updated continuously

```bash
./pgsc-mirror.linux --config config.toml
```

This is the normal long-lived mode. It catches up immediately, then keeps checking and maintaining the mirror until stopped. A separate first synchronization is optional: the long-lived command initializes an empty mirror itself.

You can stop it with `Ctrl+C`, reboot the machine, or leave it down for several days. Starting the same command again resumes durable partial work where possible and catches up safely. Keep `state.path` and `state.work_dir` on persistent disk to retain that progress.

By default, the running process:

- Checks for upstream changes every 6 hours.
- Performs a complete reconciliation every 7 days.
- Verifies a deterministic sample every 24 hours.
- Retries failed maintenance after 5 minutes without exiting.

These defaults can be changed in TOML:

```toml
[service]
update_interval = "6h"
reconcile_interval = "168h"
verify_interval = "24h"
error_backoff = "5m"
```

## What must be kept locally?

If GCS is enabled, no local runtime file is required to preserve the published mirror. The canonical data is the object set under the configured GCS bucket and prefix.

| Local item | What happens if it is lost? |
|---|---|
| `config.toml` | The mirror remains valid, but recreating the exact bucket, prefix, paths, and operating policy becomes harder. Back this file up. It contains no credentials. |
| `state.path` | Run history, maintenance timestamps, and transfer checkpoints are lost. The next start performs extra reconciliation and rebuilds operational state from the published mirror. |
| `state.work_dir` | Incomplete downloads and staging files are lost and must be transferred again. Published objects are unaffected. |
| Binary and logs | They can be replaced or rebuilt and are not part of the mirror. |
| `local.root` | Critical when this is a local-only mirror. When GCS is also enabled, it is a replaceable local copy and GCS is authoritative. |

Therefore, for a GCS-backed mirror, back up the TOML for operational reproducibility and protect the GCS objects themselves. Keeping `state.path` and `state.work_dir` on persistent disk avoids wasted work but is not required to preserve already published data.

## Running more than one copy

Two processes pointed at the exact same GCS bucket **and prefix** will not intentionally reconcile or publish at the same time. Reconciliation uses a renewable conditional-write lease stored in GCS. One process acquires it; the other reports that the lease is held and, in long-running mode, retries later. Immutable object writes and compare-and-swap publication provide additional protection.

This is safe for publication correctness, but two permanently active processes are wasteful. Each machine has its own SQLite schedule, so both still perform read-only update checks and verification, and the second process may repeat a full upstream audit after the first releases the lease. That creates unnecessary upstream traffic.

The recommended arrangement is one active long-running process and, if desired, a second installed but stopped standby. After a graceful shutdown the lease is released immediately. After a hard failure, the standby can take over when the renewable lease expires; the example configuration uses 15 minutes.

## Configuration

See [`config.example.toml`](config.example.toml) for a complete configuration. The principal deployment settings are:

- `[targets]`, `[local]`, and `[gcs]`: where the mirror is stored.
- `[identity]`: the operator identity sent to the upstream service.
- `[transfer]`: concurrency, retry, scratch-size, and lease limits.
- `[state]`: the SQLite operational index and durable download workspace.
- `[service]`: the long-running maintenance schedule.

No credentials belong in TOML. GCS uses Application Default Credentials. All institution-specific paths, buckets, prefixes, projects, contacts, and identities are configurable rather than compiled into the program.

## Common commands

| Command | Behavior |
|---|---|
| no command, or `run` | Continuously catches up and maintains the mirror until stopped. |
| `reconcile` | Performs a complete checksum audit and repair, then exits. |
| `status` | Reports mirror pointers, score counts, withdrawals, and recent run status. |
| `verify` | Verifies a deterministic sample; add `--full` to verify every blob. |
| `plan` | Reads the upstream inventory and reports proposed changes without publishing. |
| `update` | Performs one lightweight change check and reconciles if needed. |
| `pull` | Makes a local target match the completed release currently published in GCS. |
| `rebuild-state` | Reconstructs SQLite from immutable release manifests. |
| `gc` | Reports conservative retention candidates; deletion requires `--apply`. |
| `probe` | Checks only explicitly named scores and can exercise GCS conditional writes. |

Every command accepts `--config`. Reports accept `--json`. The explicit long-lived form is `pgsc-mirror --config config.toml run`.

## Build

Go 1.25 or newer is required. The release binary is static and does not use CGo:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags="-s -w" \
  -o pgsc-mirror.linux ./cmd/pgsc-mirror
```

`make linux` produces the same `.linux` artifact. The multi-stage `Dockerfile` produces a non-root minimal image with CA certificates.

## Design and data integrity

Published releases are immutable and become current only after every expected object is present, so an interruption cannot expose a partial release. Scoring files are verified over their compressed bytes and preserved without normalization. New and revised files also receive a bounded header inspection whose descriptive schema observation is stored in the manifest.

The object layout, atomic publication process, restart behavior, bounded scratch model, and header inspection rules are documented in [`docs/design.md`](docs/design.md).

Institution-specific transformations and derived near-copies should live outside this preservation mirror. [`docs/derived-workflows.md`](docs/derived-workflows.md) describes the immutable release contract that downstream jobs can consume.

Withdrawn IDs are recorded rather than immediately deleted. `gc` protects the current release, recent releases, releases inside the configured grace period, and every blob they reference.

## Development and tests

The normal test suite uses tiny synthetic fixtures and does not contact PGS Catalog or GCS:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Optional GCS integration tests run only when `PGSC_MIRROR_GCS_TEST_BUCKET` is set.

## License and data terms

The software is available under the MIT License. PGS Catalog score licenses vary; the mirror records their license metadata but does not replace upstream terms. Operators and downstream users remain responsible for complying with those terms.

This project is independent and is not affiliated with or endorsed by EMBL-EBI, the PGS Catalog, or its funders.
