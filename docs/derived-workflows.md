# Derived workflows

`pgsc-mirror` is the preservation layer. It publishes byte-identical PGS Catalog files and enough immutable metadata for another tool to determine exactly what it consumed. Analytical products belong in a separate layer.

## Public contract

A consumer resolves `LATEST.json`, pins the referenced immutable manifest, and reads its gzip-compressed JSONL entries. The schemas are documented in [`latest.schema.json`](latest.schema.json) and [`manifest.schema.json`](manifest.schema.json). Consumers should retain at least:

- the input mirror release ID;
- the score's PGS ID and source MD5;
- the score key and GCS generation;
- the header inspector version, type, and schema SHA-256;
- the derivative's own checksum, status, and tool version.

The source MD5 is the work identity. A PGS ID whose MD5 is already complete under the same derivative tool/configuration version does not need to be processed again. A changed MD5 is new work even when the PGS ID is unchanged. Withdrawn entries remain historical inputs and should be retained according to the derivative product's own policy.

A mirror release can also change solely because `pgsc-mirror` refreshed a versioned annotation. Such a successor may contain the same raw MD5s and upstream metadata snapshots as its predecessor but a newer manifest annotation. Consumers should therefore diff work by source MD5 and their own tool/configuration version, while separately deciding whether a new annotation version changes validation or quarantine decisions.

Header `type` is a broad, human-readable classification. `schema_sha256` distinguishes exact ordered column and metadata-key variants within that type. Consumers that require particular fields should validate `columns` directly rather than assuming that every file sharing a type has identical optional columns. An `unrecognized` or `unreadable` observation is explicit quarantine input, not permission to guess.

## Private continuous jobs

A private derivative pipeline should maintain its own state database and storage prefix. A typical run:

1. Resolves and pins the public mirror release.
2. Diffs manifest entries against derivative state using source MD5 plus the derivative tool/configuration version.
3. Reads the pinned score generations without modifying them.
4. Writes immutable, checksum-addressed derivative objects.
5. Publishes an immutable derivative manifest containing input provenance.
6. Advances its own pointer only after all required outputs are complete.

This mirrors the raw publisher's restart and atomicity properties while keeping failures in an analytical transformation from blocking preservation of new upstream data. A one-off utility can use the same contract without publishing a release.

## Deliberate non-features

The public mirror does not define institution-specific variant IDs, normalized schemas, private buckets, scheduler identities, callbacks, or executable plugins. Those choices can change independently without destabilizing the preservation path or imposing one institution's conventions on public users.
