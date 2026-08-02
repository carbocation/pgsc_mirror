# Manifest TSV

Each release publishes an immutable UTF-8 TSV at `releases/{release_id}/manifest.tsv`. `LATEST.manifest.tsv` is a mutable convenience copy of the current release. The file uses a header row, LF line endings, tab delimiters, and standard CSV quoting with a tab delimiter for fields containing tabs, quotes, or newlines.

Columns, in order:

1. `release_id`
2. `pgs_id`
3. `pgs_name`
4. `trait_reported`
5. `trait_mapped`
6. `trait_efo`
7. `genome_build`
8. `status`
9. `score_key`
10. `gcs_generation`
11. `source_md5`
12. `size_bytes`
13. `source_url`
14. `upstream_etag`
15. `upstream_last_modified`
16. `first_seen_at`
17. `last_seen_at`
18. `license`
19. `header_inspector_version`
20. `header_status`
21. `header_type`
22. `header_format_version`
23. `header_delimiter`
24. `header_schema_sha256`
25. `header_columns_json`
26. `header_metadata_keys_json`
27. `header_sections_json`
28. `header_comment_lines`
29. `header_warnings_json`
30. `header_error`

`pgs_name`, `trait_reported`, `trait_mapped`, and `trait_efo` come from the release's preserved PGS Catalog metadata snapshot. Multiple mapped traits and identifiers retain the upstream delimiter and order. The four `*_json` columns contain JSON arrays so nested header observations remain lossless. Header columns are empty when no inspection is recorded. Timestamps use RFC 3339 with nanosecond precision when present.

`LATEST.json` records `manifest_tsv_key` and `manifest_tsv_sha256`. Reproducible consumers should read that immutable key and verify its checksum. A reader using `LATEST.manifest.tsv` can compare the first column with `LATEST.json.release_id`; retry if they differ during recovery from an interrupted publication.
