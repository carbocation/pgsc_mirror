# Manifest TSV

Each release publishes an immutable UTF-8 TSV at `releases/{release_id}/manifest.tsv`. `LATEST.manifest.tsv` is a mutable convenience copy of the current release. The file uses a header row, LF line endings, tab delimiters, and standard CSV quoting with a tab delimiter for fields containing tabs, quotes, or newlines.

Columns, in order:

1. `release_id`
2. `pgs_id`
3. `pgs_name`
4. `trait_reported`
5. `trait_mapped`
6. `trait_efo`
7. `release_date`
8. `genome_build`
9. `status`
10. `score_key`
11. `gcs_generation`
12. `source_md5`
13. `size_bytes`
14. `source_url`
15. `upstream_etag`
16. `upstream_last_modified`
17. `first_seen_at`
18. `last_seen_at`
19. `license`
20. `header_inspector_version`
21. `header_status`
22. `header_type`
23. `header_format_version`
24. `header_delimiter`
25. `header_schema_sha256`
26. `header_columns_json`
27. `header_metadata_keys_json`
28. `header_sections_json`
29. `header_comment_lines`
30. `header_warnings_json`
31. `header_error`

`pgs_name`, `trait_reported`, `trait_mapped`, `trait_efo`, and `release_date` come from the release's preserved PGS Catalog metadata snapshot. `release_date` is the catalog's per-score release date in `YYYY-MM-DD` form; the mirror release timestamp remains `LATEST.json.published_at`. Multiple mapped traits and identifiers retain the upstream delimiter and order. The four `*_json` columns contain JSON arrays so nested header observations remain lossless. Header columns are empty when no inspection is recorded. Timestamps use RFC 3339 with nanosecond precision when present.

`LATEST.json` records `manifest_tsv_key` and `manifest_tsv_sha256`. Reproducible consumers should read that immutable key and verify its checksum. A reader using `LATEST.manifest.tsv` can compare the first column with `LATEST.json.release_id`; retry if they differ during recovery from an interrupted publication.
