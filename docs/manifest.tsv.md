# Manifest TSV

Each release publishes an immutable UTF-8 TSV at `releases/{release_id}/manifest.tsv`. `LATEST.manifest.tsv` is a mutable convenience copy of the current release. The file uses a header row, LF line endings, tab delimiters, and standard CSV quoting with a tab delimiter for fields containing tabs, quotes, or newlines.

Columns, in order:

1. `release_id`
2. `pgs_id`
3. `genome_build`
4. `status`
5. `score_key`
6. `gcs_generation`
7. `source_md5`
8. `size_bytes`
9. `source_url`
10. `upstream_etag`
11. `upstream_last_modified`
12. `first_seen_at`
13. `last_seen_at`
14. `license`
15. `header_inspector_version`
16. `header_status`
17. `header_type`
18. `header_format_version`
19. `header_delimiter`
20. `header_schema_sha256`
21. `header_columns_json`
22. `header_metadata_keys_json`
23. `header_sections_json`
24. `header_comment_lines`
25. `header_warnings_json`
26. `header_error`

The four `*_json` columns contain JSON arrays so nested header observations remain lossless. Header columns are empty when no inspection is recorded. Timestamps use RFC 3339 with nanosecond precision when present.

`LATEST.json` records `manifest_tsv_key` and `manifest_tsv_sha256`. Reproducible consumers should read that immutable key and verify its checksum. A reader using `LATEST.manifest.tsv` can compare the first column with `LATEST.json.release_id`; retry if they differ during recovery from an interrupted publication.
