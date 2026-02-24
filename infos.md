# Infos Split Specification (Draft v5)

## 1. Objective

Split `import/grinfo/*.jsonl` into:

1. `import/urlinfos/*.jsonl`
2. `import/uploadinfos/*.jsonl`
3. `import/pdfinfos/*.jsonl`

`grinfo` remains read-only during transition and is not auto-deleted by migration.


## 2. Identity And Join

1. Global source identity is `unique_code`.
2. Join field in all split ledgers is `record_key`.
3. `record_key` must be ASCII-only.
4. `record_key` is derived from source `unique_code` using canonicalization rules.
5. If source `unique_code` is non-ASCII/invalid, use deterministic ASCII fallback derivation.
6. Cross-ledger joins are done using `record_key`.


## 3. Partitioning

1. All three namespaces use the same partition key.
2. Partition year is derived from `gr_date` year.
3. Missing/invalid `gr_date` goes to `unknown`.
4. Cross-namespace partition mismatch for same `record_key` is validation error.


## 4. Record Models

### 4.1 URLInfo

Required fields:

1. `record_key`
2. `unique_code`
3. `title`
4. `department_name`
5. `department_code`
6. `gr_date`
7. `source_url`
8. `first_seen_crawl_date`
9. `last_seen_crawl_date`
10. `first_seen_run_type`
11. `created_at_utc`
12. `updated_at_utc`

Rules:

1. `department_code` must be canonical short code (`mahagri`, `mahfin`, etc.).
2. `unique_code` in `urlinfos` stores the canonicalized key value used for joins.


### 4.2 UploadInfo

Required fields:

1. `record_key`
2. `state`
3. `download`
4. `wayback`
5. `archive`
6. `hf`
7. `created_at_utc`
8. `updated_at_utc`

Rules:

1. `state` is top-level workflow field.
2. Do not store `unique_code` in `uploadinfos`.
3. Do not store top-level `attempt_counts`; keep attempts inside each stage object.
4. Do not store temporary local path fields for download stage.
5. Use one persisted storage object (`hf`) for file path/hash/sync metadata.

Recommended object shapes:

1. `download`: `{status, error, attempts}`
2. `wayback`: `{status, url, content_url, archive_time, archive_sha1, archive_length, archive_mimetype, archive_status_code, error, attempts}`
3. `archive`: `{status, identifier, url, error, attempts}`
4. `hf`: `{status, path, hash, backend, commit_hash, error, synced_at_utc, attempts}`

Hash rule:

1. Store hash as `uploadinfos.lfs.hash`.
2. Migration initializes `hf.hash` as `null`.
3. Hash is filled by a later backfill command (not during migration).


### 4.3 PDFInfo

For all rows:

1. `record_key`
2. `status`
3. `created_at_utc`
4. `updated_at_utc`

Rules:

1. Do not store `unique_code` in `pdfinfos`.
2. If `status=not_attempted`, keep only the four fields above.
3. Allowed `status`: `not_attempted`, `success`, `failed`, `missing_pdf`.
4. For non-`not_attempted`, include extracted fields:
   - `error`
   - `file_size`
   - `page_count`
   - `pages_with_images`
   - `has_any_page_image`
   - `total_font_count`
   - `fonts`
   - `unresolved_word_count`
   - `language`
5. For `status=missing_pdf`, extracted metadata fields are `null`.


## 5. Config

File: `import/import_config.yaml`

Defaults:

1. `attempt_thresholds.download: 3`
2. `attempt_thresholds.wayback: 3`
3. `attempt_thresholds.archive: 3`
4. `attempt_thresholds.hf: 3`

Runtime:

1. Per-stage threshold checks.
2. Stage skips at threshold.
3. Commands support ignore-threshold switch.


## 6. Migration Script

Script: `import/src/migrate_infos.py`  
Command: `migrate-infos`

Source:

1. `import/grinfo/*.jsonl` (read-only)

Targets:

1. `import/urlinfos/*.jsonl`
2. `import/uploadinfos/*.jsonl`
3. `import/pdfinfos/*.jsonl`

Behavior:

1. Fail if any target namespace already has JSONL files.
2. One migrated row per source `unique_code` in each namespace.
3. Write ASCII `record_key` in all rows.
4. Preserve timestamps/state/metadata where applicable.
5. If source has no `pdf_info`, write `pdfinfos` row with `status=not_attempted`.
6. Do not copy legacy `download.path`.
7. Do not compute `hf.hash` in migration.
8. Support `--dry-run`.
9. Support `--summary-only`.
10. Do not modify/delete `import/grinfo/*.jsonl`.


## 7. Validation

1. No duplicate `record_key` inside each namespace.
2. `urlinfos.record_key == urlinfos.unique_code`.
3. `record_key` is ASCII-only.
4. Cross-namespace partition consistency.
5. Upload workflow/state consistency.
6. PDF status-shape consistency (`not_attempted` minimal row shape).


## 8. Command Cutover (Later)

1. Migrate first and manually inspect data.
2. Then update existing Python commands to read/write split ledgers.
3. Keep `grinfo` until cutover verification is complete.
