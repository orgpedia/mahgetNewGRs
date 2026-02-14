# Infos Split Specification (Draft v4)

## 1. Objective

Split monolithic rows in `import/grinfo/*.jsonl` into three ledgers:

1. `urlinfos`: source website discovery metadata.
2. `uploadinfos`: processing/upload lifecycle metadata (`download`, `wayback`, `archive`, `lfs`, `hf`).
3. `pdfinfos`: extracted PDF metadata.

`import/grinfo/*.jsonl` remains temporary during transition. It is removed only after migration validation and full command cutover, and not automatically by migration.


## 2. Identity And Join Fields

Each row in all three ledgers must contain:

1. `unique_code` (canonical identity, snake_case).
2. `record_key` (join field; must equal `unique_code`).

Rules:

1. One row per `unique_code` in each namespace.
2. `record_key == unique_code` always.
3. Cross-file joins use `record_key` (or `unique_code` equivalently).


## 3. Directories And Partitioning

Directories:

1. `import/urlinfos/YYYY.jsonl`
2. `import/urlinfos/unknown.jsonl`
3. `import/uploadinfos/YYYY.jsonl`
4. `import/uploadinfos/unknown.jsonl`
5. `import/pdfinfos/YYYY.jsonl`
6. `import/pdfinfos/unknown.jsonl`

Partition policy (all three namespaces):

1. Partition year is derived from `gr_date` year.
2. Missing/invalid `gr_date` goes to `unknown`.
3. Cross-namespace partition mismatch for same `record_key` is a validation error.


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


### 4.2 UploadInfo

Required fields:

1. `record_key`
2. `unique_code`
3. `state`
4. `attempt_counts`
5. `download`
6. `wayback`
7. `archive`
8. `lfs`
9. `hf`
10. `created_at_utc`
11. `updated_at_utc`

Ownership rule:

1. `state` exists only in `uploadinfos`.

`attempt_counts` keys:

1. `download`
2. `wayback`
3. `archive`
4. `lfs`
5. `hf`

`download` object:

1. Tracks attempt status/metadata for download stage.
2. Temporary local path fields must not be persisted.
3. Canonical local PDF location is `uploadinfos.lfs.path`.
4. Recommended fields: `status`, `hash`, `size`, `error` (no `path`).


### 4.3 PDFInfo

Required fields:

1. `record_key`
2. `unique_code`
3. `status`
4. `error`
5. `file_size`
6. `page_count`
7. `pages_with_images`
8. `has_any_page_image`
9. `font_count`
10. `fonts`
11. `unresolved_word_count`
12. `language`
13. `created_at_utc`
14. `updated_at_utc`

Missing PDF rule:

1. If local PDF is unavailable, write `status=missing_pdf`.
2. For `status=missing_pdf`, extracted metadata fields are `null`.

Status defaults:

1. Allowed values: `not_attempted`, `success`, `failed`, `missing_pdf`.
2. For `status=not_attempted`, `error` should be `null`.


## 5. No-Duplication Rule

1. `urlinfos` owns discovery fields.
2. `uploadinfos` owns stage/upload fields and retry counters.
3. `pdfinfos` owns extracted PDF metadata.
4. Shared fields across all three are only identity/join fields: `unique_code`, `record_key`.
5. Temporary/transient runtime values must not be persisted.


## 6. Creation And Updates

1. `urlinfos` row is created at discovery.
2. `uploadinfos` row is created lazily on first stage run.
3. `pdfinfos` row is created lazily on first PDF-info run (or first missing-PDF write).
4. Updates are namespace-local only.
5. JSONL writes must be atomic.


## 7. Attempt Threshold Configuration

Config file: `import/import_config.yaml`

Required keys and defaults:

1. `attempt_thresholds.download: 3`
2. `attempt_thresholds.wayback: 3`
3. `attempt_thresholds.archive: 3`
4. `attempt_thresholds.lfs: 3`
5. `attempt_thresholds.hf: 3`

Runtime behavior:

1. Each stage checks its own threshold from config.
2. Stage is skipped when threshold is reached.
3. Stage commands must support CLI switch to ignore threshold checks.


## 8. Migration Script Specification

Script and command:

1. Script: `import/src/migrate_infos.py`
2. CLI command: `migrate-infos`

Source and targets:

1. Source: `import/grinfo/*.jsonl`
2. Targets:
   - `import/urlinfos/*.jsonl`
   - `import/uploadinfos/*.jsonl`
   - `import/pdfinfos/*.jsonl`

Behavior:

1. If any target namespace already contains JSONL files, migration must fail.
2. Migration is one-row-per-`unique_code` in each namespace.
3. `record_key` is written in all rows and equals `unique_code`.
4. Preserve existing state, attempts, timestamps, and metadata.
5. If source row has no `pdf_info`, still create a `pdfinfos` row with:
   - `status=not_attempted`
   - `error=null`
   - extracted metadata fields as `null`
6. Migration does not delete `import/grinfo/*.jsonl`.
7. Migration command must support:
   - `--dry-run`
   - `--summary-only`
8. Legacy `download.path` from `grinfo` is not copied into `uploadinfos`.

Validation required after migration:

1. `record_key == unique_code` in all three namespaces.
2. No duplicate `unique_code` in any namespace.
3. Cross-namespace partition consistency.
4. State-machine consistency in `uploadinfos`.
5. PDF missing/not-attempted consistency in `pdfinfos`.


## 9. Expected Command Changes (Spec Only)

Yes, commands will need to move to the new ledgers. Expected changes:

1. Discovery/ingestion commands (`baseline-ledger`, `append-ledger`, crawl portions of `daily/monthly`, `job-gr-site`) should write/read `urlinfos` as primary discovery store.
2. Stage execution commands (`job-gr-site` download stage, `job-wayback`, `job-archive`, `weekly` retry paths) should read `urlinfos` + `uploadinfos`, and write only `uploadinfos` for stage transitions.
3. PDF import/backfill commands (`import-pdfs`, `backfill-lfs-path`, `download-pdfs`) should update `uploadinfos.lfs` and related `uploadinfos` stage metadata.
4. `pdf-info` should read local PDF location from `uploadinfos.lfs.path` and write only `pdfinfos`.
5. Validation command should become cross-ledger validation across `urlinfos`, `uploadinfos`, `pdfinfos` instead of single `grinfo`.
6. `status-readme` should compute status by joining the three namespaces on `record_key`.
7. `sync-hf` should sync `import/urlinfos`, `import/uploadinfos`, `import/pdfinfos`, and `LFS/`; `import/grinfo` remains transitional only.
8. Workflow wrappers (`daily`, `weekly`, `monthly`) should orchestrate commands against split ledgers and stop depending on monolithic `grinfo`.


## 10. Final Cutover Rule

1. Keep `import/grinfo/*.jsonl` until:
   - migration completed,
   - cross-ledger validation passes,
   - command cutover completed.
2. Removal of `grinfo` is explicit/manual and not done by migration script automatically.
