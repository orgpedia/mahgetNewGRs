# Unified Maharashtra GR Pipeline Specification

## 1. Objective

Build a new repository that combines capabilities of `mahgetGR` and `mahgetAllGR` into a single state-driven pipeline using yearly JSONL ledgers.

The system must:

1. Discover GR infos from `gr.maharashtra.gov.in`.
2. Persist each GR as a canonical record keyed by `unique_code`.
3. Progress records through download, wayback, and archive stages.
4. Support local execution via `make` and automated execution via GitHub Actions.
5. Sync updated ledger data and `LFS/` artifacts to a fixed Hugging Face dataset path after each job commit.
6. Implement all workflow logic as importable command objects with thin CLI wrappers so local testing and module reuse share the same execution path.
7. Maintain an auto-generated repository status table in `README.md` after each run-oriented command.


## 2. Canonical Identity And Partitioning

1. Canonical ID: `unique_code`.
2. Ledger partitioning key: `gr_date` year.
3. Ledger files:
   - `import/grinfo/YYYY.jsonl` for valid `gr_date`.
   - `import/grinfo/unknown.jsonl` if `gr_date` is missing or invalid.
4. JSONL files are plain text and must not be gzipped.


## 3. Record Model (`GRInfo`)

Minimum required fields:

1. `unique_code` (string, primary key)
2. `title` (string)
3. `department_name` (string)
4. `department_code` (string)
5. `gr_date` (string: `YYYY-MM-DD` when valid)
6. `source_url` (string, PDF URL from source site)
7. `lfs_path` (string or null; canonical local PDF path under `LFS/...` when file exists)
8. `state` (enum from section 4)
9. `attempt_counts` (object: `download`, `wayback`, `archive`)
10. `download` (object: path/status/hash/size/error)
11. `wayback` (object: archive metadata/status/error)
12. `archive` (object: identifier/url/status/error)
13. `first_seen_crawl_date` (date)
14. `last_seen_crawl_date` (date)
15. `first_seen_run_type` (`daily` or `monthly`)
16. `created_at_utc` (timestamp)
17. `updated_at_utc` (timestamp)

Field semantics for crawl-history fields:

1. `first_seen_crawl_date`:
   - The crawl date on which this `unique_code` is first discovered and inserted into ledger.
2. `last_seen_crawl_date`:
   - The latest monthly full-reconciliation crawl date on which this `unique_code` is observed.
   - This field is updated only during monthly crawl reconciliation.
3. `first_seen_run_type`:
   - The run type that first inserted the record (`daily` or `monthly`).
   - This field is immutable after first insert.
4. Initialization/update rule:
   - On first insert, set `first_seen_crawl_date == last_seen_crawl_date`.
   - In later runs, only monthly reconciliation may advance `last_seen_crawl_date`.


## 4. Normalized States

Use these exact constants:

1. `FETCHED`
2. `DOWNLOAD_SUCCESS`
3. `DOWNLOAD_FAILED`
4. `WAYBACK_UPLOADED`
5. `WAYBACK_UPLOAD_FAILED`
6. `ARCHIVE_UPLOADED_WITH_WAYBACK_URL`
7. `ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL`
8. `ARCHIVE_UPLOADED_WITHOUT_DOCUMENT`


## 5. State Progression Rules

1. `FETCHED -> DOWNLOAD_SUCCESS` when PDF download succeeds.
2. `FETCHED -> DOWNLOAD_FAILED` when PDF download fails.
3. `DOWNLOAD_SUCCESS -> WAYBACK_UPLOADED` when wayback succeeds.
4. `DOWNLOAD_SUCCESS -> WAYBACK_UPLOAD_FAILED` when wayback fails.
5. `WAYBACK_UPLOADED -> ARCHIVE_UPLOADED_WITH_WAYBACK_URL` when archive upload succeeds with wayback URL.
6. `WAYBACK_UPLOAD_FAILED -> ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL` when archive upload succeeds without wayback URL.
7. `DOWNLOAD_FAILED -> ARCHIVE_UPLOADED_WITHOUT_DOCUMENT` when archive metadata-only fallback is used.
8. `ARCHIVE_UPLOADED_WITHOUT_DOCUMENT` is not terminal. Weekly retry may:
   - re-attempt download,
   - and if download succeeds, upload document to the same existing archive identifier/item.


## 6. Retry And Failure Policy

1. Stage retry policy: exactly one retry per stage (max 2 attempts total including first attempt).
2. Service-failure safeguard: if 10 consecutive service failures occur in a stage job, stop that stage early.
3. Remaining unprocessed records are left for later runs.


## 7. Crawl/Processing Schedules

### 7.1 Daily

Goal: only process newly discovered GRs.

1. Crawl department-wise (not date-range strategy).
2. Add only records whose `unique_code` does not exist in ledger.
3. Daily crawl early-stop rule:
   - stop on first page containing any known `unique_code`.
4. After discovering new records, process only those new records through:
   - download stage,
   - wayback stage,
   - archive stage.

### 7.2 Weekly

Goal: retry incomplete/failed processing only (no new crawling).

1. Do not crawl GR listings.
2. Retry only records with failed/incomplete stage outcomes:
   - `DOWNLOAD_FAILED`,
   - `WAYBACK_UPLOAD_FAILED`,
   - archive-failed/incomplete cases,
   - `ARCHIVE_UPLOADED_WITHOUT_DOCUMENT` (attempt document recovery and same-item archive update).

### 7.3 Monthly

Goal: full discovery reconciliation.

1. Crawl full site department-wise.
2. Compare crawled records against ledger by `unique_code`.
3. Add only missing records as `FETCHED`.
4. Do not run wayback/archive in monthly.
5. Crawl date metadata behavior:
   - For newly inserted records: set both `first_seen_crawl_date` and `last_seen_crawl_date` to the current monthly crawl date.
   - For already-known records that are seen in monthly crawl: update only `last_seen_crawl_date` to the current monthly crawl date.


## 8. LFS And Repository Layout

1. PDFs:
   - `LFS/pdfs/{department_code}/{YYYY-MM}/{unique_code}.pdf`
2. HTML snapshots (every GitHub Action run):
   - `LFS/html/{workflow}/{YYYY-MM-DD}/{run_id}/...`
3. Run logs/artifacts:
   - `LFS/runs/{workflow}/{run_id}/...`
4. Repository layout should follow `mahgetGR` conventions while adding v1 ledger/LFS requirements:
   - `import/src/` for CLI entry and importable command modules.
   - `import/grinfo/` for canonical yearly JSONL ledgers (source of truth).
   - `import/documents/` for stage artifacts/intermediate JSON outputs.
   - `import/websites/gr.maharashtra.gov.in/` for crawled site HTML.
   - `import/logs/` for run logs.
   - `flow/src/` for orchestration/export flow scripts.
   - `export/orgpedia_mahgetNewGR/` for export outputs.
   - `LFS/` for PDFs, HTML snapshots, and run artifacts.
5. There should be no separate top-level `data/` directory in v1.


## 9. GitHub Actions Design

There are three kinds of GitHub Action schedules:

1. `daily` workflow
2. `weekly` workflow
3. `monthly` workflow

Processing is split into separate jobs:

1. `gr-site`
2. `wayback`
3. `archive`

Rules:

1. Each job has a 6-hour limit.
2. One commit to `main` after each job.
3. After each job commit, run `make sync-hf` to sync `import/grinfo/` and `LFS/` to Hugging Face dataset path.
4. Use fixed Hugging Face dataset repo path.
5. Use `HF_TOKEN` for authentication.
6. Hugging Face sync must support Hub storage backend behavior:
   - prefer Xet when available,
   - keep Git LFS compatibility fallback.
7. `sync-hf` must fail the job if storage upload/push verification fails for the active backend.
8. Wayback SPN2 and Archive authentication should use shared credentials:
   - `IA_ACCESS_KEY`
   - `IA_SECRET_KEY`

### 9.1 Hugging Face Storage Upload Guarantee

`make sync-hf` must implement an explicit Hugging Face storage-aware sync contract:

1. Sync target paths: `import/grinfo/` and `LFS/`.
2. Support backend selection (`auto`, `xet`, `lfs`) with `auto` preferring Xet when available.
3. For `lfs` backend: ensure LFS tracking is active for `LFS/**` in `.gitattributes`, and push corresponding LFS objects.
4. For `xet` backend: configure Xet client integration and verify push consistency for the target branch.
5. Run post-push verification for the active backend and fail on unresolved upload/push state.
6. Emit a concise sync summary: active backend, number of ledger files synced, number of `LFS/` files synced, and final commit hash.


## 10. Makefile-First Execution

All workflows must run through `make` targets so local and CI behavior match.

Expected targets:

1. `make daily`
2. `make weekly`
3. `make monthly`
4. `make job-gr-site`
5. `make job-wayback`
6. `make job-archive`
7. `make validate`
8. `make status-readme`
9. `make import-pdfs` (one-time backfill utility)
10. `make append-ledger` (incremental append utility)
11. `make sync-hf`

`make sync-hf` contract:

1. Push ledger files from `import/grinfo/` to the Hugging Face dataset path.
2. Push `LFS/` files using the active HF storage backend (`xet` preferred, `lfs` fallback).
3. Exit non-zero if any active-backend upload or verification step fails.

## 10.1 Repository Status Reporting

1. `README.md` must contain an auto-generated status section bounded by markers:
   - `<!-- STATUS_TABLE_START -->`
   - `<!-- STATUS_TABLE_END -->`
2. A dedicated command must compute status from current ledger/LFS and rewrite this section.
3. At minimum, the status section must report:
   - total records,
   - state distribution,
   - stage progress (`download`, `wayback`, `archive`) with pending/retryable counts,
   - generation timestamp (UTC).
4. Run-oriented make targets (`daily`, `weekly`, `monthly`, stage jobs, baseline build) must refresh this section after execution.

## 10.2 Local Environment Loading

1. Local CLI entrypoints must auto-load a `.env` file before argument parsing.
2. `.env` discovery should search from current working directory upward.
3. Existing process environment variables must take precedence over `.env` values by default.
4. `.env` must be ignored in git.

## 10.3 One-time PDF Backfill Import

1. Provide a one-time command to import existing local PDF folders into canonical storage:
   - command: `import-pdfs`
   - required input: source directory path.
2. Imported files must be placed under:
   - `LFS/pdfs/{department_code}/{YYYY-MM}/{unique_code}.pdf`
3. `unique_code` should be parsed from PDF filename; `YYYY-MM` should be derived from ledger `gr_date` when available, otherwise `unknown`.
4. Command should optionally (default enabled) run HF sync after import using existing `sync-hf` configuration.
5. `department_code` directory segment must be derived from the matching ledger record and use canonical abbreviations (e.g. `mahedu`, `mahfin`) via shared mapping logic.
6. If a PDF filename resolves to a `unique_code` that is not present in the ledger, the file must be skipped and reported.
7. Successful imports/downloads must set top-level ledger `lfs_path` to the canonical local path; missing files must leave/set `lfs_path` as `null`.

## 10.4 Incremental Ledger Append (`append-ledger`)

1. Provide an incremental append command to add newly seen GR infos without rebuilding baseline:
   - command: `append-ledger`
   - source: `mahgetGR` only.
2. Candidate rows must be normalized using the same canonicalization rules used by baseline ingestion (including canonical `unique_code` derivation).
3. Dedup key is `unique_code` across all existing ledger partitions.
4. If a candidate `unique_code` already exists in ledger, skip it.
5. Existing ledger rows must not be modified by this command (strict append-only behavior).
6. Candidates with missing/invalid `unique_code` must be skipped and counted.
7. New records must be inserted using the standard ledger record template/defaults and partitioned by `gr_date` year (or `unknown`).
8. Command must print console summary counters at minimum:
   - scanned candidates,
   - appended,
   - skipped existing,
   - skipped invalid/missing unique code.
9. Command should support `--dry-run` to compute/report counts without writing.

## 10.5 One-time `lfs_path` Backfill (`backfill-lfs-path`)

1. Provide a one-time command to backfill top-level `lfs_path` values in existing ledgers:
   - command: `backfill-lfs-path`.
2. Command must scan local LFS PDF roots and match PDFs to records by `unique_code` (with canonical expected path preference).
3. If a local PDF is found, set `lfs_path` to a repository-relative path (for example `LFS/mahGRs/pdfs/...`).
4. If no local PDF is found, set `lfs_path` to `null`.
5. Command must not change immutable identity fields.
6. Command must print summary counters (scanned, updated, unchanged, set non-null, cleared to null).
7. Command should support `--dry-run`.


## 11. Data Update Semantics

1. `unique_code` lookup spans all yearly files plus `unknown.jsonl`.
2. Inserts only for unknown IDs.
3. Updates preserve record identity and modify mutable fields (`state`, attempts, metadata, errors, `lfs_path`, timestamps).
4. Writes must be atomic to avoid partial ledger corruption.
5. `last_seen_crawl_date` is advanced only by monthly full-reconciliation crawl when a known `unique_code` is observed.
6. Canonical ledger location is `import/grinfo/` (not `data/`).
7. `append-ledger` is append-only and must not update existing rows.


## 12. Archive Recovery Requirement

When a record is in `ARCHIVE_UPLOADED_WITHOUT_DOCUMENT` and download later succeeds:

1. Reuse the same archive item/identifier.
2. Upload/attach document to that existing item.
3. Update state to:
   - `ARCHIVE_UPLOADED_WITH_WAYBACK_URL` if wayback URL exists,
   - else `ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL`.


## 13. Non-Goals (Initial Version)

1. No additional DB index (SQLite, etc.) in v1.
2. No gzip for ledger files.
3. No alternate crawl mode beyond department-wise for monthly.


## 14. Command-Object Architecture Requirement

1. Every executable workflow/job must be represented by an importable command object (or equivalent callable class/function) with a stable `run(...)` interface.
2. CLI commands must be thin wrappers that:
   - parse args/env,
   - build runtime context/dependencies,
   - invoke the same importable command object used by tests and other modules.
3. Business logic must not live in Makefile or GitHub Actions YAML; those layers only trigger CLI targets.
4. Command objects must support dependency injection for network, filesystem, wayback, archive, and Hugging Face clients to enable deterministic local tests.
5. Each command must return structured outcomes (status, counters, errors) suitable for logging, testing assertions, and downstream orchestration.


## 15. Implementation Plan With Review Checkpoints

### Stage 0: Foundation And Contracts

1. Define project structure for:
   - domain models,
   - state machine,
   - ledger I/O,
   - command objects,
   - adapters/clients,
   - CLI entrypoints.
2. Define core typed contracts:
   - `GRInfo`,
   - state enum/constants,
   - command context/result objects.
3. **Checkpoint**: review folder layout, type definitions, and command interfaces before behavior is implemented.

### Stage 1: Ledger And State Engine

1. Implement JSONL partitioning (`YYYY.jsonl`, `unknown.jsonl`) and cross-file `unique_code` lookup.
2. Implement atomic write/update semantics and mutable-field patching.
3. Implement state progression validation and retry counters.
4. **Checkpoint**: run ledger/state unit tests; inspect sample ledger updates for insert/update correctness.

### Stage 2: GR Site Discovery (`gr-site` core)

1. Implement department-wise crawler and normalized fetched record creation.
2. Implement daily early-stop rule (first known `unique_code` page).
3. Implement monthly full reconciliation insert-only behavior.
4. **Checkpoint**: review crawl outputs and confirm daily vs monthly discovery rules with test fixtures.

### Stage 3: Download Stage Command

1. Implement download processing for eligible states with max-2-attempt policy.
2. Store PDFs at `LFS/pdfs/{department_code}/{YYYY-MM}/{unique_code}.pdf`.
3. Record hashes/sizes/errors and enforce 10-consecutive-service-failure early stop.
4. **Checkpoint**: verify downloaded files and state transitions (`FETCHED -> DOWNLOAD_SUCCESS/FAILED`).

### Stage 4: Wayback Stage Command

1. Implement wayback upload for `DOWNLOAD_SUCCESS` records.
2. Persist wayback metadata/errors and retry policy.
3. Enforce stage early-stop safeguard after 10 consecutive service failures.
4. **Checkpoint**: validate transitions (`DOWNLOAD_SUCCESS -> WAYBACK_UPLOADED/WAYBACK_UPLOAD_FAILED`) and metadata fields.

### Stage 5: Archive Stage Command And Recovery

1. Implement archive upload paths:
   - with wayback URL,
   - without wayback URL,
   - metadata-only fallback without document.
2. Implement recovery path for `ARCHIVE_UPLOADED_WITHOUT_DOCUMENT` using same archive identifier/item.
3. Persist archive identifiers/URLs/errors and resulting final states.
4. **Checkpoint**: review recovery scenario tests proving same-item document attachment behavior.

### Stage 6: Workflow Commands And CLI

1. Implement top-level workflow commands:
   - `daily`,
   - `weekly`,
   - `monthly`.
2. Implement job commands:
   - `job-gr-site`,
   - `job-wayback`,
   - `job-archive`.
3. Ensure CLI and programmatic invocation call identical command objects.
4. **Checkpoint**: run each command both via CLI and direct import in tests; compare equivalent results.

### Stage 7: Makefile, Validation, And CI Wiring

1. Wire all required make targets:
   - `daily`,
   - `weekly`,
   - `monthly`,
   - `job-gr-site`,
   - `job-wayback`,
   - `job-archive`,
   - `validate`,
   - `status-readme`,
   - `import-pdfs`,
   - `append-ledger`,
   - `sync-hf`.
2. Implement GitHub Actions workflows with 6-hour job limits and one commit per job.
3. Ensure post-job Hugging Face sync with fixed dataset path and `HF_TOKEN`.
4. Ensure `sync-hf` supports Xet-first upload with LFS fallback and fails fast on active-backend verification errors.
5. **Checkpoint**: dry-run workflows locally via make targets and validate expected artifacts/log outputs.

### Stage 8: Hardening, Documentation, And Handover

1. Add integration tests for end-to-end daily/weekly/monthly flows with mocks/fakes.
2. Document local runbooks, environment variables, and failure recovery playbooks.
3. Provide programmatic usage examples showing import and execution of command objects.
4. **Checkpoint**: final review of docs + test reports; approve release-ready baseline.
