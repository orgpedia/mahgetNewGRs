# Import Pipeline Guide

Unified Maharashtra GR pipeline using yearly JSONL ledgers in `import/grinfo/`.

## CLI

All commands run through:

```bash
python3 import/src/cli.py <command> [options]
```

Main commands:

- `baseline-ledger` - build initial ledger from `mahgetGR` and `mahgetAllGR`
- `append-ledger` - append-only incremental ingest from `mahgetGR` into existing ledger
- `backfill-lfs-path` - one-time refresh of ledger `lfs_path` based on local PDF files
- `job-download-pdf` - download stage for eligible records
- `job-import-pdf` - import local PDF folders into `LFS/pdfs` + optional HF sync
- `job-pdf-info` - extract PDF metadata (pages/images/fonts/language/file size) into ledger `pdf_info`
- `wrk-download-upload-pdfinfo` - one batch workflow: download PDFs -> upload those files to HF -> compute `pdf_info` for that batch
- `validate-ledger` - validate schema/state/partition consistency
- `update-readme-status` - refresh README status table from current ledger
- `job-gr-site` - crawl reconciliation only (`daily|weekly|monthly`)
- `job-wayback` - Wayback SPN2 stage
- `job-archive` - Archive stage (+ metadata fallback/recovery)
- `sync-hf` - sync local artifacts with HF dataset using `huggingface_hub` APIs (upload/download)

## Make targets

Use `make help` for available targets. Required spec targets:

- `make job-gr-site`
- `make job-wayback`
- `make job-archive`
- `make job-download-pdf`
- `make job-import-pdf`
- `make job-pdf-info`
- `make wrk-download-upload-pdfinfo`
- `make validate`
- `make status-readme`
- `make append-ledger`
- `make backfill-lfs-path`
- `make sync-hf`

## Credentials

Environment variables used by stages:

- Wayback SPN2 + Archive.org: `IA_ACCESS_KEY`, `IA_SECRET_KEY`
- HF sync token: `HF_TOKEN`
- HF sync defaults: `import/import_config.yaml` -> `hf` section (`dataset_repo_url`, `dataset_repo_id`, `dataset_repo_path`, `upload_large_folder_mode`, `upload_large_folder_threshold`)

## Hugging Face sync modes

`sync-hf` is API-based (no local git clone required) and supports:

- upload everything under local sync path:
  - `python3 import/src/cli.py sync-hf --mode upload --hf-repo-path LFS/mahGRs`
- upload only one prefix:
  - `python3 import/src/cli.py sync-hf --mode upload --hf-repo-path LFS/mahGRs --prefix pdfs/mahagri/2026-01`
- download one prefix:
  - `python3 import/src/cli.py sync-hf --mode download --hf-repo-path LFS/mahGRs --prefix pdfs/mahagri/2026-01`
- download one file:
  - `python3 import/src/cli.py sync-hf --mode download --hf-repo-path LFS/mahGRs --file pdfs/mahagri/2026-01/202601011234567890.pdf`

By default, both upload and download exclude `import/grinfo`; pass `--include-ledger` only if you intentionally want ledger files in HF sync.

Large upload behavior:

- `sync-hf` auto-uses `upload_large_folder` for big directory uploads (default threshold: 100 files).
- Override with:
  - `--large-folder-mode auto|always|never`
  - `--large-folder-threshold <count>`
  - `--large-folder-num-workers <n>`

## Setup templates

- Local env template: `.env`
- GitHub Web UI secrets/variables checklist: `GITHUB_ACTIONS_WEB_UI_TEMPLATE.md`

All local CLI entrypoints auto-load `.env` (searching current directory upward) before argument parsing.

## One-time PDF import

Use this when you already have downloaded PDFs in a local directory:

```bash
python3 import/src/cli.py job-import-pdf \
  --source-dir /path/to/existing/pdfs \
  --hf-repo-path LFS/mahGRs
```

Notes:

- File names should contain a `unique_code` (16-22 digit token).
- Destination path is `LFS/pdfs/<department_code>/<YYYY-MM>/<unique_code>.pdf`.
- `department_code` and `gr_date` month are derived from ledger records.
- Files without a matching ledger record are skipped.
- `department_code` uses the shared abbreviation map in `import/src/department_codes.py` (for example, School Education -> `mahedu`).

## `lfs_path` field

- Ledger records now include top-level `lfs_path`.
- It stores the local PDF location (for example `LFS/mahGRs/pdfs/mahagri/2025-10/202510131610444101.pdf`) when present.
- If no local PDF exists for that record, `lfs_path` is `null`.
- Use `python3 import/src/cli.py backfill-lfs-path --ledger-dir import/grinfo` for one-time backfill on existing ledgers.

## `pdf_info` field

- `job-pdf-info` reads local PDFs resolved from `lfs_path` (fallback: `download.path`) and stores extracted metadata in `record.pdf_info`.
- Requires `pymupdf` (`pip install pymupdf`) in the active environment.
- Stored metadata includes:
  - `page_count`
  - `has_any_page_image` + `pages_with_images`
  - `fonts` dictionary keyed by `font_num` with font metadata and `words` (used-word counts)
  - `language` inferred from Unicode-script word counts
  - `file_size`
- By default, records with existing `pdf_info.status=success` are skipped for speed. Use `--force` to recompute.
- Use `--verbose` to print per-record processing details (skip reasons, extracted stats, and errors).
