#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from department_codes import department_code_from_name
from ledger_engine import LedgerStore, to_ledger_relative_path
from local_env import load_local_env
from sync_hf_job import SyncHFConfig, SyncHFError, resolve_hf_repo_path, run_sync_hf


LONG_DIGITS_RE = re.compile(r"\d{16,22}")
CONTROL_CATEGORIES = {"Cf", "Cc"}


@dataclass(frozen=True)
class ImportPdfsConfig:
    source_dir: Path
    ledger_dir: Path
    lfs_pdf_root: Path
    recursive: bool
    overwrite: bool
    dry_run: bool
    sync_hf: bool
    hf_repo_path: Path | None
    hf_remote_name: str
    hf_branch: str
    hf_commit_message: str
    hf_storage_backend: str
    hf_no_verify_storage: bool
    hf_skip_push: bool
    hf_token: str
    hf_remote_url: str


@dataclass
class ImportPdfsReport:
    scanned_pdf_files: int = 0
    copied_files: int = 0
    overwritten_files: int = 0
    skipped_identical: int = 0
    skipped_conflicts: int = 0
    skipped_non_pdf: int = 0
    skipped_no_unique_code: int = 0
    missing_ledger_record: int = 0


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = "".join(ch for ch in text if unicodedata.category(ch) not in CONTROL_CATEGORIES)
    return text.strip()


def extract_unique_code(file_name: str) -> str:
    text = clean_text(file_name)
    if not text:
        return ""
    match = LONG_DIGITS_RE.search(text)
    if match:
        return match.group(0)
    stem = Path(text).stem
    digits = "".join(ch for ch in stem if ch.isdigit())
    if 16 <= len(digits) <= 22:
        return digits
    if len(digits) > 22:
        return digits[:22]
    return ""


def year_month_from_gr_date(gr_date: Any) -> str:
    text = clean_text(gr_date)
    if len(text) >= 7 and len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:7]
    return "unknown"


def sha1_file(file_path: Path) -> str:
    digest = hashlib.sha1()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest().upper()


def iter_pdf_files(source_dir: Path, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(path for path in source_dir.rglob("*") if path.is_file())
    return sorted(path for path in source_dir.iterdir() if path.is_file())


def _copy_pdf(src: Path, dst: Path, *, overwrite: bool, dry_run: bool) -> str:
    if dst.exists():
        src_hash = sha1_file(src)
        dst_hash = sha1_file(dst)
        if src_hash == dst_hash:
            return "identical"
        if not overwrite:
            return "conflict"
        if dry_run:
            return "overwritten"
        shutil.copy2(src, dst)
        return "overwritten"

    if dry_run:
        return "copied"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return "copied"


def run_import_pdfs(config: ImportPdfsConfig) -> ImportPdfsReport:
    if not config.source_dir.exists() or not config.source_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found: {config.source_dir}")

    store = LedgerStore(config.ledger_dir)
    report = ImportPdfsReport()

    files = iter_pdf_files(config.source_dir, config.recursive)
    for file_path in files:
        if file_path.suffix.lower() != ".pdf":
            report.skipped_non_pdf += 1
            continue

        report.scanned_pdf_files += 1
        unique_code = extract_unique_code(file_path.name)
        if not unique_code:
            report.skipped_no_unique_code += 1
            continue

        record = store.find(unique_code)
        if record is None:
            report.missing_ledger_record += 1
            continue

        record_department = clean_text(record.get("department_code"))
        if not record_department:
            record_department = clean_text(record.get("department_name"))
        department_code = department_code_from_name(record_department)
        year_month = year_month_from_gr_date(record.get("gr_date"))

        destination = config.lfs_pdf_root / department_code / year_month / f"{unique_code}.pdf"
        outcome = _copy_pdf(
            file_path,
            destination,
            overwrite=config.overwrite,
            dry_run=config.dry_run,
        )
        if outcome == "copied":
            report.copied_files += 1
        elif outcome == "overwritten":
            report.overwritten_files += 1
        elif outcome == "identical":
            report.skipped_identical += 1
        elif outcome == "conflict":
            report.skipped_conflicts += 1

        if not config.dry_run and destination.exists() and destination.is_file():
            destination_relpath = to_ledger_relative_path(destination)
            if record.get("lfs_path") == destination_relpath:
                continue
            store.upsert(
                {
                    "unique_code": unique_code,
                    "lfs_path": destination_relpath,
                }
            )

    return report


def _print_report(report: ImportPdfsReport) -> None:
    print("import-pdfs:")
    print(f"  scanned_pdf_files: {report.scanned_pdf_files}")
    print(f"  copied_files: {report.copied_files}")
    print(f"  overwritten_files: {report.overwritten_files}")
    print(f"  skipped_identical: {report.skipped_identical}")
    print(f"  skipped_conflicts: {report.skipped_conflicts}")
    print(f"  skipped_non_pdf: {report.skipped_non_pdf}")
    print(f"  skipped_no_unique_code: {report.skipped_no_unique_code}")
    print(f"  missing_ledger_record: {report.missing_ledger_record}")


def _run_hf_sync(config: ImportPdfsConfig) -> int:
    if config.hf_repo_path is None:
        print("import-pdfs: --hf-repo-path is required unless --skip-sync-hf is used")
        return 2

    try:
        hf_repo_path = resolve_hf_repo_path(str(config.hf_repo_path))
    except SyncHFError as exc:
        print(f"import-pdfs sync error: {exc}")
        print("import-pdfs hint: use --skip-sync-hf to run local import only.")
        return 2

    sync_config = SyncHFConfig(
        source_root=Path(".").resolve(),
        hf_repo_path=hf_repo_path,
        remote_name=config.hf_remote_name,
        branch=config.hf_branch,
        commit_message=config.hf_commit_message,
        dry_run=config.dry_run,
        skip_push=config.hf_skip_push,
        verify_storage=not config.hf_no_verify_storage,
        storage_backend=config.hf_storage_backend,
        hf_token=config.hf_token,
        hf_remote_url=config.hf_remote_url,
    )
    try:
        run_sync_hf(sync_config)
    except SyncHFError as exc:
        print(f"import-pdfs sync error: {exc}")
        return 1
    return 0


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "Import existing PDF files into LFS/pdfs/<department>/<YYYY-MM>/<unique_code>.pdf and optionally sync to HF."
    )
    parser.add_argument("--source-dir", required=True, help="Directory containing PDF files to import")
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger directory for department and gr_date lookup")
    parser.add_argument("--lfs-pdf-root", default="LFS/pdfs", help="LFS PDF root directory")
    parser.add_argument("--no-recursive", action="store_true", help="Do not recurse under source directory")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite destination PDF when hashes differ")
    parser.add_argument("--dry-run", action="store_true", help="Plan actions without writing files")
    parser.add_argument("--skip-sync-hf", action="store_true", help="Skip Hugging Face sync after import")

    parser.add_argument(
        "--hf-repo-path",
        default=os.environ.get("HF_DATASET_REPO_PATH", ""),
        help="Local data directory used by sync-hf upload",
    )
    parser.add_argument("--hf-remote-name", default="origin", help="Deprecated compatibility flag")
    parser.add_argument("--hf-branch", default="main", help="Deprecated compatibility flag")
    parser.add_argument("--hf-commit-message", default="import PDFs into LFS/pdfs", help="Commit message used by HF sync")
    parser.add_argument(
        "--hf-storage-backend",
        choices=("auto", "xet", "lfs"),
        default=os.environ.get("HF_STORAGE_BACKEND", "auto"),
        help="Deprecated compatibility flag (ignored by API-based HF sync)",
    )
    parser.add_argument("--hf-no-verify-storage", action="store_true", help="Skip post-upload/download verification")
    parser.add_argument("--hf-skip-push", action="store_true", help="Skip upload during HF sync")
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token override")
    parser.add_argument("--hf-remote-url", default=os.environ.get("HF_DATASET_REPO_URL", ""), help="HF remote URL override")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    config = ImportPdfsConfig(
        source_dir=Path(args.source_dir).resolve(),
        ledger_dir=Path(args.ledger_dir).resolve(),
        lfs_pdf_root=Path(args.lfs_pdf_root).resolve(),
        recursive=not args.no_recursive,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        sync_hf=not args.skip_sync_hf,
        hf_repo_path=Path(args.hf_repo_path).resolve() if args.hf_repo_path else None,
        hf_remote_name=args.hf_remote_name,
        hf_branch=args.hf_branch,
        hf_commit_message=args.hf_commit_message,
        hf_storage_backend=args.hf_storage_backend,
        hf_no_verify_storage=args.hf_no_verify_storage,
        hf_skip_push=args.hf_skip_push,
        hf_token=args.hf_token,
        hf_remote_url=args.hf_remote_url,
    )

    report = run_import_pdfs(config)
    _print_report(report)

    if config.sync_hf:
        return _run_hf_sync(config)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import PDFs into LFS/pdfs and sync to HF.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
