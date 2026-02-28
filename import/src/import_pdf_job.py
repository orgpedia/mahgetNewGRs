#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path

from import_config import load_import_config
from info_store import InfoStore as LedgerStore
from job_utils import JobRunResult, is_record_within_lookback, load_code_filter, print_stage_report
from local_env import load_local_env
from sync_hf_job import (
    LARGE_FOLDER_MODE_ALWAYS,
    LARGE_FOLDER_MODE_AUTO,
    LARGE_FOLDER_MODE_NEVER,
    SyncHFConfig,
    SyncHFError,
    resolve_hf_repo_id,
    resolve_hf_repo_path,
    run_sync_hf,
)


@dataclass(frozen=True)
class ImportPdfJobConfig:
    max_records: int
    lookback_days: int
    code_filter: set[str] = field(default_factory=set)
    verbose: bool = False


@dataclass(frozen=True)
class ImportPdfStageConfig:
    dry_run: bool
    hf_repo_path: Path
    hf_repo_id: str
    hf_remote_url: str
    hf_token: str
    hf_branch: str
    hf_commit_message: str
    hf_no_verify_storage: bool
    hf_skip_push: bool
    hf_large_folder_mode: str
    hf_large_folder_print_report: bool
    verbose: bool = False


@dataclass
class ImportPdfJobReport:
    selected: int = 0
    processed: int = 0
    skipped: int = 0
    skipped_missing_record: int = 0
    skipped_not_downloaded: int = 0
    skipped_missing_file: int = 0
    upload_input_files: int = 0
    uploaded_files: int = 0
    processed_codes: list[str] = field(default_factory=list)
    uploaded_codes: list[str] = field(default_factory=list)
    skipped_codes: list[str] = field(default_factory=list)


def _verbose_job_log(config: ImportPdfJobConfig, message: str) -> None:
    if config.verbose:
        print(f"[import-pdf verbose] {message}")


def _verbose_log(config: ImportPdfStageConfig, message: str) -> None:
    if config.verbose:
        print(f"[import-pdf verbose] {message}")


def _resolve_existing_file(path_value: str) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    probe = path if path.is_absolute() else (Path.cwd() / path)
    if not probe.exists() or not probe.is_file():
        return None
    return probe.resolve()


def _download_success_file(record: dict) -> Path | None:
    download_obj = record.get("download")
    if not isinstance(download_obj, dict):
        return None
    status = str(download_obj.get("status") or "").strip()
    if status != "success":
        return None
    return _resolve_existing_file(str(download_obj.get("path") or ""))


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        code = str(value).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        output.append(code)
    return output


def _to_hf_relative_paths(file_paths: list[Path], hf_repo_path: Path) -> tuple[str, ...]:
    rel_paths: list[str] = []
    seen: set[str] = set()
    resolved_root = hf_repo_path.resolve()
    for file_path in file_paths:
        resolved_file = file_path.resolve()
        try:
            rel = resolved_file.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise SyncHFError(
                f"Downloaded PDF is outside --hf-repo-path: {resolved_file} (hf_repo_path={resolved_root})"
            ) from exc
        if rel not in seen:
            seen.add(rel)
            rel_paths.append(rel)
    return tuple(rel_paths)


def select_candidates(config: ImportPdfJobConfig, store: LedgerStore) -> list[str]:
    selected: list[str] = []
    scanned = 0
    for record in store.iter_records():
        unique_code = str(record.get("unique_code") or "").strip()
        if not unique_code:
            continue
        scanned += 1
        if config.code_filter and unique_code not in config.code_filter:
            continue
        if not is_record_within_lookback(record, config.lookback_days):
            continue
        download_obj = record.get("download")
        if not isinstance(download_obj, dict):
            continue
        status = str(download_obj.get("status") or "").strip()
        path = str(download_obj.get("path") or "").strip()
        if status != "success" or not path:
            continue
        selected.append(unique_code)
        if config.max_records > 0 and len(selected) >= config.max_records:
            break
    _verbose_job_log(
        config,
        (
            f"scanned={scanned} selected={len(selected)} max_records={config.max_records} "
            f"lookback_days={config.lookback_days} code_filter_size={len(config.code_filter)}"
        ),
    )
    if config.verbose and selected:
        preview = selected[:25]
        print(f"[import-pdf verbose] selected codes preview ({len(preview)}/{len(selected)}):")
        for code in preview:
            print(f"[import-pdf verbose]   {code}")
    return selected


def run_selected(
    selected_codes: list[str],
    stage_config: ImportPdfStageConfig,
    store: LedgerStore,
) -> JobRunResult[ImportPdfJobReport]:
    codes = _dedupe_keep_order(selected_codes)
    report = ImportPdfJobReport(selected=len(codes))
    _verbose_log(
        stage_config,
        (
            f"selected={report.selected} dry_run={stage_config.dry_run} "
            f"hf_repo_path={stage_config.hf_repo_path} hf_repo_id={stage_config.hf_repo_id}"
        ),
    )

    file_paths: list[Path] = []
    for index, unique_code in enumerate(codes, start=1):
        _verbose_log(stage_config, f"[{index}/{len(codes)}] code={unique_code}")
        record = store.find(unique_code)
        if record is None:
            report.skipped += 1
            report.skipped_missing_record += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=missing_record")
            continue

        report.processed += 1
        report.processed_codes.append(unique_code)
        file_path = _download_success_file(record)
        if file_path is None:
            report.skipped += 1
            report.skipped_not_downloaded += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=download_not_ready")
            continue
        if not file_path.exists() or not file_path.is_file():
            report.skipped += 1
            report.skipped_missing_file += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=missing_file path={file_path}")
            continue

        file_paths.append(file_path)
        report.uploaded_codes.append(unique_code)
        _verbose_log(stage_config, f"[ready] code={unique_code} file={file_path}")

    if not file_paths:
        _verbose_log(stage_config, "no uploadable files found for selected codes")
        return JobRunResult(report=report)

    report.upload_input_files = len(file_paths)

    try:
        relative_paths = _to_hf_relative_paths(file_paths, stage_config.hf_repo_path)
        _verbose_log(
            stage_config,
            f"upload_input_files={len(file_paths)} unique_repo_paths={len(relative_paths)}",
        )
        if stage_config.verbose and relative_paths:
            preview = list(relative_paths[:25])
            print(f"[import-pdf verbose] upload path preview ({len(preview)}/{len(relative_paths)}):")
            for path in preview:
                print(f"[import-pdf verbose]   {path}")
        sync_config = SyncHFConfig(
            source_root=Path(".").resolve(),
            hf_repo_path=stage_config.hf_repo_path,
            remote_name="origin",
            branch=stage_config.hf_branch,
            commit_message=stage_config.hf_commit_message,
            dry_run=stage_config.dry_run,
            skip_push=stage_config.hf_skip_push,
            verify_storage=not stage_config.hf_no_verify_storage,
            storage_backend="auto",
            hf_token=stage_config.hf_token,
            hf_remote_url=stage_config.hf_remote_url,
            hf_repo_id=stage_config.hf_repo_id,
            mode="upload",
            file_paths=relative_paths,
            large_folder_mode=stage_config.hf_large_folder_mode,
            large_folder_print_report=stage_config.hf_large_folder_print_report,
        )
        run_sync_hf(sync_config)
        report.uploaded_files = len(relative_paths)
        _verbose_log(stage_config, f"upload completed uploaded_files={report.uploaded_files}")
    except SyncHFError as exc:
        _verbose_log(stage_config, f"upload failed error={exc}")
        return JobRunResult(report=report, fatal_error=f"import-pdf upload failed: {exc}")

    return JobRunResult(report=report)


def _print_report(report: ImportPdfJobReport) -> None:
    print_stage_report(
        "Import PDF job",
        selected=report.selected,
        processed=report.processed,
        success=report.uploaded_files,
        failed=0,
        skipped=report.skipped,
        service_failures=0,
        stopped_early=False,
        extras={
            "upload_input_files": report.upload_input_files,
            "uploaded_files": report.uploaded_files,
            "skipped_missing_record": report.skipped_missing_record,
            "skipped_not_downloaded": report.skipped_not_downloaded,
            "skipped_missing_file": report.skipped_missing_file,
        },
    )


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = "Upload downloaded PDFs (from ledger download.path) to Hugging Face for selected records."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory")
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on records processed")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Use only ledger records dated within the last N days (0 means no date filter)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan actions without remote upload")
    parser.add_argument("--verbose", action="store_true", help="Print detailed selection and upload status")

    parser.add_argument(
        "--hf-repo-path",
        default=hf_defaults.dataset_repo_path,
        help="Local HF data directory (default from import/import_config.yaml).",
    )
    parser.add_argument(
        "--hf-repo-id",
        default=hf_defaults.dataset_repo_id,
        help="HF dataset repo id (namespace/name).",
    )
    parser.add_argument(
        "--hf-remote-url",
        default=hf_defaults.dataset_repo_url,
        help="HF remote URL (used to infer repo id if needed).",
    )
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token override")
    parser.add_argument("--hf-branch", default="main", help="HF revision/branch for upload")
    parser.add_argument(
        "--hf-commit-message",
        default="workflow: upload downloaded PDFs batch",
        help="Commit message used by HF upload",
    )
    parser.add_argument("--hf-no-verify-storage", action="store_true", help="Skip post-upload verification checks")
    parser.add_argument("--hf-skip-push", action="store_true", help="Skip remote upload during HF step")
    parser.add_argument(
        "--large-folder-mode",
        choices=(LARGE_FOLDER_MODE_AUTO, LARGE_FOLDER_MODE_ALWAYS, LARGE_FOLDER_MODE_NEVER),
        default=hf_defaults.upload_large_folder_mode or LARGE_FOLDER_MODE_AUTO,
        help="Large-folder strategy for HF upload: auto (default), always, never.",
    )
    parser.add_argument(
        "--no-large-folder-report",
        action="store_true",
        help="Disable upload_large_folder progress/status report output.",
    )
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)

    try:
        hf_repo_path = resolve_hf_repo_path(args.hf_repo_path)
        hf_repo_id = resolve_hf_repo_id(args.hf_repo_id, args.hf_remote_url)
    except SyncHFError as exc:
        print(f"import-pdf config error: {exc}")
        return 2

    store = LedgerStore(Path(args.ledger_dir).resolve())
    job_config = ImportPdfJobConfig(
        max_records=max(0, args.max_records),
        lookback_days=max(0, args.lookback_days),
        code_filter=code_filter,
        verbose=args.verbose,
    )
    stage_config = ImportPdfStageConfig(
        dry_run=args.dry_run,
        hf_repo_path=hf_repo_path,
        hf_repo_id=hf_repo_id,
        hf_remote_url=args.hf_remote_url,
        hf_token=args.hf_token,
        hf_branch=args.hf_branch,
        hf_commit_message=args.hf_commit_message,
        hf_no_verify_storage=args.hf_no_verify_storage,
        hf_skip_push=args.hf_skip_push,
        hf_large_folder_mode=args.large_folder_mode,
        hf_large_folder_print_report=not args.no_large_folder_report,
        verbose=args.verbose,
    )
    selected_codes = select_candidates(job_config, store)
    result = run_selected(selected_codes, stage_config, store)
    _print_report(result.report)
    if result.fatal_error:
        print(result.fatal_error)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upload downloaded PDFs to Hugging Face.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
