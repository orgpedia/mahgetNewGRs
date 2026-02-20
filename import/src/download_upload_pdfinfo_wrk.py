#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import download_pdf_job
import pdf_info_job
import sync_hf_job
from import_config import load_import_config
from info_store import InfoStore as LedgerStore
from job_utils import load_code_filter, parse_state_list, print_stage_report
from local_env import load_local_env


DEFAULT_MAX_RUNTIME_MINUTES = 345


@dataclass(frozen=True)
class DownloadUploadPdfInfoWorkflowConfig:
    ledger_dir: Path
    batch_size: int
    max_records: int
    max_runtime_minutes: int
    lookback_days: int
    timeout_sec: int
    service_failure_limit: int
    dry_run: bool
    lfs_root: Path
    code_filter: set[str] = field(default_factory=set)
    allowed_states: set[str] = field(default_factory=set)
    skip_upload: bool = False
    hf_repo_path: Path = Path(".")
    hf_repo_id: str = ""
    hf_remote_url: str = ""
    hf_token: str = ""
    hf_branch: str = "main"
    hf_commit_message: str = "workflow: upload downloaded PDFs"
    hf_no_verify_storage: bool = False
    hf_skip_push: bool = False
    pdf_force: bool = False
    pdf_mark_missing: bool = False
    pdf_verbose: bool = False
    verbose: bool = False


@dataclass
class DownloadUploadPdfInfoWorkflowReport:
    batches_run: int = 0
    stop_reason: str = ""
    total_download_selected: int = 0
    total_download_processed: int = 0
    total_download_success: int = 0
    total_download_failed: int = 0
    total_download_skipped: int = 0
    total_download_service_failures: int = 0
    total_batch_success_with_files: int = 0
    total_uploaded_files: int = 0
    total_pdf_info_processed: int = 0
    total_pdf_info_updated: int = 0
    total_deleted_files: int = 0
    total_cleanup_skipped: int = 0
    last_download_report: download_pdf_job.DownloadStageReport | None = None
    last_pdf_info_report: pdf_info_job.PdfInfoReport | None = None


def _runtime_seconds(max_runtime_minutes: int) -> int:
    minutes = max(0, int(max_runtime_minutes))
    return minutes * 60


def _runtime_reached(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


def _print_batch_summary(
    *,
    batch_index: int,
    elapsed_seconds: float,
    download_report: download_pdf_job.DownloadStageReport,
    success_files: int,
    uploaded_files: int,
    pdf_processed: int,
    pdf_updated: int,
    deleted_files: int,
) -> None:
    print(
        "Batch summary "
        f"#{batch_index}: "
        f"download(sel={download_report.selected}, proc={download_report.processed}, "
        f"ok={download_report.success}, fail={download_report.failed}, skip={download_report.skipped}) "
        f"files={success_files} upload={uploaded_files} "
        f"pdfinfo(proc={pdf_processed}, upd={pdf_updated}) "
        f"cleanup(del={deleted_files}) "
        f"elapsed={elapsed_seconds:.1f}s"
    )


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


def _verbose_log(config: DownloadUploadPdfInfoWorkflowConfig, message: str) -> None:
    if config.verbose:
        print(f"[verbose] {message}")


def _verbose_preview_codes(config: DownloadUploadPdfInfoWorkflowConfig, label: str, codes: list[str], limit: int = 25) -> None:
    if not config.verbose:
        return
    if not codes:
        print(f"[verbose] {label}: 0")
        return
    shown = codes[: max(0, limit)]
    print(f"[verbose] {label}: {len(codes)} (showing {len(shown)})")
    for code in shown:
        print(f"[verbose]   {code}")


def _verbose_preview_paths(config: DownloadUploadPdfInfoWorkflowConfig, label: str, paths: tuple[str, ...], limit: int = 25) -> None:
    if not config.verbose:
        return
    if not paths:
        print(f"[verbose] {label}: 0")
        return
    shown = list(paths[: max(0, limit)])
    print(f"[verbose] {label}: {len(paths)} (showing {len(shown)})")
    for path in shown:
        print(f"[verbose]   {path}")


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
    first = _resolve_existing_file(str(download_obj.get("path") or ""))
    if first is not None:
        return first
    return _resolve_existing_file(str(record.get("lfs_path") or ""))


def _collect_batch_files(
    *,
    ledger_dir: Path,
    processed_codes: list[str],
) -> tuple[list[str], list[Path]]:
    store = LedgerStore(ledger_dir)
    successful_codes: list[str] = []
    file_paths: list[Path] = []

    for code in _dedupe_keep_order(processed_codes):
        record = store.find(code)
        if record is None:
            continue
        file_path = _download_success_file(record)
        if file_path is None:
            continue
        successful_codes.append(code)
        file_paths.append(file_path)

    return successful_codes, file_paths


def _to_hf_relative_paths(file_paths: list[Path], hf_repo_path: Path) -> tuple[str, ...]:
    rel_paths: list[str] = []
    seen: set[str] = set()
    resolved_root = hf_repo_path.resolve()
    for file_path in file_paths:
        resolved_file = file_path.resolve()
        try:
            rel = resolved_file.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise sync_hf_job.SyncHFError(
                f"Downloaded PDF is outside --hf-repo-path: {resolved_file} (hf_repo_path={resolved_root})"
            ) from exc
        if rel not in seen:
            seen.add(rel)
            rel_paths.append(rel)
    return tuple(rel_paths)


def _run_upload_batch(config: DownloadUploadPdfInfoWorkflowConfig, file_paths: list[Path]) -> int:
    if config.skip_upload:
        print("Upload stage: skipped by flag.")
        return 0
    if not file_paths:
        print("Upload stage: nothing to upload for this batch.")
        return 0

    _verbose_log(config, f"upload input files: {len(file_paths)}")
    relative_paths = _to_hf_relative_paths(file_paths, config.hf_repo_path)
    _verbose_preview_paths(config, "upload relative paths", relative_paths)
    sync_config = sync_hf_job.SyncHFConfig(
        source_root=Path(".").resolve(),
        hf_repo_path=config.hf_repo_path,
        remote_name="origin",
        branch=config.hf_branch,
        commit_message=config.hf_commit_message,
        dry_run=config.dry_run,
        skip_push=config.hf_skip_push,
        verify_storage=not config.hf_no_verify_storage,
        storage_backend="auto",
        hf_token=config.hf_token,
        hf_remote_url=config.hf_remote_url,
        hf_repo_id=config.hf_repo_id,
        mode=sync_hf_job.MODE_UPLOAD,
        file_paths=relative_paths,
    )
    sync_hf_job.run_sync_hf(sync_config)
    return len(relative_paths)


def _cleanup_batch_files(config: DownloadUploadPdfInfoWorkflowConfig, file_paths: list[Path]) -> tuple[int, int]:
    if config.dry_run or not file_paths:
        return 0, 0

    root = config.lfs_root.resolve()
    removed = 0
    skipped = 0
    seen: set[str] = set()
    for file_path in file_paths:
        try:
            resolved = file_path.resolve()
        except Exception:
            skipped += 1
            continue

        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)

        try:
            resolved.relative_to(root)
        except ValueError:
            skipped += 1
            _verbose_log(config, f"[skip cleanup] outside lfs-root: {resolved}")
            continue

        if not resolved.exists():
            continue
        if not resolved.is_file():
            skipped += 1
            _verbose_log(config, f"[skip cleanup] not a regular file: {resolved}")
            continue

        try:
            resolved.unlink()
            removed += 1
            _verbose_log(config, f"[cleanup] removed {resolved}")
        except Exception as exc:
            skipped += 1
            _verbose_log(config, f"[cleanup error] {resolved}: {exc}")
            continue

        parent = resolved.parent
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    return removed, skipped


def run_workflow(config: DownloadUploadPdfInfoWorkflowConfig) -> tuple[int, DownloadUploadPdfInfoWorkflowReport | None]:
    max_runtime_seconds = _runtime_seconds(config.max_runtime_minutes)
    deadline_monotonic = time.monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None

    print("Workflow: download -> upload -> pdf-info")
    print(f"Batch size: {config.batch_size}")
    print(f"Max records: {config.max_records}")
    print(f"Max runtime minutes: {config.max_runtime_minutes}")
    _verbose_log(config, f"ledger_dir={config.ledger_dir}")
    _verbose_log(config, f"lfs_root={config.lfs_root}")
    _verbose_log(config, f"hf_repo_path={config.hf_repo_path}")
    _verbose_log(config, f"hf_repo_id={config.hf_repo_id}")
    _verbose_log(config, f"dry_run={config.dry_run} skip_upload={config.skip_upload}")
    _verbose_log(config, f"max_runtime_seconds={max_runtime_seconds}")
    _verbose_log(config, f"lookback_days={config.lookback_days}")
    _verbose_log(config, f"allowed_states={sorted(config.allowed_states)}")
    _verbose_log(config, f"code_filter_size={len(config.code_filter)}")

    report = DownloadUploadPdfInfoWorkflowReport()
    batch_index = 0

    while True:
        if config.max_records > 0 and report.total_download_processed >= config.max_records:
            report.stop_reason = "max_records_reached"
            print(f"Reached max-records limit ({config.max_records}). Stopping workflow.")
            return 0, report

        remaining_records = config.max_records - report.total_download_processed if config.max_records > 0 else 0
        if config.batch_size > 0:
            batch_limit = config.batch_size
            if config.max_records > 0:
                batch_limit = min(batch_limit, max(0, remaining_records))
        else:
            batch_limit = max(0, remaining_records) if config.max_records > 0 else 0

        batch_index += 1
        print(f"Batch #{batch_index}")
        batch_started_at = time.monotonic()
        uploaded_files = 0
        pdf_processed = 0
        pdf_updated = 0
        deleted_files = 0

        download_config = download_pdf_job.DownloadStageConfig(
            ledger_dir=config.ledger_dir,
            lfs_root=config.lfs_root,
            timeout_sec=max(1, config.timeout_sec),
            service_failure_limit=max(1, config.service_failure_limit),
            max_records=max(0, batch_limit),
            lookback_days=max(0, config.lookback_days),
            dry_run=config.dry_run,
            code_filter=config.code_filter,
            allowed_states=config.allowed_states,
        )
        download_report = download_pdf_job.run_download_stage(download_config)
        report.batches_run = batch_index
        report.last_download_report = download_report
        report.total_download_selected += download_report.selected
        report.total_download_processed += download_report.processed
        report.total_download_success += download_report.success
        report.total_download_failed += download_report.failed
        report.total_download_skipped += download_report.skipped
        report.total_download_service_failures += download_report.service_failures

        if config.verbose:
            print_stage_report(
                "Download stage",
                selected=download_report.selected,
                processed=download_report.processed,
                success=download_report.success,
                failed=download_report.failed,
                skipped=download_report.skipped,
                service_failures=download_report.service_failures,
                stopped_early=download_report.stopped_early,
            )
        _verbose_preview_codes(config, "download processed codes", download_report.processed_codes)

        if download_report.selected == 0:
            _print_batch_summary(
                batch_index=batch_index,
                elapsed_seconds=time.monotonic() - batch_started_at,
                download_report=download_report,
                success_files=0,
                uploaded_files=uploaded_files,
                pdf_processed=pdf_processed,
                pdf_updated=pdf_updated,
                deleted_files=deleted_files,
            )
            report.stop_reason = "no_eligible_records"
            print("No eligible records remain. Stopping workflow.")
            return 0, report

        success_codes, file_paths = _collect_batch_files(
            ledger_dir=config.ledger_dir,
            processed_codes=download_report.processed_codes,
        )
        report.total_batch_success_with_files += len(success_codes)
        _verbose_log(config, f"batch success records with local PDFs: {len(success_codes)}")
        _verbose_preview_codes(config, "batch success codes", success_codes)
        if config.verbose and success_codes:
            show_count = min(len(success_codes), 25)
            print(f"[verbose] success code -> file path (showing {show_count})")
            for code, path in list(zip(success_codes, file_paths))[:show_count]:
                print(f"[verbose]   {code} -> {path}")

        if not success_codes:
            _print_batch_summary(
                batch_index=batch_index,
                elapsed_seconds=time.monotonic() - batch_started_at,
                download_report=download_report,
                success_files=0,
                uploaded_files=uploaded_files,
                pdf_processed=pdf_processed,
                pdf_updated=pdf_updated,
                deleted_files=deleted_files,
            )
            print("No successful downloaded files in this batch. Skipping upload and pdf-info for this batch.")
            if config.dry_run:
                report.stop_reason = "dry_run_single_batch"
                print("Dry-run mode keeps candidate state unchanged; stopping after one batch.")
                return 0, report
            if download_report.processed == 0:
                report.stop_reason = "no_progress"
                print("No actionable records were processed in this batch. Stopping to avoid an infinite loop.")
                return 0, report
            if _runtime_reached(deadline_monotonic):
                report.stop_reason = "max_runtime_reached"
                print(
                    "Reached max runtime limit after completing current batch "
                    f"({max_runtime_seconds} seconds). Stopping workflow."
                )
                return 0, report
            continue

        try:
            uploaded_files = _run_upload_batch(config, file_paths)
        except sync_hf_job.SyncHFError as exc:
            print(f"Upload stage failed: {exc}")
            report.stop_reason = "upload_failed"
            return 1, report
        report.total_uploaded_files += uploaded_files

        _verbose_log(config, f"running pdf-info for {len(success_codes)} records")
        if config.verbose and not config.pdf_verbose:
            print("[skip filter] non-batch records are filtered out (enable --pdf-verbose for full per-record logs)")
        try:
            pdf_report = pdf_info_job.run_pdf_info_stage(
                pdf_info_job.PdfInfoConfig(
                    ledger_dir=config.ledger_dir,
                    max_records=max(0, len(success_codes)),
                    lookback_days=max(0, config.lookback_days),
                    dry_run=config.dry_run,
                    force=config.pdf_force,
                    mark_missing=config.pdf_mark_missing,
                    verbose=config.pdf_verbose,
                    code_filter=set(success_codes),
                )
            )
        except pdf_info_job.PdfInfoDependencyError as exc:
            print(f"pdf-info stage failed: {exc}")
            report.stop_reason = "pdf_info_failed"
            return 2, report

        report.last_pdf_info_report = pdf_report
        report.total_pdf_info_processed += pdf_report.processed_records
        report.total_pdf_info_updated += pdf_report.updated_records
        pdf_processed = pdf_report.processed_records
        pdf_updated = pdf_report.updated_records
        if config.verbose:
            pdf_info_job.print_pdf_info_stage_report(pdf_report, dry_run=config.dry_run)

        deleted_files, skipped_cleanup = _cleanup_batch_files(config, file_paths)
        report.total_deleted_files += deleted_files
        report.total_cleanup_skipped += skipped_cleanup
        _print_batch_summary(
            batch_index=batch_index,
            elapsed_seconds=time.monotonic() - batch_started_at,
            download_report=download_report,
            success_files=len(success_codes),
            uploaded_files=uploaded_files,
            pdf_processed=pdf_processed,
            pdf_updated=pdf_updated,
            deleted_files=deleted_files,
        )
        _verbose_log(config, f"cleanup skipped entries: {skipped_cleanup}")

        if config.dry_run:
            report.stop_reason = "dry_run_single_batch"
            print("Dry-run mode keeps candidate state unchanged; stopping after one batch.")
            return 0, report

        if _runtime_reached(deadline_monotonic):
            report.stop_reason = "max_runtime_reached"
            print(
                "Reached max runtime limit after completing current batch "
                f"({max_runtime_seconds} seconds). Stopping workflow."
            )
            return 0, report


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = "Run one batch workflow: download PDFs, upload those PDFs to HF, then compute pdf_info."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--batch-size", type=int, default=25, help="Maximum records in one workflow batch")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on total records processed across batches")
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=DEFAULT_MAX_RUNTIME_MINUTES,
        help="Stop workflow once runtime reaches N minutes (default: 345 = 5h45m; 0 disables)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Process only records dated within the last N days (0 means no date filter)",
    )
    parser.add_argument(
        "--lfs-root",
        default="",
        help="Download destination root. Default: <hf-repo-path>/pdfs",
    )
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument("--allowed-state", action="append", default=[], help="Override download-stage allowed states")
    parser.add_argument("--timeout-sec", type=int, default=30, help="Download timeout seconds")
    parser.add_argument(
        "--service-failure-limit",
        type=int,
        default=10,
        help="Stop download stage after N consecutive service failures",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan workflow without writing files/ledger")
    parser.add_argument("--skip-upload", action="store_true", help="Skip HF upload step")

    parser.add_argument(
        "--hf-repo-path",
        default=hf_defaults.dataset_repo_path,
        help="Local HF data directory (default from `import/import_config.yaml`).",
    )
    parser.add_argument(
        "--hf-repo-id",
        default=hf_defaults.dataset_repo_id,
        help="HF dataset repo id (`namespace/name`).",
    )
    parser.add_argument(
        "--hf-remote-url",
        default=hf_defaults.dataset_repo_url,
        help="HF dataset remote URL (used to infer repo id if needed).",
    )
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token override")
    parser.add_argument("--hf-branch", default="main", help="HF revision/branch for upload")
    parser.add_argument(
        "--hf-commit-message",
        default="workflow: upload downloaded PDFs batch",
        help="Commit message used for HF upload",
    )
    parser.add_argument("--hf-no-verify-storage", action="store_true", help="Skip post-upload verification checks")
    parser.add_argument("--hf-skip-push", action="store_true", help="Skip remote upload during HF step")

    parser.add_argument("--pdf-force", action="store_true", help="Recompute pdf_info even when status is already success")
    parser.add_argument("--pdf-mark-missing", action="store_true", help="Mark pdf_info as missing_pdf when local file is absent")
    parser.add_argument("--pdf-verbose", action="store_true", help="Verbose per-record logs for pdf-info stage")
    parser.add_argument("--verbose", action="store_true", help="Verbose workflow progress logging")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    allowed_states = parse_state_list(args.allowed_state) or set(download_pdf_job.DEFAULT_ALLOWED_STATES)

    try:
        hf_repo_path = sync_hf_job.resolve_hf_repo_path(args.hf_repo_path)
        hf_repo_id = sync_hf_job.resolve_hf_repo_id(args.hf_repo_id, args.hf_remote_url)
    except sync_hf_job.SyncHFError as exc:
        print(f"workflow config error: {exc}")
        return 2

    lfs_root = Path(args.lfs_root).resolve() if args.lfs_root else (hf_repo_path / "pdfs").resolve()
    config = DownloadUploadPdfInfoWorkflowConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        batch_size=max(0, args.batch_size),
        max_records=max(0, args.max_records),
        max_runtime_minutes=max(0, args.max_runtime_minutes),
        lookback_days=max(0, args.lookback_days),
        timeout_sec=max(1, args.timeout_sec),
        service_failure_limit=max(1, args.service_failure_limit),
        dry_run=args.dry_run,
        lfs_root=lfs_root,
        code_filter=code_filter,
        allowed_states=allowed_states,
        skip_upload=args.skip_upload,
        hf_repo_path=hf_repo_path,
        hf_repo_id=hf_repo_id,
        hf_remote_url=args.hf_remote_url,
        hf_token=args.hf_token,
        hf_branch=args.hf_branch,
        hf_commit_message=args.hf_commit_message,
        hf_no_verify_storage=args.hf_no_verify_storage,
        hf_skip_push=args.hf_skip_push,
        pdf_force=args.pdf_force,
        pdf_mark_missing=args.pdf_mark_missing,
        pdf_verbose=args.pdf_verbose,
        verbose=args.verbose,
    )
    exit_code, report = run_workflow(config)
    if report is not None:
        print("Workflow summary:")
        print(f"  batches_run: {report.batches_run}")
        print(f"  stop_reason: {report.stop_reason or 'completed'}")
        print(f"  total_download_selected: {report.total_download_selected}")
        print(f"  total_download_processed: {report.total_download_processed}")
        print(f"  total_download_success: {report.total_download_success}")
        print(f"  total_download_failed: {report.total_download_failed}")
        print(f"  total_download_skipped: {report.total_download_skipped}")
        print(f"  total_download_service_failures: {report.total_download_service_failures}")
        print(f"  total_batch_success_with_files: {report.total_batch_success_with_files}")
        print(f"  total_uploaded_files: {report.total_uploaded_files}")
        print(f"  total_pdf_info_processed: {report.total_pdf_info_processed}")
        print(f"  total_pdf_info_updated: {report.total_pdf_info_updated}")
        print(f"  total_deleted_files: {report.total_deleted_files}")
        print(f"  total_cleanup_skipped: {report.total_cleanup_skipped}")
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch workflow for download/upload/pdf-info.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
