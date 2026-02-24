#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import download_pdf_job
import import_pdf_job
import pdf_info_job
import sync_hf_job
from import_config import load_import_config
from info_store import InfoStore as LedgerStore
from job_utils import chunked, load_code_filter, parse_state_list, print_stage_report
from local_env import load_local_env


DEFAULT_MAX_RUNTIME_MINUTES = 340


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
    partitions_total: int = 0
    partitions_scanned: int = 0
    partitions_with_candidates: int = 0
    stop_reason: str = ""
    total_selected: int = 0
    total_download_selected: int = 0
    total_download_processed: int = 0
    total_download_success: int = 0
    total_download_failed: int = 0
    total_download_skipped: int = 0
    total_download_service_failures: int = 0
    total_uploaded_files: int = 0
    total_pdf_info_processed: int = 0
    total_pdf_info_updated: int = 0
    last_download_report: download_pdf_job.DownloadStageReport | None = None
    last_import_report: import_pdf_job.ImportPdfJobReport | None = None
    last_pdf_info_report: pdf_info_job.PdfInfoReport | None = None


def _runtime_seconds(max_runtime_minutes: int) -> int:
    minutes = max(0, int(max_runtime_minutes))
    return minutes * 60


def _runtime_reached(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


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


def run_workflow(config: DownloadUploadPdfInfoWorkflowConfig) -> tuple[int, DownloadUploadPdfInfoWorkflowReport | None]:
    max_runtime_seconds = _runtime_seconds(config.max_runtime_minutes)
    deadline_monotonic = time.monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None

    print("Workflow: download -> import-pdf(upload) -> pdf-info")
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
    partitions = LedgerStore.list_url_partitions(config.ledger_dir)
    report.partitions_total = len(partitions)
    _verbose_log(config, f"partitions_total={report.partitions_total}")
    if config.verbose and partitions:
        preview = partitions[:25]
        print(f"[verbose] partition preview ({len(preview)}/{len(partitions)}):")
        for partition in preview:
            print(f"[verbose]   {partition}")

    remaining_records = max(0, config.max_records)
    any_selected = False

    for partition in partitions:
        if config.max_records > 0 and remaining_records <= 0:
            break
        if _runtime_reached(deadline_monotonic):
            report.stop_reason = "max_runtime_reached"
            print(
                "Reached max runtime limit before starting next partition "
                f"({max_runtime_seconds} seconds). Stopping workflow."
            )
            return 0, report

        report.partitions_scanned += 1
        print(f"Partition: {partition}")
        _verbose_log(config, f"loading partition={partition}")
        store = LedgerStore(config.ledger_dir, partitions={partition})

        partition_max_records = remaining_records if config.max_records > 0 else 0
        download_job_config = download_pdf_job.DownloadJobConfig(
            max_records=partition_max_records,
            lookback_days=max(0, config.lookback_days),
            code_filter=config.code_filter,
            allowed_states=config.allowed_states,
            verbose=config.verbose,
        )

        selected_codes = download_pdf_job.select_candidates(download_job_config, store)
        _verbose_preview_codes(config, f"partition {partition} selected codes", selected_codes)
        if not selected_codes:
            continue

        any_selected = True
        report.partitions_with_candidates += 1
        report.total_selected += len(selected_codes)
        if config.max_records > 0:
            remaining_records = max(0, remaining_records - len(selected_codes))

        effective_batch_size = config.batch_size if config.batch_size > 0 else len(selected_codes)
        batches = chunked(selected_codes, effective_batch_size)

        for batch_codes in batches:
            if _runtime_reached(deadline_monotonic):
                report.stop_reason = "max_runtime_reached"
                print(
                    "Reached max runtime limit before starting next batch "
                    f"({max_runtime_seconds} seconds). Stopping workflow."
                )
                return 0, report

            report.batches_run += 1
            print(f"Batch #{report.batches_run} ({partition})")
            _verbose_preview_codes(config, "batch input codes", batch_codes)

            download_stage_config = download_pdf_job.DownloadStageConfig(
                lfs_root=config.lfs_root,
                timeout_sec=max(1, config.timeout_sec),
                service_failure_limit=max(1, config.service_failure_limit),
                dry_run=config.dry_run,
                verbose=config.verbose,
            )
            download_result = download_pdf_job.run_selected(batch_codes, download_stage_config, store)
            download_report = download_result.report
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

            if download_result.fatal_error:
                print(download_result.fatal_error)
                report.stop_reason = "download_fatal"
                return 1, report

            success_codes = list(download_report.success_codes)
            _verbose_preview_codes(config, "download success codes", success_codes)
            if not success_codes:
                print("No successful downloads in this batch. Skipping upload and pdf-info for this batch.")
                if config.dry_run:
                    report.stop_reason = "dry_run_single_batch"
                    print("Dry-run mode keeps candidate state unchanged; stopping after one batch.")
                    return 0, report
                continue

            if config.skip_upload:
                print("Upload stage: skipped by flag.")
            else:
                import_stage_config = import_pdf_job.ImportPdfStageConfig(
                    dry_run=config.dry_run,
                    hf_repo_path=config.hf_repo_path,
                    hf_repo_id=config.hf_repo_id,
                    hf_remote_url=config.hf_remote_url,
                    hf_token=config.hf_token,
                    hf_branch=config.hf_branch,
                    hf_commit_message=config.hf_commit_message,
                    hf_no_verify_storage=config.hf_no_verify_storage,
                    hf_skip_push=config.hf_skip_push,
                    verbose=config.verbose,
                )
                import_result = import_pdf_job.run_selected(success_codes, import_stage_config, store)
                report.last_import_report = import_result.report
                report.total_uploaded_files += import_result.report.uploaded_files
                if import_result.fatal_error:
                    print(import_result.fatal_error)
                    report.stop_reason = "import_pdf_fatal"
                    return 1, report

            pdf_stage_config = pdf_info_job.PdfInfoStageConfig(
                dry_run=config.dry_run,
                force=config.pdf_force,
                mark_missing=config.pdf_mark_missing,
                verbose=(config.pdf_verbose or config.verbose),
            )
            pdf_result = pdf_info_job.run_selected(success_codes, pdf_stage_config, store)
            if pdf_result.fatal_error:
                print(f"pdf-info stage failed: {pdf_result.fatal_error}")
                report.stop_reason = "pdf_info_fatal"
                return 2, report

            pdf_report = pdf_result.report
            report.last_pdf_info_report = pdf_report
            report.total_pdf_info_processed += pdf_report.processed_records
            report.total_pdf_info_updated += pdf_report.updated_records
            if config.verbose:
                pdf_info_job.print_pdf_info_stage_report(pdf_report, dry_run=config.dry_run)

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

    if not any_selected:
        report.stop_reason = "no_eligible_records"
        print("No eligible records remain. Stopping workflow.")
        return 0, report

    report.stop_reason = "completed"
    return 0, report


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = "Run partition-wise batched workflow: per year partition, download -> upload selected PDFs -> pdf-info."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--batch-size", type=int, default=25, help="Maximum records in one workflow batch")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on total records processed across workflow")
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=DEFAULT_MAX_RUNTIME_MINUTES,
        help="Stop workflow once runtime reaches N minutes (default: 340 = 5h40m; 0 disables)",
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
        print(f"  partitions_total: {report.partitions_total}")
        print(f"  partitions_scanned: {report.partitions_scanned}")
        print(f"  partitions_with_candidates: {report.partitions_with_candidates}")
        print(f"  stop_reason: {report.stop_reason or 'completed'}")
        print(f"  total_selected: {report.total_selected}")
        print(f"  total_download_selected: {report.total_download_selected}")
        print(f"  total_download_processed: {report.total_download_processed}")
        print(f"  total_download_success: {report.total_download_success}")
        print(f"  total_download_failed: {report.total_download_failed}")
        print(f"  total_download_skipped: {report.total_download_skipped}")
        print(f"  total_download_service_failures: {report.total_download_service_failures}")
        print(f"  total_uploaded_files: {report.total_uploaded_files}")
        print(f"  total_pdf_info_processed: {report.total_pdf_info_processed}")
        print(f"  total_pdf_info_updated: {report.total_pdf_info_updated}")
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
