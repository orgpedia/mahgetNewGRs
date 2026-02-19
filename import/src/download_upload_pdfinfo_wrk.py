#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path

import download_pdf_job
import pdf_info_job
import sync_hf_job
from import_config import load_import_config
from info_store import InfoStore as LedgerStore
from job_utils import load_code_filter, parse_state_list, print_stage_report
from local_env import load_local_env


@dataclass(frozen=True)
class DownloadUploadPdfInfoWorkflowConfig:
    ledger_dir: Path
    batch_size: int
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


@dataclass
class DownloadUploadPdfInfoWorkflowReport:
    download_report: download_pdf_job.DownloadStageReport
    batch_success_codes: list[str] = field(default_factory=list)
    batch_upload_files: int = 0
    pdf_info_report: pdf_info_job.PdfInfoReport | None = None


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

    relative_paths = _to_hf_relative_paths(file_paths, config.hf_repo_path)
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


def run_workflow(config: DownloadUploadPdfInfoWorkflowConfig) -> tuple[int, DownloadUploadPdfInfoWorkflowReport | None]:
    print("Workflow: download -> upload -> pdf-info")
    print(f"Batch size: {config.batch_size}")

    download_config = download_pdf_job.DownloadStageConfig(
        ledger_dir=config.ledger_dir,
        lfs_root=config.lfs_root,
        timeout_sec=max(1, config.timeout_sec),
        service_failure_limit=max(1, config.service_failure_limit),
        max_records=max(0, config.batch_size),
        dry_run=config.dry_run,
        code_filter=config.code_filter,
        allowed_states=config.allowed_states,
    )
    download_report = download_pdf_job.run_download_stage(download_config)
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

    success_codes, file_paths = _collect_batch_files(
        ledger_dir=config.ledger_dir,
        processed_codes=download_report.processed_codes,
    )
    print(f"Batch success records with local PDFs: {len(success_codes)}")

    report = DownloadUploadPdfInfoWorkflowReport(
        download_report=download_report,
        batch_success_codes=success_codes,
    )

    if not success_codes:
        print("No successful downloaded files in this batch. Skipping upload and pdf-info.")
        return 0, report

    try:
        uploaded_files = _run_upload_batch(config, file_paths)
    except sync_hf_job.SyncHFError as exc:
        print(f"Upload stage failed: {exc}")
        return 1, report

    report.batch_upload_files = uploaded_files

    try:
        pdf_report = pdf_info_job.run_pdf_info_stage(
            pdf_info_job.PdfInfoConfig(
                ledger_dir=config.ledger_dir,
                max_records=max(0, len(success_codes)),
                dry_run=config.dry_run,
                force=config.pdf_force,
                mark_missing=config.pdf_mark_missing,
                verbose=config.pdf_verbose,
                code_filter=set(success_codes),
            )
        )
    except pdf_info_job.PdfInfoDependencyError as exc:
        print(f"pdf-info stage failed: {exc}")
        return 2, report

    report.pdf_info_report = pdf_report
    pdf_info_job.print_pdf_info_stage_report(pdf_report, dry_run=config.dry_run)
    return 0, report


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = "Run one batch workflow: download PDFs, upload those PDFs to HF, then compute pdf_info."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--batch-size", type=int, default=100, help="Maximum records in one workflow batch")
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
    )
    exit_code, report = run_workflow(config)
    if report is not None:
        print("Workflow summary:")
        print(f"  downloaded_processed: {report.download_report.processed}")
        print(f"  downloaded_success: {report.download_report.success}")
        print(f"  batch_success_with_files: {len(report.batch_success_codes)}")
        print(f"  uploaded_files: {report.batch_upload_files}")
        if report.pdf_info_report is not None:
            print(f"  pdf_info_processed: {report.pdf_info_report.processed_records}")
            print(f"  pdf_info_updated: {report.pdf_info_report.updated_records}")
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
