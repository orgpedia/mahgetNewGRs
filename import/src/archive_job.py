#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from job_utils import detect_service_failure, filter_stage_records, load_code_filter, parse_state_list, print_stage_report
from info_store import InfoStore as LedgerStore
from ledger_engine import RetryLimitExceededError
from local_env import load_local_env


DEFAULT_ALLOWED_STATES = {
    "WAYBACK_UPLOADED",
    "WAYBACK_UPLOAD_FAILED",
    "DOWNLOAD_FAILED",
    "DOWNLOAD_SUCCESS",
}

SAFE_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class ArchiveJobConfig:
    ledger_dir: Path
    max_records: int
    dry_run: bool
    service_failure_limit: int
    code_filter: set[str] = field(default_factory=set)
    allowed_states: set[str] = field(default_factory=lambda: set(DEFAULT_ALLOWED_STATES))
    ia_access_key: str = ""
    ia_secret_key: str = ""
    metadata_only_fallback: bool = True


@dataclass
class ArchiveJobReport:
    selected: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    metadata_fallback: int = 0
    service_failures: int = 0
    stopped_early: bool = False
    processed_codes: list[str] = field(default_factory=list)


def _safe_identifier(unique_code: str) -> str:
    text = SAFE_IDENTIFIER_RE.sub("-", unique_code).strip("-")
    return text or "unknown"


def _archive_identifier(record: dict[str, Any]) -> str:
    archive_obj = record.get("archive", {})
    existing = archive_obj.get("identifier") if isinstance(archive_obj, dict) else ""
    if isinstance(existing, str) and existing.strip():
        return existing.strip()
    unique_code = str(record.get("unique_code") or "unknown")
    return f"in.gov.maharashtra.gr.{_safe_identifier(unique_code)}"


def _archive_detail_url(identifier: str) -> str:
    return f"https://archive.org/details/{identifier}"


def _build_metadata(record: dict[str, Any], identifier: str) -> dict[str, Any]:
    department_name = str(record.get("department_name") or "")
    wayback_url = ""
    wayback_obj = record.get("wayback", {})
    if isinstance(wayback_obj, dict):
        wayback_url = str(wayback_obj.get("url") or "")
    metadata = {
        "collection": "maharashtragr",
        "mediatype": "texts",
        "title": f"Maharashtra GR: #{record.get('unique_code', '')}",
        "creator": "Government of Maharashtra",
        "department": department_name,
        "unique_code": str(record.get("unique_code") or ""),
        "url": str(record.get("source_url") or ""),
    }
    gr_date = str(record.get("gr_date") or "").strip()
    if gr_date:
        metadata["date"] = gr_date
    if wayback_url:
        metadata["wayback_url"] = wayback_url
    title = str(record.get("title") or "").strip()
    if title:
        metadata["description"] = title
    if department_name:
        metadata["subject"] = ["Maharashtra Government Resolutions", department_name]
    metadata["identifier"] = identifier
    return metadata


def _upload_pdf_to_archive(
    *,
    record: dict[str, Any],
    pdf_path: Path,
    identifier: str,
    config: ArchiveJobConfig,
) -> tuple[bool, str, str, bool]:
    try:
        import internetarchive as ia  # type: ignore
    except Exception:
        return False, "", "internetarchive_not_installed", False

    metadata = _build_metadata(record, identifier)
    try:
        if config.ia_access_key and config.ia_secret_key:
            item = ia.get_item(identifier, {"s3": {"access": config.ia_access_key, "secret": config.ia_secret_key}})
            responses = item.upload(
                str(pdf_path),
                metadata=metadata,
                access_key=config.ia_access_key,
                secret_key=config.ia_secret_key,
                validate_identifier=True,
            )
        else:
            item = ia.get_item(identifier)
            responses = item.upload(str(pdf_path), metadata=metadata, validate_identifier=True)

        response_list = list(responses)
        response_url = ""
        if response_list:
            response_url = str(getattr(response_list[0], "url", "") or "")
        if not response_url:
            response_url = f"https://s3.us.archive.org/{identifier}/{pdf_path.name}"
        return True, response_url, "", False
    except Exception as exc:
        return False, "", f"archive_upload_exception:{exc}", True


def run_archive_job(config: ArchiveJobConfig) -> ArchiveJobReport:
    store = LedgerStore(config.ledger_dir)
    report = ArchiveJobReport()

    candidates = filter_stage_records(
        store,
        allowed_states=config.allowed_states,
        stage="archive",
        code_filter=config.code_filter,
        max_attempts=2,
    )
    report.selected = len(candidates)
    limit = config.max_records if config.max_records > 0 else len(candidates)
    consecutive_service_failures = 0

    for item in candidates[:limit]:
        unique_code = item.unique_code
        record = item.record
        report.processed += 1
        report.processed_codes.append(unique_code)

        archive_obj = record.get("archive", {})
        wayback_obj = record.get("wayback", {})
        wayback_url = str(wayback_obj.get("url") or "").strip() if isinstance(wayback_obj, dict) else ""
        has_wayback_url = bool(wayback_url)
        identifier = _archive_identifier(record)

        download_obj = record.get("download", {})
        download_status = str(download_obj.get("status") or "") if isinstance(download_obj, dict) else ""
        download_path = str(download_obj.get("path") or "") if isinstance(download_obj, dict) else ""
        has_document = download_status == "success" and bool(download_path)

        if config.dry_run:
            continue

        try:
            if not has_document:
                if not config.metadata_only_fallback:
                    report.skipped += 1
                    continue
                detail_url = str(archive_obj.get("url") or "").strip() if isinstance(archive_obj, dict) else ""
                if not detail_url:
                    detail_url = _archive_detail_url(identifier)
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="archive",
                    success=True,
                    metadata={"identifier": identifier, "url": detail_url},
                    has_document=False,
                    has_wayback_url=False,
                )
                report.success += 1
                report.metadata_fallback += 1
                consecutive_service_failures = 0
                continue

            pdf_path = Path(download_path)
            if not pdf_path.is_absolute():
                pdf_path = Path.cwd() / pdf_path
            if not pdf_path.exists():
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="archive",
                    success=False,
                    error="missing_download_file",
                )
                report.failed += 1
                consecutive_service_failures = 0
                continue

            upload_ok, archive_url, error_text, service_failure = _upload_pdf_to_archive(
                record=record,
                pdf_path=pdf_path,
                identifier=identifier,
                config=config,
            )
            if upload_ok:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="archive",
                    success=True,
                    metadata={"identifier": identifier, "url": archive_url},
                    has_document=True,
                    has_wayback_url=has_wayback_url,
                )
                report.success += 1
                consecutive_service_failures = 0
            else:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="archive",
                    success=False,
                    error=error_text or "archive_upload_failed",
                )
                report.failed += 1
                if service_failure:
                    report.service_failures += 1
                    consecutive_service_failures += 1
                else:
                    consecutive_service_failures = 0
        except RetryLimitExceededError:
            report.skipped += 1
        except Exception as exc:
            report.failed += 1
            report.service_failures += 1
            consecutive_service_failures += 1
            try:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="archive",
                    success=False,
                    error=f"archive_exception:{exc}",
                )
            except Exception:
                pass

        if consecutive_service_failures >= max(1, config.service_failure_limit):
            report.stopped_early = True
            break

    return report


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run archive upload stage for eligible ledger records."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument("--allowed-state", action="append", default=[], help="Override allowed states")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on records processed")
    parser.add_argument(
        "--service-failure-limit",
        type=int,
        default=10,
        help="Stop after N consecutive service failures",
    )
    parser.add_argument("--ia-access-key", default="", help="Archive.org access key (or IA_ACCESS_KEY env)")
    parser.add_argument("--ia-secret-key", default="", help="Archive.org secret key (or IA_SECRET_KEY env)")
    parser.add_argument(
        "--no-metadata-fallback",
        action="store_true",
        help="Disable metadata-only fallback for download-failed records",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan records without uploading")
    return parser


def _print_report(report: ArchiveJobReport) -> None:
    print_stage_report(
        "Archive job",
        selected=report.selected,
        processed=report.processed,
        success=report.success,
        failed=report.failed,
        skipped=report.skipped,
        service_failures=report.service_failures,
        stopped_early=report.stopped_early,
        extras={"metadata_fallback": report.metadata_fallback},
    )


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    allowed_states = parse_state_list(args.allowed_state) or set(DEFAULT_ALLOWED_STATES)
    access_key = args.ia_access_key or os.environ.get("IA_ACCESS_KEY", "")
    secret_key = args.ia_secret_key or os.environ.get("IA_SECRET_KEY", "")

    config = ArchiveJobConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        service_failure_limit=max(1, args.service_failure_limit),
        code_filter=code_filter,
        allowed_states=allowed_states,
        ia_access_key=access_key,
        ia_secret_key=secret_key,
        metadata_only_fallback=not args.no_metadata_fallback,
    )
    report = run_archive_job(config)
    _print_report(report)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run archive upload stage.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
