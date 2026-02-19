#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from info_store import InfoStore as LedgerStore
from job_utils import (
    detect_service_failure,
    ensure_parent_dir,
    filter_stage_records,
    load_code_filter,
    parse_state_list,
    print_stage_report,
    sha1_file,
)
from ledger_engine import RetryLimitExceededError, to_ledger_relative_path
from local_env import load_local_env


SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

DEFAULT_ALLOWED_STATES = {
    "FETCHED",
    "DOWNLOAD_FAILED",
    "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT",
}


@dataclass(frozen=True)
class DownloadStageConfig:
    ledger_dir: Path
    lfs_root: Path
    timeout_sec: int
    service_failure_limit: int
    max_records: int
    dry_run: bool
    code_filter: set[str] = field(default_factory=set)
    allowed_states: set[str] = field(default_factory=lambda: set(DEFAULT_ALLOWED_STATES))


@dataclass
class DownloadStageReport:
    selected: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    service_failures: int = 0
    stopped_early: bool = False
    processed_codes: list[str] = field(default_factory=list)


def safe_filename(value: str) -> str:
    text = SAFE_FILENAME_RE.sub("_", value).strip("_")
    return text or "unknown"


def derive_pdf_path(record: dict, lfs_root: Path) -> Path:
    existing_lfs_path = record.get("lfs_path")
    if isinstance(existing_lfs_path, str) and existing_lfs_path.strip():
        return Path(existing_lfs_path.strip())

    download_obj = record.get("download", {})
    if isinstance(download_obj, dict):
        existing = download_obj.get("path")
        if isinstance(existing, str) and existing.strip():
            existing_path = Path(existing.strip())
            probe = existing_path if existing_path.is_absolute() else (Path.cwd() / existing_path)
            if probe.exists() and probe.is_file():
                return existing_path

    department_code = str(record.get("department_code") or "unknown").strip() or "unknown"
    unique_code = safe_filename(str(record.get("unique_code") or "unknown"))
    gr_date = str(record.get("gr_date") or "").strip()
    year_month = gr_date[:7] if len(gr_date) >= 7 and gr_date[4] == "-" else "unknown"
    return lfs_root / department_code / year_month / f"{unique_code}.pdf"


def sha1_bytes(content: bytes) -> str:
    digest = hashlib.sha1()
    digest.update(content)
    return digest.hexdigest().upper()


def _download_content(url: str, timeout_sec: int) -> tuple[int | None, bytes | None, str]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            status_code = int(getattr(response, "status", 200))
            content = response.read()
            return status_code, content, ""
    except urllib.error.HTTPError as exc:
        return int(exc.code), None, f"http_{exc.code}"
    except urllib.error.URLError as exc:
        return None, None, f"url_error:{exc.reason}"
    except TimeoutError:
        return None, None, "timeout"
    except Exception as exc:
        return None, None, f"download_exception:{exc}"


def run_download_stage(config: DownloadStageConfig) -> DownloadStageReport:
    store = LedgerStore(config.ledger_dir)
    report = DownloadStageReport()

    candidates = filter_stage_records(
        store,
        allowed_states=config.allowed_states,
        stage="download",
        code_filter=config.code_filter,
        max_attempts=2,
    )
    report.selected = len(candidates)

    limit = config.max_records if config.max_records > 0 else len(candidates)
    consecutive_service_failures = 0

    for item in candidates[:limit]:
        record = item.record
        unique_code = item.unique_code
        source_url = str(record.get("source_url") or "").strip()
        if not source_url:
            report.skipped += 1
            continue

        report.processed += 1
        report.processed_codes.append(unique_code)

        pdf_path = derive_pdf_path(record, config.lfs_root)
        if not pdf_path.is_absolute():
            pdf_path = Path.cwd() / pdf_path

        if config.dry_run:
            continue

        try:
            if pdf_path.exists() and pdf_path.is_file():
                file_size = pdf_path.stat().st_size
                file_hash = sha1_file(pdf_path)
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=True,
                    metadata={
                        "path": to_ledger_relative_path(pdf_path),
                        "hash": file_hash,
                        "size": file_size,
                    },
                )
                report.success += 1
                consecutive_service_failures = 0
                continue

            status_code, content, error_text = _download_content(source_url, config.timeout_sec)
            is_service_failure = detect_service_failure(status_code=status_code)
            if status_code == 200 and content is not None:
                ensure_parent_dir(pdf_path)
                pdf_path.write_bytes(content)
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=True,
                    metadata={
                        "path": to_ledger_relative_path(pdf_path),
                        "hash": sha1_bytes(content),
                        "size": len(content),
                    },
                )
                report.success += 1
                consecutive_service_failures = 0
            else:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=False,
                    error=error_text or f"download_failed_{status_code}",
                )
                report.failed += 1
                if is_service_failure:
                    consecutive_service_failures += 1
                    report.service_failures += 1
                else:
                    consecutive_service_failures = 0
        except RetryLimitExceededError:
            report.skipped += 1
        except Exception as exc:
            report.failed += 1
            report.service_failures += 1
            consecutive_service_failures += 1
            if not config.dry_run:
                try:
                    store.apply_stage_result(
                        unique_code=unique_code,
                        stage="download",
                        success=False,
                        error=f"download_exception:{exc}",
                    )
                except Exception:
                    pass

        if consecutive_service_failures >= config.service_failure_limit:
            report.stopped_early = True
            break

    return report


def _print_report(report: DownloadStageReport) -> None:
    print_stage_report(
        "Download PDF job",
        selected=report.selected,
        processed=report.processed,
        success=report.success,
        failed=report.failed,
        skipped=report.skipped,
        service_failures=report.service_failures,
        stopped_early=report.stopped_early,
    )


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run PDF download stage for eligible ledger records."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--lfs-root", default="LFS/pdfs", help="LFS PDF root directory")
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument("--allowed-state", action="append", default=[], help="Override allowed states")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on records processed")
    parser.add_argument("--timeout-sec", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument(
        "--service-failure-limit",
        type=int,
        default=10,
        help="Stop after N consecutive service failures",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan records without downloading")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    allowed_states = parse_state_list(args.allowed_state) or set(DEFAULT_ALLOWED_STATES)

    config = DownloadStageConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        lfs_root=Path(args.lfs_root).resolve(),
        timeout_sec=max(1, args.timeout_sec),
        service_failure_limit=max(1, args.service_failure_limit),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        code_filter=code_filter,
        allowed_states=allowed_states,
    )
    report = run_download_stage(config)
    _print_report(report)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PDF download stage.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
