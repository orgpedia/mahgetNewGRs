#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import re
import urllib.error
import urllib.request
import time
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path

from info_store import InfoStore as LedgerStore
from job_utils import (
    JobRunResult,
    StageRecord,
    detect_service_failure,
    ensure_parent_dir,
    filter_stage_records,
    is_record_within_lookback,
    load_code_filter,
    parse_state_list,
    print_stage_report,
    sha1_file,
)
from ledger_engine import InvalidTransitionError, RetryLimitExceededError, to_ledger_relative_path
from local_env import load_local_env


SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

DEFAULT_ALLOWED_STATES = {
    "FETCHED",
    "DOWNLOAD_FAILED",
    "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT",
}


@dataclass(frozen=True)
class DownloadJobConfig:
    max_records: int
    lookback_days: int
    code_filter: set[str] = field(default_factory=set)
    allowed_states: set[str] = field(default_factory=lambda: set(DEFAULT_ALLOWED_STATES))
    verbose: bool = False


@dataclass(frozen=True)
class DownloadStageConfig:
    lfs_root: Path
    timeout_sec: int
    service_failure_limit: int
    dry_run: bool
    verbose: bool = False


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
    success_codes: list[str] = field(default_factory=list)
    failed_codes: list[str] = field(default_factory=list)
    skipped_codes: list[str] = field(default_factory=list)


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


def _missing_lfs_path(record: dict) -> bool:
    value = record.get("lfs_path")
    if isinstance(value, str):
        return not value.strip()
    return True


def _record_gr_datetime(record: dict) -> datetime | None:
    text = str(record.get("gr_date") or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        candidate = text[:10]
        try:
            return datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            return None
    return None


def _download_candidate_sort_key(item: StageRecord) -> tuple[int, int, str]:
    parsed = _record_gr_datetime(item.record)
    if parsed is not None:
        return (0, -int(parsed.strftime("%Y%m%d")), item.unique_code)
    return (1, 0, item.unique_code)


def _select_download_candidates(store: LedgerStore, config: DownloadJobConfig) -> list[StageRecord]:
    stage_candidates = filter_stage_records(
        store,
        allowed_states=config.allowed_states,
        stage="download",
        code_filter=config.code_filter,
        max_attempts=2,
        lookback_days=config.lookback_days,
    )
    print(f'Stage candidates: {len(stage_candidates)}')
    by_code: dict[str, StageRecord] = {item.unique_code: item for item in stage_candidates}

    result_candidates = []
    for stage_record in stage_candidates:
        record = stage_record.record
        unique_code = str(record.get("unique_code") or "").strip()

        if config.code_filter and unique_code not in config.code_filter:
            continue

        if not _missing_lfs_path(record):
            continue
        result_candidates.append(stage_record)
    print(f'Result candidates: {len(result_candidates)}')
    return result_candidates


def _download_content(url: str, timeout_sec: int) -> tuple[int | None, bytes | None, str]:
    request = urllib.request.Request(url, method="GET")
    time.sleep(1)
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


def _verbose_log(config: DownloadStageConfig, message: str) -> None:
    if config.verbose:
        print(f"[download verbose] {message}")


def _verbose_job_log(config: DownloadJobConfig, message: str) -> None:
    if config.verbose:
        print(f"[download verbose] {message}")


def _verbose_saved(
    config: DownloadStageConfig,
    *,
    unique_code: str,
    success: bool,
    error: str = "",
) -> None:
    if not config.verbose:
        return
    status = "success" if success else "failed"
    suffix = f" error={error}" if error else ""
    print(f"**[download save] code={unique_code} status={status}{suffix}**")


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


def select_candidates(config: DownloadJobConfig, store: LedgerStore) -> list[str]:
    candidates = _select_download_candidates(store, config)
    limit = config.max_records if config.max_records > 0 else len(candidates)
    selected_codes = [item.unique_code for item in candidates[:limit]]
    _verbose_job_log(
        config,
        (
            f"candidate_count={len(candidates)} selected={len(selected_codes)} "
            f"max_records={config.max_records} lookback_days={config.lookback_days} "
            f"code_filter_size={len(config.code_filter)}"
        ),
    )
    if config.verbose and selected_codes:
        preview = selected_codes[:25]
        print(f"[download verbose] selected codes preview ({len(preview)}/{len(selected_codes)}):")
        for code in preview:
            print(f"[download verbose]   {code}")
    return selected_codes


def run_selected(
    selected_codes: list[str],
    stage_config: DownloadStageConfig,
    store: LedgerStore,
) -> JobRunResult[DownloadStageReport]:
    report = DownloadStageReport(selected=len(_dedupe_keep_order(selected_codes)))
    consecutive_service_failures = 0
    codes = _dedupe_keep_order(selected_codes)

    _verbose_log(
        stage_config,
        (
            f"selected={report.selected} dry_run={stage_config.dry_run} "
            f"timeout_sec={stage_config.timeout_sec} service_failure_limit={stage_config.service_failure_limit}"
        ),
    )

    for index, unique_code in enumerate(codes, start=1):
        _verbose_log(stage_config, f"[{index}/{len(codes)}] code={unique_code}")
        record = store.find(unique_code)
        if record is None:
            report.skipped += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=missing_record")
            continue

        source_url = str(record.get("source_url") or "").strip()
        if not source_url:
            report.skipped += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=missing_source_url")
            continue

        report.processed += 1
        report.processed_codes.append(unique_code)

        pdf_path = derive_pdf_path(record, stage_config.lfs_root)
        if not pdf_path.is_absolute():
            pdf_path = Path.cwd() / pdf_path
        planned_path = to_ledger_relative_path(pdf_path)
        _verbose_log(stage_config, f"[plan] code={unique_code} url={source_url} path={planned_path}")

        if stage_config.dry_run:
            continue

        try:
            if pdf_path.exists() and pdf_path.is_file():
                file_size = pdf_path.stat().st_size
                file_hash = sha1_file(pdf_path)
                metadata = {
                    "path": to_ledger_relative_path(pdf_path),
                    "hash": file_hash,
                    "size": file_size,
                }
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=True,
                    metadata=metadata,
                )
                _verbose_saved(stage_config, unique_code=unique_code, success=True)
                report.success += 1
                report.success_codes.append(unique_code)
                consecutive_service_failures = 0
                _verbose_log(stage_config, f"[reuse] code={unique_code} path={metadata['path']} size={file_size}")
                continue

            _verbose_log(stage_config, f"[download] code={unique_code} url={source_url}")
            status_code, content, error_text = _download_content(source_url, stage_config.timeout_sec)
            is_service_failure = detect_service_failure(status_code=status_code)
            if status_code == 200 and content is not None:
                ensure_parent_dir(pdf_path)
                pdf_path.write_bytes(content)
                metadata = {
                    "path": to_ledger_relative_path(pdf_path),
                    "hash": sha1_bytes(content),
                    "size": len(content),
                }
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=True,
                    metadata=metadata,
                )
                _verbose_saved(stage_config, unique_code=unique_code, success=True)
                report.success += 1
                report.success_codes.append(unique_code)
                consecutive_service_failures = 0
                _verbose_log(
                    stage_config,
                    f"[success] code={unique_code} status={status_code} path={metadata['path']} size={metadata['size']}",
                )
            else:
                failure_text = error_text or f"download_failed_{status_code}"
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=False,
                    error=failure_text,
                )
                _verbose_saved(stage_config, unique_code=unique_code, success=False, error=failure_text)
                report.failed += 1
                report.failed_codes.append(unique_code)
                _verbose_log(
                    stage_config,
                    f"[failed] code={unique_code} status={status_code if status_code is not None else '-'} "
                    f"error={failure_text} url={source_url}",
                )
                if is_service_failure:
                    consecutive_service_failures += 1
                    report.service_failures += 1
                    _verbose_log(
                        stage_config,
                        f"[service-failure] consecutive={consecutive_service_failures}/{stage_config.service_failure_limit}",
                    )
                else:
                    consecutive_service_failures = 0
        except RetryLimitExceededError:
            report.skipped += 1
            report.skipped_codes.append(unique_code)
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=retry_limit_exceeded")
        except InvalidTransitionError as exc:
            report.skipped += 1
            report.skipped_codes.append(unique_code)
            consecutive_service_failures = 0
            _verbose_log(stage_config, f"[skip] code={unique_code} reason=invalid_transition error={exc}")
        except Exception as exc:
            report.failed += 1
            report.failed_codes.append(unique_code)
            report.service_failures += 1
            consecutive_service_failures += 1
            _verbose_log(stage_config, f"[exception] code={unique_code} error={exc}")
            try:
                failure_text = f"download_exception:{exc}"
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="download",
                    success=False,
                    error=failure_text,
                )
                _verbose_saved(stage_config, unique_code=unique_code, success=False, error=failure_text)
            except Exception:
                pass

        if consecutive_service_failures >= stage_config.service_failure_limit:
            report.stopped_early = True
            _verbose_log(
                stage_config,
                "stopping_early reason=service_failure_limit "
                f"consecutive={consecutive_service_failures}",
            )
            break

    _verbose_log(
        stage_config,
        "completed "
        f"processed={report.processed} success={report.success} failed={report.failed} "
        f"skipped={report.skipped} service_failures={report.service_failures} stopped_early={report.stopped_early}",
    )

    if report.stopped_early:
        return JobRunResult(
            report=report,
            fatal_error=(
                "download job stopped early after hitting consecutive service failure limit "
                f"({stage_config.service_failure_limit})"
            ),
        )
    return JobRunResult(report=report)


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
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Process only records dated within the last N days (0 means no date filter)",
    )
    parser.add_argument("--timeout-sec", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument(
        "--service-failure-limit",
        type=int,
        default=10,
        help="Stop after N consecutive service failures",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan records without downloading")
    parser.add_argument("--verbose", action="store_true", help="Print per-record download status details")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    allowed_states = parse_state_list(args.allowed_state) or set(DEFAULT_ALLOWED_STATES)

    store = LedgerStore(Path(args.ledger_dir).resolve())
    job_config = DownloadJobConfig(
        max_records=max(0, args.max_records),
        lookback_days=max(0, args.lookback_days),
        code_filter=code_filter,
        allowed_states=allowed_states,
        verbose=args.verbose,
    )
    stage_config = DownloadStageConfig(
        lfs_root=Path(args.lfs_root).resolve(),
        timeout_sec=max(1, args.timeout_sec),
        service_failure_limit=max(1, args.service_failure_limit),
        dry_run=args.dry_run,
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
