#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from job_utils import detect_service_failure, filter_stage_records, load_code_filter, parse_state_list
from info_store import InfoStore as LedgerStore
from ledger_engine import RetryLimitExceededError
from local_env import load_local_env


DEFAULT_ALLOWED_STATES = {"DOWNLOAD_SUCCESS", "WAYBACK_UPLOAD_FAILED"}
SPN2_SAVE_URL = "https://web.archive.org/save"
SPN2_STATUS_URL_FMT = "https://web.archive.org/save/status/{job_id}"


class SPN2ClientError(Exception):
    pass


@dataclass(frozen=True)
class WaybackJobConfig:
    ledger_dir: Path
    max_records: int
    dry_run: bool
    timeout_sec: int
    poll_interval_sec: float
    poll_timeout_sec: int
    service_failure_limit: int
    code_filter: set[str] = field(default_factory=set)
    allowed_states: set[str] = field(default_factory=lambda: set(DEFAULT_ALLOWED_STATES))
    access_key: str = ""
    secret_key: str = ""
    capture_all: bool = False
    skip_first_archive: bool = False
    delay_wb_availability: bool = True


@dataclass
class WaybackJobReport:
    selected: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    service_failures: int = 0
    stopped_early: bool = False
    processed_codes: list[str] = field(default_factory=list)


def _auth_header(access_key: str, secret_key: str) -> str:
    if access_key and secret_key:
        return f"LOW {access_key}:{secret_key}"
    return ""


def _http_json(
    *,
    url: str,
    method: str,
    data: dict[str, Any] | None,
    headers: dict[str, str],
    timeout_sec: int,
) -> tuple[int | None, dict[str, Any] | None, str]:
    encoded_data = None
    if data is not None:
        encoded_data = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(url=url, data=encoded_data, method=method)
    for key, value in headers.items():
        if value:
            request.add_header(key, value)

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            status_code = int(getattr(response, "status", 200))
            payload = response.read().decode("utf-8", errors="replace").strip()
            if not payload:
                return status_code, {}, ""
            try:
                return status_code, json.loads(payload), ""
            except json.JSONDecodeError:
                return status_code, {}, ""
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace").strip()
        body: dict[str, Any] = {}
        if payload:
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    body = parsed
            except json.JSONDecodeError:
                pass
        return int(exc.code), body, f"http_{exc.code}"
    except urllib.error.URLError as exc:
        return None, None, f"url_error:{exc.reason}"
    except TimeoutError:
        return None, None, "timeout"
    except Exception as exc:
        return None, None, f"http_exception:{exc}"


def _build_archive_urls(original_url: str, timestamp: str) -> tuple[str, str]:
    archive_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
    content_url = f"https://web.archive.org/web/{timestamp}id_/{original_url}"
    return archive_url, content_url


def _submit_spn2(url: str, config: WaybackJobConfig) -> tuple[int | None, dict[str, Any] | None, str]:
    payload = {
        "url": url,
        "capture_all": "1" if config.capture_all else "0",
        "skip_first_archive": "1" if config.skip_first_archive else "0",
        "delay_wb_availability": "1" if config.delay_wb_availability else "0",
    }
    headers = {
        "Accept": "application/json",
        "Authorization": _auth_header(config.access_key, config.secret_key),
    }
    return _http_json(
        url=SPN2_SAVE_URL,
        method="POST",
        data=payload,
        headers=headers,
        timeout_sec=config.timeout_sec,
    )


def _poll_spn2(job_id: str, config: WaybackJobConfig) -> tuple[int | None, dict[str, Any] | None, str]:
    deadline = time.time() + max(1, config.poll_timeout_sec)
    headers = {
        "Accept": "application/json",
        "Authorization": _auth_header(config.access_key, config.secret_key),
    }

    while True:
        status_code, body, error_text = _http_json(
            url=SPN2_STATUS_URL_FMT.format(job_id=job_id),
            method="GET",
            data=None,
            headers=headers,
            timeout_sec=config.timeout_sec,
        )
        if error_text:
            return status_code, body, error_text
        if not isinstance(body, dict):
            return status_code, body, "invalid_status_response"

        status_value = str(body.get("status", "")).lower()
        if status_value in {"success", "error"}:
            return status_code, body, ""

        if time.time() >= deadline:
            return status_code, body, "spn2_poll_timeout"
        time.sleep(max(0.1, config.poll_interval_sec))


def _run_single_wayback(
    record: dict[str, Any],
    config: WaybackJobConfig,
) -> tuple[bool, dict[str, Any], str, bool]:
    source_url = str(record.get("source_url") or "").strip()
    if not source_url:
        return False, {}, "missing_source_url", False

    submit_status, submit_body, submit_error = _submit_spn2(source_url, config)
    if submit_error:
        return False, {}, submit_error, detect_service_failure(submit_status)

    if not isinstance(submit_body, dict):
        return False, {}, "spn2_submit_invalid_response", False

    submit_state = str(submit_body.get("status", "")).lower()
    if submit_state == "error":
        error_message = str(submit_body.get("message") or submit_body.get("status_ext") or "spn2_submit_error")
        return False, {}, error_message, detect_service_failure(submit_status)

    timestamp = str(submit_body.get("timestamp") or "").strip()
    job_id = str(submit_body.get("job_id") or "").strip()
    if submit_state == "success" and timestamp:
        archive_url, content_url = _build_archive_urls(source_url, timestamp)
        return True, {
            "url": archive_url,
            "content_url": content_url,
            "archive_time": timestamp,
            "archive_status_code": str(submit_status or ""),
            "archive_mimetype": "",
            "archive_length": None,
            "archive_sha1": "",
        }, "", False

    if not job_id:
        return False, {}, "spn2_missing_job_id", False

    poll_status, poll_body, poll_error = _poll_spn2(job_id, config)
    if poll_error:
        return False, {}, poll_error, detect_service_failure(poll_status)
    if not isinstance(poll_body, dict):
        return False, {}, "spn2_poll_invalid_response", False

    poll_state = str(poll_body.get("status", "")).lower()
    if poll_state != "success":
        error_message = str(poll_body.get("message") or poll_body.get("status_ext") or "spn2_poll_error")
        return False, {}, error_message, detect_service_failure(poll_status)

    timestamp = str(poll_body.get("timestamp") or "").strip()
    if not timestamp:
        return False, {}, "spn2_missing_timestamp", False

    archive_url, content_url = _build_archive_urls(source_url, timestamp)
    return True, {
        "url": archive_url,
        "content_url": content_url,
        "archive_time": timestamp,
        "archive_status_code": str(poll_status or ""),
        "archive_mimetype": "",
        "archive_length": None,
        "archive_sha1": "",
    }, "", False


def run_wayback_job(config: WaybackJobConfig) -> WaybackJobReport:
    store = LedgerStore(config.ledger_dir)
    report = WaybackJobReport()

    candidates = filter_stage_records(
        store,
        allowed_states=config.allowed_states,
        stage="wayback",
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

        if config.dry_run:
            continue

        try:
            success, metadata, error_text, service_failure = _run_single_wayback(record, config)
            if success:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="wayback",
                    success=True,
                    metadata=metadata,
                )
                report.success += 1
                consecutive_service_failures = 0
            else:
                store.apply_stage_result(
                    unique_code=unique_code,
                    stage="wayback",
                    success=False,
                    error=error_text or "wayback_upload_failed",
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
                    stage="wayback",
                    success=False,
                    error=f"wayback_exception:{exc}",
                )
            except Exception:
                pass

        if consecutive_service_failures >= max(1, config.service_failure_limit):
            report.stopped_early = True
            break

    return report


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run Wayback SPN2 upload stage for eligible ledger records."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument("--allowed-state", action="append", default=[], help="Override allowed states")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on number of records processed")
    parser.add_argument("--timeout-sec", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument("--poll-interval-sec", type=float, default=1.5, help="SPN2 poll interval seconds")
    parser.add_argument("--poll-timeout-sec", type=int, default=90, help="SPN2 poll timeout seconds")
    parser.add_argument(
        "--service-failure-limit",
        type=int,
        default=10,
        help="Stop after N consecutive service failures",
    )
    parser.add_argument(
        "--ia-access-key",
        default="",
        help="SPN2 access key (or use IA_ACCESS_KEY)",
    )
    parser.add_argument(
        "--ia-secret-key",
        default="",
        help="SPN2 secret key (or use IA_SECRET_KEY)",
    )
    parser.add_argument("--capture-all", action="store_true", help="SPN2 capture_all=1")
    parser.add_argument("--skip-first-archive", action="store_true", help="SPN2 skip_first_archive=1")
    parser.add_argument(
        "--no-delay-wb-availability",
        action="store_true",
        help="Set SPN2 delay_wb_availability=0",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan records without uploading")
    return parser


def _print_report(report: WaybackJobReport) -> None:
    print("Wayback job:")
    print(f"  selected: {report.selected}")
    print(f"  processed: {report.processed}")
    print(f"  success: {report.success}")
    print(f"  failed: {report.failed}")
    print(f"  skipped: {report.skipped}")
    print(f"  service_failures: {report.service_failures}")
    print(f"  stopped_early: {report.stopped_early}")


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    allowed_states = parse_state_list(args.allowed_state) or set(DEFAULT_ALLOWED_STATES)
    access_key = args.ia_access_key or os.environ.get("IA_ACCESS_KEY", "")
    secret_key = args.ia_secret_key or os.environ.get("IA_SECRET_KEY", "")

    config = WaybackJobConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        timeout_sec=max(1, args.timeout_sec),
        poll_interval_sec=max(0.1, args.poll_interval_sec),
        poll_timeout_sec=max(1, args.poll_timeout_sec),
        service_failure_limit=max(1, args.service_failure_limit),
        code_filter=code_filter,
        allowed_states=allowed_states,
        access_key=access_key,
        secret_key=secret_key,
        capture_all=args.capture_all,
        skip_first_archive=args.skip_first_archive,
        delay_wb_availability=not args.no_delay_wb_availability,
    )

    report = run_wayback_job(config)
    _print_report(report)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Wayback SPN2 upload stage.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
