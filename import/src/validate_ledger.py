#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from local_env import load_local_env


ALLOWED_STATES = {
    "FETCHED",
    "DOWNLOAD_SUCCESS",
    "DOWNLOAD_FAILED",
    "WAYBACK_UPLOADED",
    "WAYBACK_UPLOAD_FAILED",
    "ARCHIVE_UPLOADED_WITH_WAYBACK_URL",
    "ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL",
    "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT",
}

REQUIRED_TOP_LEVEL_FIELDS = {
    "unique_code",
    "title",
    "department_name",
    "department_code",
    "gr_date",
    "source_url",
    "lfs_path",
    "state",
    "attempt_counts",
    "download",
    "wayback",
    "archive",
    "first_seen_crawl_date",
    "last_seen_crawl_date",
    "first_seen_run_type",
    "created_at_utc",
    "updated_at_utc",
}

EXPECTED_ATTEMPT_FIELDS = {"download", "wayback", "archive"}
EXPECTED_DOWNLOAD_FIELDS = {"path", "status", "hash", "size", "error"}
EXPECTED_WAYBACK_FIELDS = {
    "url",
    "content_url",
    "archive_time",
    "archive_sha1",
    "archive_length",
    "archive_mimetype",
    "archive_status_code",
    "status",
    "error",
}
EXPECTED_ARCHIVE_FIELDS = {"identifier", "url", "status", "error"}

ALLOWED_DOWNLOAD_STATUS = {"not_attempted", "attempted", "success", "failed"}
ALLOWED_WAYBACK_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_ARCHIVE_STATUS = {"not_attempted", "success", "failed"}


@dataclass
class Issue:
    severity: str
    code: str
    message: str
    file_path: Path
    line_no: int


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Validate yearly ledger JSONL files in import/grinfo."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger directory containing *.jsonl files")
    parser.add_argument(
        "--max-issues-per-code",
        type=int,
        default=20,
        help="Maximum printed examples per issue code",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return non-zero exit code when warnings are present",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate yearly ledger JSONL files in import/grinfo.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def add_issue(
    issues: list[Issue],
    severity: str,
    code: str,
    message: str,
    file_path: Path,
    line_no: int,
) -> None:
    issues.append(
        Issue(
            severity=severity,
            code=code,
            message=message,
            file_path=file_path,
            line_no=line_no,
        )
    )


def parse_iso_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_iso_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def validate_partition_against_gr_date(
    partition_key: str,
    gr_date_value: Any,
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    gr_date_parsed = parse_iso_date(gr_date_value)
    if partition_key == "unknown":
        if gr_date_parsed is not None:
            add_issue(
                issues,
                "error",
                "unknown_partition_has_valid_gr_date",
                f"unknown.jsonl record has valid gr_date={gr_date_value}",
                file_path,
                line_no,
            )
        return

    if gr_date_parsed is None:
        add_issue(
            issues,
            "error",
            "partition_record_missing_valid_gr_date",
            f"Partition year={partition_key} has invalid gr_date={gr_date_value}",
            file_path,
            line_no,
        )
        return

    if str(gr_date_parsed.year) != partition_key:
        add_issue(
            issues,
            "error",
            "partition_year_mismatch",
            f"Partition year={partition_key} but gr_date={gr_date_parsed.isoformat()}",
            file_path,
            line_no,
        )


def validate_attempt_counts(
    attempts: Any,
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    if not isinstance(attempts, dict):
        add_issue(
            issues,
            "error",
            "attempt_counts_not_object",
            "attempt_counts must be an object",
            file_path,
            line_no,
        )
        return

    missing_keys = sorted(EXPECTED_ATTEMPT_FIELDS - set(attempts.keys()))
    if missing_keys:
        add_issue(
            issues,
            "error",
            "attempt_counts_missing_keys",
            f"attempt_counts missing keys: {missing_keys}",
            file_path,
            line_no,
        )

    for key in EXPECTED_ATTEMPT_FIELDS:
        value = attempts.get(key)
        if not isinstance(value, int):
            add_issue(
                issues,
                "error",
                "attempt_counts_invalid_type",
                f"attempt_counts.{key} must be int, got {type(value).__name__}",
                file_path,
                line_no,
            )
            continue
        if value < 0 or value > 2:
            add_issue(
                issues,
                "error",
                "attempt_counts_out_of_range",
                f"attempt_counts.{key} must be in [0,2], got {value}",
                file_path,
                line_no,
            )


def validate_stage_objects(
    record: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    download = record.get("download")
    wayback = record.get("wayback")
    archive = record.get("archive")

    if not isinstance(download, dict):
        add_issue(issues, "error", "download_not_object", "download must be object", file_path, line_no)
    if not isinstance(wayback, dict):
        add_issue(issues, "error", "wayback_not_object", "wayback must be object", file_path, line_no)
    if not isinstance(archive, dict):
        add_issue(issues, "error", "archive_not_object", "archive must be object", file_path, line_no)
    if not isinstance(download, dict) or not isinstance(wayback, dict) or not isinstance(archive, dict):
        return

    missing_download = sorted(EXPECTED_DOWNLOAD_FIELDS - set(download.keys()))
    if missing_download:
        add_issue(
            issues,
            "warning",
            "download_missing_keys",
            f"download missing keys: {missing_download}",
            file_path,
            line_no,
        )

    missing_wayback = sorted(EXPECTED_WAYBACK_FIELDS - set(wayback.keys()))
    if missing_wayback:
        add_issue(
            issues,
            "warning",
            "wayback_missing_keys",
            f"wayback missing keys: {missing_wayback}",
            file_path,
            line_no,
        )

    missing_archive = sorted(EXPECTED_ARCHIVE_FIELDS - set(archive.keys()))
    if missing_archive:
        add_issue(
            issues,
            "warning",
            "archive_missing_keys",
            f"archive missing keys: {missing_archive}",
            file_path,
            line_no,
        )

    download_status = download.get("status")
    wayback_status = wayback.get("status")
    archive_status = archive.get("status")

    if download_status not in ALLOWED_DOWNLOAD_STATUS:
        add_issue(
            issues,
            "error",
            "download_status_invalid",
            f"download.status={download_status!r} is invalid",
            file_path,
            line_no,
        )
    if wayback_status not in ALLOWED_WAYBACK_STATUS:
        add_issue(
            issues,
            "error",
            "wayback_status_invalid",
            f"wayback.status={wayback_status!r} is invalid",
            file_path,
            line_no,
        )
    if archive_status not in ALLOWED_ARCHIVE_STATUS:
        add_issue(
            issues,
            "error",
            "archive_status_invalid",
            f"archive.status={archive_status!r} is invalid",
            file_path,
            line_no,
        )


def validate_lfs_path(
    record: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    value = record.get("lfs_path")
    if value is None:
        return
    if not isinstance(value, str):
        add_issue(
            issues,
            "error",
            "lfs_path_invalid_type",
            f"lfs_path must be string or null, got {type(value).__name__}",
            file_path,
            line_no,
        )
        return
    if not value.strip():
        add_issue(
            issues,
            "error",
            "lfs_path_empty_string",
            "lfs_path must be null when file is unavailable, not empty string",
            file_path,
            line_no,
        )


def validate_state_consistency(
    record: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    state = record.get("state")
    if state not in ALLOWED_STATES:
        add_issue(
            issues,
            "error",
            "state_invalid",
            f"state={state!r} is not in allowed states",
            file_path,
            line_no,
        )
        return

    download = record.get("download", {})
    wayback = record.get("wayback", {})
    archive = record.get("archive", {})
    download_status = download.get("status")
    wayback_status = wayback.get("status")
    archive_status = archive.get("status")
    wayback_url = wayback.get("url")

    if state == "FETCHED":
        if download_status not in {"not_attempted", "attempted"}:
            add_issue(
                issues,
                "warning",
                "state_fetched_download_status",
                f"FETCHED usually expects download.status in {{not_attempted, attempted}}, got {download_status}",
                file_path,
                line_no,
            )
        if archive_status == "success":
            add_issue(
                issues,
                "error",
                "state_fetched_archive_success",
                "FETCHED cannot have archive.status=success",
                file_path,
                line_no,
            )

    elif state == "DOWNLOAD_SUCCESS":
        if download_status != "success":
            add_issue(
                issues,
                "error",
                "state_download_success_mismatch",
                f"DOWNLOAD_SUCCESS requires download.status=success, got {download_status}",
                file_path,
                line_no,
            )
        if archive_status == "success":
            add_issue(
                issues,
                "error",
                "state_download_success_archive_success",
                "DOWNLOAD_SUCCESS cannot have archive.status=success",
                file_path,
                line_no,
            )

    elif state == "DOWNLOAD_FAILED":
        if download_status != "failed":
            add_issue(
                issues,
                "error",
                "state_download_failed_mismatch",
                f"DOWNLOAD_FAILED requires download.status=failed, got {download_status}",
                file_path,
                line_no,
            )
        if archive_status == "success":
            add_issue(
                issues,
                "error",
                "state_download_failed_archive_success",
                "DOWNLOAD_FAILED cannot have archive.status=success",
                file_path,
                line_no,
            )

    elif state == "WAYBACK_UPLOADED":
        if wayback_status != "success":
            add_issue(
                issues,
                "error",
                "state_wayback_uploaded_mismatch",
                f"WAYBACK_UPLOADED requires wayback.status=success, got {wayback_status}",
                file_path,
                line_no,
            )
        if archive_status == "success":
            add_issue(
                issues,
                "error",
                "state_wayback_uploaded_archive_success",
                "WAYBACK_UPLOADED cannot have archive.status=success",
                file_path,
                line_no,
            )

    elif state == "WAYBACK_UPLOAD_FAILED":
        if wayback_status != "failed":
            add_issue(
                issues,
                "error",
                "state_wayback_failed_mismatch",
                f"WAYBACK_UPLOAD_FAILED requires wayback.status=failed, got {wayback_status}",
                file_path,
                line_no,
            )
        if archive_status == "success":
            add_issue(
                issues,
                "error",
                "state_wayback_failed_archive_success",
                "WAYBACK_UPLOAD_FAILED cannot have archive.status=success",
                file_path,
                line_no,
            )

    elif state == "ARCHIVE_UPLOADED_WITH_WAYBACK_URL":
        if archive_status != "success":
            add_issue(
                issues,
                "error",
                "state_archive_with_wayback_archive_status",
                f"ARCHIVE_UPLOADED_WITH_WAYBACK_URL requires archive.status=success, got {archive_status}",
                file_path,
                line_no,
            )
        if not isinstance(wayback_url, str) or not wayback_url.strip():
            add_issue(
                issues,
                "error",
                "state_archive_with_wayback_missing_url",
                "ARCHIVE_UPLOADED_WITH_WAYBACK_URL requires non-empty wayback.url",
                file_path,
                line_no,
            )

    elif state == "ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL":
        if archive_status != "success":
            add_issue(
                issues,
                "error",
                "state_archive_without_wayback_archive_status",
                f"ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL requires archive.status=success, got {archive_status}",
                file_path,
                line_no,
            )
        if isinstance(wayback_url, str) and wayback_url.strip():
            add_issue(
                issues,
                "warning",
                "state_archive_without_wayback_has_url",
                "ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL has non-empty wayback.url",
                file_path,
                line_no,
            )

    elif state == "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT":
        if archive_status != "success":
            add_issue(
                issues,
                "error",
                "state_archive_without_document_archive_status",
                f"ARCHIVE_UPLOADED_WITHOUT_DOCUMENT requires archive.status=success, got {archive_status}",
                file_path,
                line_no,
            )
        if download_status == "success":
            add_issue(
                issues,
                "warning",
                "state_archive_without_document_download_success",
                "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT should not have download.status=success",
                file_path,
                line_no,
            )


def validate_dates_and_timestamps(
    record: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    first_seen = parse_iso_date(record.get("first_seen_crawl_date"))
    last_seen = parse_iso_date(record.get("last_seen_crawl_date"))
    if first_seen is None:
        add_issue(
            issues,
            "error",
            "first_seen_crawl_date_invalid",
            f"Invalid first_seen_crawl_date={record.get('first_seen_crawl_date')!r}",
            file_path,
            line_no,
        )
    if last_seen is None:
        add_issue(
            issues,
            "error",
            "last_seen_crawl_date_invalid",
            f"Invalid last_seen_crawl_date={record.get('last_seen_crawl_date')!r}",
            file_path,
            line_no,
        )
    if first_seen and last_seen and first_seen > last_seen:
        add_issue(
            issues,
            "error",
            "crawl_date_order_invalid",
            f"first_seen_crawl_date={first_seen} > last_seen_crawl_date={last_seen}",
            file_path,
            line_no,
        )

    run_type = record.get("first_seen_run_type")
    if run_type not in {"daily", "monthly"}:
        add_issue(
            issues,
            "error",
            "first_seen_run_type_invalid",
            f"Invalid first_seen_run_type={run_type!r}",
            file_path,
            line_no,
        )

    created_at = parse_iso_timestamp(record.get("created_at_utc"))
    updated_at = parse_iso_timestamp(record.get("updated_at_utc"))
    if created_at is None:
        add_issue(
            issues,
            "error",
            "created_at_invalid",
            f"Invalid created_at_utc={record.get('created_at_utc')!r}",
            file_path,
            line_no,
        )
    if updated_at is None:
        add_issue(
            issues,
            "error",
            "updated_at_invalid",
            f"Invalid updated_at_utc={record.get('updated_at_utc')!r}",
            file_path,
            line_no,
        )
    if created_at and updated_at and created_at > updated_at:
        add_issue(
            issues,
            "warning",
            "created_after_updated",
            f"created_at_utc={created_at} > updated_at_utc={updated_at}",
            file_path,
            line_no,
        )


def validate_ledger(ledger_dir: Path) -> tuple[list[Issue], Counter, int]:
    issues: list[Issue] = []
    state_counts: Counter = Counter()
    duplicate_tracker: dict[str, tuple[Path, int]] = {}
    total_records = 0

    jsonl_files = sorted(ledger_dir.glob("*.jsonl"))
    if not jsonl_files:
        add_issue(
            issues,
            "error",
            "ledger_files_missing",
            f"No *.jsonl files found in {ledger_dir}",
            ledger_dir,
            0,
        )
        return issues, state_counts, total_records

    for jsonl_file in jsonl_files:
        partition_key = jsonl_file.stem
        if partition_key != "unknown" and (len(partition_key) != 4 or not partition_key.isdigit()):
            add_issue(
                issues,
                "error",
                "invalid_partition_filename",
                f"Invalid partition filename: {jsonl_file.name}",
                jsonl_file,
                0,
            )
            continue

        with jsonl_file.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line_text = line.strip()
                if not line_text:
                    continue
                total_records += 1
                try:
                    record = json.loads(line_text)
                except json.JSONDecodeError as exc:
                    add_issue(
                        issues,
                        "error",
                        "json_decode_error",
                        f"JSON parse failed: {exc}",
                        jsonl_file,
                        line_no,
                    )
                    continue

                if not isinstance(record, dict):
                    add_issue(
                        issues,
                        "error",
                        "record_not_object",
                        f"Record must be object, got {type(record).__name__}",
                        jsonl_file,
                        line_no,
                    )
                    continue

                missing_fields = sorted(REQUIRED_TOP_LEVEL_FIELDS - set(record.keys()))
                if missing_fields:
                    add_issue(
                        issues,
                        "error",
                        "record_missing_required_fields",
                        f"Missing required fields: {missing_fields}",
                        jsonl_file,
                        line_no,
                    )
                    continue

                unique_code = record.get("unique_code")
                if not isinstance(unique_code, str) or not unique_code.strip():
                    add_issue(
                        issues,
                        "error",
                        "unique_code_invalid",
                        f"Invalid unique_code={unique_code!r}",
                        jsonl_file,
                        line_no,
                    )
                else:
                    if unique_code in duplicate_tracker:
                        first_file, first_line = duplicate_tracker[unique_code]
                        add_issue(
                            issues,
                            "error",
                            "duplicate_unique_code",
                            (
                                f"Duplicate unique_code={unique_code} (first seen at "
                                f"{first_file}:{first_line})"
                            ),
                            jsonl_file,
                            line_no,
                        )
                    else:
                        duplicate_tracker[unique_code] = (jsonl_file, line_no)

                source_url = record.get("source_url")
                if not isinstance(source_url, str) or not source_url.strip():
                    add_issue(
                        issues,
                        "warning",
                        "source_url_empty",
                        "source_url is empty",
                        jsonl_file,
                        line_no,
                    )

                validate_partition_against_gr_date(
                    partition_key=partition_key,
                    gr_date_value=record.get("gr_date"),
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )
                validate_attempt_counts(
                    attempts=record.get("attempt_counts"),
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )
                validate_stage_objects(
                    record=record,
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )
                validate_lfs_path(
                    record=record,
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )
                validate_state_consistency(
                    record=record,
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )
                validate_dates_and_timestamps(
                    record=record,
                    issues=issues,
                    file_path=jsonl_file,
                    line_no=line_no,
                )

                state = record.get("state")
                if isinstance(state, str):
                    state_counts[state] += 1

    return issues, state_counts, total_records


def print_report(
    issues: list[Issue],
    state_counts: Counter,
    total_records: int,
    ledger_dir: Path,
    max_issues_per_code: int,
) -> tuple[int, int]:
    errors = [issue for issue in issues if issue.severity == "error"]
    warnings = [issue for issue in issues if issue.severity == "warning"]

    print(f"Ledger dir: {ledger_dir}")
    print(f"Records checked: {total_records}")
    print(f"Errors: {len(errors)}")
    print(f"Warnings: {len(warnings)}")

    print("State counts:")
    for state_name, count in sorted(state_counts.items()):
        print(f"  {state_name}: {count}")

    grouped_issues: dict[tuple[str, str], list[Issue]] = defaultdict(list)
    for issue in issues:
        grouped_issues[(issue.severity, issue.code)].append(issue)

    if grouped_issues:
        print("Issues:")
        for (severity, code), group in sorted(grouped_issues.items()):
            print(f"  [{severity}] {code}: {len(group)}")
            for item in group[:max_issues_per_code]:
                print(
                    "    "
                    f"{item.file_path}:{item.line_no}: {item.message}"
                )
            hidden = len(group) - max_issues_per_code
            if hidden > 0:
                print(f"    ... {hidden} more")

    return len(errors), len(warnings)


def run_from_args(args: argparse.Namespace) -> int:
    ledger_dir = Path(args.ledger_dir).resolve()
    issues, state_counts, total_records = validate_ledger(ledger_dir=ledger_dir)
    error_count, warning_count = print_report(
        issues=issues,
        state_counts=state_counts,
        total_records=total_records,
        ledger_dir=ledger_dir,
        max_issues_per_code=args.max_issues_per_code,
    )

    if error_count > 0:
        return 1
    if args.fail_on_warning and warning_count > 0:
        return 2
    return 0


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
