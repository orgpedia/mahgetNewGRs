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


URL_NS = "urlinfos"
UPLOAD_NS = "uploadinfos"
PDF_NS = "pdfinfos"
NAMESPACES = (URL_NS, UPLOAD_NS, PDF_NS)

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

URL_REQUIRED_FIELDS = {
    "record_key",
    "unique_code",
    "title",
    "department_name",
    "department_code",
    "gr_date",
    "source_url",
    "first_seen_crawl_date",
    "last_seen_crawl_date",
    "first_seen_run_type",
    "created_at_utc",
    "updated_at_utc",
}

UPLOAD_REQUIRED_FIELDS = {
    "record_key",
    "state",
    "download",
    "wayback",
    "archive",
    "hf",
    "created_at_utc",
    "updated_at_utc",
}

PDF_REQUIRED_FIELDS = {
    "record_key",
    "status",
    "created_at_utc",
}
PDF_OPTIONAL_FIELDS = {"updated_at_utc"}

EXPECTED_DOWNLOAD_FIELDS = {"status", "error", "attempts"}
EXPECTED_WAYBACK_FIELDS = {
    "status",
    "url",
    "content_url",
    "archive_time",
    "archive_sha1",
    "archive_length",
    "archive_mimetype",
    "archive_status_code",
    "error",
    "attempts",
}
EXPECTED_ARCHIVE_FIELDS = {"status", "identifier", "url", "error", "attempts"}
EXPECTED_HF_FIELDS = {"status", "path", "hash", "backend", "commit_hash", "error", "synced_at_utc", "attempts"}

ALLOWED_DOWNLOAD_STATUS = {"not_attempted", "attempted", "success", "failed"}
ALLOWED_WAYBACK_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_ARCHIVE_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_HF_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_PDF_STATUS = {"not_attempted", "success", "failed", "missing_pdf"}

PDF_NON_ATTEMPTED_ALLOWED_FIELDS = PDF_REQUIRED_FIELDS | PDF_OPTIONAL_FIELDS
PDF_EXTRACTED_FIELDS = {
    "file_size",
    "page_count",
    "pages_with_images",
    "has_any_page_image",
    "font_count",
    "fonts",
    "unresolved_word_count",
    "language",
}
PDF_NON_ATTEMPTED_REQUIRED_FIELDS = {"error"} | PDF_EXTRACTED_FIELDS


@dataclass
class Issue:
    severity: str
    code: str
    message: str
    file_path: Path
    line_no: int


@dataclass(frozen=True)
class RowRef:
    namespace: str
    partition: str
    file_path: Path
    line_no: int
    row: dict[str, Any]


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Validate split ledger JSONL files in import/{urlinfos,uploadinfos,pdfinfos}."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root or any ledger namespace directory")
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
    parser = argparse.ArgumentParser(
        description="Validate split ledger JSONL files in import/{urlinfos,uploadinfos,pdfinfos}."
    )
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def _sort_partition_key(name: str) -> tuple[int, int, str]:
    if name.isdigit():
        return (0, int(name), name)
    if name == "unknown":
        return (2, 0, name)
    return (1, 0, name)


def resolve_ledger_root(path: Path) -> Path:
    requested = path.resolve()
    if any((requested / ns).exists() for ns in NAMESPACES):
        return requested
    if requested.name in set(NAMESPACES) | {"grinfo"}:
        parent = requested.parent
        if any((parent / ns).exists() for ns in NAMESPACES):
            return parent
    return requested


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


def is_ascii_text(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def validate_partition_name(partition: str, issues: list[Issue], file_path: Path, line_no: int) -> bool:
    if partition == "unknown":
        return True
    if len(partition) == 4 and partition.isdigit():
        return True
    add_issue(
        issues,
        "error",
        "invalid_partition_filename",
        f"Invalid partition filename: {file_path.name}",
        file_path,
        line_no,
    )
    return False


def validate_partition_against_gr_date(
    *,
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


def validate_required_fields(
    *,
    row: dict[str, Any],
    required_fields: set[str],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
    namespace: str,
) -> bool:
    missing_fields = sorted(required_fields - set(row.keys()))
    if not missing_fields:
        return True
    add_issue(
        issues,
        "error",
        "record_missing_required_fields",
        f"{namespace} missing required fields: {missing_fields}",
        file_path,
        line_no,
    )
    return False


def validate_record_key(
    *,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> str:
    record_key = row.get("record_key")
    if not isinstance(record_key, str) or not record_key.strip():
        add_issue(
            issues,
            "error",
            "record_key_invalid",
            f"Invalid record_key={record_key!r}",
            file_path,
            line_no,
        )
        return ""
    record_key = record_key.strip()
    if not is_ascii_text(record_key):
        add_issue(
            issues,
            "error",
            "record_key_non_ascii",
            f"record_key must be ASCII-only, got {record_key!r}",
            file_path,
            line_no,
        )
    return record_key


def validate_timestamps(
    *,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
    require_updated: bool = True,
) -> None:
    created_at = parse_iso_timestamp(row.get("created_at_utc"))
    updated_at = parse_iso_timestamp(row.get("updated_at_utc"))
    if created_at is None:
        add_issue(
            issues,
            "error",
            "created_at_invalid",
            f"Invalid created_at_utc={row.get('created_at_utc')!r}",
            file_path,
            line_no,
        )
    if require_updated and updated_at is None:
        add_issue(
            issues,
            "error",
            "updated_at_invalid",
            f"Invalid updated_at_utc={row.get('updated_at_utc')!r}",
            file_path,
            line_no,
        )
    if not require_updated and "updated_at_utc" in row and updated_at is None:
        add_issue(
            issues,
            "warning",
            "updated_at_invalid_optional",
            f"Optional updated_at_utc is invalid: {row.get('updated_at_utc')!r}",
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


def validate_url_row(
    *,
    partition: str,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    if not validate_required_fields(
        row=row,
        required_fields=URL_REQUIRED_FIELDS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
        namespace=URL_NS,
    ):
        return

    record_key = validate_record_key(row=row, issues=issues, file_path=file_path, line_no=line_no)
    unique_code = row.get("unique_code")
    if not isinstance(unique_code, str) or not unique_code.strip():
        add_issue(
            issues,
            "error",
            "unique_code_invalid",
            f"Invalid unique_code={unique_code!r}",
            file_path,
            line_no,
        )
    elif record_key and unique_code.strip() != record_key:
        add_issue(
            issues,
            "error",
            "record_key_unique_code_mismatch",
            f"record_key={record_key!r} must equal unique_code={unique_code!r} in {URL_NS}",
            file_path,
            line_no,
        )

    source_url = row.get("source_url")
    if not isinstance(source_url, str) or not source_url.strip():
        add_issue(
            issues,
            "warning",
            "source_url_empty",
            "source_url is empty",
            file_path,
            line_no,
        )

    department_code = row.get("department_code")
    if not isinstance(department_code, str) or not department_code.strip():
        add_issue(
            issues,
            "error",
            "department_code_invalid",
            f"Invalid department_code={department_code!r}",
            file_path,
            line_no,
        )
    elif department_code != "unknown" and not department_code.startswith("mah"):
        add_issue(
            issues,
            "warning",
            "department_code_non_canonical",
            f"department_code={department_code!r} is not canonical mah* code",
            file_path,
            line_no,
        )

    validate_partition_against_gr_date(
        partition_key=partition,
        gr_date_value=row.get("gr_date"),
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )

    first_seen = parse_iso_date(row.get("first_seen_crawl_date"))
    last_seen = parse_iso_date(row.get("last_seen_crawl_date"))
    if first_seen is None:
        add_issue(
            issues,
            "error",
            "first_seen_crawl_date_invalid",
            f"Invalid first_seen_crawl_date={row.get('first_seen_crawl_date')!r}",
            file_path,
            line_no,
        )
    if last_seen is None:
        add_issue(
            issues,
            "error",
            "last_seen_crawl_date_invalid",
            f"Invalid last_seen_crawl_date={row.get('last_seen_crawl_date')!r}",
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

    run_type = row.get("first_seen_run_type")
    if run_type not in {"daily", "monthly"}:
        add_issue(
            issues,
            "error",
            "first_seen_run_type_invalid",
            f"Invalid first_seen_run_type={run_type!r}",
            file_path,
            line_no,
        )

    validate_timestamps(row=row, issues=issues, file_path=file_path, line_no=line_no)


def validate_attempt_field(
    *,
    stage_obj: dict[str, Any],
    stage_name: str,
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    attempts = stage_obj.get("attempts")
    if not isinstance(attempts, int):
        add_issue(
            issues,
            "error",
            "attempts_invalid_type",
            f"{stage_name}.attempts must be int, got {type(attempts).__name__}",
            file_path,
            line_no,
        )
        return
    if attempts < 0:
        add_issue(
            issues,
            "error",
            "attempts_negative",
            f"{stage_name}.attempts must be >= 0, got {attempts}",
            file_path,
            line_no,
        )


def validate_stage_object(
    *,
    row: dict[str, Any],
    stage_name: str,
    expected_fields: set[str],
    allowed_status: set[str],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> dict[str, Any] | None:
    value = row.get(stage_name)
    if not isinstance(value, dict):
        add_issue(
            issues,
            "error",
            f"{stage_name}_not_object",
            f"{stage_name} must be object",
            file_path,
            line_no,
        )
        return None

    missing_fields = sorted(expected_fields - set(value.keys()))
    if missing_fields:
        add_issue(
            issues,
            "warning",
            f"{stage_name}_missing_keys",
            f"{stage_name} missing keys: {missing_fields}",
            file_path,
            line_no,
        )

    status = value.get("status")
    if status not in allowed_status:
        add_issue(
            issues,
            "error",
            f"{stage_name}_status_invalid",
            f"{stage_name}.status={status!r} is invalid",
            file_path,
            line_no,
        )

    validate_attempt_field(
        stage_obj=value,
        stage_name=stage_name,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )
    return value


def validate_upload_state_consistency(
    *,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    state = row.get("state")
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

    download = row.get("download", {})
    wayback = row.get("wayback", {})
    archive = row.get("archive", {})
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


def validate_upload_row(
    *,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    if not validate_required_fields(
        row=row,
        required_fields=UPLOAD_REQUIRED_FIELDS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
        namespace=UPLOAD_NS,
    ):
        return

    validate_record_key(row=row, issues=issues, file_path=file_path, line_no=line_no)
    if "unique_code" in row:
        add_issue(
            issues,
            "error",
            "uploadinfos_has_unique_code",
            "uploadinfos row must not contain unique_code",
            file_path,
            line_no,
        )
    if "attempt_counts" in row:
        add_issue(
            issues,
            "error",
            "uploadinfos_has_top_level_attempt_counts",
            "uploadinfos row must not contain top-level attempt_counts",
            file_path,
            line_no,
        )

    validate_stage_object(
        row=row,
        stage_name="download",
        expected_fields=EXPECTED_DOWNLOAD_FIELDS,
        allowed_status=ALLOWED_DOWNLOAD_STATUS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )
    validate_stage_object(
        row=row,
        stage_name="wayback",
        expected_fields=EXPECTED_WAYBACK_FIELDS,
        allowed_status=ALLOWED_WAYBACK_STATUS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )
    validate_stage_object(
        row=row,
        stage_name="archive",
        expected_fields=EXPECTED_ARCHIVE_FIELDS,
        allowed_status=ALLOWED_ARCHIVE_STATUS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )
    validate_stage_object(
        row=row,
        stage_name="hf",
        expected_fields=EXPECTED_HF_FIELDS,
        allowed_status=ALLOWED_HF_STATUS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
    )

    validate_upload_state_consistency(row=row, issues=issues, file_path=file_path, line_no=line_no)
    validate_timestamps(row=row, issues=issues, file_path=file_path, line_no=line_no)


def validate_pdf_row(
    *,
    row: dict[str, Any],
    issues: list[Issue],
    file_path: Path,
    line_no: int,
) -> None:
    if not validate_required_fields(
        row=row,
        required_fields=PDF_REQUIRED_FIELDS,
        issues=issues,
        file_path=file_path,
        line_no=line_no,
        namespace=PDF_NS,
    ):
        return

    validate_record_key(row=row, issues=issues, file_path=file_path, line_no=line_no)
    if "unique_code" in row:
        add_issue(
            issues,
            "error",
            "pdfinfos_has_unique_code",
            "pdfinfos row must not contain unique_code",
            file_path,
            line_no,
        )

    status = row.get("status")
    if status not in ALLOWED_PDF_STATUS:
        add_issue(
            issues,
            "error",
            "pdf_status_invalid",
            f"pdf status={status!r} is invalid",
            file_path,
            line_no,
        )
        validate_timestamps(row=row, issues=issues, file_path=file_path, line_no=line_no, require_updated=False)
        return

    if status == "not_attempted":
        extra_fields = sorted(set(row.keys()) - PDF_NON_ATTEMPTED_ALLOWED_FIELDS)
        if extra_fields:
            add_issue(
                issues,
                "error",
                "pdf_not_attempted_has_extra_fields",
                f"pdfinfos status=not_attempted must not include extra fields: {extra_fields}",
                file_path,
                line_no,
            )
        validate_timestamps(row=row, issues=issues, file_path=file_path, line_no=line_no, require_updated=False)
        return

    missing_non_attempted = sorted(PDF_NON_ATTEMPTED_REQUIRED_FIELDS - set(row.keys()))
    if missing_non_attempted:
        add_issue(
            issues,
            "warning",
            "pdf_non_attempted_missing_fields",
            f"pdfinfos status={status} missing fields: {missing_non_attempted}",
            file_path,
            line_no,
        )

    error_value = row.get("error")
    if error_value is not None and not isinstance(error_value, str):
        add_issue(
            issues,
            "warning",
            "pdf_error_invalid_type",
            f"pdf error should be string or null, got {type(error_value).__name__}",
            file_path,
            line_no,
        )

    file_size = row.get("file_size")
    if file_size is not None and not isinstance(file_size, int):
        add_issue(
            issues,
            "error",
            "pdf_file_size_invalid_type",
            f"file_size must be int or null, got {type(file_size).__name__}",
            file_path,
            line_no,
        )

    page_count = row.get("page_count")
    if page_count is not None and not isinstance(page_count, int):
        add_issue(
            issues,
            "error",
            "pdf_page_count_invalid_type",
            f"page_count must be int or null, got {type(page_count).__name__}",
            file_path,
            line_no,
        )

    pages_with_images = row.get("pages_with_images")
    if pages_with_images is not None and not isinstance(pages_with_images, int):
        add_issue(
            issues,
            "warning",
            "pdf_pages_with_images_invalid_type",
            f"pages_with_images should be int or null, got {type(pages_with_images).__name__}",
            file_path,
            line_no,
        )

    has_any_page_image = row.get("has_any_page_image")
    if has_any_page_image is not None and not isinstance(has_any_page_image, bool):
        add_issue(
            issues,
            "warning",
            "pdf_has_any_page_image_invalid_type",
            f"has_any_page_image should be bool or null, got {type(has_any_page_image).__name__}",
            file_path,
            line_no,
        )

    fonts = row.get("fonts")
    if fonts is not None and not isinstance(fonts, dict):
        add_issue(
            issues,
            "warning",
            "pdf_fonts_invalid_type",
            f"fonts should be object or null, got {type(fonts).__name__}",
            file_path,
            line_no,
        )

    language = row.get("language")
    if language is not None and not isinstance(language, dict):
        add_issue(
            issues,
            "warning",
            "pdf_language_invalid_type",
            f"language should be object or null, got {type(language).__name__}",
            file_path,
            line_no,
        )

    if status == "missing_pdf":
        for field_name in sorted(PDF_EXTRACTED_FIELDS):
            if row.get(field_name) is not None:
                add_issue(
                    issues,
                    "error",
                    "pdf_missing_pdf_non_null_metadata",
                    f"status=missing_pdf requires {field_name}=null, got {row.get(field_name)!r}",
                    file_path,
                    line_no,
                )

    validate_timestamps(row=row, issues=issues, file_path=file_path, line_no=line_no, require_updated=False)


def validate_namespace_rows(
    *,
    root_dir: Path,
    namespace: str,
    issues: list[Issue],
    state_counts: Counter,
) -> tuple[dict[str, RowRef], int]:
    namespace_dir = root_dir / namespace
    if not namespace_dir.exists() or not namespace_dir.is_dir():
        add_issue(
            issues,
            "error",
            "namespace_missing",
            f"Missing namespace directory: {namespace_dir}",
            namespace_dir,
            0,
        )
        return {}, 0

    rows_by_key: dict[str, RowRef] = {}
    total_records = 0
    jsonl_files = sorted(namespace_dir.glob("*.jsonl"), key=lambda item: _sort_partition_key(item.stem))
    if not jsonl_files:
        add_issue(
            issues,
            "error",
            "namespace_files_missing",
            f"No *.jsonl files found in {namespace_dir}",
            namespace_dir,
            0,
        )
        return {}, 0

    for jsonl_file in jsonl_files:
        partition = jsonl_file.stem
        if not validate_partition_name(partition=partition, issues=issues, file_path=jsonl_file, line_no=0):
            continue

        with jsonl_file.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line_text = line.strip()
                if not line_text:
                    continue
                total_records += 1
                try:
                    row = json.loads(line_text)
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

                if not isinstance(row, dict):
                    add_issue(
                        issues,
                        "error",
                        "record_not_object",
                        f"Record must be object, got {type(row).__name__}",
                        jsonl_file,
                        line_no,
                    )
                    continue

                if namespace == URL_NS:
                    validate_url_row(
                        partition=partition,
                        row=row,
                        issues=issues,
                        file_path=jsonl_file,
                        line_no=line_no,
                    )
                elif namespace == UPLOAD_NS:
                    validate_upload_row(row=row, issues=issues, file_path=jsonl_file, line_no=line_no)
                    state_value = row.get("state")
                    if isinstance(state_value, str):
                        state_counts[state_value] += 1
                else:
                    validate_pdf_row(row=row, issues=issues, file_path=jsonl_file, line_no=line_no)

                record_key = row.get("record_key")
                if not isinstance(record_key, str) or not record_key.strip():
                    continue
                record_key = record_key.strip()
                existing_ref = rows_by_key.get(record_key)
                if existing_ref is not None:
                    add_issue(
                        issues,
                        "error",
                        "duplicate_record_key",
                        (
                            f"Duplicate record_key={record_key!r} in {namespace} "
                            f"(first seen at {existing_ref.file_path}:{existing_ref.line_no})"
                        ),
                        jsonl_file,
                        line_no,
                    )
                    continue
                rows_by_key[record_key] = RowRef(
                    namespace=namespace,
                    partition=partition,
                    file_path=jsonl_file,
                    line_no=line_no,
                    row=row,
                )

    return rows_by_key, total_records


def validate_cross_namespace(
    *,
    refs_by_ns: dict[str, dict[str, RowRef]],
    issues: list[Issue],
) -> None:
    all_keys: set[str] = set()
    for refs in refs_by_ns.values():
        all_keys.update(refs.keys())

    for record_key in sorted(all_keys):
        present_refs: dict[str, RowRef] = {
            ns: refs_by_ns[ns][record_key]
            for ns in NAMESPACES
            if record_key in refs_by_ns[ns]
        }
        missing = [ns for ns in NAMESPACES if ns not in present_refs]
        if missing:
            anchor = next(iter(present_refs.values()))
            add_issue(
                issues,
                "error",
                "record_key_missing_namespace",
                f"record_key={record_key!r} missing namespaces: {missing}",
                anchor.file_path,
                anchor.line_no,
            )
            continue

        partitions = {ref.partition for ref in present_refs.values()}
        if len(partitions) > 1:
            anchor = present_refs[URL_NS]
            add_issue(
                issues,
                "error",
                "cross_namespace_partition_mismatch",
                f"record_key={record_key!r} has namespace partitions: {sorted(partitions)}",
                anchor.file_path,
                anchor.line_no,
            )


def validate_ledger(ledger_dir: Path) -> tuple[list[Issue], Counter, dict[str, int]]:
    issues: list[Issue] = []
    state_counts: Counter = Counter()
    namespace_counts: dict[str, int] = {ns: 0 for ns in NAMESPACES}

    root_dir = resolve_ledger_root(ledger_dir)
    refs_by_ns: dict[str, dict[str, RowRef]] = {}

    for namespace in NAMESPACES:
        refs, count = validate_namespace_rows(
            root_dir=root_dir,
            namespace=namespace,
            issues=issues,
            state_counts=state_counts,
        )
        refs_by_ns[namespace] = refs
        namespace_counts[namespace] = count

    validate_cross_namespace(refs_by_ns=refs_by_ns, issues=issues)
    return issues, state_counts, namespace_counts


def print_report(
    *,
    issues: list[Issue],
    state_counts: Counter,
    namespace_counts: dict[str, int],
    ledger_dir: Path,
    max_issues_per_code: int,
) -> tuple[int, int]:
    errors = [issue for issue in issues if issue.severity == "error"]
    warnings = [issue for issue in issues if issue.severity == "warning"]

    root_dir = resolve_ledger_root(ledger_dir)
    print(f"Ledger root: {root_dir}")
    print("Records checked:")
    for namespace in NAMESPACES:
        print(f"  {namespace}: {namespace_counts.get(namespace, 0)}")
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
                print(f"    {item.file_path}:{item.line_no}: {item.message}")
            hidden = len(group) - max_issues_per_code
            if hidden > 0:
                print(f"    ... {hidden} more")

    return len(errors), len(warnings)


def run_from_args(args: argparse.Namespace) -> int:
    ledger_dir = Path(args.ledger_dir).resolve()
    issues, state_counts, namespace_counts = validate_ledger(ledger_dir=ledger_dir)
    error_count, warning_count = print_report(
        issues=issues,
        state_counts=state_counts,
        namespace_counts=namespace_counts,
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
