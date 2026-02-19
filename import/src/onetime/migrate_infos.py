#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, TextIO
from urllib.parse import unquote

from department_codes import DEPARTMENT_CODE_TO_NAME, department_code_from_name
from ledger_engine import partition_for_gr_date, utc_now_text
from local_env import load_local_env


LONG_DIGITS_RE = re.compile(r"\d{16,22}")
SAFE_TEXT_RE = re.compile(r"[^A-Za-z0-9._-]+")
ALLOWED_DOWNLOAD_STATUS = {"not_attempted", "attempted", "success", "failed"}
ALLOWED_WAYBACK_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_ARCHIVE_STATUS = {"not_attempted", "success", "failed"}
ALLOWED_PDF_STATUS = {"not_attempted", "success", "failed", "missing_pdf"}


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class MigrateInfosConfig:
    source_ledger_dir: Path
    urlinfos_dir: Path
    uploadinfos_dir: Path
    pdfinfos_dir: Path
    dry_run: bool
    summary_only: bool


@dataclass
class MigrateInfosReport:
    dry_run: bool
    source_files: int = 0
    source_rows: int = 0
    unique_codes: int = 0
    url_rows: int = 0
    upload_rows: int = 0
    pdf_rows: int = 0
    written_partition_files: int = 0
    source_partition_counts: Counter[str] = field(default_factory=Counter)
    url_partition_counts: Counter[str] = field(default_factory=Counter)
    upload_partition_counts: Counter[str] = field(default_factory=Counter)
    pdf_partition_counts: Counter[str] = field(default_factory=Counter)


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "Split import/grinfo/*.jsonl into import/urlinfos, import/uploadinfos, and import/pdfinfos."
    )
    parser.add_argument(
        "--source-ledger-dir",
        default="import/grinfo",
        help="Source grinfo ledger directory containing *.jsonl",
    )
    parser.add_argument(
        "--urlinfos-dir",
        default="import/urlinfos",
        help="Target URL infos directory",
    )
    parser.add_argument(
        "--uploadinfos-dir",
        default="import/uploadinfos",
        help="Target upload infos directory",
    )
    parser.add_argument(
        "--pdfinfos-dir",
        default="import/pdfinfos",
        help="Target PDF infos directory",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing files")
    parser.add_argument("--summary-only", action="store_true", help="Print only summary metrics")
    return parser


def _partition_sort_key(file_path: Path) -> tuple[int, int, str]:
    stem = file_path.stem
    if stem.isdigit():
        return (0, -int(stem), stem)
    if stem == "unknown":
        return (2, 0, stem)
    return (1, 0, stem)


def _list_source_files(source_dir: Path) -> list[Path]:
    files = sorted(source_dir.glob("*.jsonl"), key=_partition_sort_key)
    return [path for path in files if path.is_file()]


def _require_target_empty(target_dir: Path, label: str) -> None:
    if target_dir.exists() and not target_dir.is_dir():
        raise MigrationError(f"{label} target exists but is not a directory: {target_dir}")
    if target_dir.exists():
        existing_jsonl = sorted(target_dir.glob("*.jsonl"))
        if existing_jsonl:
            raise MigrationError(
                f"{label} target already contains JSONL files ({len(existing_jsonl)}): {target_dir}"
            )


def _iter_source_rows(source_files: list[Path]) -> Iterator[tuple[Path, int, dict[str, Any]]]:
    for file_path in source_files:
        with file_path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise MigrationError(f"Invalid JSON in {file_path}:{line_no}: {exc}") from exc
                if not isinstance(obj, dict):
                    raise MigrationError(f"Expected JSON object in {file_path}:{line_no}")
                yield file_path, line_no, obj


def _non_empty_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text else None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.lstrip("-").isdigit():
            try:
                return int(text)
            except ValueError:
                return None
    return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
    return None


def _status(value: Any, allowed: set[str], default: str) -> str:
    text = _non_empty_text(value, default)
    return text if text in allowed else default


def _source_timestamp(row: dict[str, Any], key: str, fallback: str) -> str:
    return _non_empty_text(row.get(key), fallback)


def _ascii_slug(value: str) -> str:
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    text = SAFE_TEXT_RE.sub("_", text).strip("_")
    return text


def _record_key_from_row(row: dict[str, Any], file_path: Path, line_no: int) -> str:
    unique_text = _non_empty_text(row.get("unique_code"), "")
    source_url = unquote(_non_empty_text(row.get("source_url"), ""))

    for candidate in (unique_text, source_url):
        match = LONG_DIGITS_RE.search(candidate)
        if match:
            return match.group(0)

    digits_only = "".join(ch for ch in unique_text if ch.isdigit())
    if len(digits_only) >= 18:
        return digits_only[:18]
    if len(digits_only) >= 16:
        return digits_only

    fallback = unique_text or Path(source_url).stem or source_url
    if fallback:
        slug = _ascii_slug(fallback)
        if slug:
            return slug
        digest = hashlib.sha1(fallback.encode("utf-8")).hexdigest()[:20]
        return f"ucode_{digest}"

    raise MigrationError(
        f"Unable to derive ASCII record_key from unique_code/source_url in {file_path}:{line_no}"
    )


def _canonical_department_code(row: dict[str, Any]) -> str:
    code_raw = _non_empty_text(row.get("department_code"), "")
    if code_raw:
        lower_code = code_raw.lower()
        if lower_code in DEPARTMENT_CODE_TO_NAME:
            return lower_code
    name_raw = _non_empty_text(row.get("department_name"), "")
    if name_raw:
        return department_code_from_name(name_raw)
    if code_raw:
        return department_code_from_name(code_raw)
    return "unknown"


def _build_urlinfo_row(row: dict[str, Any], record_key: str, now_text: str) -> dict[str, Any]:
    created_at = _source_timestamp(row, "created_at_utc", now_text)
    updated_at = _source_timestamp(row, "updated_at_utc", created_at)
    return {
        "record_key": record_key,
        "unique_code": record_key,
        "title": _non_empty_text(row.get("title"), ""),
        "department_name": _non_empty_text(row.get("department_name"), ""),
        "department_code": _canonical_department_code(row),
        "gr_date": _non_empty_text(row.get("gr_date"), ""),
        "source_url": _non_empty_text(row.get("source_url"), ""),
        "first_seen_crawl_date": _non_empty_text(row.get("first_seen_crawl_date"), ""),
        "last_seen_crawl_date": _non_empty_text(row.get("last_seen_crawl_date"), ""),
        "first_seen_run_type": _non_empty_text(row.get("first_seen_run_type"), "daily"),
        "created_at_utc": created_at,
        "updated_at_utc": updated_at,
    }


def _build_uploadinfo_row(row: dict[str, Any], record_key: str, now_text: str) -> dict[str, Any]:
    created_at = _source_timestamp(row, "created_at_utc", now_text)
    updated_at = _source_timestamp(row, "updated_at_utc", created_at)

    attempts_src = row.get("attempt_counts") if isinstance(row.get("attempt_counts"), dict) else {}
    download_attempts = _int_or_none(attempts_src.get("download"))
    wayback_attempts = _int_or_none(attempts_src.get("wayback"))
    archive_attempts = _int_or_none(attempts_src.get("archive"))
    hf_attempts = _int_or_none(attempts_src.get("hf"))
    if hf_attempts is None:
        hf_attempts = _int_or_none(attempts_src.get("lfs"))

    download_attempts = max(0, download_attempts) if download_attempts is not None else 0
    wayback_attempts = max(0, wayback_attempts) if wayback_attempts is not None else 0
    archive_attempts = max(0, archive_attempts) if archive_attempts is not None else 0
    hf_attempts = max(0, hf_attempts) if hf_attempts is not None else 0

    download_src = row.get("download") if isinstance(row.get("download"), dict) else {}
    wayback_src = row.get("wayback") if isinstance(row.get("wayback"), dict) else {}
    archive_src = row.get("archive") if isinstance(row.get("archive"), dict) else {}

    hf_path = _optional_text(row.get("lfs_path"))
    hf_status = "success" if hf_path else "not_attempted"

    return {
        "record_key": record_key,
        "state": _non_empty_text(row.get("state"), "FETCHED"),
        "download": {
            "status": _status(download_src.get("status"), ALLOWED_DOWNLOAD_STATUS, "not_attempted"),
            "error": _non_empty_text(download_src.get("error"), ""),
            "attempts": download_attempts,
        },
        "wayback": {
            "status": _status(wayback_src.get("status"), ALLOWED_WAYBACK_STATUS, "not_attempted"),
            "url": _non_empty_text(wayback_src.get("url"), ""),
            "content_url": _non_empty_text(wayback_src.get("content_url"), ""),
            "archive_time": _non_empty_text(wayback_src.get("archive_time"), ""),
            "archive_sha1": _non_empty_text(wayback_src.get("archive_sha1"), ""),
            "archive_length": _int_or_none(wayback_src.get("archive_length")),
            "archive_mimetype": _non_empty_text(wayback_src.get("archive_mimetype"), ""),
            "archive_status_code": _non_empty_text(wayback_src.get("archive_status_code"), ""),
            "error": _non_empty_text(wayback_src.get("error"), ""),
            "attempts": wayback_attempts,
        },
        "archive": {
            "status": _status(archive_src.get("status"), ALLOWED_ARCHIVE_STATUS, "not_attempted"),
            "identifier": _non_empty_text(archive_src.get("identifier"), ""),
            "url": _non_empty_text(archive_src.get("url"), ""),
            "error": _non_empty_text(archive_src.get("error"), ""),
            "attempts": archive_attempts,
        },
        "hf": {
            "status": hf_status,
            "path": hf_path,
            "hash": None,
            "backend": None,
            "commit_hash": None,
            "error": None,
            "synced_at_utc": updated_at if hf_path else None,
            "attempts": hf_attempts,
        },
        "created_at_utc": created_at,
        "updated_at_utc": updated_at,
    }


def _build_pdfinfo_row(row: dict[str, Any], record_key: str, now_text: str) -> dict[str, Any]:
    created_at = _source_timestamp(row, "created_at_utc", now_text)
    updated_at = _source_timestamp(row, "updated_at_utc", created_at)

    base: dict[str, Any] = {
        "record_key": record_key,
        "status": "not_attempted",
        "created_at_utc": created_at,
        "updated_at_utc": updated_at,
    }

    pdf_src = row.get("pdf_info")
    if not isinstance(pdf_src, dict):
        return base

    status = _status(pdf_src.get("status"), ALLOWED_PDF_STATUS, "not_attempted")
    if status == "not_attempted":
        return base

    out: dict[str, Any] = dict(base)
    out["status"] = status

    out["error"] = _optional_text(pdf_src.get("error"))
    if status in {"missing_pdf", "failed"}:
        out["file_size"] = None
        out["page_count"] = None
        out["pages_with_images"] = None
        out["has_any_page_image"] = None
        out["font_count"] = None
        out["fonts"] = None
        out["unresolved_word_count"] = None
        out["language"] = None
        return out

    out["file_size"] = _int_or_none(pdf_src.get("file_size"))
    out["page_count"] = _int_or_none(pdf_src.get("page_count"))
    out["pages_with_images"] = _int_or_none(pdf_src.get("pages_with_images"))
    out["has_any_page_image"] = _bool_or_none(pdf_src.get("has_any_page_image"))
    out["font_count"] = _int_or_none(pdf_src.get("font_count"))
    out["fonts"] = pdf_src.get("fonts") if isinstance(pdf_src.get("fonts"), dict) else None
    out["unresolved_word_count"] = _int_or_none(pdf_src.get("unresolved_word_count"))
    out["language"] = pdf_src.get("language") if isinstance(pdf_src.get("language"), dict) else None
    return out


def _scan_source(source_files: list[Path], report: MigrateInfosReport) -> None:
    seen_locations: dict[str, tuple[Path, int]] = {}
    for file_path, line_no, row in _iter_source_rows(source_files):
        record_key = _record_key_from_row(row, file_path, line_no)
        previous = seen_locations.get(record_key)
        if previous is not None:
            prev_file, prev_line = previous
            raise MigrationError(
                f"Duplicate record_key={record_key} at {file_path}:{line_no} "
                f"(first seen at {prev_file}:{prev_line})"
            )
        seen_locations[record_key] = (file_path, line_no)
        report.source_rows += 1
        partition = partition_for_gr_date(row.get("gr_date"))
        report.source_partition_counts[partition] += 1
    report.unique_codes = len(seen_locations)


def _open_partition_writer(
    root: Path,
    partition: str,
    handle_cache: dict[Path, TextIO],
) -> TextIO:
    file_path = root / f"{partition}.jsonl"
    handle = handle_cache.get(file_path)
    if handle is not None:
        return handle
    root.mkdir(parents=True, exist_ok=True)
    handle = file_path.open("a", encoding="utf-8")
    handle_cache[file_path] = handle
    return handle


def _close_all_handles(handle_cache: dict[Path, TextIO]) -> None:
    for handle in handle_cache.values():
        handle.close()
    handle_cache.clear()


def run_migrate_infos(config: MigrateInfosConfig) -> MigrateInfosReport:
    if not config.source_ledger_dir.exists() or not config.source_ledger_dir.is_dir():
        raise MigrationError(f"Source ledger directory not found: {config.source_ledger_dir}")

    source_files = _list_source_files(config.source_ledger_dir)
    if not source_files:
        raise MigrationError(f"No source JSONL files found under: {config.source_ledger_dir}")

    _require_target_empty(config.urlinfos_dir, "urlinfos")
    _require_target_empty(config.uploadinfos_dir, "uploadinfos")
    _require_target_empty(config.pdfinfos_dir, "pdfinfos")

    report = MigrateInfosReport(dry_run=config.dry_run)
    report.source_files = len(source_files)
    _scan_source(source_files, report)

    now_text = utc_now_text()
    temp_root: Path | None = None
    handle_cache: dict[Path, TextIO] = {}

    try:
        if not config.dry_run:
            temp_root = config.source_ledger_dir.parent / f".migrate_infos_tmp_{time.time_ns()}"
            url_temp_root = temp_root / "urlinfos"
            upload_temp_root = temp_root / "uploadinfos"
            pdf_temp_root = temp_root / "pdfinfos"
        else:
            url_temp_root = Path("/dev/null")
            upload_temp_root = Path("/dev/null")
            pdf_temp_root = Path("/dev/null")

        for file_path, line_no, row in _iter_source_rows(source_files):
            record_key = _record_key_from_row(row, file_path, line_no)
            partition = partition_for_gr_date(row.get("gr_date"))

            url_row = _build_urlinfo_row(row, record_key, now_text)
            upload_row = _build_uploadinfo_row(row, record_key, now_text)
            pdf_row = _build_pdfinfo_row(row, record_key, now_text)

            report.url_rows += 1
            report.upload_rows += 1
            report.pdf_rows += 1
            report.url_partition_counts[partition] += 1
            report.upload_partition_counts[partition] += 1
            report.pdf_partition_counts[partition] += 1

            if config.dry_run:
                continue

            url_handle = _open_partition_writer(url_temp_root, partition, handle_cache)
            url_handle.write(json.dumps(url_row, ensure_ascii=False, sort_keys=True))
            url_handle.write("\n")

            upload_handle = _open_partition_writer(upload_temp_root, partition, handle_cache)
            upload_handle.write(json.dumps(upload_row, ensure_ascii=False, sort_keys=True))
            upload_handle.write("\n")

            pdf_handle = _open_partition_writer(pdf_temp_root, partition, handle_cache)
            pdf_handle.write(json.dumps(pdf_row, ensure_ascii=False, sort_keys=True))
            pdf_handle.write("\n")

        _close_all_handles(handle_cache)

        if not config.dry_run and temp_root is not None:
            for namespace, temp_dir, target_dir in (
                ("urlinfos", temp_root / "urlinfos", config.urlinfos_dir),
                ("uploadinfos", temp_root / "uploadinfos", config.uploadinfos_dir),
                ("pdfinfos", temp_root / "pdfinfos", config.pdfinfos_dir),
            ):
                target_dir.mkdir(parents=True, exist_ok=True)
                for temp_file in sorted(temp_dir.glob("*.jsonl"), key=_partition_sort_key):
                    final_path = target_dir / temp_file.name
                    if final_path.exists():
                        raise MigrationError(
                            f"{namespace} target file already exists at commit stage: {final_path}"
                        )
                    temp_file.replace(final_path)
                    report.written_partition_files += 1

    finally:
        _close_all_handles(handle_cache)
        if temp_root is not None:
            shutil.rmtree(temp_root, ignore_errors=True)

    return report


def _print_counter(name: str, counter: Counter[str]) -> None:
    print(f"  {name}:")
    for partition in sorted(counter.keys(), key=lambda value: _partition_sort_key(Path(f"{value}.jsonl"))):
        print(f"    {partition}: {counter[partition]}")


def _print_report(report: MigrateInfosReport, *, summary_only: bool) -> None:
    print("migrate-infos:")
    print(f"  dry_run: {report.dry_run}")
    print(f"  source_files: {report.source_files}")
    print(f"  source_rows: {report.source_rows}")
    print(f"  unique_codes: {report.unique_codes}")
    print(f"  url_rows: {report.url_rows}")
    print(f"  upload_rows: {report.upload_rows}")
    print(f"  pdf_rows: {report.pdf_rows}")
    print(f"  written_partition_files: {report.written_partition_files}")

    if summary_only:
        return

    _print_counter("source_partition_counts", report.source_partition_counts)
    _print_counter("url_partition_counts", report.url_partition_counts)
    _print_counter("upload_partition_counts", report.upload_partition_counts)
    _print_counter("pdf_partition_counts", report.pdf_partition_counts)


def run_from_args(args: argparse.Namespace) -> int:
    config = MigrateInfosConfig(
        source_ledger_dir=Path(args.source_ledger_dir).resolve(),
        urlinfos_dir=Path(args.urlinfos_dir).resolve(),
        uploadinfos_dir=Path(args.uploadinfos_dir).resolve(),
        pdfinfos_dir=Path(args.pdfinfos_dir).resolve(),
        dry_run=args.dry_run,
        summary_only=args.summary_only,
    )
    try:
        report = run_migrate_infos(config)
    except MigrationError as exc:
        print(f"ERROR: {exc}")
        return 1

    _print_report(report, summary_only=config.summary_only)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Split grinfo into urlinfos/uploadinfos/pdfinfos.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
