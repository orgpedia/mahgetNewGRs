#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from department_codes import department_code_from_name
from local_env import load_local_env


STATE_FETCHED = "FETCHED"
STATE_DOWNLOAD_SUCCESS = "DOWNLOAD_SUCCESS"
STATE_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
STATE_WAYBACK_UPLOADED = "WAYBACK_UPLOADED"
STATE_WAYBACK_UPLOAD_FAILED = "WAYBACK_UPLOAD_FAILED"
STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL = "ARCHIVE_UPLOADED_WITH_WAYBACK_URL"
STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL = "ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL"

LONG_DIGITS_RE = re.compile(r"\d{16,22}")
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+")
CONTROL_CATEGORIES = {"Cf", "Cc"}
EMPTY_TEXT = {"", "-", "None", "null", "NULL"}
MIN_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class SourceRepo:
    name: str
    root: Path
    run_type: str
    priority: int


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Build import/grinfo baseline JSONL ledger from mahgetGR and mahgetAllGR datasets."
    parser.add_argument("--mahgetgr-root", default="mahgetGR", help="Path to mahgetGR repository root")
    parser.add_argument(
        "--mahgetallgr-root",
        default="mahgetAllGR",
        help="Path to mahgetAllGR repository root",
    )
    parser.add_argument(
        "--output-dir",
        default="import/grinfo",
        help="Output directory for yearly ledger JSONL files",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Delete existing *.jsonl files in output directory before writing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build in memory and print summary without writing files",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build import/grinfo baseline JSONL ledger from mahgetGR and mahgetAllGR datasets."
    )
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def read_json_rows(file_path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if file_path.suffix == ".gz" else open
    with opener(file_path, "rt", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array in {file_path}")
    rows: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = "".join(ch for ch in text if unicodedata.category(ch) not in CONTROL_CATEGORIES)
    return text.strip()


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text and text not in EMPTY_TEXT:
            return text
    return ""


def parse_gr_date(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_datetime_utc(value: Any) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S %Z%z",
        "%Y-%m-%d %H:%M:%S UTC%z",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def canonical_unique_code(unique_code: Any, source_url: Any) -> str:
    code_text = clean_text(unique_code)
    url_text = unquote(clean_text(source_url))

    for candidate in (code_text, url_text):
        long_match = LONG_DIGITS_RE.search(candidate)
        if long_match:
            return long_match.group(0)

    digits_only = "".join(ch for ch in code_text if ch.isdigit())
    if len(digits_only) >= 18:
        return digits_only[:18]
    if len(digits_only) >= 16:
        return digits_only

    if code_text:
        return code_text
    if url_text:
        return url_text
    return ""


def parse_size_bytes(kb_value: Any) -> int | None:
    text = clean_text(kb_value).replace(",", "")
    if not text:
        return None
    match = NUMERIC_RE.search(text)
    if not match:
        return None
    try:
        as_float = float(match.group(0))
    except ValueError:
        return None
    if as_float <= 0:
        return None
    return int(as_float * 1024)


def parse_int(value: Any) -> int | None:
    text = clean_text(value).replace(",", "")
    if not text:
        return None
    if text.isdigit():
        return int(text)
    match = NUMERIC_RE.search(text)
    if not match:
        return None
    try:
        return int(float(match.group(0)))
    except ValueError:
        return None


def safe_filename(value: str) -> str:
    text = SAFE_FILENAME_RE.sub("_", value).strip("_")
    return text or "unknown"


def existing_stage_files(root: Path, stage: str) -> list[Path]:
    stage_candidates: dict[str, tuple[str, ...]] = {
        "merged": (
            "import/documents/merged_fetch.json",
            "import/documents/merged_fetch.json.gz",
            "import/documents/merged_fetch_old.json.gz",
        ),
        "wayback": (
            "import/documents/wayback.json",
            "import/documents/wayback.json.gz",
        ),
        "archive": (
            "import/documents/archive.json",
            "import/documents/archive_old.json.gz",
        ),
        "pdfs": (
            "import/documents/pdfs.json",
        ),
    }
    files: list[Path] = []
    for relative_path in stage_candidates[stage]:
        candidate = root / relative_path
        if candidate.exists():
            files.append(candidate)
    return files


def score_merged_row(row: dict[str, Any], source_priority: int) -> tuple[int, int, int, datetime, int]:
    completeness = int(bool(first_non_empty(row.get("Title"))))
    completeness += sum(
        bool(first_non_empty(row.get(field)))
        for field in ("Department Name", "G.R. Date", "Download", "Unique Code")
    )
    gr_date_ok = 1 if parse_gr_date(row.get("G.R. Date")) else 0
    crawl_dt = parse_datetime_utc(row.get("download_time_utc"))
    has_crawl = 1 if crawl_dt else 0
    return (completeness, gr_date_ok, has_crawl, crawl_dt or MIN_DATETIME, source_priority)


def score_wayback_row(row: dict[str, Any], source_priority: int) -> tuple[int, int, int, int, int, int]:
    link_success = 1 if row.get("link_success") is True else 0
    archive_url = 1 if first_non_empty(row.get("archive_url")) else 0
    content_url = 1 if first_non_empty(row.get("content_url")) else 0
    sha1 = 1 if first_non_empty(row.get("archive_sha1")) else 0
    status_ok = 1 if clean_text(row.get("archive_status_code")) == "200" else 0
    return (link_success, archive_url, content_url, sha1, status_ok, source_priority)


def score_archive_row(row: dict[str, Any], source_priority: int) -> tuple[int, int, int, int, int]:
    upload_success = 1 if row.get("upload_success") is True else 0
    identifier = 1 if first_non_empty(row.get("identifier")) else 0
    archive_url = 1 if first_non_empty(row.get("archive_url")) else 0
    wayback_url = 1 if first_non_empty(row.get("wayback_url")) else 0
    return (upload_success, identifier, archive_url, wayback_url, source_priority)


def score_pdf_row(row: dict[str, Any], source_priority: int) -> tuple[int, int, datetime, int]:
    success = 1 if row.get("download_success") is True else 0
    download_dt = parse_datetime_utc(row.get("download_time_utc"))
    has_dt = 1 if download_dt else 0
    return (success, has_dt, download_dt or MIN_DATETIME, source_priority)


def update_best_row(
    best_rows: dict[str, dict[str, Any]],
    best_scores: dict[str, tuple[Any, ...]],
    unique_code: str,
    row: dict[str, Any],
    score: tuple[Any, ...],
) -> None:
    current_score = best_scores.get(unique_code)
    if current_score is None or score > current_score:
        best_scores[unique_code] = score
        best_rows[unique_code] = row


def derive_crawl_fields(
    unique_code: str,
    merged_observations: dict[str, list[tuple[date, str]]],
    fallback_observations: dict[str, list[tuple[date, str]]],
    seen_run_types: dict[str, set[str]],
    default_date: date,
) -> tuple[str, str, str]:
    observations = merged_observations.get(unique_code) or fallback_observations.get(unique_code) or []

    if not observations:
        run_types = seen_run_types.get(unique_code, set())
        first_run_type = "monthly" if "monthly" in run_types else "daily"
        iso_date = default_date.isoformat()
        return iso_date, iso_date, first_run_type

    first_date = min(observed_date for observed_date, _ in observations)
    earliest_run_types = {run_type for observed_date, run_type in observations if observed_date == first_date}
    first_run_type = "monthly" if "monthly" in earliest_run_types else "daily"

    monthly_dates = [observed_date for observed_date, run_type in observations if run_type == "monthly"]
    last_date = max(monthly_dates) if monthly_dates else first_date

    return first_date.isoformat(), last_date.isoformat(), first_run_type


def derive_state(
    archive_success: bool,
    archive_attempted: bool,
    wayback_success: bool,
    wayback_attempted: bool,
    download_success: bool | None,
) -> str:
    if archive_success:
        if wayback_success:
            return STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL
        return STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL

    if archive_attempted:
        if wayback_success:
            return STATE_WAYBACK_UPLOADED
        if wayback_attempted:
            return STATE_WAYBACK_UPLOAD_FAILED
        if download_success is False:
            return STATE_DOWNLOAD_FAILED
        if download_success is True:
            return STATE_DOWNLOAD_SUCCESS
        return STATE_FETCHED

    if wayback_success:
        return STATE_WAYBACK_UPLOADED
    if wayback_attempted:
        return STATE_WAYBACK_UPLOAD_FAILED
    if download_success is True:
        return STATE_DOWNLOAD_SUCCESS
    if download_success is False:
        return STATE_DOWNLOAD_FAILED
    return STATE_FETCHED


def atomic_write_text(file_path: Path, content: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_name(f".{file_path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(file_path)


def build_baseline_records(
    source_repos: list[SourceRepo],
) -> tuple[dict[str, list[dict[str, Any]]], Counter, int]:
    merged_rows_by_code: dict[str, dict[str, Any]] = {}
    merged_scores_by_code: dict[str, tuple[Any, ...]] = {}
    wayback_rows_by_code: dict[str, dict[str, Any]] = {}
    wayback_scores_by_code: dict[str, tuple[Any, ...]] = {}
    archive_rows_by_code: dict[str, dict[str, Any]] = {}
    archive_scores_by_code: dict[str, tuple[Any, ...]] = {}
    pdf_rows_by_code: dict[str, dict[str, Any]] = {}
    pdf_scores_by_code: dict[str, tuple[Any, ...]] = {}

    merged_observations: dict[str, list[tuple[date, str]]] = defaultdict(list)
    fallback_observations: dict[str, list[tuple[date, str]]] = defaultdict(list)
    seen_run_types: dict[str, set[str]] = defaultdict(set)
    stage_attempts: dict[str, Counter] = {
        "download": Counter(),
        "wayback": Counter(),
        "archive": Counter(),
    }

    all_unique_codes: set[str] = set()

    for source in source_repos:
        for merged_file in existing_stage_files(source.root, "merged"):
            for row in read_json_rows(merged_file):
                code = canonical_unique_code(row.get("Unique Code"), row.get("Download") or row.get("url"))
                if not code:
                    continue
                all_unique_codes.add(code)
                seen_run_types[code].add(source.run_type)
                crawl_dt = parse_datetime_utc(row.get("download_time_utc"))
                if crawl_dt:
                    merged_observations[code].append((crawl_dt.date(), source.run_type))
                update_best_row(
                    merged_rows_by_code,
                    merged_scores_by_code,
                    code,
                    row,
                    score_merged_row(row, source.priority),
                )

        for wayback_file in existing_stage_files(source.root, "wayback"):
            for row in read_json_rows(wayback_file):
                code = canonical_unique_code(row.get("Unique Code"), row.get("url"))
                if not code:
                    continue
                all_unique_codes.add(code)
                seen_run_types[code].add(source.run_type)
                stage_attempts["wayback"][code] += 1
                update_best_row(
                    wayback_rows_by_code,
                    wayback_scores_by_code,
                    code,
                    row,
                    score_wayback_row(row, source.priority),
                )

        for archive_file in existing_stage_files(source.root, "archive"):
            for row in read_json_rows(archive_file):
                code = canonical_unique_code(row.get("Unique Code"), row.get("Download") or row.get("url"))
                if not code:
                    continue
                all_unique_codes.add(code)
                seen_run_types[code].add(source.run_type)
                stage_attempts["archive"][code] += 1
                archive_dt = parse_datetime_utc(row.get("download_time_utc"))
                if archive_dt:
                    fallback_observations[code].append((archive_dt.date(), source.run_type))
                update_best_row(
                    archive_rows_by_code,
                    archive_scores_by_code,
                    code,
                    row,
                    score_archive_row(row, source.priority),
                )

        for pdf_file in existing_stage_files(source.root, "pdfs"):
            for row in read_json_rows(pdf_file):
                code = canonical_unique_code(row.get("Unique Code"), row.get("url"))
                if not code:
                    continue
                all_unique_codes.add(code)
                seen_run_types[code].add(source.run_type)
                stage_attempts["download"][code] += 1
                pdf_dt = parse_datetime_utc(row.get("download_time_utc"))
                if pdf_dt:
                    fallback_observations[code].append((pdf_dt.date(), source.run_type))
                update_best_row(
                    pdf_rows_by_code,
                    pdf_scores_by_code,
                    code,
                    row,
                    score_pdf_row(row, source.priority),
                )

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    now_utc_text = now_utc.isoformat().replace("+00:00", "Z")
    today_utc = now_utc.date()

    partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    state_counts: Counter = Counter()

    for unique_code in sorted(all_unique_codes):
        merged_row = merged_rows_by_code.get(unique_code, {})
        wayback_row = wayback_rows_by_code.get(unique_code, {})
        archive_row = archive_rows_by_code.get(unique_code, {})
        pdf_row = pdf_rows_by_code.get(unique_code, {})

        title = first_non_empty(merged_row.get("Title"), archive_row.get("Title"))
        department_name = first_non_empty(merged_row.get("Department Name"), archive_row.get("Department Name"))
        department_code = department_code_from_name(department_name)
        source_url = first_non_empty(
            merged_row.get("Download"),
            merged_row.get("url"),
            archive_row.get("Download"),
            archive_row.get("url"),
            wayback_row.get("url"),
        )

        gr_date_iso = parse_gr_date(first_non_empty(merged_row.get("G.R. Date"), archive_row.get("G.R. Date")))
        partition_key = gr_date_iso[:4] if gr_date_iso else "unknown"
        year_month = gr_date_iso[:7] if gr_date_iso else "unknown"

        wayback_url = first_non_empty(wayback_row.get("archive_url"), archive_row.get("wayback_url"))
        wayback_success = bool(wayback_url) or (wayback_row.get("link_success") is True)
        wayback_attempted = stage_attempts["wayback"][unique_code] > 0 or ("wayback_url" in archive_row)

        archive_success = archive_row.get("upload_success") is True
        archive_attempted = stage_attempts["archive"][unique_code] > 0

        explicit_download_success = pdf_row.get("download_success")
        download_success: bool | None
        if explicit_download_success is True:
            download_success = True
        elif explicit_download_success is False:
            download_success = False
        elif archive_success or wayback_success:
            download_success = True
        elif archive_attempted and not wayback_attempted:
            download_success = False
        else:
            download_success = None

        if download_success is True:
            download_status = "success"
        elif download_success is False:
            download_status = "failed"
        elif stage_attempts["download"][unique_code] > 0 or wayback_attempted or archive_attempted:
            download_status = "attempted"
        else:
            download_status = "not_attempted"

        if wayback_success:
            wayback_status = "success"
        elif wayback_attempted:
            wayback_status = "failed"
        else:
            wayback_status = "not_attempted"

        if archive_success:
            archive_status = "success"
        elif archive_attempted:
            archive_status = "failed"
        else:
            archive_status = "not_attempted"

        state = derive_state(
            archive_success=archive_success,
            archive_attempted=archive_attempted,
            wayback_success=wayback_success,
            wayback_attempted=wayback_attempted,
            download_success=download_success,
        )
        state_counts[state] += 1

        first_seen_crawl_date, last_seen_crawl_date, first_seen_run_type = derive_crawl_fields(
            unique_code=unique_code,
            merged_observations=merged_observations,
            fallback_observations=fallback_observations,
            seen_run_types=seen_run_types,
            default_date=today_utc,
        )

        stage_download_attempts = stage_attempts["download"][unique_code]
        if stage_download_attempts == 0 and (wayback_attempted or archive_attempted or download_status != "not_attempted"):
            stage_download_attempts = 1
        stage_wayback_attempts = stage_attempts["wayback"][unique_code]
        if stage_wayback_attempts == 0 and wayback_attempted:
            stage_wayback_attempts = 1
        stage_archive_attempts = stage_attempts["archive"][unique_code]

        file_size_bytes = parse_size_bytes(
            first_non_empty(merged_row.get("File Size (KB)"), merged_row.get("File Size"), archive_row.get("File Size (KB)"), archive_row.get("File Size"))
        )
        wayback_length = parse_int(wayback_row.get("archive_length"))
        download_size = wayback_length if wayback_length else file_size_bytes
        download_hash = first_non_empty(wayback_row.get("archive_sha1"))

        safe_code = safe_filename(unique_code)
        download_path = ""
        if source_url:
            download_path = f"LFS/pdfs/{department_code}/{year_month}/{safe_code}.pdf"
        lfs_path: str | None = None
        if download_path:
            candidate_path = Path(download_path)
            if candidate_path.exists() and candidate_path.is_file():
                lfs_path = candidate_path.as_posix()

        download_error = ""
        if download_status == "failed":
            download_error = first_non_empty(pdf_row.get("status")) or "download_failed"

        wayback_error = "wayback_upload_failed" if wayback_status == "failed" else ""
        archive_error = "archive_upload_failed" if archive_status == "failed" else ""

        record = {
            "unique_code": unique_code,
            "title": title,
            "department_name": department_name,
            "department_code": department_code,
            "gr_date": gr_date_iso or "",
            "source_url": source_url,
            "lfs_path": lfs_path,
            "state": state,
            "attempt_counts": {
                "download": min(2, max(0, stage_download_attempts)),
                "wayback": min(2, max(0, stage_wayback_attempts)),
                "archive": min(2, max(0, stage_archive_attempts)),
            },
            "download": {
                "path": download_path,
                "status": download_status,
                "hash": download_hash,
                "size": download_size,
                "error": download_error,
            },
            "wayback": {
                "url": wayback_url,
                "content_url": first_non_empty(wayback_row.get("content_url")),
                "archive_time": first_non_empty(wayback_row.get("archive_time")),
                "archive_sha1": first_non_empty(wayback_row.get("archive_sha1")),
                "archive_length": wayback_length,
                "archive_mimetype": first_non_empty(wayback_row.get("archive_mimetype")),
                "archive_status_code": first_non_empty(wayback_row.get("archive_status_code")),
                "status": wayback_status,
                "error": wayback_error,
            },
            "archive": {
                "identifier": first_non_empty(archive_row.get("identifier")),
                "url": first_non_empty(archive_row.get("archive_url")),
                "status": archive_status,
                "error": archive_error,
            },
            "first_seen_crawl_date": first_seen_crawl_date,
            "last_seen_crawl_date": last_seen_crawl_date,
            "first_seen_run_type": first_seen_run_type,
            "created_at_utc": now_utc_text,
            "updated_at_utc": now_utc_text,
        }
        partitions[partition_key].append(record)

    for partition_rows in partitions.values():
        partition_rows.sort(key=lambda row: row["unique_code"])

    return partitions, state_counts, len(all_unique_codes)


def write_partitions(output_dir: Path, partitions: dict[str, list[dict[str, Any]]], clean_output: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if clean_output:
        for stale_file in output_dir.glob("*.jsonl"):
            stale_file.unlink()

    for partition_key, rows in sorted(partitions.items()):
        output_file = output_dir / f"{partition_key}.jsonl"
        lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
        content = "\n".join(lines)
        if content:
            content += "\n"
        atomic_write_text(output_file, content)


def run_from_args(args: argparse.Namespace) -> int:
    source_repos = [
        SourceRepo(
            name="mahgetAllGR",
            root=Path(args.mahgetallgr_root).resolve(),
            run_type="monthly",
            priority=1,
        ),
        SourceRepo(
            name="mahgetGR",
            root=Path(args.mahgetgr_root).resolve(),
            run_type="daily",
            priority=2,
        ),
    ]

    for source in source_repos:
        if not source.root.exists():
            raise FileNotFoundError(f"Source repository path not found: {source.root}")

    partitions, state_counts, total_records = build_baseline_records(source_repos)
    output_dir = Path(args.output_dir).resolve()

    if not args.dry_run:
        write_partitions(output_dir=output_dir, partitions=partitions, clean_output=args.clean_output)

    print(f"Records: {total_records}")
    print(f"Partitions: {len(partitions)}")
    print(f"Output dir: {output_dir}")
    print(f"Writes enabled: {not args.dry_run}")
    print("States:")
    for state_name, count in sorted(state_counts.items()):
        print(f"  {state_name}: {count}")
    print("Top partitions:")
    top_partitions = sorted(((key, len(rows)) for key, rows in partitions.items()), key=lambda item: item[1], reverse=True)
    for partition_key, count in top_partitions[:10]:
        print(f"  {partition_key}.jsonl: {count}")
    return 0


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
