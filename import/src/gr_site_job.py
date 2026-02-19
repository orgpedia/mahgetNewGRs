#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from department_codes import department_code_from_name
from info_store import InfoStore as LedgerStore
from ledger_engine import STATE_FETCHED
from local_env import load_local_env
from download_pdf_job import DownloadStageConfig, DownloadStageReport, run_download_stage


LONG_DIGITS_RE = re.compile(r"\d{16,22}")
CONTROL_CATEGORIES = {"Cf", "Cc"}
PAGE_INDEX_RE = re.compile(r"-(\d+)\.html$")


@dataclass(frozen=True)
class CrawledRecord:
    unique_code: str
    title: str
    department_name: str
    department_code: str
    gr_date: str
    source_url: str
    crawl_date: str
    page_index: int
    order_index: int
    source_file: str


@dataclass
class GRSiteReport:
    mode: str
    input_records: int = 0
    discovered_codes: list[str] | None = None
    inserted_codes: list[str] | None = None
    touched_codes: list[str] | None = None
    stop_pages: int = 0
    writes_enabled: bool = False
    download_report: DownloadStageReport | None = None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = "".join(ch for ch in text if unicodedata.category(ch) not in CONTROL_CATEGORIES)
    return text.strip()


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def parse_gr_date(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


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


def parse_page_index(html_file: Any) -> int:
    text = clean_text(html_file)
    if not text:
        return 10**9
    match = PAGE_INDEX_RE.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 10**9
    return 10**9


def parse_crawl_date(value: Any, fallback_date: str) -> str:
    parsed_dt = parse_datetime_utc(value)
    if parsed_dt is not None:
        return parsed_dt.date().isoformat()
    parsed_day = parse_gr_date(value)
    if parsed_day:
        return parsed_day
    return fallback_date


def load_rows_from_json(file_path: Path) -> list[dict[str, Any]]:
    with file_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {file_path}")
    return [item for item in data if isinstance(item, dict)]


def gather_source_files(source_dir: Path | None, source_json_files: list[Path]) -> list[Path]:
    source_files: list[Path] = []
    if source_dir is not None:
        source_files.extend(sorted(source_dir.glob("**/GRs_log.json")))
    source_files.extend(source_json_files)
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in source_files:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(resolved)
    return deduped


def normalize_rows(source_files: list[Path], fallback_crawl_date: str) -> list[CrawledRecord]:
    records: list[CrawledRecord] = []
    order_index = 0
    for source_file in source_files:
        rows = load_rows_from_json(source_file)
        for row in rows:
            source_url = first_non_empty(row.get("Download"), row.get("url"))
            unique_code = canonical_unique_code(row.get("Unique Code"), source_url)
            if not unique_code:
                continue

            department_name = first_non_empty(row.get("Department Name"))
            records.append(
                CrawledRecord(
                    unique_code=unique_code,
                    title=first_non_empty(row.get("Title")),
                    department_name=department_name,
                    department_code=department_code_from_name(department_name),
                    gr_date=parse_gr_date(row.get("G.R. Date")),
                    source_url=source_url,
                    crawl_date=parse_crawl_date(row.get("download_time_utc"), fallback_crawl_date),
                    page_index=parse_page_index(row.get("html_file")),
                    order_index=order_index,
                    source_file=str(source_file),
                )
            )
            order_index += 1
    return records


def group_daily_by_department_and_page(records: list[CrawledRecord]) -> dict[str, dict[int, list[CrawledRecord]]]:
    grouped: dict[str, dict[int, list[CrawledRecord]]] = defaultdict(lambda: defaultdict(list))
    for record in records:
        dept_key = record.department_code or "unknown"
        grouped[dept_key][record.page_index].append(record)
    for dept_pages in grouped.values():
        for page_records in dept_pages.values():
            page_records.sort(key=lambda item: item.order_index)
    return grouped


def record_patch(record: CrawledRecord) -> dict[str, Any]:
    return {
        "unique_code": record.unique_code,
        "title": record.title,
        "department_name": record.department_name,
        "department_code": record.department_code,
        "gr_date": record.gr_date,
        "source_url": record.source_url,
        "state": STATE_FETCHED,
    }


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Process gr-site crawl logs and reconcile into ledger (no PDF download in this command)."
    parser.add_argument(
        "--mode",
        required=True,
        choices=("daily", "weekly", "monthly"),
        help="Run mode",
    )
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument(
        "--source-dir",
        default="import/websites/gr.maharashtra.gov.in",
        help="Directory to scan recursively for GRs_log.json files (daily/monthly)",
    )
    parser.add_argument(
        "--source-json",
        action="append",
        default=[],
        help="Additional GRs JSON files (same row schema as GRs_log.json)",
    )
    parser.add_argument("--crawl-date", default="", help="Override crawl date (YYYY-MM-DD)")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap on records applied")
    parser.add_argument("--dry-run", action="store_true", help="Plan actions without writing ledger")
    parser.add_argument("--show-sample", type=int, default=10, help="Print up to N sample codes")
    return parser


def _summarize_counts(label: str, count: int) -> None:
    print(f"{label}: {count}")


def _print_samples(name: str, values: list[str], limit: int) -> None:
    if limit <= 0 or not values:
        return
    print(f"{name} sample ({min(limit, len(values))}):")
    for value in values[:limit]:
        print(f"  {value}")


def _print_download_report(download_report: DownloadStageReport) -> None:
    print("Download stage:")
    _summarize_counts("  selected", download_report.selected)
    _summarize_counts("  processed", download_report.processed)
    _summarize_counts("  success", download_report.success)
    _summarize_counts("  failed", download_report.failed)
    _summarize_counts("  skipped", download_report.skipped)
    _summarize_counts("  service_failures", download_report.service_failures)
    print(f"  stopped_early: {download_report.stopped_early}")


def run_daily(
    store: LedgerStore,
    records: list[CrawledRecord],
    *,
    crawl_date: str,
    max_records: int,
    dry_run: bool,
    show_sample: int,
    skip_download: bool,
    lfs_root: Path,
    download_timeout_sec: int,
    service_failure_limit: int,
) -> GRSiteReport:
    known_codes = set(rec.get("unique_code") for rec in store.iter_records() if isinstance(rec.get("unique_code"), str))
    grouped = group_daily_by_department_and_page(records)

    discovered: list[CrawledRecord] = []
    discovered_codes_seen: set[str] = set()
    stop_pages: dict[str, int] = {}

    for department_code, pages in sorted(grouped.items()):
        for page_index in sorted(pages.keys()):
            page_records = pages[page_index]
            page_has_known = any(record.unique_code in known_codes for record in page_records)
            for record in page_records:
                if record.unique_code in known_codes:
                    continue
                if record.unique_code in discovered_codes_seen:
                    continue
                discovered.append(record)
                discovered_codes_seen.add(record.unique_code)
            if page_has_known:
                stop_pages[department_code] = page_index
                break

    if max_records > 0:
        discovered = discovered[:max_records]

    if not dry_run:
        for record in discovered:
            store.insert(record_patch(record), run_type="daily", crawl_date=record.crawl_date or crawl_date)

    report = GRSiteReport(
        mode="daily",
        input_records=len(records),
        discovered_codes=[record.unique_code for record in discovered],
        stop_pages=len(stop_pages),
        writes_enabled=not dry_run,
    )

    if not skip_download and report.discovered_codes:
        report.download_report = run_download_stage(
            DownloadStageConfig(
                ledger_dir=store.ledger_dir,
                lfs_root=lfs_root.resolve(),
                timeout_sec=max(1, download_timeout_sec),
                service_failure_limit=max(1, service_failure_limit),
                max_records=max_records,
                dry_run=dry_run,
                code_filter=set(report.discovered_codes),
                allowed_states={"FETCHED"},
            )
        )

    print("Mode: daily")
    _summarize_counts("Input records", report.input_records)
    _summarize_counts("Known ledger records", len(known_codes))
    _summarize_counts("Departments with early-stop", report.stop_pages)
    _summarize_counts("Discovered new records", len(report.discovered_codes or []))
    print(f"Writes enabled: {report.writes_enabled}")
    _print_samples("Discovered codes", report.discovered_codes or [], show_sample)
    if report.download_report:
        _print_download_report(report.download_report)
    return report


def run_weekly(
    store: LedgerStore,
    *,
    max_records: int,
    dry_run: bool,
    skip_download: bool,
    lfs_root: Path,
    download_timeout_sec: int,
    service_failure_limit: int,
) -> GRSiteReport:
    report = GRSiteReport(mode="weekly", input_records=0, writes_enabled=not dry_run)
    print("Mode: weekly")
    print("Crawl step: skipped (weekly policy)")
    if skip_download:
        print("Download stage: skipped by flag")
        return report

    report.download_report = run_download_stage(
        DownloadStageConfig(
            ledger_dir=store.ledger_dir,
            lfs_root=lfs_root.resolve(),
            timeout_sec=max(1, download_timeout_sec),
            service_failure_limit=max(1, service_failure_limit),
            max_records=max_records,
            dry_run=dry_run,
            allowed_states={"DOWNLOAD_FAILED", "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT"},
        )
    )
    _print_download_report(report.download_report)
    return report


def run_monthly(
    store: LedgerStore,
    records: list[CrawledRecord],
    *,
    crawl_date: str,
    max_records: int,
    dry_run: bool,
    show_sample: int,
) -> GRSiteReport:
    record_by_code: dict[str, CrawledRecord] = {}
    for record in records:
        existing = record_by_code.get(record.unique_code)
        if existing is None:
            record_by_code[record.unique_code] = record
            continue
        existing_score = sum(bool(getattr(existing, field)) for field in ("title", "department_name", "gr_date", "source_url"))
        candidate_score = sum(bool(getattr(record, field)) for field in ("title", "department_name", "gr_date", "source_url"))
        if candidate_score > existing_score:
            record_by_code[record.unique_code] = record
            continue
        if candidate_score == existing_score and (record.crawl_date or "") > (existing.crawl_date or ""):
            record_by_code[record.unique_code] = record

    monthly_records = sorted(record_by_code.values(), key=lambda item: item.unique_code)
    if max_records > 0:
        monthly_records = monthly_records[:max_records]

    known_codes = set(rec.get("unique_code") for rec in store.iter_records() if isinstance(rec.get("unique_code"), str))
    inserted_codes: list[str] = []
    touched_codes: list[str] = []

    if not dry_run:
        for record in monthly_records:
            if record.unique_code in known_codes:
                store.update({"unique_code": record.unique_code}, run_type="monthly", crawl_date=record.crawl_date or crawl_date)
                touched_codes.append(record.unique_code)
            else:
                store.insert(record_patch(record), run_type="monthly", crawl_date=record.crawl_date or crawl_date)
                inserted_codes.append(record.unique_code)
                known_codes.add(record.unique_code)
    else:
        for record in monthly_records:
            if record.unique_code in known_codes:
                touched_codes.append(record.unique_code)
            else:
                inserted_codes.append(record.unique_code)
                known_codes.add(record.unique_code)

    report = GRSiteReport(
        mode="monthly",
        input_records=len(records),
        inserted_codes=inserted_codes,
        touched_codes=touched_codes,
        writes_enabled=not dry_run,
    )

    print("Mode: monthly")
    _summarize_counts("Input records", report.input_records)
    _summarize_counts("Unique crawled codes", len(record_by_code))
    _summarize_counts("Inserted missing records", len(report.inserted_codes or []))
    _summarize_counts("Touched known records", len(report.touched_codes or []))
    print(f"Writes enabled: {report.writes_enabled}")
    _print_samples("Inserted codes", report.inserted_codes or [], show_sample)
    _print_samples("Touched codes", report.touched_codes or [], show_sample)
    return report


def run_from_args(args: argparse.Namespace) -> int:
    ledger_dir = Path(args.ledger_dir).resolve()
    store = LedgerStore(ledger_dir)
    print("gr-site: PDF download stage is disabled in this command.")

    if args.crawl_date:
        crawl_date = parse_gr_date(args.crawl_date)
        if not crawl_date:
            print(f"Invalid --crawl-date: {args.crawl_date}")
            return 2
    else:
        crawl_date = datetime.now(timezone.utc).date().isoformat()

    if args.mode == "weekly":
        run_weekly(
            store=store,
            max_records=max(0, args.max_records),
            dry_run=args.dry_run,
            skip_download=True,
            lfs_root=Path("LFS/pdfs"),
            download_timeout_sec=30,
            service_failure_limit=10,
        )
        return 0

    source_dir = Path(args.source_dir).resolve() if args.source_dir else None
    source_json_files = [Path(path).resolve() for path in args.source_json]
    source_files = gather_source_files(source_dir=source_dir, source_json_files=source_json_files)
    if not source_files:
        print("No source crawl files found. Use --source-dir or --source-json.")
        return 2

    records = normalize_rows(source_files=source_files, fallback_crawl_date=crawl_date)
    if not records:
        print("No usable crawl records found after normalization.")
        return 2

    if args.mode == "daily":
        run_daily(
            store=store,
            records=records,
            crawl_date=crawl_date,
            max_records=max(0, args.max_records),
            dry_run=args.dry_run,
            show_sample=max(0, args.show_sample),
            skip_download=True,
            lfs_root=Path("LFS/pdfs"),
            download_timeout_sec=30,
            service_failure_limit=10,
        )
        return 0

    run_monthly(
        store=store,
        records=records,
        crawl_date=crawl_date,
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        show_sample=max(0, args.show_sample),
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process gr-site crawl logs and reconcile into ledger.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
