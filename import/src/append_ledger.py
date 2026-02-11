#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from build_baseline_ledger import (
    canonical_unique_code,
    existing_stage_files,
    first_non_empty,
    parse_datetime_utc,
    parse_gr_date,
    read_json_rows,
    score_archive_row,
    score_merged_row,
    score_pdf_row,
    score_wayback_row,
    update_best_row,
)
from department_codes import department_code_from_name
from ledger_engine import LedgerStore
from local_env import load_local_env


@dataclass(frozen=True)
class AppendLedgerConfig:
    mahgetgr_root: Path
    ledger_dir: Path
    dry_run: bool


@dataclass
class AppendLedgerReport:
    scanned_rows: int = 0
    scanned_candidates: int = 0
    appended: int = 0
    skipped_existing_unique_code: int = 0
    skipped_invalid_unique_code: int = 0


@dataclass(frozen=True)
class CandidateRecord:
    unique_code: str
    title: str
    department_name: str
    department_code: str
    gr_date: str
    source_url: str
    crawl_date: date | None


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Append new GR records from mahgetGR into existing ledger (append-only)."
    parser.add_argument("--mahgetgr-root", default="mahgetGR", help="Path to mahgetGR repository root")
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger directory")
    parser.add_argument("--dry-run", action="store_true", help="Show append counts without writing")
    return parser


def _build_candidates(mahgetgr_root: Path, report: AppendLedgerReport) -> list[CandidateRecord]:
    merged_rows_by_code: dict[str, dict] = {}
    merged_scores_by_code: dict[str, tuple] = {}
    wayback_rows_by_code: dict[str, dict] = {}
    wayback_scores_by_code: dict[str, tuple] = {}
    archive_rows_by_code: dict[str, dict] = {}
    archive_scores_by_code: dict[str, tuple] = {}
    pdf_rows_by_code: dict[str, dict] = {}
    pdf_scores_by_code: dict[str, tuple] = {}
    first_observed_date: dict[str, date] = {}
    all_unique_codes: set[str] = set()

    def register_observed_date(unique_code: str, row: dict) -> None:
        observed_dt = parse_datetime_utc(row.get("download_time_utc"))
        if observed_dt is None:
            return
        observed_date = observed_dt.date()
        existing_date = first_observed_date.get(unique_code)
        if existing_date is None or observed_date < existing_date:
            first_observed_date[unique_code] = observed_date

    source_priority = 2  # mahgetGR daily source priority used in baseline

    for merged_file in existing_stage_files(mahgetgr_root, "merged"):
        for row in read_json_rows(merged_file):
            report.scanned_rows += 1
            unique_code = canonical_unique_code(row.get("Unique Code"), row.get("Download") or row.get("url"))
            if not unique_code:
                report.skipped_invalid_unique_code += 1
                continue
            all_unique_codes.add(unique_code)
            register_observed_date(unique_code, row)
            update_best_row(
                merged_rows_by_code,
                merged_scores_by_code,
                unique_code,
                row,
                score_merged_row(row, source_priority),
            )

    for wayback_file in existing_stage_files(mahgetgr_root, "wayback"):
        for row in read_json_rows(wayback_file):
            report.scanned_rows += 1
            unique_code = canonical_unique_code(row.get("Unique Code"), row.get("url"))
            if not unique_code:
                report.skipped_invalid_unique_code += 1
                continue
            all_unique_codes.add(unique_code)
            register_observed_date(unique_code, row)
            update_best_row(
                wayback_rows_by_code,
                wayback_scores_by_code,
                unique_code,
                row,
                score_wayback_row(row, source_priority),
            )

    for archive_file in existing_stage_files(mahgetgr_root, "archive"):
        for row in read_json_rows(archive_file):
            report.scanned_rows += 1
            unique_code = canonical_unique_code(row.get("Unique Code"), row.get("Download") or row.get("url"))
            if not unique_code:
                report.skipped_invalid_unique_code += 1
                continue
            all_unique_codes.add(unique_code)
            register_observed_date(unique_code, row)
            update_best_row(
                archive_rows_by_code,
                archive_scores_by_code,
                unique_code,
                row,
                score_archive_row(row, source_priority),
            )

    for pdf_file in existing_stage_files(mahgetgr_root, "pdfs"):
        for row in read_json_rows(pdf_file):
            report.scanned_rows += 1
            unique_code = canonical_unique_code(row.get("Unique Code"), row.get("url"))
            if not unique_code:
                report.skipped_invalid_unique_code += 1
                continue
            all_unique_codes.add(unique_code)
            register_observed_date(unique_code, row)
            update_best_row(
                pdf_rows_by_code,
                pdf_scores_by_code,
                unique_code,
                row,
                score_pdf_row(row, source_priority),
            )

    candidates: list[CandidateRecord] = []
    for unique_code in sorted(all_unique_codes):
        merged_row = merged_rows_by_code.get(unique_code, {})
        archive_row = archive_rows_by_code.get(unique_code, {})
        wayback_row = wayback_rows_by_code.get(unique_code, {})
        pdf_row = pdf_rows_by_code.get(unique_code, {})

        title = first_non_empty(merged_row.get("Title"), archive_row.get("Title"))
        department_name = first_non_empty(merged_row.get("Department Name"), archive_row.get("Department Name"))
        department_code = department_code_from_name(department_name)
        gr_date = parse_gr_date(first_non_empty(merged_row.get("G.R. Date"), archive_row.get("G.R. Date"))) or ""
        source_url = first_non_empty(
            merged_row.get("Download"),
            merged_row.get("url"),
            archive_row.get("Download"),
            archive_row.get("url"),
            wayback_row.get("url"),
            pdf_row.get("url"),
        )

        candidates.append(
            CandidateRecord(
                unique_code=unique_code,
                title=title,
                department_name=department_name,
                department_code=department_code,
                gr_date=gr_date,
                source_url=source_url,
                crawl_date=first_observed_date.get(unique_code),
            )
        )

    report.scanned_candidates = len(candidates)
    return candidates


def run_append_ledger(config: AppendLedgerConfig) -> AppendLedgerReport:
    if not config.mahgetgr_root.exists():
        raise FileNotFoundError(f"mahgetGR root not found: {config.mahgetgr_root}")
    if not config.mahgetgr_root.is_dir():
        raise NotADirectoryError(f"mahgetGR root is not a directory: {config.mahgetgr_root}")

    report = AppendLedgerReport()
    candidates = _build_candidates(config.mahgetgr_root, report)
    store = LedgerStore(config.ledger_dir)

    for candidate in candidates:
        if store.exists(candidate.unique_code):
            report.skipped_existing_unique_code += 1
            continue

        report.appended += 1
        if config.dry_run:
            continue

        record = {
            "unique_code": candidate.unique_code,
            "title": candidate.title,
            "department_name": candidate.department_name,
            "department_code": candidate.department_code,
            "gr_date": candidate.gr_date,
            "source_url": candidate.source_url,
        }
        store.upsert(
            record,
            run_type="daily",
            crawl_date=candidate.crawl_date,
        )

    return report


def _print_report(report: AppendLedgerReport, *, dry_run: bool) -> None:
    print("append-ledger:")
    print(f"  dry_run: {dry_run}")
    print(f"  scanned_rows: {report.scanned_rows}")
    print(f"  scanned_candidates: {report.scanned_candidates}")
    print(f"  appended: {report.appended}")
    print(f"  skipped_existing_unique_code: {report.skipped_existing_unique_code}")
    print(f"  skipped_invalid_unique_code: {report.skipped_invalid_unique_code}")


def run_from_args(args: argparse.Namespace) -> int:
    config = AppendLedgerConfig(
        mahgetgr_root=Path(args.mahgetgr_root).resolve(),
        ledger_dir=Path(args.ledger_dir).resolve(),
        dry_run=args.dry_run,
    )
    report = run_append_ledger(config)
    _print_report(report, dry_run=config.dry_run)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Append new GR records into existing ledger.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
