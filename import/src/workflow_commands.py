#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import archive_job
import gr_site_job
import wayback_job
from archive_job import ArchiveJobConfig, run_archive_job
from gr_site_job import gather_source_files, normalize_rows, parse_gr_date, run_daily, run_monthly, run_weekly
from info_store import InfoStore as LedgerStore
from wayback_job import WaybackJobConfig, run_wayback_job


def _add_common_workflow_args(parser: argparse.ArgumentParser, include_sources: bool = True) -> argparse.ArgumentParser:
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--max-records", type=int, default=0, help="Optional cap for each stage")
    parser.add_argument("--dry-run", action="store_true", help="Plan actions without writing")
    parser.add_argument("--service-failure-limit", type=int, default=10, help="Stop stage after N service failures")
    parser.add_argument("--show-sample", type=int, default=10, help="Print up to N sample codes")
    parser.add_argument("--crawl-date", default="", help="Crawl date override YYYY-MM-DD")
    parser.add_argument("--skip-download", action="store_true", help="Skip download stage")
    parser.add_argument("--skip-wayback", action="store_true", help="Skip wayback stage")
    parser.add_argument("--skip-archive", action="store_true", help="Skip archive stage")
    parser.add_argument("--lfs-root", default="LFS/pdfs", help="LFS PDF root")
    parser.add_argument("--download-timeout-sec", type=int, default=30, help="Download timeout seconds")
    parser.add_argument("--wayback-timeout-sec", type=int, default=30, help="Wayback HTTP timeout seconds")
    parser.add_argument("--wayback-poll-interval-sec", type=float, default=1.5, help="Wayback poll interval seconds")
    parser.add_argument("--wayback-poll-timeout-sec", type=int, default=90, help="Wayback poll timeout seconds")
    parser.add_argument("--ia-access-key", default="", help="IA_ACCESS_KEY override (used by Wayback and Archive)")
    parser.add_argument("--ia-secret-key", default="", help="IA_SECRET_KEY override (used by Wayback and Archive)")
    if include_sources:
        parser.add_argument(
            "--source-dir",
            default="import/websites/gr.maharashtra.gov.in",
            help="Source crawl directory for daily/monthly",
        )
        parser.add_argument(
            "--source-json",
            action="append",
            default=[],
            help="Additional GR rows JSON files",
        )
    return parser


def configure_daily_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run daily workflow: gr-site discovery+download, wayback, archive."
    return _add_common_workflow_args(parser, include_sources=True)


def configure_weekly_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run weekly workflow: retry download, wayback, archive (no crawl)."
    return _add_common_workflow_args(parser, include_sources=False)


def configure_monthly_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Run monthly workflow: full crawl reconciliation only."
    parser = _add_common_workflow_args(parser, include_sources=True)
    parser.set_defaults(skip_wayback=True, skip_archive=True)
    return parser


def _resolve_crawl_date(value: str) -> str:
    if not value:
        return datetime.now(timezone.utc).date().isoformat()
    parsed = parse_gr_date(value)
    if not parsed:
        raise ValueError(f"Invalid crawl date: {value}")
    return parsed


def _run_wayback_stage(args: argparse.Namespace, code_filter: set[str] | None = None) -> wayback_job.WaybackJobReport | None:
    if args.skip_wayback:
        print("Wayback stage: skipped")
        return None
    config = WaybackJobConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        timeout_sec=max(1, args.wayback_timeout_sec),
        poll_interval_sec=max(0.1, args.wayback_poll_interval_sec),
        poll_timeout_sec=max(1, args.wayback_poll_timeout_sec),
        service_failure_limit=max(1, args.service_failure_limit),
        code_filter=code_filter or set(),
        allowed_states=set(wayback_job.DEFAULT_ALLOWED_STATES),
        access_key=args.ia_access_key or os.environ.get("IA_ACCESS_KEY", ""),
        secret_key=args.ia_secret_key or os.environ.get("IA_SECRET_KEY", ""),
    )
    report = run_wayback_job(config)
    print("Wayback stage summary:")
    print(f"  selected={report.selected} processed={report.processed} success={report.success} failed={report.failed} skipped={report.skipped} stopped_early={report.stopped_early}")
    return report


def _run_archive_stage(args: argparse.Namespace, code_filter: set[str] | None = None) -> archive_job.ArchiveJobReport | None:
    if args.skip_archive:
        print("Archive stage: skipped")
        return None
    config = ArchiveJobConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        service_failure_limit=max(1, args.service_failure_limit),
        code_filter=code_filter or set(),
        allowed_states=set(archive_job.DEFAULT_ALLOWED_STATES),
        ia_access_key=args.ia_access_key or os.environ.get("IA_ACCESS_KEY", ""),
        ia_secret_key=args.ia_secret_key or os.environ.get("IA_SECRET_KEY", ""),
        metadata_only_fallback=True,
    )
    report = run_archive_job(config)
    print("Archive stage summary:")
    print(
        "  "
        f"selected={report.selected} processed={report.processed} success={report.success} "
        f"failed={report.failed} skipped={report.skipped} metadata_fallback={report.metadata_fallback} "
        f"stopped_early={report.stopped_early}"
    )
    return report


def run_daily_workflow(args: argparse.Namespace) -> int:
    crawl_date = _resolve_crawl_date(args.crawl_date)
    source_dir = Path(args.source_dir).resolve() if args.source_dir else None
    source_json_files = [Path(path).resolve() for path in args.source_json]
    source_files = gather_source_files(source_dir=source_dir, source_json_files=source_json_files)
    if not source_files:
        print("Daily workflow: no source crawl files found.")
        return 2
    records = normalize_rows(source_files, crawl_date)
    if not records:
        print("Daily workflow: no usable records after normalization.")
        return 2

    store = LedgerStore(Path(args.ledger_dir).resolve())
    gr_report = run_daily(
        store=store,
        records=records,
        crawl_date=crawl_date,
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        show_sample=max(0, args.show_sample),
        skip_download=args.skip_download,
        lfs_root=Path(args.lfs_root),
        download_timeout_sec=args.download_timeout_sec,
        service_failure_limit=args.service_failure_limit,
    )
    discovered_codes = set(gr_report.discovered_codes or [])
    if not discovered_codes:
        print("Daily workflow: no newly discovered records.")
        return 0

    _run_wayback_stage(args, discovered_codes)
    _run_archive_stage(args, discovered_codes)
    return 0


def run_weekly_workflow(args: argparse.Namespace) -> int:
    store = LedgerStore(Path(args.ledger_dir).resolve())
    run_weekly(
        store=store,
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        skip_download=args.skip_download,
        lfs_root=Path(args.lfs_root),
        download_timeout_sec=args.download_timeout_sec,
        service_failure_limit=args.service_failure_limit,
    )
    _run_wayback_stage(args, None)
    _run_archive_stage(args, None)
    return 0


def run_monthly_workflow(args: argparse.Namespace) -> int:
    crawl_date = _resolve_crawl_date(args.crawl_date)
    source_dir = Path(args.source_dir).resolve() if args.source_dir else None
    source_json_files = [Path(path).resolve() for path in args.source_json]
    source_files = gather_source_files(source_dir=source_dir, source_json_files=source_json_files)
    if not source_files:
        print("Monthly workflow: no source crawl files found.")
        return 2
    records = normalize_rows(source_files, crawl_date)
    if not records:
        print("Monthly workflow: no usable records after normalization.")
        return 2

    store = LedgerStore(Path(args.ledger_dir).resolve())
    run_monthly(
        store=store,
        records=records,
        crawl_date=crawl_date,
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        show_sample=max(0, args.show_sample),
    )
    print("Monthly workflow complete (wayback/archive intentionally skipped).")
    return 0
