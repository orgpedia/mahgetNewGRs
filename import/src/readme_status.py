#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from info_store import InfoStore as LedgerStore
from ledger_engine import (
    STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
    STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT,
    STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    STATE_DOWNLOAD_FAILED,
    STATE_DOWNLOAD_SUCCESS,
    STATE_FETCHED,
    STATE_WAYBACK_UPLOADED,
    STATE_WAYBACK_UPLOAD_FAILED,
)
from local_env import load_local_env


START_MARKER = "<!-- STATUS_TABLE_START -->"
END_MARKER = "<!-- STATUS_TABLE_END -->"
STATUS_SECTION_TITLE = "## Repository Status"

STATE_ORDER = [
    STATE_FETCHED,
    STATE_DOWNLOAD_SUCCESS,
    STATE_DOWNLOAD_FAILED,
    STATE_WAYBACK_UPLOADED,
    STATE_WAYBACK_UPLOAD_FAILED,
    STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
    STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT,
]


@dataclass(frozen=True)
class RepositoryStatus:
    generated_at_utc: str
    total_records: int
    partitions: int
    pdf_files: int
    state_counts: dict[str, int]
    download_success: int
    download_failed: int
    download_pending: int
    download_retryable: int
    wayback_success: int
    wayback_failed: int
    wayback_pending: int
    wayback_retryable: int
    archive_success: int
    archive_failed: int
    archive_pending: int
    archive_retryable: int


def _stage_status(record: dict[str, Any], stage_name: str) -> str:
    stage_obj = record.get(stage_name, {})
    if not isinstance(stage_obj, dict):
        return "not_attempted"
    status = str(stage_obj.get("status") or "").strip()
    return status or "not_attempted"


def _attempt_count(record: dict[str, Any], stage_name: str) -> int:
    attempts = record.get("attempt_counts", {})
    if not isinstance(attempts, dict):
        return 0
    value = attempts.get(stage_name, 0)
    if isinstance(value, int):
        return value
    return 0


def _count_pdf_files(lfs_pdf_root: Path) -> int:
    if not lfs_pdf_root.exists():
        return 0
    return sum(1 for path in lfs_pdf_root.rglob("*.pdf") if path.is_file())


def compute_repository_status(ledger_dir: Path, lfs_pdf_root: Path) -> RepositoryStatus:
    store = LedgerStore(ledger_dir)
    records = store.iter_records()
    total_records = len(records)
    state_counter: Counter[str] = Counter()

    download_success = 0
    download_failed = 0
    download_retryable = 0

    wayback_success = 0
    wayback_failed = 0
    wayback_pending = 0
    wayback_retryable = 0

    archive_success = 0
    archive_failed = 0
    archive_pending = 0
    archive_retryable = 0

    for record in records:
        state = str(record.get("state") or "").strip() or "UNKNOWN"
        state_counter[state] += 1

        download_status = _stage_status(record, "download")
        wayback_status = _stage_status(record, "wayback")
        archive_status = _stage_status(record, "archive")

        if download_status == "success":
            download_success += 1
        elif download_status == "failed":
            download_failed += 1
            if _attempt_count(record, "download") < 2:
                download_retryable += 1

        if wayback_status == "success":
            wayback_success += 1
        elif wayback_status == "failed":
            wayback_failed += 1
            if _attempt_count(record, "wayback") < 2:
                wayback_retryable += 1
        elif wayback_status == "not_attempted" and download_status == "success":
            wayback_pending += 1

        if archive_status == "success":
            archive_success += 1
        elif archive_status == "failed":
            archive_failed += 1
            if _attempt_count(record, "archive") < 2:
                archive_retryable += 1
        elif archive_status == "not_attempted" and (
            wayback_status in {"success", "failed"} or download_status == "failed"
        ):
            archive_pending += 1

    download_pending = max(0, total_records - download_success - download_failed)

    return RepositoryStatus(
        generated_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        total_records=total_records,
        partitions=len(store.list_partitions()),
        pdf_files=_count_pdf_files(lfs_pdf_root),
        state_counts=dict(state_counter),
        download_success=download_success,
        download_failed=download_failed,
        download_pending=download_pending,
        download_retryable=download_retryable,
        wayback_success=wayback_success,
        wayback_failed=wayback_failed,
        wayback_pending=wayback_pending,
        wayback_retryable=wayback_retryable,
        archive_success=archive_success,
        archive_failed=archive_failed,
        archive_pending=archive_pending,
        archive_retryable=archive_retryable,
    )


def render_status_markdown(status: RepositoryStatus) -> str:
    lines: list[str] = []
    lines.append(f"_Last updated (UTC): {status.generated_at_utc}_")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("| --- | ---: |")
    lines.append(f"| Total records | {status.total_records} |")
    lines.append(f"| Ledger partitions | {status.partitions} |")
    lines.append(f"| Downloaded PDF files (`LFS/pdfs`) | {status.pdf_files} |")
    lines.append("")
    lines.append("### State Distribution")
    lines.append("")
    lines.append("| State | Count |")
    lines.append("| --- | ---: |")
    emitted = set()
    for state_name in STATE_ORDER:
        lines.append(f"| `{state_name}` | {status.state_counts.get(state_name, 0)} |")
        emitted.add(state_name)
    for state_name in sorted(name for name in status.state_counts.keys() if name not in emitted):
        lines.append(f"| `{state_name}` | {status.state_counts.get(state_name, 0)} |")
    lines.append("")
    lines.append("### Stage Progress")
    lines.append("")
    lines.append("| Stage | Success | Failed | Pending | Retryable |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    lines.append(
        f"| Download | {status.download_success} | {status.download_failed} | {status.download_pending} | {status.download_retryable} |"
    )
    lines.append(
        f"| Wayback | {status.wayback_success} | {status.wayback_failed} | {status.wayback_pending} | {status.wayback_retryable} |"
    )
    lines.append(
        f"| Archive | {status.archive_success} | {status.archive_failed} | {status.archive_pending} | {status.archive_retryable} |"
    )
    return "\n".join(lines)


def update_readme_status(*, readme_path: Path, content: str) -> bool:
    if not readme_path.exists():
        raise FileNotFoundError(f"README not found: {readme_path}")

    original = readme_path.read_text(encoding="utf-8")
    block = f"{START_MARKER}\n{content}\n{END_MARKER}"

    if START_MARKER in original and END_MARKER in original:
        start = original.index(START_MARKER)
        end = original.index(END_MARKER, start) + len(END_MARKER)
        updated = original[:start] + block + original[end:]
    else:
        suffix = ""
        if not original.endswith("\n"):
            suffix += "\n"
        suffix += f"\n{STATUS_SECTION_TITLE}\n\n{block}\n"
        updated = original + suffix

    if updated == original:
        return False
    readme_path.write_text(updated, encoding="utf-8")
    return True


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Update README with auto-generated repository status table."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--readme-path", default="README.md", help="README path to update")
    parser.add_argument("--lfs-pdf-root", default="LFS/pdfs", help="LFS PDF root for file count")
    parser.add_argument("--print-only", action="store_true", help="Print status markdown without writing README")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    ledger_dir = Path(args.ledger_dir).resolve()
    readme_path = Path(args.readme_path).resolve()
    lfs_pdf_root = Path(args.lfs_pdf_root).resolve()

    status = compute_repository_status(ledger_dir=ledger_dir, lfs_pdf_root=lfs_pdf_root)
    markdown = render_status_markdown(status)

    if args.print_only:
        print(markdown)
        return 0

    changed = update_readme_status(readme_path=readme_path, content=markdown)
    print("update-readme-status:")
    print(f"  readme: {readme_path}")
    print(f"  changed: {changed}")
    print(f"  total_records: {status.total_records}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update README status table.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
