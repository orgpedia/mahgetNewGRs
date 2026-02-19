#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ledger_engine import to_ledger_relative_path, utc_now_text
from local_env import load_local_env


LONG_DIGITS_RE = re.compile(r"\d{16,22}")
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class BackfillLfsPathConfig:
    ledger_dir: Path
    lfs_pdf_roots: tuple[Path, ...]
    dry_run: bool


@dataclass
class BackfillLfsPathReport:
    scanned_records: int = 0
    updated_records: int = 0
    unchanged_records: int = 0
    resolved_non_null: int = 0
    resolved_null: int = 0
    set_non_null: int = 0
    cleared_to_null: int = 0
    partitions_changed: int = 0


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Backfill top-level lfs_path in ledger records by checking local LFS PDF files."
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger directory containing *.jsonl files")
    parser.add_argument(
        "--lfs-pdf-root",
        action="append",
        default=[],
        help="LFS PDF root to scan (repeatable). Defaults: LFS/mahGRs/pdfs, LFS/pdfs",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and report changes without writing files")
    return parser


def _safe_filename(value: str) -> str:
    text = SAFE_FILENAME_RE.sub("_", value).strip("_")
    return text or "unknown"


def _extract_unique_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = LONG_DIGITS_RE.search(text)
    if match:
        return match.group(0)
    digits = "".join(ch for ch in text if ch.isdigit())
    if 16 <= len(digits) <= 22:
        return digits
    if len(digits) > 22:
        return digits[:22]
    return ""


def _year_month_from_gr_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:7]
    return "unknown"


def _resolve_existing_file(path_value: Any) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    probe = path if path.is_absolute() else (Path.cwd() / path)
    if probe.exists() and probe.is_file():
        return probe
    return None


def _candidate_paths_from_record(record: dict[str, Any], lfs_pdf_roots: tuple[Path, ...]) -> list[Path]:
    candidates: list[Path] = []

    existing_lfs = _resolve_existing_file(record.get("lfs_path"))
    if existing_lfs is not None:
        candidates.append(existing_lfs)

    download = record.get("download", {})
    if isinstance(download, dict):
        existing_download = _resolve_existing_file(download.get("path"))
        if existing_download is not None:
            candidates.append(existing_download)

    unique_code = _extract_unique_code(record.get("unique_code"))
    if unique_code:
        department_code = str(record.get("department_code") or "unknown").strip() or "unknown"
        year_month = _year_month_from_gr_date(record.get("gr_date"))
        filename = f"{_safe_filename(unique_code)}.pdf"
        for root in lfs_pdf_roots:
            candidates.append(root / department_code / year_month / filename)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _build_unique_code_index(lfs_pdf_roots: tuple[Path, ...]) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for root in lfs_pdf_roots:
        if not root.exists() or not root.is_dir():
            continue
        for pdf_file in root.rglob("*.pdf"):
            unique_code = _extract_unique_code(pdf_file.stem)
            if not unique_code or unique_code in index:
                continue
            index[unique_code] = pdf_file
    return index


def _resolve_lfs_path(
    record: dict[str, Any],
    lfs_pdf_roots: tuple[Path, ...],
    indexed_pdf_paths: dict[str, Path],
) -> str | None:
    for candidate in _candidate_paths_from_record(record, lfs_pdf_roots):
        if candidate.exists() and candidate.is_file():
            return to_ledger_relative_path(candidate)

    unique_code = _extract_unique_code(record.get("unique_code"))
    if unique_code:
        indexed = indexed_pdf_paths.get(unique_code)
        if indexed is not None and indexed.exists() and indexed.is_file():
            return to_ledger_relative_path(indexed)
    return None


def _normalize_current_lfs_path(record: dict[str, Any]) -> str | None:
    if "lfs_path" not in record:
        return None
    current = record.get("lfs_path")
    if current is None:
        return None
    text = str(current).strip()
    if not text:
        return None
    return to_ledger_relative_path(text)


def _atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    temp_path.replace(path)


def run_backfill_lfs_path(config: BackfillLfsPathConfig) -> BackfillLfsPathReport:
    if not config.ledger_dir.exists() or not config.ledger_dir.is_dir():
        raise FileNotFoundError(f"Ledger directory not found: {config.ledger_dir}")

    report = BackfillLfsPathReport()
    indexed_pdf_paths = _build_unique_code_index(config.lfs_pdf_roots)
    now_text = utc_now_text()

    for ledger_file in sorted(config.ledger_dir.glob("*.jsonl")):
        rows: list[dict[str, Any]] = []
        file_changed = False

        with ledger_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                obj = json.loads(text)
                if not isinstance(obj, dict):
                    continue

                report.scanned_records += 1
                resolved_lfs_path = _resolve_lfs_path(
                    record=obj,
                    lfs_pdf_roots=config.lfs_pdf_roots,
                    indexed_pdf_paths=indexed_pdf_paths,
                )
                if resolved_lfs_path is None:
                    report.resolved_null += 1
                else:
                    report.resolved_non_null += 1

                current_lfs_path = _normalize_current_lfs_path(obj)
                if ("lfs_path" not in obj) or (current_lfs_path != resolved_lfs_path):
                    if current_lfs_path is None and resolved_lfs_path is not None:
                        report.set_non_null += 1
                    if current_lfs_path is not None and resolved_lfs_path is None:
                        report.cleared_to_null += 1
                    obj["lfs_path"] = resolved_lfs_path
                    obj["updated_at_utc"] = now_text
                    report.updated_records += 1
                    file_changed = True
                else:
                    report.unchanged_records += 1

                rows.append(obj)

        if file_changed:
            report.partitions_changed += 1
            if not config.dry_run:
                _atomic_write_jsonl(ledger_file, rows)

    return report


def _print_report(report: BackfillLfsPathReport, *, roots: tuple[Path, ...], dry_run: bool) -> None:
    print("backfill-lfs-path:")
    print(f"  dry_run: {dry_run}")
    print("  lfs_pdf_roots:")
    for root in roots:
        print(f"    - {root}")
    print(f"  scanned_records: {report.scanned_records}")
    print(f"  updated_records: {report.updated_records}")
    print(f"  unchanged_records: {report.unchanged_records}")
    print(f"  resolved_non_null: {report.resolved_non_null}")
    print(f"  resolved_null: {report.resolved_null}")
    print(f"  set_non_null: {report.set_non_null}")
    print(f"  cleared_to_null: {report.cleared_to_null}")
    print(f"  partitions_changed: {report.partitions_changed}")


def _default_lfs_roots() -> list[Path]:
    return [
        Path("LFS/mahGRs/pdfs").resolve(),
        Path("LFS/pdfs").resolve(),
    ]


def run_from_args(args: argparse.Namespace) -> int:
    lfs_roots = [Path(path).resolve() for path in args.lfs_pdf_root] if args.lfs_pdf_root else _default_lfs_roots()

    config = BackfillLfsPathConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        lfs_pdf_roots=tuple(lfs_roots),
        dry_run=args.dry_run,
    )
    report = run_backfill_lfs_path(config)
    _print_report(report, roots=config.lfs_pdf_roots, dry_run=config.dry_run)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill ledger lfs_path values.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
