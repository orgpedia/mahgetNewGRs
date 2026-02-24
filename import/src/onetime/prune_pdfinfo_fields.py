#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from local_env import load_local_env
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from local_env import load_local_env


DROP_FONT_FIELDS = {"basefont", "encoding", "ext", "font_num", "referencer", "resource_name"}


@dataclass
class PruneReport:
    files_scanned: int = 0
    files_updated: int = 0
    rows_scanned: int = 0
    rows_updated: int = 0
    fonts_updated: int = 0
    total_font_count_renamed: int = 0
    updated_at_removed: int = 0


def _partition_sort_key(file_path: Path) -> tuple[int, int, str]:
    stem = file_path.stem
    if stem.isdigit():
        return (0, -int(stem), stem)
    if stem == "unknown":
        return (2, 0, stem)
    return (1, 0, stem)


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


def _normalized_font_word_count(font_obj: dict[str, Any]) -> int | None:
    value = font_obj.get("word_count")
    if value is None and "words" in font_obj:
        value = font_obj.get("words")
    parsed = _int_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _scrub_font_obj(font_obj: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    changed = False
    out_font: dict[str, Any] = {}
    for key, value in font_obj.items():
        if key in DROP_FONT_FIELDS or key in {"words", "word_count"}:
            changed = True
            continue
        out_font[key] = value

    word_count = _normalized_font_word_count(font_obj)
    if word_count is None:
        # Keep only fonts that have a positive word count.
        return None, True

    out_font["word_count"] = word_count
    if font_obj.get("word_count") != word_count or "words" in font_obj:
        changed = True

    return out_font, changed


def _scrub_row(row: dict[str, Any], report: PruneReport) -> bool:
    changed = False

    removed_updated = False
    if "updated_at_utc" in row:
        row.pop("updated_at_utc", None)
        removed_updated = True
    if "updated_at" in row:
        row.pop("updated_at", None)
        removed_updated = True
    if removed_updated:
        report.updated_at_removed += 1
        changed = True

    if "font_count" in row:
        if "total_font_count" not in row:
            row["total_font_count"] = row.get("font_count")
        row.pop("font_count", None)
        report.total_font_count_renamed += 1
        changed = True

    fonts = row.get("fonts")
    if not isinstance(fonts, dict):
        return changed

    fonts_changed = False
    next_fonts: dict[str, Any] = {}
    for font_key, font_value in fonts.items():
        if not isinstance(font_value, dict):
            fonts_changed = True
            report.fonts_updated += 1
            continue
        scrubbed_font, font_changed = _scrub_font_obj(font_value)
        if scrubbed_font is not None:
            next_fonts[str(font_key)] = scrubbed_font
        if font_changed:
            fonts_changed = True
            report.fonts_updated += 1

    if fonts_changed:
        row["fonts"] = next_fonts
        changed = True

    return changed


def _process_file(file_path: Path, *, dry_run: bool, verbose: bool, report: PruneReport) -> None:
    lines_out: list[str] = []
    file_changed = False

    with file_path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {file_path}:{line_no}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"Expected JSON object in {file_path}:{line_no}")

            report.rows_scanned += 1
            row_changed = _scrub_row(row, report)
            if row_changed:
                report.rows_updated += 1
                file_changed = True
            lines_out.append(json.dumps(row, ensure_ascii=False, sort_keys=True))

    report.files_scanned += 1
    if not file_changed:
        return

    report.files_updated += 1
    if verbose:
        print(f"[updated] {file_path}")
    if dry_run:
        return

    temp_path = file_path.with_name(f".{file_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        for line in lines_out:
            handle.write(line)
            handle.write("\n")
    temp_path.replace(file_path)


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "One-time scrub for import/pdfinfos/*.jsonl.\n"
        "Removes pdf row updated_at_utc, renames font_count->total_font_count, "
        "and prunes verbose font metadata fields."
    )
    parser.add_argument(
        "--pdfinfos-dir",
        default="import/pdfinfos",
        help="Directory containing pdfinfos JSONL partitions",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing files")
    parser.add_argument("--verbose", action="store_true", help="Print each updated file path")
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    pdfinfos_dir = Path(args.pdfinfos_dir).resolve()
    if not pdfinfos_dir.exists() or not pdfinfos_dir.is_dir():
        print(f"pdfinfos directory not found: {pdfinfos_dir}")
        return 2

    files = sorted((path for path in pdfinfos_dir.glob("*.jsonl") if path.is_file()), key=_partition_sort_key)
    report = PruneReport()
    for file_path in files:
        _process_file(file_path, dry_run=args.dry_run, verbose=args.verbose, report=report)

    print("Prune PDF info fields summary:")
    print(f"  dry_run: {args.dry_run}")
    print(f"  files_scanned: {report.files_scanned}")
    print(f"  files_updated: {report.files_updated}")
    print(f"  rows_scanned: {report.rows_scanned}")
    print(f"  rows_updated: {report.rows_updated}")
    print(f"  fonts_updated: {report.fonts_updated}")
    print(f"  total_font_count_renamed: {report.total_font_count_renamed}")
    print(f"  updated_at_removed: {report.updated_at_removed}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-time scrub of pdfinfos JSONL fields.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
