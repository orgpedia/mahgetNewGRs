#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# Update this allowlist as needed when more fonts should be treated as English.
DEFAULT_ENGLISH_FONTS = {
    "Arial",
    "Arial,Bold",
    "Arial Italic",
    "Arial Unicode MS",
    "Calibri",
    "Calibri,Bold",
    "Calibri,Italic",
    "Cambria",
    "Cambria,Bold",
    "Courier New",
    "Helvetica",
    "Helvetica,Bold",
    "Symbol",
    "Tahoma",
    "Times New Roman",
    "Times New Roman,Bold",
    "Times New Roman,Italic",
    "Verdana",
}


@dataclass(frozen=True)
class ExportRow:
    record_key: str
    hf_path: str
    font_entries: list[tuple[str, int]]


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "Merge uploadinfos and pdfinfos on record_key and export non-English font "
        "Devanagari counts for rows that have a valid hf.path."
    )
    parser.add_argument("--year", default="2023", help="Partition year to read from import/*infos/<year>.jsonl")
    parser.add_argument("--uploadinfos-dir", default="import/uploadinfos", help="Directory containing uploadinfo JSONL files")
    parser.add_argument("--pdfinfos-dir", default="import/pdfinfos", help="Directory containing pdfinfo JSONL files")
    parser.add_argument(
        "--output",
        default="export/non_english_fonts_2023.csv",
        help="CSV output path",
    )
    parser.add_argument(
        "--english-font",
        action="append",
        dest="english_fonts",
        default=None,
        help="Add a font name to the English-font allowlist. Can be passed multiple times.",
    )
    return parser


def _read_jsonl_objects(file_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
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
            rows.append(row)
    return rows


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_font_name(font_name: Any) -> str:
    text = _normalize_text(font_name)
    if not text:
        return ""
    if "+" in text:
        _, _, text = text.partition("+")
    return text.strip()


def _has_valid_hf_path(upload_row: dict[str, Any]) -> str:
    hf = upload_row.get("hf")
    if not isinstance(hf, dict):
        return ""
    path_value = hf.get("path")
    if not isinstance(path_value, str):
        return ""
    return path_value.strip()


def _build_upload_index(upload_rows: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in upload_rows:
        record_key = _normalize_text(row.get("record_key"))
        hf_path = _has_valid_hf_path(row)
        if not record_key or not hf_path:
            continue
        index[record_key] = hf_path
    return index


def _devanagari_count(font_obj: Any) -> int:
    if not isinstance(font_obj, dict):
        return 0
    script_counts = font_obj.get("script_word_counts")
    if not isinstance(script_counts, dict):
        return 0
    count = script_counts.get("Devanagari", 0)
    return count if isinstance(count, int) and count > 0 else 0


def _collect_non_english_fonts(
    pdf_row: dict[str, Any], english_fonts: set[str]
) -> list[tuple[str, int]]:
    fonts = pdf_row.get("fonts")
    if not isinstance(fonts, dict):
        return []

    font_totals: dict[str, int] = {}
    for font_obj in fonts.values():
        font_name = _normalize_font_name(font_obj.get("name") if isinstance(font_obj, dict) else None)
        devanagari_words = _devanagari_count(font_obj)
        if not font_name or devanagari_words <= 0:
            continue
        if font_name in english_fonts:
            continue
        font_totals[font_name] = font_totals.get(font_name, 0) + devanagari_words

    return [(font_name, font_totals[font_name]) for font_name in sorted(font_totals)]


def export_rows(
    upload_rows: list[dict[str, Any]], pdf_rows: list[dict[str, Any]], english_fonts: set[str]
) -> list[ExportRow]:
    upload_index = _build_upload_index(upload_rows)
    exported: list[ExportRow] = []

    for pdf_row in pdf_rows:
        record_key = _normalize_text(pdf_row.get("record_key"))
        if not record_key:
            continue
        hf_path = upload_index.get(record_key, "")
        if not hf_path:
            continue
        font_entries = _collect_non_english_fonts(pdf_row, english_fonts)
        exported.append(ExportRow(record_key=record_key, hf_path=hf_path, font_entries=font_entries))

    return exported


def write_csv(rows: list[ExportRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["record_key", "hf.path", "fonts"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "record_key": row.record_key,
                    "hf.path": row.hf_path,
                    "fonts": repr(row.font_entries),
                }
            )


def run_from_args(args: argparse.Namespace) -> int:
    year = _normalize_text(args.year)
    upload_path = Path(args.uploadinfos_dir).resolve() / f"{year}.jsonl"
    pdf_path = Path(args.pdfinfos_dir).resolve() / f"{year}.jsonl"
    output_path = Path(args.output).resolve()

    if not upload_path.is_file():
        raise FileNotFoundError(f"uploadinfo file not found: {upload_path}")
    if not pdf_path.is_file():
        raise FileNotFoundError(f"pdfinfo file not found: {pdf_path}")

    english_fonts = set(DEFAULT_ENGLISH_FONTS)
    if args.english_fonts:
        english_fonts.update(_normalize_text(font) for font in args.english_fonts if _normalize_text(font))

    upload_rows = _read_jsonl_objects(upload_path)
    pdf_rows = _read_jsonl_objects(pdf_path)
    rows = export_rows(upload_rows, pdf_rows, english_fonts)
    write_csv(rows, output_path)

    print(f"year={year}")
    print(f"upload_rows={len(upload_rows)}")
    print(f"pdf_rows={len(pdf_rows)}")
    print(f"exported_rows={len(rows)}")
    print(f"output={output_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export non-English font Devanagari counts to CSV.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> int:
    return run_from_args(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
