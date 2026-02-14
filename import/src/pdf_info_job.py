#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from ledger_engine import to_ledger_relative_path, utc_now_text
from local_env import load_local_env


WORD_RE = re.compile(r"\w+", flags=re.UNICODE)
FONT_PREFIX_RE = re.compile(r"^[A-Z]{6}\+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

SCRIPT_TO_LANGUAGE = {
    "Devanagari": "mr_hi",
    "Latin": "en",
    "Arabic": "ar_ur",
    "Gujarati": "gu",
    "Bengali": "bn",
    "Gurmukhi": "pa",
    "Tamil": "ta",
    "Telugu": "te",
    "Kannada": "kn",
    "Malayalam": "ml",
    "Oriya": "or",
}


class PdfInfoDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class PdfInfoConfig:
    ledger_dir: Path
    max_records: int
    dry_run: bool
    force: bool
    mark_missing: bool
    verbose: bool


@dataclass
class PdfInfoReport:
    scanned_records: int = 0
    processed_records: int = 0
    updated_records: int = 0
    unchanged_records: int = 0
    skipped_already_success: int = 0
    skipped_no_local_pdf: int = 0
    extraction_failed: int = 0
    partitions_changed: int = 0


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "Extract PDF metadata for ledger records and store it in record.pdf_info. "
        "Includes page count, image presence, used-font word counts, language inference, and file size."
    )
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger directory containing *.jsonl files")
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Maximum records to process (0 means no limit)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute/report changes without writing ledger files")
    parser.add_argument("--force", action="store_true", help="Recompute even when pdf_info.status is already success")
    parser.add_argument(
        "--mark-missing",
        action="store_true",
        help="Set pdf_info.status=missing_pdf when no local PDF path resolves",
    )
    parser.add_argument("--verbose", action="store_true", help="Print detailed per-record processing logs")
    return parser


def _load_fitz() -> Any:
    try:
        import pymupdf as fitz  # type: ignore

        return fitz
    except Exception:
        try:
            import fitz  # type: ignore

            return fitz
        except Exception as exc:
            raise PdfInfoDependencyError(
                "PyMuPDF is required. Install dependency 'pymupdf' before running pdf-info."
            ) from exc


def _atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    temp_path.replace(path)


def _resolve_existing_file(path_value: Any) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    probe = path if path.is_absolute() else (Path.cwd() / path)
    if not probe.exists() or not probe.is_file():
        return None
    return probe.resolve()


def _resolve_pdf_path(record: dict[str, Any]) -> Path | None:
    candidates: list[Any] = []
    candidates.append(record.get("lfs_path"))
    download = record.get("download")
    if isinstance(download, dict):
        candidates.append(download.get("path"))
    for candidate in candidates:
        resolved = _resolve_existing_file(candidate)
        if resolved is not None:
            return resolved
    return None


@lru_cache(maxsize=8192)
def _normalize_font_name(font_name: str) -> str:
    text = str(font_name or "").strip()
    if not text:
        return ""
    text = FONT_PREFIX_RE.sub("", text)
    return NON_ALNUM_RE.sub("", text.lower())


def _font_name_candidates(*names: Any) -> set[str]:
    result: set[str] = set()
    for name in names:
        normalized = _normalize_font_name(str(name or ""))
        if normalized:
            result.add(normalized)
    return result


def _safe_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except Exception:
        return default


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except Exception:
        return None


def _parse_font_entry(raw_font: Any) -> dict[str, Any]:
    if not isinstance(raw_font, (tuple, list)):
        return {
            "font_num": -1,
            "ext": "",
            "type": "",
            "basefont": "",
            "name": "",
            "encoding": "",
            "referencer": None,
        }

    return {
        "font_num": _safe_int(raw_font[0] if len(raw_font) > 0 else -1, -1),
        "ext": str(raw_font[1] if len(raw_font) > 1 else ""),
        "type": str(raw_font[2] if len(raw_font) > 2 else ""),
        "basefont": str(raw_font[3] if len(raw_font) > 3 else ""),
        "name": str(raw_font[4] if len(raw_font) > 4 else ""),
        "encoding": str(raw_font[5] if len(raw_font) > 5 else ""),
        "referencer": _safe_optional_int(raw_font[6] if len(raw_font) > 6 else None),
    }


def _iter_spans(text_dict: dict[str, Any]) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    for block in text_dict.get("blocks", []):
        if not isinstance(block, dict):
            continue
        for line in block.get("lines", []):
            if not isinstance(line, dict):
                continue
            for span in line.get("spans", []):
                if isinstance(span, dict):
                    spans.append(span)
    return spans


def _extract_words(text: str) -> list[str]:
    words: list[str] = []
    for token in WORD_RE.findall(text or ""):
        if any(ch.isalpha() for ch in token):
            words.append(token)
    return words


def _script_for_codepoint(codepoint: int) -> str:
    if 0x0900 <= codepoint <= 0x097F or 0xA8E0 <= codepoint <= 0xA8FF:
        return "Devanagari"
    if 0x0980 <= codepoint <= 0x09FF:
        return "Bengali"
    if 0x0A00 <= codepoint <= 0x0A7F:
        return "Gurmukhi"
    if 0x0A80 <= codepoint <= 0x0AFF:
        return "Gujarati"
    if 0x0B00 <= codepoint <= 0x0B7F:
        return "Oriya"
    if 0x0B80 <= codepoint <= 0x0BFF:
        return "Tamil"
    if 0x0C00 <= codepoint <= 0x0C7F:
        return "Telugu"
    if 0x0C80 <= codepoint <= 0x0CFF:
        return "Kannada"
    if 0x0D00 <= codepoint <= 0x0D7F:
        return "Malayalam"
    if 0x0600 <= codepoint <= 0x06FF or 0x0750 <= codepoint <= 0x077F or 0x08A0 <= codepoint <= 0x08FF:
        return "Arabic"
    if (
        0x0041 <= codepoint <= 0x005A
        or 0x0061 <= codepoint <= 0x007A
        or 0x00C0 <= codepoint <= 0x024F
        or 0x1E00 <= codepoint <= 0x1EFF
    ):
        return "Latin"
    return "Other"


@lru_cache(maxsize=65536)
def _script_for_word(word: str) -> str:
    counts: Counter[str] = Counter()
    for ch in word:
        if not ch.isalpha():
            continue
        counts[_script_for_codepoint(ord(ch))] += 1
    if not counts:
        return "Other"
    return max(sorted(counts.keys()), key=lambda key: counts[key])


def _increment_counter(counter_dict: dict[str, int], key: str, value: int = 1) -> None:
    counter_dict[key] = counter_dict.get(key, 0) + value


def _sorted_count_dict(values: dict[str, int]) -> dict[str, int]:
    return {key: values[key] for key in sorted(values, key=lambda name: (-values[name], name))}


def _resolve_font_key(
    font_name: str,
    page_font_lookup: dict[str, set[str]],
    global_font_lookup: dict[str, set[str]],
) -> str | None:
    normalized = _normalize_font_name(font_name)
    if not normalized:
        return None

    page_keys = page_font_lookup.get(normalized, set())
    if len(page_keys) == 1:
        return next(iter(page_keys))
    if len(page_keys) > 1:
        return sorted(page_keys)[0]

    global_keys = global_font_lookup.get(normalized, set())
    if len(global_keys) == 1:
        return next(iter(global_keys))
    if len(global_keys) > 1:
        return sorted(global_keys)[0]
    return None


def _page_has_images(page: Any) -> bool:
    try:
        return bool(page.get_image_info())
    except Exception:
        try:
            return bool(page.get_images(full=True))
        except Exception:
            return False


def _inferred_language(script_word_counts: dict[str, int]) -> str:
    filtered = {key: value for key, value in script_word_counts.items() if key != "Other" and value > 0}
    if not filtered:
        return "unknown"
    top_script = max(sorted(filtered.keys()), key=lambda key: filtered[key])
    return SCRIPT_TO_LANGUAGE.get(top_script, top_script.lower())


def extract_pdf_info(pdf_path: Path, fitz: Any) -> dict[str, Any]:
    file_size = pdf_path.stat().st_size
    page_count = 0
    fonts: dict[str, dict[str, Any]] = {}
    global_font_lookup: dict[str, set[str]] = defaultdict(set)
    script_word_counts: dict[str, int] = {}
    unresolved_word_count = 0
    pages_with_images = 0
    total_words = 0

    with fitz.open(pdf_path) as doc:
        page_count = int(doc.page_count)
        for page_index in range(page_count):
            page = doc.load_page(page_index)
            page_font_lookup: dict[str, set[str]] = defaultdict(set)

            try:
                raw_fonts = doc.get_page_fonts(page_index, full=True)
            except Exception:
                try:
                    raw_fonts = page.get_fonts(full=True)
                except Exception:
                    raw_fonts = []
            for raw_font in raw_fonts:
                font_entry = _parse_font_entry(raw_font)
                font_num = font_entry.get("font_num", -1)
                if font_num < 0:
                    continue
                key = str(font_num)
                if key not in fonts:
                    fonts[key] = {
                        "font_num": font_num,
                        "name": font_entry.get("basefont") or font_entry.get("name") or "",
                        "basefont": font_entry.get("basefont", ""),
                        "resource_name": font_entry.get("name", ""),
                        "type": font_entry.get("type", ""),
                        "ext": font_entry.get("ext", ""),
                        "encoding": font_entry.get("encoding", ""),
                        "referencer": font_entry.get("referencer"),
                        "words": 0,
                        "script_word_counts": {},
                    }

                candidates = _font_name_candidates(font_entry.get("basefont"), font_entry.get("name"))
                for candidate in candidates:
                    page_font_lookup[candidate].add(key)
                    global_font_lookup[candidate].add(key)

            if _page_has_images(page):
                pages_with_images += 1

            text_dict = page.get_text("dict")
            spans = _iter_spans(text_dict if isinstance(text_dict, dict) else {})
            for span in spans:
                span_text = str(span.get("text") or "")
                words = _extract_words(span_text)
                if not words:
                    continue

                total_words += len(words)
                font_key = _resolve_font_key(
                    font_name=str(span.get("font") or ""),
                    page_font_lookup=page_font_lookup,
                    global_font_lookup=global_font_lookup,
                )
                if font_key is None or font_key not in fonts:
                    unresolved_word_count += len(words)
                    for word in words:
                        script = _script_for_word(word)
                        _increment_counter(script_word_counts, script, 1)
                    continue

                font_info = fonts[font_key]
                font_info["words"] = int(font_info.get("words", 0)) + len(words)
                per_font_scripts = font_info.get("script_word_counts", {})
                if not isinstance(per_font_scripts, dict):
                    per_font_scripts = {}
                    font_info["script_word_counts"] = per_font_scripts

                for word in words:
                    script = _script_for_word(word)
                    _increment_counter(per_font_scripts, script, 1)
                    _increment_counter(script_word_counts, script, 1)

    sorted_fonts = {}
    for font_key in sorted(fonts.keys(), key=lambda value: int(value)):
        font_info = fonts[font_key]
        font_info["script_word_counts"] = _sorted_count_dict(dict(font_info.get("script_word_counts", {})))
        sorted_fonts[font_key] = font_info

    sorted_scripts = _sorted_count_dict(script_word_counts)
    return {
        "status": "success",
        "error": "",
        "path": to_ledger_relative_path(pdf_path),
        "file_size": file_size,
        "page_count": page_count,
        "pages_with_images": pages_with_images,
        "has_any_page_image": pages_with_images > 0,
        "font_count": len(sorted_fonts),
        "fonts": sorted_fonts,
        "unresolved_word_count": unresolved_word_count,
        "language": {
            "inferred": _inferred_language(sorted_scripts),
            "script_word_counts": sorted_scripts,
            "total_words": total_words,
        },
    }


def _build_missing_info() -> dict[str, Any]:
    return {
        "status": "missing_pdf",
        "error": "local_pdf_not_found",
        "path": "",
        "file_size": None,
        "page_count": None,
        "pages_with_images": 0,
        "has_any_page_image": False,
        "font_count": 0,
        "fonts": {},
        "unresolved_word_count": 0,
        "language": {
            "inferred": "unknown",
            "script_word_counts": {},
            "total_words": 0,
        },
    }


def _build_failed_info(path: Path | None, error: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "error": error.strip() or "pdf_info_failed",
        "path": to_ledger_relative_path(path) if path is not None else "",
        "file_size": None,
        "page_count": None,
        "pages_with_images": 0,
        "has_any_page_image": False,
        "font_count": 0,
        "fonts": {},
        "unresolved_word_count": 0,
        "language": {
            "inferred": "unknown",
            "script_word_counts": {},
            "total_words": 0,
        },
    }


def _is_success_pdf_info(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return str(value.get("status") or "").strip() == "success"


def _verbose_log(config: PdfInfoConfig, message: str) -> None:
    if config.verbose:
        print(message)


def _record_label(partition: str, unique_code: str) -> str:
    if unique_code:
        return f"{partition}:{unique_code}"
    return f"{partition}:<missing-unique_code>"


def run_pdf_info(config: PdfInfoConfig) -> PdfInfoReport:
    if not config.ledger_dir.exists() or not config.ledger_dir.is_dir():
        raise FileNotFoundError(f"Ledger directory not found: {config.ledger_dir}")

    fitz = _load_fitz()
    report = PdfInfoReport()
    now_text = utc_now_text()
    max_records = max(0, config.max_records)

    for ledger_file in sorted(config.ledger_dir.glob("*.jsonl")):
        partition = ledger_file.stem
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

                unique_code = str(obj.get("unique_code") or "").strip()
                label = _record_label(partition, unique_code)
                report.scanned_records += 1
                if max_records > 0 and report.processed_records >= max_records:
                    _verbose_log(config, f"[skip limit] {label}")
                    rows.append(obj)
                    continue

                current_pdf_info = obj.get("pdf_info")
                if not config.force and _is_success_pdf_info(current_pdf_info):
                    report.skipped_already_success += 1
                    _verbose_log(config, f"[skip success] {label}")
                    rows.append(obj)
                    continue

                pdf_path = _resolve_pdf_path(obj)
                if pdf_path is None:
                    report.skipped_no_local_pdf += 1
                    if not config.mark_missing:
                        _verbose_log(config, f"[skip no-pdf] {label}")
                        rows.append(obj)
                        continue
                    _verbose_log(config, f"[mark missing] {label}")
                    next_pdf_info = _build_missing_info()
                else:
                    _verbose_log(config, f"[process] {label} path={to_ledger_relative_path(pdf_path)}")
                    try:
                        next_pdf_info = extract_pdf_info(pdf_path=pdf_path, fitz=fitz)
                        language = next_pdf_info.get("language", {})
                        inferred = ""
                        if isinstance(language, dict):
                            inferred = str(language.get("inferred") or "")
                        _verbose_log(
                            config,
                            (
                                f"[extracted] {label} pages={next_pdf_info.get('page_count')} "
                                f"images={next_pdf_info.get('pages_with_images')} "
                                f"fonts={next_pdf_info.get('font_count')} "
                                f"words={language.get('total_words') if isinstance(language, dict) else ''} "
                                f"lang={inferred or 'unknown'}"
                            ),
                        )
                    except Exception as exc:
                        report.extraction_failed += 1
                        _verbose_log(config, f"[failed] {label} error={exc}")
                        next_pdf_info = _build_failed_info(pdf_path, str(exc))

                report.processed_records += 1
                if current_pdf_info == next_pdf_info:
                    report.unchanged_records += 1
                    _verbose_log(config, f"[unchanged] {label}")
                    rows.append(obj)
                    continue

                report.updated_records += 1
                if not config.dry_run:
                    obj["pdf_info"] = next_pdf_info
                    obj["updated_at_utc"] = now_text
                    file_changed = True
                    _verbose_log(config, f"[updated] {label}")
                else:
                    _verbose_log(config, f"[dry-run update] {label}")
                rows.append(obj)

        if file_changed:
            report.partitions_changed += 1
            if not config.dry_run:
                _atomic_write_jsonl(ledger_file, rows)

    return report


def _print_report(report: PdfInfoReport, *, dry_run: bool) -> None:
    print("pdf-info:")
    print(f"  dry_run: {dry_run}")
    print(f"  scanned_records: {report.scanned_records}")
    print(f"  processed_records: {report.processed_records}")
    print(f"  updated_records: {report.updated_records}")
    print(f"  unchanged_records: {report.unchanged_records}")
    print(f"  skipped_already_success: {report.skipped_already_success}")
    print(f"  skipped_no_local_pdf: {report.skipped_no_local_pdf}")
    print(f"  extraction_failed: {report.extraction_failed}")
    print(f"  partitions_changed: {report.partitions_changed}")


def run_from_args(args: argparse.Namespace) -> int:
    config = PdfInfoConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        max_records=max(0, args.max_records),
        dry_run=args.dry_run,
        force=args.force,
        mark_missing=args.mark_missing,
        verbose=args.verbose,
    )
    try:
        report = run_pdf_info(config)
    except PdfInfoDependencyError as exc:
        print(f"pdf-info error: {exc}")
        return 2

    _print_report(report, dry_run=config.dry_run)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract/store PDF metadata in ledger records.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
