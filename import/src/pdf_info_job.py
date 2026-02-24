#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from info_store import InfoStore as LedgerStore
from job_utils import JobRunResult, is_record_within_lookback, load_code_filter
from ledger_engine import to_ledger_relative_path
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
PDF_INFO_UPDATE_BATCH_SIZE = 100


class PdfInfoDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class PdfInfoJobConfig:
    max_records: int
    lookback_days: int
    code_filter: set[str] = field(default_factory=set)
    verbose: bool = False


@dataclass(frozen=True)
class PdfInfoStageConfig:
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
    skipped_lookback: int = 0
    extraction_failed: int = 0
    partitions_changed: int = 0


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


def _parse_font_entry(raw_font: Any) -> dict[str, Any]:
    if not isinstance(raw_font, (tuple, list)):
        return {
            "font_num": -1,
            "type": "",
            "basefont": "",
            "name": "",
        }

    return {
        "font_num": _safe_int(raw_font[0] if len(raw_font) > 0 else -1, -1),
        "type": str(raw_font[2] if len(raw_font) > 2 else ""),
        "basefont": str(raw_font[3] if len(raw_font) > 3 else ""),
        "name": str(raw_font[4] if len(raw_font) > 4 else ""),
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
                        "name": font_entry.get("basefont") or font_entry.get("name") or "",
                        "type": font_entry.get("type", ""),
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
        word_count = int(font_info.get("words", 0) or 0)
        if word_count <= 0:
            continue
        sorted_font_info: dict[str, Any] = {
            "name": str(font_info.get("name") or ""),
            "type": str(font_info.get("type") or ""),
            "script_word_counts": _sorted_count_dict(dict(font_info.get("script_word_counts", {}))),
            "word_count": word_count,
        }
        sorted_fonts[font_key] = sorted_font_info

    sorted_scripts = _sorted_count_dict(script_word_counts)
    return {
        "status": "success",
        "error": "",
        "path": to_ledger_relative_path(pdf_path),
        "file_size": file_size,
        "page_count": page_count,
        "pages_with_images": pages_with_images,
        "has_any_page_image": pages_with_images > 0,
        "total_font_count": len(fonts),
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
        "file_size": None,
        "page_count": None,
        "pages_with_images": None,
        "has_any_page_image": None,
        "total_font_count": None,
        "fonts": None,
        "unresolved_word_count": None,
        "language": None,
    }


def _build_failed_info(error: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "error": error.strip() or "pdf_info_failed",
        "file_size": None,
        "page_count": None,
        "pages_with_images": 0,
        "has_any_page_image": False,
        "total_font_count": 0,
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


def _verbose_log(config: PdfInfoStageConfig, message: str) -> None:
    if config.verbose:
        print(message)


def _verbose_job_log(config: PdfInfoJobConfig, message: str) -> None:
    if config.verbose:
        print(message)


def _record_label(unique_code: str) -> str:
    if unique_code:
        return unique_code
    return "<missing-unique_code>"


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        code = str(value).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        output.append(code)
    return output


def select_candidates(config: PdfInfoJobConfig, store: LedgerStore) -> list[str]:
    selected: list[str] = []
    scanned = 0
    for record in store.iter_records():
        unique_code = str(record.get("unique_code") or "").strip()
        if not unique_code:
            continue
        scanned += 1
        if config.code_filter and unique_code not in config.code_filter:
            continue
        if not is_record_within_lookback(record, config.lookback_days):
            continue
        selected.append(unique_code)
        if config.max_records > 0 and len(selected) >= config.max_records:
            break
    _verbose_job_log(
        config,
        (
            f"[pdf-info verbose] scanned={scanned} selected={len(selected)} "
            f"max_records={config.max_records} lookback_days={config.lookback_days} "
            f"code_filter_size={len(config.code_filter)}"
        ),
    )
    if config.verbose and selected:
        preview = selected[:25]
        print(f"[pdf-info verbose] selected codes preview ({len(preview)}/{len(selected)}):")
        for code in preview:
            print(f"[pdf-info verbose]   {code}")
    return selected


def run_selected(
    selected_codes: list[str],
    stage_config: PdfInfoStageConfig,
    store: LedgerStore,
) -> JobRunResult[PdfInfoReport]:
    try:
        fitz = _load_fitz()
    except PdfInfoDependencyError as exc:
        return JobRunResult(report=PdfInfoReport(), fatal_error=str(exc))

    report = PdfInfoReport()
    changed_partitions: set[str] = set()
    pending_updates: list[dict[str, Any]] = []
    codes = _dedupe_keep_order(selected_codes)
    _verbose_log(
        stage_config,
        (
            f"[pdf-info verbose] selected={len(codes)} dry_run={stage_config.dry_run} "
            f"force={stage_config.force} mark_missing={stage_config.mark_missing}"
        ),
    )

    def _flush_pending_updates() -> None:
        nonlocal pending_updates
        if not pending_updates:
            return
        results = store.update_many(pending_updates)
        for result in results:
            changed_partitions.add(result.partition)
        pending_updates = []

    for unique_code in codes:
        record = store.find(unique_code)
        report.scanned_records += 1
        label = _record_label(unique_code)

        if record is None:
            _verbose_log(stage_config, f"[skip missing] {label}")
            continue

        current_pdf_info = record.get("pdf_info")
        if not stage_config.force and _is_success_pdf_info(current_pdf_info):
            report.skipped_already_success += 1
            _verbose_log(stage_config, f"[skip success] {label}")
            continue

        pdf_path = _resolve_pdf_path(record)
        if pdf_path is None:
            report.skipped_no_local_pdf += 1
            if not stage_config.mark_missing:
                _verbose_log(stage_config, f"[skip no-pdf] {label}")
                continue
            _verbose_log(stage_config, f"[mark missing] {label}")
            next_pdf_info = _build_missing_info()
        else:
            _verbose_log(stage_config, f"[process] {label} path={to_ledger_relative_path(pdf_path)}")
            try:
                next_pdf_info = extract_pdf_info(pdf_path=pdf_path, fitz=fitz)
                language = next_pdf_info.get("language", {})
                inferred = ""
                if isinstance(language, dict):
                    inferred = str(language.get("inferred") or "")
                _verbose_log(
                    stage_config,
                    (
                        f"[extracted] {label} pages={next_pdf_info.get('page_count')} "
                        f"images={next_pdf_info.get('pages_with_images')} "
                        f"fonts={next_pdf_info.get('total_font_count')} "
                        f"words={language.get('total_words') if isinstance(language, dict) else ''} "
                        f"lang={inferred or 'unknown'}"
                    ),
                )
            except Exception as exc:
                report.extraction_failed += 1
                _verbose_log(stage_config, f"[failed] {label} error={exc}")
                next_pdf_info = _build_failed_info(str(exc))

        report.processed_records += 1
        if current_pdf_info == next_pdf_info:
            report.unchanged_records += 1
            _verbose_log(stage_config, f"[unchanged] {label}")
            continue

        report.updated_records += 1
        if stage_config.dry_run:
            _verbose_log(stage_config, f"[dry-run update] {label}")
            continue

        pending_updates.append({"unique_code": unique_code, "pdf_info": next_pdf_info})
        if len(pending_updates) >= PDF_INFO_UPDATE_BATCH_SIZE:
            _flush_pending_updates()
        _verbose_log(stage_config, f"[updated] {label}")

    if not stage_config.dry_run:
        _flush_pending_updates()
        report.partitions_changed = len(changed_partitions)

    return JobRunResult(report=report)


def print_pdf_info_stage_report(report: PdfInfoReport, *, dry_run: bool) -> None:
    print("pdf-info:")
    print(f"  dry_run: {dry_run}")
    print(f"  scanned_records: {report.scanned_records}")
    print(f"  processed_records: {report.processed_records}")
    print(f"  updated_records: {report.updated_records}")
    print(f"  unchanged_records: {report.unchanged_records}")
    print(f"  skipped_already_success: {report.skipped_already_success}")
    print(f"  skipped_no_local_pdf: {report.skipped_no_local_pdf}")
    print(f"  skipped_lookback: {report.skipped_lookback}")
    print(f"  extraction_failed: {report.extraction_failed}")
    print(f"  partitions_changed: {report.partitions_changed}")


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = (
        "Extract PDF metadata for ledger records and store it in record.pdf_info. "
        "Includes page count, image presence, used-font word counts, language inference, and file size."
    )
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument("--codes-file", default="", help="Optional file containing unique codes to process")
    parser.add_argument("--code", action="append", default=[], help="Explicit unique_code values to process")
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Maximum records to process (0 means no limit)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Process only records dated within the last N days (0 means no date filter)",
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


def run_from_args(args: argparse.Namespace) -> int:
    code_filter = load_code_filter(args.code, args.codes_file or None)
    store = LedgerStore(Path(args.ledger_dir).resolve())
    job_config = PdfInfoJobConfig(
        max_records=max(0, args.max_records),
        lookback_days=max(0, args.lookback_days),
        code_filter=code_filter,
        verbose=args.verbose,
    )
    stage_config = PdfInfoStageConfig(
        dry_run=args.dry_run,
        force=args.force,
        mark_missing=args.mark_missing,
        verbose=args.verbose,
    )
    selected_codes = select_candidates(job_config, store)
    result = run_selected(selected_codes, stage_config, store)
    if result.fatal_error:
        print(f"pdf-info error: {result.fatal_error}")
        return 2

    print_pdf_info_stage_report(result.report, dry_run=stage_config.dry_run)
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
