#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from import_config import load_import_config
from info_store import InfoStore
from local_env import load_local_env

try:
    from huggingface_hub import HfApi
except Exception:
    HfApi = None


LONG_DIGITS_RE = re.compile(r"\d{16,22}")
HF_REPO_URL_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?huggingface\.co/datasets/([^/?#]+/[^/?#]+)"
)


@dataclass(frozen=True)
class BackfillHFPathConfig:
    ledger_dir: Path
    hf_repo_id: str
    hf_token: str
    dry_run: bool
    batch_size: int
    verbose: bool


@dataclass
class BackfillHFPathReport:
    pdf_files_scanned: int = 0
    pdf_codes_indexed: int = 0
    pdf_codes_ambiguous: int = 0
    upload_rows_scanned: int = 0
    missing_hf_path_rows: int = 0
    candidate_rows_with_pdf: int = 0
    skipped_no_pdf: int = 0
    skipped_ambiguous_pdf_code: int = 0
    updates_applied: int = 0
    batches_written: int = 0


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = (
        "One-time backfill: set uploadinfos[].hf.path from remote HF dataset PDFs for rows where hf.path is missing."
    )
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument(
        "--hf-repo-id",
        default=hf_defaults.dataset_repo_id,
        help="HF dataset repo id (`namespace/name`; default from import/import_config.yaml).",
    )
    parser.add_argument(
        "--hf-repo-url",
        default=hf_defaults.dataset_repo_url,
        help="HF dataset URL used to infer repo id when --hf-repo-id is empty.",
    )
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token override")
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of updates per write batch")
    parser.add_argument("--dry-run", action="store_true", help="Only report planned updates")
    parser.add_argument("--verbose", action="store_true", help="Print detailed progress")
    return parser


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _code_candidates_from_stem(stem: str) -> list[str]:
    text = stem.strip()
    if not text:
        return []
    candidates = [text]
    match = LONG_DIGITS_RE.search(text)
    if match:
        digits = match.group(0)
        if digits not in candidates:
            candidates.append(digits)
    return candidates


def _require_hf_hub() -> None:
    if HfApi is None:
        raise RuntimeError(
            "`huggingface_hub` is required for backfill_hf_path. Install with `pip install huggingface_hub`."
        )


def _extract_repo_id_from_url(repo_url: str) -> str:
    text = (repo_url or "").strip()
    if not text:
        return ""
    match = HF_REPO_URL_PATTERN.search(text)
    if match:
        return match.group(1).strip("/")
    if "://" not in text and text.count("/") == 1:
        return text.strip("/")
    return ""


def _resolve_repo_id(repo_id: str, repo_url: str) -> str:
    normalized_repo_id = (repo_id or "").strip().strip("/")
    if not normalized_repo_id:
        normalized_repo_id = _extract_repo_id_from_url(repo_url)
    if not normalized_repo_id:
        raise ValueError(
            "Unable to resolve HF dataset repo id. Set --hf-repo-id/--hf-repo-url "
            "or configure hf.dataset_repo_id/hf.dataset_repo_url in import/import_config.yaml."
        )
    if normalized_repo_id.count("/") != 1:
        raise ValueError(f"Invalid HF dataset repo id: {normalized_repo_id} (expected `namespace/name`).")
    return normalized_repo_id


def _list_remote_pdf_paths(hf_repo_id: str, *, hf_token: str, verbose: bool) -> list[str]:
    _require_hf_hub()
    token = (hf_token or "").strip() or None
    api = HfApi(token=token)
    try:
        repo_files = api.list_repo_files(repo_id=hf_repo_id, repo_type="dataset", token=token)
    except Exception as exc:
        raise RuntimeError(f"Failed to list files from HF dataset repo `{hf_repo_id}`: {exc}") from exc

    pdf_paths = sorted(path for path in repo_files if isinstance(path, str) and path.lower().endswith(".pdf"))
    if verbose:
        print(f"[hf] repo_id={hf_repo_id} files={len(repo_files)} pdfs={len(pdf_paths)}")
    return pdf_paths


def _build_pdf_index(pdf_paths: list[str], *, verbose: bool) -> tuple[dict[str, str], set[str], int]:
    code_to_relpath: dict[str, str] = {}
    ambiguous_codes: set[str] = set()
    scanned = 0

    for relpath in pdf_paths:
        scanned += 1
        for code in _code_candidates_from_stem(Path(relpath).stem):
            if code in ambiguous_codes:
                continue
            existing = code_to_relpath.get(code)
            if existing is None:
                code_to_relpath[code] = relpath
                continue
            if existing != relpath:
                ambiguous_codes.add(code)
                del code_to_relpath[code]
                if verbose:
                    print(f"[ambiguous] code={code} paths={existing} | {relpath}")

    return code_to_relpath, ambiguous_codes, scanned


def _iter_upload_rows(upload_dir: Path):
    for file_path in sorted(upload_dir.glob("*.jsonl")):
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    yield obj


def _has_hf_path(row: dict[str, Any]) -> bool:
    hf = row.get("hf")
    if not isinstance(hf, dict):
        return False
    path_value = hf.get("path")
    return isinstance(path_value, str) and bool(path_value.strip())


def run_backfill_hf_path(config: BackfillHFPathConfig) -> BackfillHFPathReport:
    store = InfoStore(config.ledger_dir)

    report = BackfillHFPathReport()
    pdf_paths = _list_remote_pdf_paths(config.hf_repo_id, hf_token=config.hf_token, verbose=config.verbose)
    pdf_index, ambiguous_codes, scanned_pdf_files = _build_pdf_index(pdf_paths, verbose=config.verbose)
    report.pdf_files_scanned = scanned_pdf_files
    report.pdf_codes_indexed = len(pdf_index)
    report.pdf_codes_ambiguous = len(ambiguous_codes)

    pending_updates: list[dict[str, Any]] = []

    def _flush_updates() -> None:
        nonlocal pending_updates
        if not pending_updates:
            return
        if config.dry_run:
            report.updates_applied += len(pending_updates)
            pending_updates = []
            return
        results = store.update_many(pending_updates)
        report.updates_applied += len(results)
        report.batches_written += 1
        pending_updates = []

    for row in _iter_upload_rows(store.upload_dir):
        report.upload_rows_scanned += 1

        record_key = _normalize_text(row.get("record_key") or row.get("unique_code"))
        if not record_key:
            continue
        if _has_hf_path(row):
            continue

        report.missing_hf_path_rows += 1

        if record_key in ambiguous_codes:
            report.skipped_ambiguous_pdf_code += 1
            continue

        relpath = pdf_index.get(record_key)
        if not relpath:
            report.skipped_no_pdf += 1
            continue

        report.candidate_rows_with_pdf += 1
        pending_updates.append(
            {
                "record_key": record_key,
                "hf": {"path": relpath},
            }
        )

        if len(pending_updates) >= max(1, config.batch_size):
            _flush_updates()

    _flush_updates()
    return report


def _print_report(report: BackfillHFPathReport, *, config: BackfillHFPathConfig) -> None:
    print("backfill-hf-path:")
    print(f"  ledger_dir: {config.ledger_dir}")
    print(f"  hf_repo_id: {config.hf_repo_id}")
    print(f"  dry_run: {config.dry_run}")
    print(f"  batch_size: {config.batch_size}")
    print(f"  pdf_files_scanned: {report.pdf_files_scanned}")
    print(f"  pdf_codes_indexed: {report.pdf_codes_indexed}")
    print(f"  pdf_codes_ambiguous: {report.pdf_codes_ambiguous}")
    print(f"  upload_rows_scanned: {report.upload_rows_scanned}")
    print(f"  missing_hf_path_rows: {report.missing_hf_path_rows}")
    print(f"  candidate_rows_with_pdf: {report.candidate_rows_with_pdf}")
    print(f"  skipped_no_pdf: {report.skipped_no_pdf}")
    print(f"  skipped_ambiguous_pdf_code: {report.skipped_ambiguous_pdf_code}")
    print(f"  updates_applied: {report.updates_applied}")
    print(f"  batches_written: {report.batches_written}")


def run_from_args(args: argparse.Namespace) -> int:
    hf_repo_id = _resolve_repo_id(args.hf_repo_id, args.hf_repo_url)
    config = BackfillHFPathConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        hf_repo_id=hf_repo_id,
        hf_token=(args.hf_token or "").strip(),
        dry_run=args.dry_run,
        batch_size=max(1, args.batch_size),
        verbose=args.verbose,
    )
    report = run_backfill_hf_path(config)
    _print_report(report, config=config)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill missing hf.path values from remote HF dataset PDFs.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
