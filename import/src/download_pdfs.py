#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import import_pdfs_job
from import_config import load_import_config
from info_store import InfoStore as LedgerStore
from local_env import load_local_env
from sync_hf_job import SyncHFConfig, SyncHFError, resolve_hf_repo_path, run_sync_hf


DEFAULT_BATCH_SUCCESS_SIZE = 25
DEFAULT_MAX_RUNTIME_SEC = 5 * 60 * 60 + 45 * 60
DEFAULT_REQUEST_INTERVAL_SEC = 1.0
DEFAULT_MAX_CONSECUTIVE_FAILURES = 10
DEFAULT_DOWNLOAD_TIMEOUT_SEC = 30
DEFAULT_SKIP_YEAR = ""
MAX_DOWNLOAD_ATTEMPTS = 2
DEFAULT_START_YEAR = 2025
URL_NS = "urlinfos"
UPLOAD_NS = "uploadinfos"


@dataclass(frozen=True)
class DownloadCandidate:
    unique_code: str
    source_url: str
    partition: str
    gr_date: str


@dataclass(frozen=True)
class DownloadPdfsConfig:
    ledger_dir: Path
    downloads_dir: Path
    lfs_pdf_root: Path
    hf_repo_path: Path
    hf_remote_name: str
    hf_branch: str
    hf_commit_message: str
    hf_storage_backend: str
    hf_no_verify_storage: bool
    hf_skip_push: bool
    hf_token: str
    hf_remote_url: str
    batch_success_size: int
    max_runtime_sec: int
    request_interval_sec: float
    max_consecutive_failures: int
    download_timeout_sec: int
    skip_year: str
    start_year: int


@dataclass
class DownloadPdfsReport:
    scanned_records: int = 0
    candidate_urls: int = 0
    attempted_downloads: int = 0
    downloaded_success: int = 0
    downloaded_failed: int = 0
    batches_flushed: int = 0
    sync_runs: int = 0
    temp_files_deleted: int = 0
    stale_temp_files_deleted: int = 0
    import_scanned_pdf_files: int = 0
    import_copied_files: int = 0
    import_overwritten_files: int = 0
    import_skipped_identical: int = 0
    import_skipped_conflicts: int = 0
    import_missing_ledger_record: int = 0
    remaining_urls: int = 0
    stop_reason: str = "completed"
    elapsed_seconds: float = 0.0


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    hf_defaults = load_import_config().hf

    parser.description = (
        "Backfill missing PDFs for ledger rows where lfs_path is null, "
        "import into LFS, then sync to Hugging Face in batches."
    )
    parser.add_argument("--ledger-dir", default="import/grinfo", help="Ledger root directory (supports split ledgers)")
    parser.add_argument(
        "--downloads-dir",
        default="import/downloads",
        help="Temporary local download directory (files are removed after successful batch import+sync)",
    )
    parser.add_argument(
        "--lfs-pdf-root",
        default="",
        help="LFS PDF root for import-pdfs (default: <hf-repo-path>/pdfs)",
    )
    parser.add_argument(
        "--hf-repo-path",
        default=hf_defaults.dataset_repo_path,
        help="Local HF sync root (default from `import/import_config.yaml`).",
    )
    parser.add_argument("--hf-remote-name", default="origin", help="Deprecated compatibility flag")
    parser.add_argument("--hf-branch", default="main", help="Branch used by sync-hf")
    parser.add_argument(
        "--hf-commit-message",
        default="download-pdfs batch sync",
        help="Commit message used by sync-hf",
    )
    parser.add_argument(
        "--hf-storage-backend",
        choices=("auto", "xet", "lfs"),
        default=os.environ.get("HF_STORAGE_BACKEND", "auto"),
        help="Deprecated compatibility flag (ignored by API-based sync)",
    )
    parser.add_argument("--hf-no-verify-storage", action="store_true", help="Skip post-sync verification")
    parser.add_argument("--hf-skip-push", action="store_true", help="Skip HF upload during batch sync")
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token override")
    parser.add_argument(
        "--hf-remote-url",
        default=hf_defaults.dataset_repo_url,
        help="HF repo URL override (default from `import/import_config.yaml`).",
    )

    parser.add_argument(
        "--batch-success-size",
        type=int,
        default=DEFAULT_BATCH_SUCCESS_SIZE,
        help="Flush import+sync after this many successful downloads",
    )
    parser.add_argument(
        "--max-runtime-sec",
        type=int,
        default=DEFAULT_MAX_RUNTIME_SEC,
        help="Hard stop time budget in seconds (partial batch will still flush)",
    )
    parser.add_argument(
        "--request-interval-sec",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL_SEC,
        help="Delay between URL requests (no parallel downloads)",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_FAILURES,
        help="Stop after this many consecutive download failures",
    )
    parser.add_argument(
        "--download-timeout-sec",
        type=int,
        default=DEFAULT_DOWNLOAD_TIMEOUT_SEC,
        help="Single download request timeout in seconds",
    )
    parser.add_argument(
        "--skip-year",
        default=DEFAULT_SKIP_YEAR,
        help="Optional: skip one yearly ledger partition (for example 2026)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help="Start year for scan order (downloads walk backward: start-year, start-year-1, ...)",
    )
    return parser


def _to_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return int(text)
        except ValueError:
            return default
    return default


def _download_attempts(record: dict[str, object]) -> int:
    attempts_obj = record.get("attempt_counts")
    if not isinstance(attempts_obj, dict):
        return 0
    return max(0, _to_int(attempts_obj.get("download"), 0))


def _partition_year(partition: str) -> int | None:
    text = partition.strip()
    if not text.isdigit():
        return None
    return int(text)


def _upsert_download_attempt(
    *,
    store: LedgerStore,
    unique_code: str,
    attempts: int,
) -> None:
    patch = {
        "unique_code": unique_code,
        "download": {
            "attempts": max(0, attempts),
        },
    }
    store.upsert(patch)


def _load_upload_partition_rows(upload_dir: Path, partition: str) -> dict[str, dict[str, object]]:
    path = upload_dir / f"{partition}.jsonl"
    if not path.exists() or not path.is_file():
        return {}

    rows: dict[str, dict[str, object]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            text = raw_line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            key = str(obj.get("record_key") or "").strip()
            if key:
                rows[key] = obj
    return rows


def _collect_candidates(
    *,
    store: LedgerStore,
    config: DownloadPdfsConfig,
    report: DownloadPdfsReport,
) -> list[DownloadCandidate]:
    candidates: list[DownloadCandidate] = []
    seen_codes: set[str] = set()
    url_dir = store.root_dir / URL_NS
    upload_dir = store.root_dir / UPLOAD_NS

    url_files = sorted(url_dir.glob("*.jsonl"), key=lambda path: path.stem, reverse=True)
    for url_file in url_files:
        partition = url_file.stem
        partition_year = _partition_year(partition)
        if partition_year is None or partition_year > config.start_year:
            continue
        if config.skip_year and partition == config.skip_year:
            continue

        upload_rows = _load_upload_partition_rows(upload_dir, partition)
        with url_file.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                text = raw_line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue

                report.scanned_records += 1
                unique_code = str(row.get("record_key") or row.get("unique_code") or "").strip()
                source_url = str(row.get("source_url") or "").strip()
                if not unique_code or unique_code in seen_codes or not source_url:
                    continue

                upload_row = upload_rows.get(unique_code, {})
                download_obj = upload_row.get("download") if isinstance(upload_row, dict) else {}
                if not isinstance(download_obj, dict):
                    download_obj = {}
                attempts = max(0, _to_int(download_obj.get("attempts"), 0))
                if attempts >= MAX_DOWNLOAD_ATTEMPTS:
                    continue

                hf_obj = upload_row.get("hf") if isinstance(upload_row, dict) else {}
                if not isinstance(hf_obj, dict):
                    hf_obj = {}
                lfs_path = hf_obj.get("path")
                if isinstance(lfs_path, str) and lfs_path.strip():
                    continue

                seen_codes.add(unique_code)
                candidates.append(
                    DownloadCandidate(
                        unique_code=unique_code,
                        source_url=source_url,
                        partition=partition,
                        gr_date=str(row.get("gr_date") or "").strip(),
                    )
                )

    candidates.sort(
        key=lambda item: (_partition_year(item.partition) or 0, item.gr_date, item.unique_code),
        reverse=True,
    )
    report.candidate_urls = len(candidates)
    return candidates


def _download_pdf(url: str, timeout_sec: int) -> tuple[bytes | None, str]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw_status = getattr(response, "status", 200)
            status_code = int(raw_status) if raw_status not in (None, "") else 200
            if status_code != 200:
                return None, f"http_{status_code}"
            return response.read(), ""
    except urllib.error.HTTPError as exc:
        return None, f"http_{exc.code}"
    except urllib.error.URLError as exc:
        return None, f"url_error:{exc.reason}"
    except TimeoutError:
        return None, "timeout"
    except Exception as exc:
        return None, f"download_exception:{exc}"


def _delete_file_if_exists(file_path: Path) -> bool:
    if file_path.exists() and file_path.is_file():
        file_path.unlink()
        return True
    return False


def _cleanup_stale_downloads(downloads_dir: Path) -> int:
    deleted = 0
    if not downloads_dir.exists():
        return deleted
    for file_path in sorted(downloads_dir.glob("*.pdf")):
        if _delete_file_if_exists(file_path):
            deleted += 1
    return deleted


def _extract_unique_code_from_pdf_path(file_path: Path) -> str:
    match = import_pdfs_job.LONG_DIGITS_RE.search(file_path.stem)
    if match:
        return match.group(0)
    digits = "".join(ch for ch in file_path.stem if ch.isdigit())
    if 16 <= len(digits) <= 22:
        return digits
    if len(digits) > 22:
        return digits[:22]
    return ""


def _resolve_batch_repo_files(
    *,
    batch_files: list[Path],
    ledger_dir: Path,
    hf_repo_path: Path,
) -> list[str]:
    store = LedgerStore(ledger_dir)
    repo_files: list[str] = []
    seen_repo_files: set[str] = set()

    for downloaded_file in batch_files:
        unique_code = _extract_unique_code_from_pdf_path(downloaded_file)
        if not unique_code:
            continue
        record = store.find(unique_code)
        if not isinstance(record, dict):
            continue
        lfs_path = record.get("lfs_path")
        if not isinstance(lfs_path, str) or not lfs_path.strip():
            continue
        absolute_path = Path(lfs_path)
        if not absolute_path.is_absolute():
            absolute_path = (Path.cwd() / absolute_path)
        if not absolute_path.exists() or not absolute_path.is_file():
            continue
        try:
            repo_relpath = absolute_path.resolve().relative_to(hf_repo_path.resolve()).as_posix()
        except ValueError:
            continue
        if repo_relpath in seen_repo_files:
            continue
        seen_repo_files.add(repo_relpath)
        repo_files.append(repo_relpath)

    return sorted(repo_files)


def _flush_batch(
    *,
    batch_files: list[Path],
    config: DownloadPdfsConfig,
    report: DownloadPdfsReport,
    resolved_hf_repo_path: Path,
) -> bool:
    if not batch_files:
        return True

    print(f"download-pdfs: flushing batch of {len(batch_files)} downloaded PDFs")
    print("download-pdfs: batch stage 1/2 -> import-pdfs")

    import_config = import_pdfs_job.ImportPdfsConfig(
        source_dir=config.downloads_dir,
        ledger_dir=config.ledger_dir,
        lfs_pdf_root=config.lfs_pdf_root,
        recursive=False,
        overwrite=False,
        dry_run=False,
        sync_hf=False,
        hf_repo_path=resolved_hf_repo_path,
        hf_remote_name=config.hf_remote_name,
        hf_branch=config.hf_branch,
        hf_commit_message=config.hf_commit_message,
        hf_storage_backend=config.hf_storage_backend,
        hf_no_verify_storage=config.hf_no_verify_storage,
        hf_skip_push=config.hf_skip_push,
        hf_token=config.hf_token,
        hf_remote_url=config.hf_remote_url,
    )
    try:
        import_report = import_pdfs_job.run_import_pdfs(import_config)
    except Exception as exc:
        print(f"download-pdfs: batch import failed: {exc}")
        return False

    import_pdfs_job._print_report(import_report)
    report.import_scanned_pdf_files += import_report.scanned_pdf_files
    report.import_copied_files += import_report.copied_files
    report.import_overwritten_files += import_report.overwritten_files
    report.import_skipped_identical += import_report.skipped_identical
    report.import_skipped_conflicts += import_report.skipped_conflicts
    report.import_missing_ledger_record += import_report.missing_ledger_record

    changed_files = import_report.copied_files + import_report.overwritten_files
    if changed_files == 0:
        print("download-pdfs: batch stage 2/2 -> sync-hf skipped (no new/overwritten files)")
    else:
        print("download-pdfs: batch stage 2/2 -> sync-hf")
        repo_files = _resolve_batch_repo_files(
            batch_files=batch_files,
            ledger_dir=config.ledger_dir,
            hf_repo_path=resolved_hf_repo_path,
        )
        if not repo_files:
            print("download-pdfs: no resolved repo files for targeted sync; falling back to full sync")

        sync_config = SyncHFConfig(
            source_root=Path(".").resolve(),
            hf_repo_path=resolved_hf_repo_path,
            remote_name=config.hf_remote_name,
            branch=config.hf_branch,
            commit_message=config.hf_commit_message,
            dry_run=False,
            skip_push=config.hf_skip_push,
            verify_storage=not config.hf_no_verify_storage,
            storage_backend=config.hf_storage_backend,
            hf_token=config.hf_token,
            hf_remote_url=config.hf_remote_url,
            file_paths=tuple(repo_files),
        )
        try:
            run_sync_hf(sync_config)
        except SyncHFError as exc:
            print(f"download-pdfs: batch sync failed: {exc}")
            return False
        except Exception as exc:
            print(f"download-pdfs: batch sync unexpected failure: {exc}")
            return False
        report.sync_runs += 1
        print("download-pdfs: batch sync completed")

    report.batches_flushed += 1

    deleted = 0
    for file_path in list(batch_files):
        if _delete_file_if_exists(file_path):
            deleted += 1
    batch_files.clear()
    report.temp_files_deleted += deleted
    return True


def run_download_pdfs(config: DownloadPdfsConfig) -> tuple[int, DownloadPdfsReport]:
    if not config.ledger_dir.exists() or not config.ledger_dir.is_dir():
        raise FileNotFoundError(f"Ledger directory not found: {config.ledger_dir}")

    resolved_hf_repo_path = resolve_hf_repo_path(str(config.hf_repo_path))

    config.downloads_dir.mkdir(parents=True, exist_ok=True)
    report = DownloadPdfsReport()
    report.stale_temp_files_deleted = _cleanup_stale_downloads(config.downloads_dir)
    store = LedgerStore(config.ledger_dir)

    candidates = _collect_candidates(store=store, config=config, report=report)
    total_candidates = len(candidates)
    report.remaining_urls = total_candidates

    print("download-pdfs:")
    print(f"  candidate_urls: {total_candidates}")
    print(f"  start_year: {config.start_year}")
    print(f"  skip_year: {config.skip_year}")
    print(f"  stale_temp_files_deleted: {report.stale_temp_files_deleted}")

    if total_candidates == 0:
        report.stop_reason = "no_candidates"
        report.elapsed_seconds = 0.0
        return 0, report

    batch_files: list[Path] = []
    start_monotonic = time.monotonic()
    consecutive_failures = 0
    exit_code = 0
    flush_failed = False

    for index, candidate in enumerate(candidates, start=1):
        elapsed = time.monotonic() - start_monotonic
        if elapsed >= max(1, config.max_runtime_sec):
            report.stop_reason = "time_limit"
            break

        if report.attempted_downloads > 0:
            time.sleep(max(0.0, config.request_interval_sec))

        record = store.find(candidate.unique_code)
        if not isinstance(record, dict):
            continue
        current_attempts = _download_attempts(record)
        if current_attempts >= MAX_DOWNLOAD_ATTEMPTS:
            continue
        attempt_number = current_attempts + 1
        _upsert_download_attempt(
            store=store,
            unique_code=candidate.unique_code,
            attempts=attempt_number,
        )

        report.attempted_downloads += 1
        report.remaining_urls = total_candidates - index + 1
        print(
            "download-pdfs: downloading "
            f"{index} of {total_candidates} "
            f"(remaining {total_candidates - index}) unique_code={candidate.unique_code}"
        )

        content, error_text = _download_pdf(candidate.source_url, timeout_sec=max(1, config.download_timeout_sec))
        if content is None:
            report.downloaded_failed += 1
            consecutive_failures += 1
            print(f"download-pdfs: failed unique_code={candidate.unique_code} error={error_text}")
            if consecutive_failures >= max(1, config.max_consecutive_failures):
                report.stop_reason = "consecutive_failures"
                break
            continue

        destination = config.downloads_dir / f"{candidate.unique_code}.pdf"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        batch_files.append(destination)

        report.downloaded_success += 1
        consecutive_failures = 0

        if len(batch_files) >= max(1, config.batch_success_size):
            if not _flush_batch(
                batch_files=batch_files,
                config=config,
                report=report,
                resolved_hf_repo_path=resolved_hf_repo_path,
            ):
                report.stop_reason = "batch_flush_failed"
                exit_code = 1
                flush_failed = True
                break

    if batch_files and not flush_failed:
        if not _flush_batch(
            batch_files=batch_files,
            config=config,
            report=report,
            resolved_hf_repo_path=resolved_hf_repo_path,
        ):
            report.stop_reason = "batch_flush_failed"
            exit_code = 1

    if report.stop_reason == "completed" and report.attempted_downloads < total_candidates:
        report.stop_reason = "stopped_early"

    report.remaining_urls = max(0, total_candidates - report.attempted_downloads)
    report.elapsed_seconds = time.monotonic() - start_monotonic
    return exit_code, report


def _print_report(report: DownloadPdfsReport) -> None:
    print("download-pdfs summary:")
    print(f"  scanned_records: {report.scanned_records}")
    print(f"  candidate_urls: {report.candidate_urls}")
    print(f"  attempted_downloads: {report.attempted_downloads}")
    print(f"  downloaded_success: {report.downloaded_success}")
    print(f"  downloaded_failed: {report.downloaded_failed}")
    print(f"  batches_flushed: {report.batches_flushed}")
    print(f"  sync_runs: {report.sync_runs}")
    print(f"  import_scanned_pdf_files: {report.import_scanned_pdf_files}")
    print(f"  import_copied_files: {report.import_copied_files}")
    print(f"  import_overwritten_files: {report.import_overwritten_files}")
    print(f"  import_skipped_identical: {report.import_skipped_identical}")
    print(f"  import_skipped_conflicts: {report.import_skipped_conflicts}")
    print(f"  import_missing_ledger_record: {report.import_missing_ledger_record}")
    print(f"  stale_temp_files_deleted: {report.stale_temp_files_deleted}")
    print(f"  temp_files_deleted: {report.temp_files_deleted}")
    print(f"  remaining_urls: {report.remaining_urls}")
    print(f"  stop_reason: {report.stop_reason}")
    print(f"  elapsed_seconds: {report.elapsed_seconds:.1f}")


def run_from_args(args: argparse.Namespace) -> int:
    hf_repo_path = Path(args.hf_repo_path).resolve()
    lfs_pdf_root_text = str(args.lfs_pdf_root or "").strip()
    lfs_pdf_root = Path(lfs_pdf_root_text).resolve() if lfs_pdf_root_text else (hf_repo_path / "pdfs").resolve()

    config = DownloadPdfsConfig(
        ledger_dir=Path(args.ledger_dir).resolve(),
        downloads_dir=Path(args.downloads_dir).resolve(),
        lfs_pdf_root=lfs_pdf_root,
        hf_repo_path=hf_repo_path,
        hf_remote_name=args.hf_remote_name,
        hf_branch=args.hf_branch,
        hf_commit_message=args.hf_commit_message,
        hf_storage_backend=args.hf_storage_backend,
        hf_no_verify_storage=args.hf_no_verify_storage,
        hf_skip_push=args.hf_skip_push,
        hf_token=args.hf_token,
        hf_remote_url=args.hf_remote_url,
        batch_success_size=max(1, args.batch_success_size),
        max_runtime_sec=max(1, args.max_runtime_sec),
        request_interval_sec=max(0.0, args.request_interval_sec),
        max_consecutive_failures=max(1, args.max_consecutive_failures),
        download_timeout_sec=max(1, args.download_timeout_sec),
        skip_year=str(args.skip_year).strip() or DEFAULT_SKIP_YEAR,
        start_year=max(1900, args.start_year),
    )
    try:
        exit_code, report = run_download_pdfs(config)
    except SyncHFError as exc:
        print(f"download-pdfs error: {exc}")
        return 2
    _print_report(report)
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download missing PDFs in controlled batches.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
