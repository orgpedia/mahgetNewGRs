#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path, PurePosixPath

from import_config import load_import_config


def get_hf_token() -> str | None:
    token = os.environ.get("HF_TOKEN")
    if token:
        return token

    potential_paths = [
        Path(".secret/.huggingface_token"),
        Path(__file__).parent.parent.parent / ".secret" / ".huggingface_token",
    ]
    for path in potential_paths:
        if not path.exists():
            continue
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download files from the configured Hugging Face dataset repo by looking up "
            "record_key entries in uploadinfos JSONL files."
        )
    )
    parser.add_argument(
        "--upload-infos-dir",
        type=Path,
        default=Path("import/uploadinfos"),
        help="Directory containing uploadinfos JSONL partitions (default: import/uploadinfos)",
    )
    parser.add_argument(
        "--record-keys-file",
        type=Path,
        default=Path("to_download.txt"),
        help="Text file containing one record_key per line (default: to_download.txt)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Local directory where downloaded files will be stored",
    )
    parser.add_argument(
        "--repo-id",
        default="",
        help="Hugging Face dataset repo id. Defaults to import/import_config.yaml hf.dataset_repo_id",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Hugging Face token. Defaults to HF_TOKEN or .secret/.huggingface_token",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned downloads without downloading files",
    )
    return parser.parse_args(argv)


def load_record_keys(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"record keys file not found: {path}")

    keys: list[str] = []
    seen: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line in seen:
            continue
        seen.add(line)
        keys.append(line)
    return keys


def find_records(upload_infos_dir: Path, wanted_keys: set[str]) -> dict[str, dict]:
    if not upload_infos_dir.exists():
        raise FileNotFoundError(f"upload infos dir not found: {upload_infos_dir}")
    if not upload_infos_dir.is_dir():
        raise NotADirectoryError(f"upload infos path is not a directory: {upload_infos_dir}")

    found: dict[str, dict] = {}
    for jsonl_path in sorted(upload_infos_dir.glob("*.jsonl")):
        if len(found) == len(wanted_keys):
            break
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"invalid JSON in {jsonl_path}:{line_number}: {exc}") from exc
                record_key = str(record.get("record_key", "")).strip()
                if not record_key or record_key not in wanted_keys or record_key in found:
                    continue
                found[record_key] = record
                if len(found) == len(wanted_keys):
                    break
    return found


def build_repo_stub(hf_path: str) -> str:
    raw_path = hf_path.strip()
    if not raw_path:
        raise ValueError("hf.path is empty")

    parts = PurePosixPath(raw_path).parts
    if len(parts) < 3:
        raise ValueError(f"hf.path must have at least 3 path segments, got: {hf_path}")
    return str(PurePosixPath(*parts[2:]))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    record_keys = load_record_keys(args.record_keys_file)
    if not record_keys:
        print("No record keys to process.")
        return 0

    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        print("Error: `huggingface_hub` library not found. Install with `pip install huggingface_hub`.", file=sys.stderr)
        return 1

    config = load_import_config(start_dir=Path.cwd())
    repo_id = (args.repo_id or config.hf.dataset_repo_id).strip()
    if not repo_id:
        print("Error: Hugging Face repo id is required. Set it in import/import_config.yaml or pass --repo-id.", file=sys.stderr)
        return 1

    token = (args.token or get_hf_token() or "").strip()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    records = find_records(args.upload_infos_dir, set(record_keys))

    missing_records = [key for key in record_keys if key not in records]
    download_count = 0
    skipped_count = 0

    for record_key in record_keys:
        record = records.get(record_key)
        if record is None:
            print(f"[missing-record] {record_key}")
            skipped_count += 1
            continue

        hf_info = record.get("hf")
        if not isinstance(hf_info, dict):
            print(f"[missing-hf] {record_key}: hf object not found")
            skipped_count += 1
            continue

        hf_path = str(hf_info.get("path") or "").strip()
        if not hf_path:
            print(f"[missing-hf-path] {record_key}")
            skipped_count += 1
            continue

        try:
            repo_stub = build_repo_stub(hf_path)
        except ValueError as exc:
            print(f"[invalid-hf-path] {record_key}: {exc}")
            skipped_count += 1
            continue

        target_path = output_dir / Path(repo_stub)
        if args.dry_run:
            print(f"[dry-run] {record_key}: {repo_stub} -> {target_path}")
            download_count += 1
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[download] {record_key}: {repo_stub}")
        hf_hub_download(
            repo_id=repo_id,
            repo_type="dataset",
            filename=repo_stub,
            local_dir=str(output_dir),
            token=token or None,
        )
        download_count += 1

    print(
        f"Done. requested={len(record_keys)} found={len(records)} "
        f"downloaded={download_count} skipped={skipped_count} missing_records={len(missing_records)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
