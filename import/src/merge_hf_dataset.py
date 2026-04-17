#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any

from local_env import load_local_env
from import_config import load_import_config

try:
    from huggingface_hub import HfApi
except ImportError:
    HfApi = None

HF_REPO_URL_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?huggingface\.co/datasets/([^/?#]+/[^/?#]+)"
)

def _require_hf_hub() -> None:
    if HfApi is None:
        print("Error: `huggingface_hub` is required. Install it with `pip install huggingface_hub`.")
        raise SystemExit(1)

def _extract_repo_id(repo_input: str) -> str:
    text = (repo_input or "").strip()
    if not text:
        return ""
    match = HF_REPO_URL_PATTERN.search(text)
    if match:
        return match.group(1).strip("/")
    if "://" not in text and text.count("/") == 1:
        return text.strip("/")
    return text

def list_remote_files(api: HfApi, repo_id: str, token: str | None) -> set[str]:
    print(f"Fetching file list from Hugging Face dataset: {repo_id}...")
    try:
        files = api.list_repo_files(repo_id=repo_id, repo_type="dataset", token=token)
        # Filter only PDF files (case-insensitive check)
        return {f for f in files if isinstance(f, str) and f.lower().endswith(".pdf")}
    except Exception as exc:
        print(f"Error listing remote files: {exc}")
        raise SystemExit(1)

def get_local_files(local_dir: Path) -> dict[str, Path]:
    local_files = {}
    for path in local_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() == ".pdf":
            # Map local relative path to repo path
            rel_path = str(path.relative_to(local_dir)).replace("\\", "/")
            local_files[rel_path] = path
    return local_files

def upload_files(
    api: HfApi,
    repo_id: str,
    token: str | None,
    local_dir: Path,
    files_to_upload: list[tuple[str, Path]],
    limit: int | None,
    dry_run: bool = False
) -> int:
    count = 0
    total = len(files_to_upload)
    
    if limit is None:
        if dry_run:
            print(f"[dry-run] Would use `upload_large_folder` to sync {total} PDFs from {local_dir}")
            return total
        
        print(f"No limit provided. Using high-performance `upload_large_folder` to sync {total} PDFs...")
        try:
            # upload_large_folder is available in huggingface_hub >= 0.17.0
            # Note: it doesn't support commit_message or allow_patterns
            if hasattr(api, "upload_large_folder"):
                print("Using `upload_large_folder` for high-performance upload...")
                api.upload_large_folder(
                    repo_id=repo_id,
                    folder_path=str(local_dir),
                    repo_type="dataset"
                )
            else:
                # Fallback to standard upload_folder if upload_large_folder isn't available
                print("`upload_large_folder` not found, falling back to `upload_folder`...")
                api.upload_folder(
                    repo_id=repo_id,
                    folder_path=str(local_dir),
                    repo_type="dataset",
                    commit_message=f"Sync {total} missing PDFs using upload_folder",
                    allow_patterns=["**/*.pdf"]
                )
            return total
        except Exception as exc:
            print(f"Error during folder upload: {exc}")
            raise SystemExit(1)

    # If limit is provided, use the granular upload_file method
    to_process = files_to_upload[:limit]
    print(f"Limit applied: will upload at most {limit} PDFs using `upload_file`...")
    
    for rel_path, local_path in to_process:
        if dry_run:
            print(f"[dry-run] Would upload: {local_path} -> {rel_path}")
        else:
            print(f"Uploading ({count + 1}/{len(to_process)}): {rel_path}...")
            try:
                api.upload_file(
                    path_or_fileobj=str(local_path),
                    path_in_repo=rel_path,
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                    commit_message=f"Upload PDF: {rel_path}"
                )
            except Exception as exc:
                print(f"Failed to upload {rel_path}: {exc}")
                continue
        count += 1
    
    return count

def get_hf_token() -> str | None:
    token = os.environ.get("HF_TOKEN")
    if token:
        return token
    
    # Fallback to .secret/.huggingface_token
    # We look for it relative to the script or in the project root
    potential_paths = [
        Path(".secret/.huggingface_token"),
        Path(__file__).parent.parent.parent.parent / ".secret" / ".huggingface_token",
    ]
    for p in potential_paths:
        if p.exists():
            try:
                return p.read_text().strip()
            except Exception:
                pass
    return None

def main() -> None:
    load_local_env()
    hf_defaults = load_import_config().hf
    _require_hf_hub()

    parser = argparse.ArgumentParser(description="Merge local directory with Hugging Face dataset.")
    parser.add_argument(
        "repo", 
        nargs="?", 
        default=hf_defaults.dataset_repo_id,
        help="Hugging Face dataset repository path or URL (e.g., 'username/dataset-name')"
    )
    parser.add_argument("local_dir", type=Path, help="Local directory to merge")
    parser.add_argument("--limit", type=int, help="Limit the number of files to upload")
    parser.add_argument("--token", help="Hugging Face API token")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded without doing it")

    args = parser.parse_args()

    token = args.token or get_hf_token()

    repo_id = _extract_repo_id(args.repo)
    if not repo_id:
        print("Error: Hugging Face dataset repository path is required.")
        print("Provide it as an argument or set 'hf.dataset_repo_id' in import_config.yaml.")
        raise SystemExit(1)
        
    if "/" not in repo_id:
        print(f"Error: Invalid repository ID '{repo_id}'. Expected 'username/dataset-name'.")
        raise SystemExit(1)

    if not args.local_dir.is_dir():
        print(f"Error: Local directory '{args.local_dir}' does not exist or is not a directory.")
        raise SystemExit(1)

    api = HfApi(token=token)
    
    # 1. Get all files in the huggingface dataset repository
    remote_files = list_remote_files(api, repo_id, token)
    
    # 2. Compares with the files in the local directory
    local_files_map = get_local_files(args.local_dir)
    local_paths = set(local_files_map.keys())
    
    # Identify differences
    only_local = sorted(list(local_paths - remote_files))
    only_remote = remote_files - local_paths
    common = local_paths & remote_files
    
    missing_files = [(path, local_files_map[path]) for path in only_local]
    
    # Print Statistics
    print("\nDataset Comparison Statistics:")
    print(f"  Common files (in both):       {len(common)}")
    print(f"  Files only in Remote (HF):    {len(only_remote)}")
    print(f"  Files only in Local (to upload): {len(only_local)}")
    print(f"  -----------------------------")
    print(f"  Total Remote files:           {len(remote_files)}")
    print(f"  Total Local files:            {len(local_paths)}")
    print("-" * 30)

    if not missing_files:
        print("All local files are already present in the remote repository.")
        return

    # 3. It then starts uploading the files as the --limit option
    uploaded_count = upload_files(
        api, 
        repo_id, 
        token, 
        args.local_dir,
        missing_files, 
        args.limit, 
        dry_run=args.dry_run
    )
    
    action = "Would have uploaded" if args.dry_run else "Successfully uploaded"
    print(f"{action} {uploaded_count} files.")

if __name__ == "__main__":
    main()
