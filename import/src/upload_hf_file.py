#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

try:
    from huggingface_hub import HfApi
except ImportError:
    print("Error: `huggingface_hub` library not found. Install with `pip install huggingface_hub`.")
    exit(1)

def get_hf_token():
    # 1. Check environment variable
    token = os.environ.get("HF_TOKEN")
    if token:
        return token
    
    # 2. Check project secret file
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

def main():
    parser = argparse.ArgumentParser(description="Upload a single file to a Hugging Face repository using API.")
    parser.add_argument("repo", help="Hugging Face repository ID (e.g., 'username/dataset-name')")
    parser.add_argument("local_path", type=Path, help="Path to the local file to upload")
    parser.add_argument("path_in_repo", help="Target path inside the repository (including filename)")
    parser.add_argument("--token", help="Hugging Face API token")
    parser.add_argument("--type", default="dataset", choices=["dataset", "model", "space"], help="Repository type (default: dataset)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without uploading")

    args = parser.parse_args()
    token = args.token or get_hf_token()

    if not token:
        print("Error: Hugging Face token not found. Set HF_TOKEN environment variable or use --token.")
        return

    if not args.local_path.exists() or not args.local_path.is_file():
        print(f"Error: Local file '{args.local_path}' does not exist or is not a file.")
        return

    api = HfApi(token=token)

    if args.dry_run:
        print(f"[dry-run] Would upload '{args.local_path}' to '{args.path_in_repo}' in {args.type} '{args.repo}'")
    else:
        print(f"Uploading '{args.local_path}' to '{args.repo}' as '{args.path_in_repo}'...")
        try:
            api.upload_file(
                path_or_fileobj=str(args.local_path),
                path_in_repo=args.path_in_repo,
                repo_id=args.repo,
                repo_type=args.type,
                token=token,
                commit_message=f"Upload file: {args.path_in_repo}"
            )
            print("Successfully uploaded.")
        except Exception as exc:
            print(f"Error during upload: {exc}")

if __name__ == "__main__":
    main()
