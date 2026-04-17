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
    
    # 2. Check project secret file (standard location for this project)
    # Adjusting path to reach project root from import/src/
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
    parser = argparse.ArgumentParser(description="Delete a file from a Hugging Face repository using API.")
    parser.add_argument("repo", help="Hugging Face repository ID (e.g., 'username/dataset-name')")
    parser.add_argument("path", help="Relative path of the file to delete inside the repository")
    parser.add_argument("--token", help="Hugging Face API token")
    parser.add_argument("--type", default="dataset", choices=["dataset", "model", "space"], help="Repository type (default: dataset)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without deleting")

    args = parser.parse_args()
    token = args.token or get_hf_token()

    if not token:
        print("Error: Hugging Face token not found. Set HF_TOKEN environment variable or use --token.")
        return

    api = HfApi(token=token)

    if args.dry_run:
        print(f"[dry-run] Would delete file '{args.path}' from {args.type} '{args.repo}'")
    else:
        print(f"Deleting '{args.path}' from {args.type} '{args.repo}'...")
        try:
            api.delete_file(
                path_in_repo=args.path,
                repo_id=args.repo,
                repo_type=args.type,
                commit_message=f"Delete file: {args.path}"
            )
            print("Successfully deleted.")
        except Exception as exc:
            print(f"Error during deletion: {exc}")

if __name__ == "__main__":
    main()
