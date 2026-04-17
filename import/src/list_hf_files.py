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
    """Resolve HF token from environment or project secrets."""
    token = os.environ.get("HF_TOKEN")
    if token:
        return token
    
    # Check project secret file (standard location for this project)
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

def format_size(size_bytes: int) -> str:
    """Return a human-readable string representation of a size in bytes."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    for unit in ["KB", "MB", "GB", "TB"]:
        size_bytes /= 1024
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
    return f"{size_bytes:.2f} PB"

def list_files(repo_id: str, repo_type: str, token: str | None = None, include_size: bool = False):
    api = HfApi(token=token)
    try:
        if include_size:
            # list_repo_tree returns objects with .path and .size attributes
            tree = api.list_repo_tree(repo_id=repo_id, repo_type=repo_type, recursive=True)
            files = []
            for item in tree:
                # Filter for files (directories don't have a 'size' attribute in the tree)
                if hasattr(item, "size") and item.size is not None:
                    files.append((item.path, item.size))
            return sorted(files)
        else:
            files = api.list_repo_files(repo_id=repo_id, repo_type=repo_type)
            return sorted(files)
    except Exception as exc:
        print(f"Error listing files for {repo_type} '{repo_id}': {exc}")
        return None

def main():
    parser = argparse.ArgumentParser(description="List all files in a Hugging Face repository.")
    parser.add_argument("repo", help="Hugging Face repository ID (e.g., 'username/dataset-name')")
    parser.add_argument(
        "--type", 
        default="dataset", 
        choices=["dataset", "model", "space"], 
        help="Repository type (default: dataset)"
    )
    parser.add_argument("--token", help="Hugging Face API token")
    parser.add_argument("--size", action="store_true", help="Include file size in output")

    args = parser.parse_args()
    token = args.token or get_hf_token()

    results = list_files(args.repo, args.type, token, include_size=args.size)
    
    if results is not None:
        if args.size:
            # Find the longest path to align sizes nicely
            max_len = max((len(f[0]) for f in results), default=0)
            for path, size in results:
                print(f"{path.ljust(max_len)}  {format_size(size)}")
        else:
            for f in results:
                print(f)

if __name__ == "__main__":
    main()
