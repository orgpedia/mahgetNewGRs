#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from local_env import load_local_env

try:
    from huggingface_hub import HfApi, hf_hub_download
except Exception:
    HfApi = None
    hf_hub_download = None


class SyncHFError(Exception):
    pass


MODE_UPLOAD = "upload"
MODE_DOWNLOAD = "download"
VALID_MODES = {MODE_UPLOAD, MODE_DOWNLOAD}
LARGE_FOLDER_MODE_AUTO = "auto"
LARGE_FOLDER_MODE_ALWAYS = "always"
LARGE_FOLDER_MODE_NEVER = "never"
VALID_LARGE_FOLDER_MODES = {
    LARGE_FOLDER_MODE_AUTO,
    LARGE_FOLDER_MODE_ALWAYS,
    LARGE_FOLDER_MODE_NEVER,
}
DEFAULT_LARGE_FOLDER_THRESHOLD = 100
HF_REPO_PATH_PLACEHOLDER = "/absolute/path/to/local/hf-dataset-clone"
LEDGER_EXCLUDE_PATTERNS = ("import/grinfo/**", "**/import/grinfo/**")
HF_REPO_URL_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?huggingface\.co/datasets/([^/?#]+/[^/?#]+)"
)


@dataclass(frozen=True)
class SyncHFConfig:
    source_root: Path
    hf_repo_path: Path
    remote_name: str
    branch: str
    commit_message: str
    dry_run: bool
    skip_push: bool
    verify_storage: bool
    storage_backend: str
    hf_token: str
    hf_remote_url: str
    hf_repo_id: str = ""
    mode: str = MODE_UPLOAD
    prefix: str = ""
    file_path: str = ""
    include_ledger: bool = False
    large_folder_mode: str = LARGE_FOLDER_MODE_AUTO
    large_folder_threshold: int = DEFAULT_LARGE_FOLDER_THRESHOLD
    large_folder_num_workers: int | None = None


def _require_hf_hub() -> None:
    if HfApi is None or hf_hub_download is None:
        raise SyncHFError(
            "`huggingface_hub` is required for sync-hf. Install with `pip install huggingface_hub`."
        )


def _looks_like_placeholder(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text == HF_REPO_PATH_PLACEHOLDER:
        return True
    return "<" in text and ">" in text


def resolve_hf_repo_path(path_text: str, *, create_if_missing: bool = False) -> Path:
    text = (path_text or "").strip()
    if not text:
        raise SyncHFError("`--hf-repo-path` is required (or set `HF_DATASET_REPO_PATH`).")
    if _looks_like_placeholder(text):
        raise SyncHFError(
            "HF local path looks like a template placeholder "
            f"(`{HF_REPO_PATH_PLACEHOLDER}`). Set a real local directory path."
        )

    path = Path(text).expanduser()
    if not path.exists():
        if not create_if_missing:
            raise SyncHFError(
                f"HF local path does not exist: {path}. Create it or set `HF_DATASET_REPO_PATH` correctly."
            )
        path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise SyncHFError(f"HF local path is not a directory: {path}")
    return path.resolve()


def _normalize_repo_relpath(path_text: str) -> str:
    text = (path_text or "").strip().replace("\\", "/")
    text = re.sub(r"/+", "/", text).lstrip("/")
    if text.endswith("/") and text != "/":
        text = text[:-1]
    if text in {"", "."}:
        return ""
    if text.startswith("../") or "/../" in text or text == "..":
        raise SyncHFError(f"Invalid repository path traversal: {path_text}")
    return text


def _extract_repo_id_from_url(repo_url: str) -> str:
    text = (repo_url or "").strip()
    if not text:
        return ""
    match = HF_REPO_URL_PATTERN.search(text)
    if match:
        return match.group(1)
    if "://" not in text and text.count("/") == 1:
        return text.strip("/")
    return ""


def resolve_hf_repo_id(repo_id_text: str, repo_url_text: str) -> str:
    repo_id = _normalize_repo_relpath(repo_id_text)
    if not repo_id:
        repo_id = _extract_repo_id_from_url(repo_url_text)

    if not repo_id:
        raise SyncHFError(
            "Unable to resolve HF dataset repo id. Set `--hf-repo-id` or `HF_DATASET_REPO_URL`."
        )
    if _looks_like_placeholder(repo_id):
        raise SyncHFError("HF dataset repo id looks like a template placeholder. Set a real repo id.")
    if repo_id.count("/") != 1:
        raise SyncHFError(f"Invalid HF dataset repo id: {repo_id} (expected `namespace/name`).")
    return repo_id


def _create_api(token: str) -> Any:
    _require_hf_hub()
    return HfApi(token=(token or "").strip() or None)


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _count_files(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    return sum(1 for item in path.rglob("*") if item.is_file())


def _contains_ledger_segment(path_text: str) -> bool:
    normalized = _normalize_repo_relpath(path_text)
    if not normalized:
        return False
    parts = Path(normalized).parts
    for idx in range(len(parts) - 1):
        if parts[idx] == "import" and parts[idx + 1] == "grinfo":
            return True
    return False


def _iter_upload_targets(local_root: Path, include_ledger: bool) -> list[tuple[Path, str]]:
    targets: list[tuple[Path, str]] = []
    for entry in sorted(local_root.iterdir(), key=lambda path: path.name):
        if entry.name in {".git", "__pycache__"}:
            continue
        if entry.name == "import":
            if not include_ledger:
                continue
            grinfo_path = entry / "grinfo"
            if grinfo_path.exists():
                targets.append((grinfo_path, "import/grinfo"))
            continue
        targets.append((entry, entry.name))
    return targets


def _upload_target(
    *,
    api: Any,
    repo_id: str,
    token: str,
    local_path: Path,
    path_in_repo: str,
    commit_message: str,
    revision: str,
    include_ledger: bool,
    dry_run: bool,
) -> int:
    file_count = _count_files(local_path)
    if file_count == 0:
        return 0

    if dry_run:
        kind = "file" if local_path.is_file() else "folder"
        print(f"[dry-run] upload {kind}: {local_path} -> {path_in_repo}")
        return file_count

    if local_path.is_file():
        if not include_ledger and _contains_ledger_segment(path_in_repo):
            return 0
        api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type="dataset",
            token=token or None,
            commit_message=commit_message,
            revision=revision or None,
        )
        return 1

    api.upload_folder(
        folder_path=str(local_path),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type="dataset",
        token=token or None,
        commit_message=commit_message,
        revision=revision or None,
        ignore_patterns=None if include_ledger else list(LEDGER_EXCLUDE_PATTERNS),
    )
    return file_count


def _supports_upload_large_folder(api: Any) -> bool:
    return hasattr(api, "upload_large_folder")


def _patterns_from_targets(targets: list[tuple[Path, str]]) -> list[str]:
    patterns: list[str] = []
    for local_path, path_in_repo in targets:
        normalized = _normalize_repo_relpath(path_in_repo)
        if not normalized:
            continue
        if local_path.is_dir():
            patterns.append(f"{normalized}/**")
        else:
            patterns.append(normalized)
    return patterns


def _normalize_large_folder_mode(value: str) -> str:
    mode = (value or LARGE_FOLDER_MODE_AUTO).strip().lower()
    if mode not in VALID_LARGE_FOLDER_MODES:
        raise SyncHFError(
            "Invalid `--large-folder-mode`. Use one of: auto, always, never."
        )
    return mode


def _should_use_large_folder(
    *,
    config: SyncHFConfig,
    api: Any,
    has_directory_target: bool,
    total_files: int,
) -> bool:
    mode = _normalize_large_folder_mode(config.large_folder_mode)

    if mode == LARGE_FOLDER_MODE_NEVER:
        return False

    if mode == LARGE_FOLDER_MODE_ALWAYS:
        if not _supports_upload_large_folder(api):
            raise SyncHFError(
                "`upload_large_folder` is not available in this `huggingface_hub` version. "
                "Upgrade `huggingface_hub` or use `--large-folder-mode never`."
            )
        if not has_directory_target:
            return False
        return True

    if not has_directory_target:
        return False
    if total_files < max(config.large_folder_threshold, 1):
        return False
    return _supports_upload_large_folder(api)


def _upload_large_folder(
    *,
    api: Any,
    repo_id: str,
    folder_path: Path,
    allow_patterns: list[str],
    revision: str,
    include_ledger: bool,
    dry_run: bool,
    num_workers: int | None,
) -> None:
    if dry_run:
        print(
            "[dry-run] upload_large_folder: "
            f"{folder_path} -> repo:{repo_id} allow_patterns={allow_patterns}"
        )
        return

    kwargs: dict[str, Any] = {
        "repo_id": repo_id,
        "folder_path": str(folder_path),
        "repo_type": "dataset",
        "allow_patterns": allow_patterns,
        "ignore_patterns": None if include_ledger else list(LEDGER_EXCLUDE_PATTERNS),
        "print_report": True,
    }
    if revision:
        kwargs["revision"] = revision
    if num_workers is not None:
        kwargs["num_workers"] = num_workers
    api.upload_large_folder(**kwargs)


def _verify_remote_targets(
    *,
    api: Any,
    repo_id: str,
    token: str,
    targets: list[tuple[str, bool]],
) -> None:
    remote_files = set(api.list_repo_files(repo_id=repo_id, repo_type="dataset", token=token or None))
    for path_in_repo, is_dir in targets:
        if is_dir:
            prefix = f"{path_in_repo.rstrip('/')}/"
            if not any(path.startswith(prefix) for path in remote_files):
                raise SyncHFError(f"Verification failed: no files found under remote prefix `{path_in_repo}`.")
        elif path_in_repo not in remote_files:
            raise SyncHFError(f"Verification failed: missing remote file `{path_in_repo}`.")


def _run_upload(config: SyncHFConfig) -> tuple[str, int]:
    if config.skip_push:
        print("sync-hf: upload skipped by flag.")
        return "", 0

    repo_id = resolve_hf_repo_id(config.hf_repo_id, config.hf_remote_url)
    api = _create_api(config.hf_token)

    selected_file = _normalize_repo_relpath(config.file_path)
    selected_prefix = _normalize_repo_relpath(config.prefix)
    if selected_file and selected_prefix:
        raise SyncHFError("Use only one of `--file` or `--prefix`.")
    if not config.include_ledger and selected_file and _contains_ledger_segment(selected_file):
        raise SyncHFError("Ledger upload is disabled by default. Use `--include-ledger` to upload `import/grinfo`.")
    if not config.include_ledger and selected_prefix and _contains_ledger_segment(selected_prefix):
        raise SyncHFError("Ledger upload is disabled by default. Use `--include-ledger` to upload `import/grinfo`.")

    targets: list[tuple[Path, str]]
    if selected_file:
        local_file = config.hf_repo_path / selected_file
        if not local_file.exists() or not local_file.is_file():
            raise SyncHFError(f"Upload file not found under local path: {local_file}")
        targets = [(local_file, selected_file)]
    elif selected_prefix:
        local_prefix = config.hf_repo_path / selected_prefix
        if not local_prefix.exists():
            raise SyncHFError(f"Upload prefix not found under local path: {local_prefix}")
        targets = [(local_prefix, selected_prefix)]
    else:
        targets = _iter_upload_targets(config.hf_repo_path, config.include_ledger)
        if not targets:
            raise SyncHFError(f"No uploadable content found in {config.hf_repo_path}")

    uploaded_files = 0
    uploaded_targets: list[tuple[str, bool]] = []
    has_directory_target = any(local_path.is_dir() for local_path, _ in targets)
    estimated_files = sum(_count_files(local_path) for local_path, _ in targets)
    use_large_folder = _should_use_large_folder(
        config=config,
        api=api,
        has_directory_target=has_directory_target,
        total_files=estimated_files,
    )

    if use_large_folder:
        allow_patterns = _patterns_from_targets(targets)
        if not allow_patterns:
            raise SyncHFError("No upload patterns resolved for upload_large_folder.")
        _upload_large_folder(
            api=api,
            repo_id=repo_id,
            folder_path=config.hf_repo_path,
            allow_patterns=allow_patterns,
            revision=config.branch,
            include_ledger=config.include_ledger,
            dry_run=config.dry_run,
            num_workers=config.large_folder_num_workers,
        )
        uploaded_files = estimated_files
        for local_path, path_in_repo in targets:
            uploaded_targets.append((path_in_repo, local_path.is_dir()))
    else:
        for local_path, path_in_repo in targets:
            uploaded_files += _upload_target(
                api=api,
                repo_id=repo_id,
                token=config.hf_token,
                local_path=local_path,
                path_in_repo=path_in_repo,
                commit_message=config.commit_message,
                revision=config.branch,
                include_ledger=config.include_ledger,
                dry_run=config.dry_run,
            )
            uploaded_targets.append((path_in_repo, local_path.is_dir()))

    if config.verify_storage and not config.dry_run and uploaded_targets:
        _verify_remote_targets(
            api=api,
            repo_id=repo_id,
            token=config.hf_token,
            targets=uploaded_targets,
        )
    return repo_id, uploaded_files


def _run_download(config: SyncHFConfig) -> tuple[str, int]:
    repo_id = resolve_hf_repo_id(config.hf_repo_id, config.hf_remote_url)
    api = _create_api(config.hf_token)

    selected_file = _normalize_repo_relpath(config.file_path)
    selected_prefix = _normalize_repo_relpath(config.prefix)
    if selected_file and selected_prefix:
        raise SyncHFError("Use only one of `--file` or `--prefix`.")
    if not config.include_ledger and selected_file and _contains_ledger_segment(selected_file):
        raise SyncHFError("Ledger download is disabled by default. Use `--include-ledger` to download `import/grinfo`.")
    if not config.include_ledger and selected_prefix and _contains_ledger_segment(selected_prefix):
        raise SyncHFError("Ledger download is disabled by default. Use `--include-ledger` to download `import/grinfo`.")

    if selected_file:
        remote_files = [selected_file]
    else:
        all_files = sorted(api.list_repo_files(repo_id=repo_id, repo_type="dataset", token=config.hf_token or None))
        if selected_prefix:
            prefix = f"{selected_prefix}/"
            remote_files = [path for path in all_files if path == selected_prefix or path.startswith(prefix)]
        else:
            remote_files = all_files
            if not config.include_ledger:
                remote_files = [path for path in remote_files if not _contains_ledger_segment(path)]

    if not remote_files:
        raise SyncHFError("No remote files matched the requested filter.")

    downloaded_files = 0
    for remote_file in remote_files:
        destination_path = config.hf_repo_path / remote_file
        if config.dry_run:
            print(f"[dry-run] download file: {remote_file} -> {destination_path}")
            downloaded_files += 1
            continue

        cached_path = hf_hub_download(
            repo_id=repo_id,
            repo_type="dataset",
            filename=remote_file,
            token=config.hf_token or None,
        )
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cached_path, destination_path)
        downloaded_files += 1

    if config.verify_storage and not config.dry_run:
        for remote_file in remote_files:
            local_file = config.hf_repo_path / remote_file
            if not local_file.exists():
                raise SyncHFError(f"Verification failed: local file missing after download: {local_file}")
    return repo_id, downloaded_files


def run_sync_hf(config: SyncHFConfig) -> None:
    mode = (config.mode or MODE_UPLOAD).strip().lower()
    if mode not in VALID_MODES:
        raise SyncHFError(f"Unsupported mode `{config.mode}`. Use `upload` or `download`.")

    if mode == MODE_UPLOAD:
        repo_id, file_count = _run_upload(config)
        print("sync-hf summary:")
        print("  mode: upload")
        if repo_id:
            print(f"  repo_id: {repo_id}")
        print(f"  files_uploaded: {file_count}")
        return

    repo_id, file_count = _run_download(config)
    print("sync-hf summary:")
    print("  mode: download")
    print(f"  repo_id: {repo_id}")
    print(f"  files_downloaded: {file_count}")


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.description = "Sync local LFS-style artifacts with Hugging Face dataset using HF Hub API."
    parser.add_argument(
        "--source-root",
        default=".",
        help="Source repository root. Used only for default path resolution.",
    )
    parser.add_argument(
        "--hf-repo-path",
        default=os.environ.get("HF_DATASET_REPO_PATH", ""),
        help="Local data directory (upload source or download destination).",
    )
    parser.add_argument(
        "--hf-repo-id",
        default=os.environ.get("HF_DATASET_REPO_ID", ""),
        help="HF dataset repo id in `namespace/name` format (optional if URL is set).",
    )
    parser.add_argument(
        "--hf-remote-url",
        default=os.environ.get("HF_DATASET_REPO_URL", ""),
        help="HF dataset URL used to infer repo id when `--hf-repo-id` is not provided.",
    )
    parser.add_argument("--hf-token", default=os.environ.get("HF_TOKEN", ""), help="HF token")
    parser.add_argument(
        "--mode",
        choices=(MODE_UPLOAD, MODE_DOWNLOAD),
        default=MODE_UPLOAD,
        help="Upload local content to HF or download remote content to local path.",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Dataset-relative directory prefix filter (upload source or download selector).",
    )
    parser.add_argument(
        "--file",
        dest="file_path",
        default="",
        help="Dataset-relative single file path (upload source or download selector).",
    )
    parser.add_argument(
        "--include-ledger",
        action="store_true",
        help="Include `import/grinfo` when uploading top-level local content.",
    )
    parser.add_argument(
        "--commit-message",
        default="sync dataset artifacts from mahgetNewGR",
        help="Commit message used by HF upload operations.",
    )
    parser.add_argument(
        "--large-folder-mode",
        choices=(LARGE_FOLDER_MODE_AUTO, LARGE_FOLDER_MODE_ALWAYS, LARGE_FOLDER_MODE_NEVER),
        default=os.environ.get("HF_UPLOAD_LARGE_FOLDER_MODE", LARGE_FOLDER_MODE_AUTO),
        help="Large-folder strategy for upload mode: auto (default), always, never.",
    )
    parser.add_argument(
        "--large-folder-threshold",
        type=int,
        default=_env_int("HF_UPLOAD_LARGE_FOLDER_THRESHOLD", DEFAULT_LARGE_FOLDER_THRESHOLD),
        help="Auto mode threshold: use upload_large_folder when file count meets/exceeds this value.",
    )
    parser.add_argument(
        "--large-folder-num-workers",
        type=int,
        default=_env_int("HF_UPLOAD_LARGE_FOLDER_NUM_WORKERS", 0),
        help="Optional worker count for upload_large_folder (0 means library default).",
    )
    parser.add_argument("--skip-push", action="store_true", help="Upload mode only: skip upload operation")
    parser.add_argument("--no-verify-storage", action="store_true", help="Skip post-operation verification checks")
    parser.add_argument("--dry-run", action="store_true", help="Print operations without uploading/downloading")

    parser.add_argument("--remote-name", default="origin", help=argparse.SUPPRESS)
    parser.add_argument("--branch", default="main", help=argparse.SUPPRESS)
    parser.add_argument(
        "--storage-backend",
        default=os.environ.get("HF_STORAGE_BACKEND", "auto"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-verify-lfs",
        dest="no_verify_storage",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def run_from_args(args: argparse.Namespace) -> int:
    mode = (args.mode or MODE_UPLOAD).strip().lower()
    if mode not in VALID_MODES:
        print(f"sync-hf error: Unsupported mode `{args.mode}`. Use `upload` or `download`.")
        return 2

    source_root = Path(args.source_root).resolve()
    repo_path_text = (args.hf_repo_path or "").strip()
    if not repo_path_text and mode == MODE_UPLOAD:
        repo_path_text = str(source_root / "LFS")

    try:
        hf_repo_path = resolve_hf_repo_path(
            repo_path_text,
            create_if_missing=(mode == MODE_DOWNLOAD),
        )
        hf_repo_id = resolve_hf_repo_id(args.hf_repo_id, args.hf_remote_url)
    except SyncHFError as exc:
        print(f"sync-hf error: {exc}")
        return 2

    config = SyncHFConfig(
        source_root=source_root,
        hf_repo_path=hf_repo_path,
        remote_name=args.remote_name,
        branch=args.branch,
        commit_message=args.commit_message,
        dry_run=args.dry_run,
        skip_push=args.skip_push,
        verify_storage=not args.no_verify_storage,
        storage_backend=args.storage_backend,
        hf_token=args.hf_token,
        hf_remote_url=args.hf_remote_url,
        hf_repo_id=hf_repo_id,
        mode=mode,
        prefix=args.prefix,
        file_path=args.file_path,
        include_ledger=args.include_ledger,
        large_folder_mode=args.large_folder_mode,
        large_folder_threshold=max(args.large_folder_threshold, 1),
        large_folder_num_workers=args.large_folder_num_workers if args.large_folder_num_workers > 0 else None,
    )
    try:
        run_sync_hf(config)
    except SyncHFError as exc:
        print(f"sync-hf error: {exc}")
        return 1
    except Exception as exc:
        print(f"sync-hf error: unexpected failure: {exc}")
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync local artifacts with Hugging Face dataset.")
    return configure_parser(parser)


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()


def main() -> None:
    load_local_env()
    args = parse_args()
    raise SystemExit(run_from_args(args))


if __name__ == "__main__":
    main()
