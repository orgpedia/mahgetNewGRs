#!/usr/bin/env python3

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


IMPORT_CONFIG_RELATIVE_PATH = Path("import") / "import_config.yaml"
DEFAULT_HF_DATASET_REPO_PATH = "LFS/mahGRs"
DEFAULT_HF_UPLOAD_LARGE_FOLDER_MODE = "auto"
DEFAULT_HF_UPLOAD_LARGE_FOLDER_THRESHOLD = 100
_INT_RE = re.compile(r"^-?\d+$")


@dataclass(frozen=True)
class HFConfig:
    dataset_repo_url: str = ""
    dataset_repo_id: str = ""
    dataset_repo_path: str = DEFAULT_HF_DATASET_REPO_PATH
    upload_large_folder_mode: str = DEFAULT_HF_UPLOAD_LARGE_FOLDER_MODE
    upload_large_folder_threshold: int = DEFAULT_HF_UPLOAD_LARGE_FOLDER_THRESHOLD


@dataclass(frozen=True)
class ImportConfig:
    attempt_thresholds: dict[str, int]
    hf: HFConfig


def _find_config_file(start_dir: Path) -> Path | None:
    current = start_dir.resolve()
    while True:
        candidate = current / IMPORT_CONFIG_RELATIVE_PATH
        if candidate.exists() and candidate.is_file():
            return candidate
        if current.parent == current:
            return None
        current = current.parent


def _parse_scalar(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if _INT_RE.match(text):
        try:
            return int(text)
        except ValueError:
            return text
    return text


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    current_map: dict[str, Any] | None = None

    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.lstrip()
        if stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(stripped)
        if indent not in {0, 2}:
            continue
        if ":" not in stripped:
            continue

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        value_text = raw_value.strip()
        if not key:
            continue

        if indent == 0:
            if not value_text:
                section: dict[str, Any] = {}
                root[key] = section
                current_map = section
            else:
                root[key] = _parse_scalar(value_text)
                current_map = None
            continue

        if current_map is None:
            continue
        current_map[key] = _parse_scalar(value_text)

    return root


def _as_string(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _as_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        if _INT_RE.match(text):
            try:
                return int(text)
            except ValueError:
                return default
    return default


def _as_int_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, raw_value in value.items():
        key_text = _as_string(key)
        if not key_text:
            continue
        result[key_text] = _as_int(raw_value, 0)
    return result


def load_import_config(*, start_dir: Path | None = None) -> ImportConfig:
    config_path = _find_config_file(start_dir or Path.cwd())
    raw: dict[str, Any] = {}
    if config_path is not None:
        try:
            raw = _parse_simple_yaml(config_path.read_text(encoding="utf-8"))
        except OSError:
            raw = {}

    hf_raw = raw.get("hf")
    if not isinstance(hf_raw, dict):
        hf_raw = {}

    dataset_repo_path = _as_string(hf_raw.get("dataset_repo_path"), DEFAULT_HF_DATASET_REPO_PATH)
    upload_mode = _as_string(hf_raw.get("upload_large_folder_mode"), DEFAULT_HF_UPLOAD_LARGE_FOLDER_MODE)
    upload_threshold = max(
        1,
        _as_int(hf_raw.get("upload_large_folder_threshold"), DEFAULT_HF_UPLOAD_LARGE_FOLDER_THRESHOLD),
    )
    hf = HFConfig(
        dataset_repo_url=_as_string(hf_raw.get("dataset_repo_url")),
        dataset_repo_id=_as_string(hf_raw.get("dataset_repo_id")),
        dataset_repo_path=dataset_repo_path,
        upload_large_folder_mode=upload_mode or DEFAULT_HF_UPLOAD_LARGE_FOLDER_MODE,
        upload_large_folder_threshold=upload_threshold,
    )
    return ImportConfig(
        attempt_thresholds=_as_int_dict(raw.get("attempt_thresholds")),
        hf=hf,
    )
