#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path


_LOADED_PATHS: set[Path] = set()


def _find_env_file(filename: str, start_dir: Path) -> Path | None:
    current = start_dir
    while True:
        candidate = current / filename
        if candidate.exists() and candidate.is_file():
            return candidate
        if current.parent == current:
            return None
        current = current.parent


def _parse_assignment(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    if text.startswith("export "):
        text = text[len("export ") :].strip()
    if "=" not in text:
        return None
    key, raw_value = text.split("=", 1)
    key = key.strip()
    if not key:
        return None
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return key, value


def load_local_env(
    *,
    filename: str = ".env",
    start_dir: Path | None = None,
    override: bool = False,
) -> Path | None:
    base_dir = (start_dir or Path.cwd()).resolve()
    env_path = _find_env_file(filename, base_dir)
    if env_path is None:
        return None
    if env_path in _LOADED_PATHS:
        return env_path

    for line in env_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_assignment(line)
        if parsed is None:
            continue
        key, value = parsed
        if not override and key in os.environ:
            continue
        os.environ[key] = value

    _LOADED_PATHS.add(env_path)
    return env_path
