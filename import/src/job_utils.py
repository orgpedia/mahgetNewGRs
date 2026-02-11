#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ledger_engine import LedgerStore


@dataclass(frozen=True)
class StageRecord:
    unique_code: str
    record: dict[str, Any]


def load_code_filter(codes: Iterable[str], codes_file: str | None) -> set[str]:
    code_set: set[str] = set()
    for code in codes:
        value = str(code).strip()
        if value:
            code_set.add(value)

    if not codes_file:
        return code_set

    path = Path(codes_file).resolve()
    if not path.exists():
        raise FileNotFoundError(f"codes file not found: {path}")

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            for item in data:
                value = str(item).strip()
                if value:
                    code_set.add(value)
        elif isinstance(data, dict):
            for key in data.keys():
                value = str(key).strip()
                if value:
                    code_set.add(value)
        else:
            raise ValueError(f"Unsupported JSON structure in codes file: {path}")
    else:
        for line in path.read_text(encoding="utf-8").splitlines():
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            code_set.add(value)

    return code_set


def parse_state_list(values: Iterable[str] | None) -> set[str]:
    if not values:
        return set()
    return {value.strip() for value in values if value and value.strip()}


def filter_stage_records(
    store: LedgerStore,
    *,
    allowed_states: set[str],
    stage: str,
    code_filter: set[str] | None = None,
    max_attempts: int = 2,
) -> list[StageRecord]:
    selected: list[StageRecord] = []
    codes = code_filter or set()
    for record in store.iter_records():
        unique_code = str(record.get("unique_code", "")).strip()
        if not unique_code:
            continue
        if codes and unique_code not in codes:
            continue
        state = str(record.get("state", "")).strip()
        if allowed_states and state not in allowed_states:
            continue
        attempts = record.get("attempt_counts", {})
        stage_attempts = attempts.get(stage, 0) if isinstance(attempts, dict) else 0
        if isinstance(stage_attempts, int) and stage_attempts >= max_attempts:
            continue
        selected.append(StageRecord(unique_code=unique_code, record=record))
    selected.sort(key=lambda item: item.unique_code)
    return selected


def detect_service_failure(status_code: int | None, exception: Exception | None = None) -> bool:
    if exception is not None:
        return True
    if status_code is None:
        return False
    if status_code == 429:
        return True
    return status_code >= 500


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
