#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Iterable, Sequence, TypeVar

from info_store import InfoStore as LedgerStore


@dataclass(frozen=True)
class StageRecord:
    unique_code: str
    record: dict[str, Any]


TReport = TypeVar("TReport")


@dataclass
class JobRunResult(Generic[TReport]):
    report: TReport
    fatal_error: str = ""

    @property
    def is_fatal(self) -> bool:
        return bool(self.fatal_error)


def chunked(items: Sequence[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


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


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    # Handle plain ISO date and ISO datetime values.
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        candidate = text[:10]
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def is_record_within_lookback(record: dict[str, Any], lookback_days: int) -> bool:
    days = max(0, int(lookback_days))
    if days == 0:
        return True

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
    for field_name in ("last_seen_crawl_date", "first_seen_crawl_date", "gr_date"):
        parsed = _parse_date(record.get(field_name))
        if parsed is not None and parsed >= cutoff:
            return True
    return False


def filter_stage_records(
    store: LedgerStore,
    *,
    allowed_states: set[str],
    stage: str,
    code_filter: set[str] | None = None,
    max_attempts: int = 2,
    lookback_days: int = 0,
) -> list[StageRecord]:
    selected: list[StageRecord] = []

    codes = code_filter or set()
    for record in store.iter_records():
        unique_code = str(record.get("unique_code", "")).strip()
        if not unique_code:
            continue
        if codes and unique_code not in codes:
            continue
        if not is_record_within_lookback(record, lookback_days):
            continue
        state = str(record.get("state", "")).strip()
        # if allowed_states and state not in allowed_states:
        #     continue
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


def sha1_file(file_path: Path) -> str:
    digest = hashlib.sha1()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest().upper()


def print_stage_report(
    label: str,
    *,
    selected: int,
    processed: int,
    success: int,
    failed: int,
    skipped: int,
    service_failures: int,
    stopped_early: bool,
    extras: dict[str, Any] | None = None,
) -> None:
    print(f"{label}:")
    print(f"  selected: {selected}")
    print(f"  processed: {processed}")
    print(f"  success: {success}")
    print(f"  failed: {failed}")
    print(f"  skipped: {skipped}")
    for key, value in (extras or {}).items():
        print(f"  {key}: {value}")
    print(f"  service_failures: {service_failures}")
    print(f"  stopped_early: {stopped_early}")
