#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


STATE_FETCHED = "FETCHED"
STATE_DOWNLOAD_SUCCESS = "DOWNLOAD_SUCCESS"
STATE_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
STATE_WAYBACK_UPLOADED = "WAYBACK_UPLOADED"
STATE_WAYBACK_UPLOAD_FAILED = "WAYBACK_UPLOAD_FAILED"
STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL = "ARCHIVE_UPLOADED_WITH_WAYBACK_URL"
STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL = "ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL"
STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT = "ARCHIVE_UPLOADED_WITHOUT_DOCUMENT"

ALLOWED_STATES = {
    STATE_FETCHED,
    STATE_DOWNLOAD_SUCCESS,
    STATE_DOWNLOAD_FAILED,
    STATE_WAYBACK_UPLOADED,
    STATE_WAYBACK_UPLOAD_FAILED,
    STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
    STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT,
}

STAGE_DOWNLOAD = "download"
STAGE_WAYBACK = "wayback"
STAGE_ARCHIVE = "archive"
STAGE_NAMES = {STAGE_DOWNLOAD, STAGE_WAYBACK, STAGE_ARCHIVE}

IMMUTABLE_FIELDS = {"unique_code", "created_at_utc", "first_seen_crawl_date", "first_seen_run_type"}
MUTABLE_FIELDS = {
    "title",
    "department_name",
    "department_code",
    "gr_date",
    "source_url",
    "lfs_path",
    "state",
    "attempt_counts",
    "download",
    "wayback",
    "archive",
    "pdf_info",
    "last_seen_crawl_date",
    "updated_at_utc",
}

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    STATE_FETCHED: {STATE_FETCHED, STATE_DOWNLOAD_SUCCESS, STATE_DOWNLOAD_FAILED},
    STATE_DOWNLOAD_SUCCESS: {
        STATE_DOWNLOAD_SUCCESS,
        STATE_WAYBACK_UPLOADED,
        STATE_WAYBACK_UPLOAD_FAILED,
        STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
        STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    },
    STATE_DOWNLOAD_FAILED: {
        STATE_DOWNLOAD_FAILED,
        STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT,
    },
    STATE_WAYBACK_UPLOADED: {
        STATE_WAYBACK_UPLOADED,
        STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
    },
    STATE_WAYBACK_UPLOAD_FAILED: {
        STATE_WAYBACK_UPLOAD_FAILED,
        STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    },
    STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL: {
        STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
    },
    STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL: {
        STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    },
    STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT: {
        STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT,
        STATE_DOWNLOAD_SUCCESS,
        STATE_DOWNLOAD_FAILED,
        STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL,
        STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL,
    },
}


class LedgerError(Exception):
    pass


class RecordNotFoundError(LedgerError):
    pass


class DuplicateUniqueCodeError(LedgerError):
    pass


class InvalidTransitionError(LedgerError):
    pass


class RetryLimitExceededError(LedgerError):
    pass


class ImmutableFieldUpdateError(LedgerError):
    pass


@dataclass(frozen=True)
class RecordLocation:
    partition: str
    index: int


@dataclass(frozen=True)
class UpsertResult:
    operation: str
    partition: str
    unique_code: str


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_date(date_text: Any) -> date | None:
    if not isinstance(date_text, str):
        return None
    text = date_text.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def partition_for_gr_date(gr_date: Any) -> str:
    parsed = parse_iso_date(gr_date)
    if parsed is None:
        return "unknown"
    return str(parsed.year)


def normalize_run_type(run_type: str | None) -> str:
    if run_type not in {"daily", "monthly"}:
        return "daily"
    return run_type


def normalize_crawl_date(crawl_date: date | str | None) -> str:
    if isinstance(crawl_date, date):
        return crawl_date.isoformat()
    if isinstance(crawl_date, str):
        parsed = parse_iso_date(crawl_date)
        if parsed:
            return parsed.isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _normalize_path_text(path_value: Any) -> str:
    if path_value is None:
        return ""
    text = str(path_value).strip()
    if not text:
        return ""
    return text.replace("\\", "/")


def to_ledger_relative_path(path_value: Any) -> str:
    text = _normalize_path_text(path_value)
    if not text:
        return ""
    path = Path(text)
    if path.is_absolute():
        try:
            return path.resolve().relative_to(Path.cwd()).as_posix()
        except ValueError:
            return path.resolve().as_posix()
    return path.as_posix()


def existing_file_ledger_path(path_value: Any) -> str | None:
    text = _normalize_path_text(path_value)
    if not text:
        return None
    path = Path(text)
    probe = path if path.is_absolute() else (Path.cwd() / path)
    if not probe.exists() or not probe.is_file():
        return None
    return to_ledger_relative_path(probe)


def default_record_template(unique_code: str) -> dict[str, Any]:
    now_text = utc_now_text()
    today_text = datetime.now(timezone.utc).date().isoformat()
    return {
        "unique_code": unique_code,
        "title": "",
        "department_name": "",
        "department_code": "unknown",
        "gr_date": "",
        "source_url": "",
        "lfs_path": None,
        "state": STATE_FETCHED,
        "attempt_counts": {
            STAGE_DOWNLOAD: 0,
            STAGE_WAYBACK: 0,
            STAGE_ARCHIVE: 0,
        },
        "download": {
            "path": "",
            "status": "not_attempted",
            "hash": "",
            "size": None,
            "error": "",
        },
        "wayback": {
            "url": "",
            "content_url": "",
            "archive_time": "",
            "archive_sha1": "",
            "archive_length": None,
            "archive_mimetype": "",
            "archive_status_code": "",
            "status": "not_attempted",
            "error": "",
        },
        "archive": {
            "identifier": "",
            "url": "",
            "status": "not_attempted",
            "error": "",
        },
        "pdf_info": {
            "status": "not_attempted",
            "error": "",
            "path": "",
            "file_size": None,
            "page_count": None,
            "pages_with_images": 0,
            "has_any_page_image": False,
            "font_count": 0,
            "fonts": {},
            "unresolved_word_count": 0,
            "language": {
                "inferred": "unknown",
                "script_word_counts": {},
                "total_words": 0,
            },
        },
        "first_seen_crawl_date": today_text,
        "last_seen_crawl_date": today_text,
        "first_seen_run_type": "daily",
        "created_at_utc": now_text,
        "updated_at_utc": now_text,
    }


class StateMachine:
    @staticmethod
    def validate_state(state: str) -> None:
        if state not in ALLOWED_STATES:
            raise InvalidTransitionError(f"Unknown state: {state}")

    @staticmethod
    def validate_transition(current_state: str, next_state: str) -> None:
        StateMachine.validate_state(current_state)
        StateMachine.validate_state(next_state)
        allowed = ALLOWED_TRANSITIONS[current_state]
        if next_state not in allowed:
            raise InvalidTransitionError(f"Invalid transition: {current_state} -> {next_state}")

    @staticmethod
    def next_state_for_stage(
        *,
        current_state: str,
        stage: str,
        success: bool,
        has_wayback_url: bool | None = None,
        has_document: bool = True,
    ) -> str:
        StateMachine.validate_state(current_state)
        if stage not in STAGE_NAMES:
            raise InvalidTransitionError(f"Unknown stage: {stage}")

        if stage == STAGE_DOWNLOAD:
            if current_state == STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT:
                return STATE_DOWNLOAD_SUCCESS if success else STATE_DOWNLOAD_FAILED
            return STATE_DOWNLOAD_SUCCESS if success else STATE_DOWNLOAD_FAILED

        if stage == STAGE_WAYBACK:
            return STATE_WAYBACK_UPLOADED if success else STATE_WAYBACK_UPLOAD_FAILED

        if stage == STAGE_ARCHIVE:
            if not success:
                return current_state
            if not has_document:
                return STATE_ARCHIVE_UPLOADED_WITHOUT_DOCUMENT
            if has_wayback_url:
                return STATE_ARCHIVE_UPLOADED_WITH_WAYBACK_URL
            return STATE_ARCHIVE_UPLOADED_WITHOUT_WAYBACK_URL

        return current_state


class LedgerStore:
    def __init__(self, ledger_dir: str | Path = "import/grinfo") -> None:
        self.ledger_dir = Path(ledger_dir).resolve()
        self.ledger_dir.mkdir(parents=True, exist_ok=True)
        self._partition_cache: dict[str, list[dict[str, Any]]] = {}
        self._index: dict[str, RecordLocation] = {}
        self.refresh_index()

    def refresh_index(self) -> None:
        self._partition_cache = {}
        self._index = {}
        for file_path in sorted(self.ledger_dir.glob("*.jsonl")):
            partition = file_path.stem
            rows = self._read_partition(partition)
            for index, row in enumerate(rows):
                unique_code = row.get("unique_code")
                if not isinstance(unique_code, str) or not unique_code:
                    continue
                if unique_code in self._index:
                    existing = self._index[unique_code]
                    raise DuplicateUniqueCodeError(
                        f"Duplicate unique_code={unique_code} in partitions {existing.partition} and {partition}"
                    )
                self._index[unique_code] = RecordLocation(partition=partition, index=index)

    def _reindex_partition(self, partition: str) -> None:
        stale_codes = [code for code, location in self._index.items() if location.partition == partition]
        for code in stale_codes:
            del self._index[code]

        rows = self._read_partition(partition)
        for index, row in enumerate(rows):
            unique_code = row.get("unique_code")
            if not isinstance(unique_code, str) or not unique_code:
                continue
            existing = self._index.get(unique_code)
            if existing is not None and existing.partition != partition:
                raise DuplicateUniqueCodeError(
                    f"Duplicate unique_code={unique_code} in partitions {existing.partition} and {partition}"
                )
            self._index[unique_code] = RecordLocation(partition=partition, index=index)

    def list_partitions(self) -> list[str]:
        return sorted(self._partition_cache.keys() | {path.stem for path in self.ledger_dir.glob("*.jsonl")})

    def iter_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for partition in self.list_partitions():
            rows = self._read_partition(partition)
            for row in rows:
                records.append(copy.deepcopy(row))
        return records

    def exists(self, unique_code: str) -> bool:
        return unique_code in self._index

    def find(self, unique_code: str) -> dict[str, Any] | None:
        location = self._index.get(unique_code)
        if location is None:
            return None
        rows = self._read_partition(location.partition)
        return copy.deepcopy(rows[location.index])

    def upsert(
        self,
        record: dict[str, Any],
        *,
        run_type: str | None = None,
        crawl_date: date | str | None = None,
    ) -> UpsertResult:
        unique_code = record.get("unique_code")
        if not isinstance(unique_code, str) or not unique_code.strip():
            raise LedgerError("upsert requires non-empty record['unique_code']")
        unique_code = unique_code.strip()

        now_text = utc_now_text()
        normalized_run_type = normalize_run_type(run_type)
        normalized_crawl_date = normalize_crawl_date(crawl_date)

        location = self._index.get(unique_code)
        if location is None:
            new_record = default_record_template(unique_code)
            self._apply_mutable_update(new_record, record, is_insert=True)
            new_record["first_seen_run_type"] = normalized_run_type
            new_record["first_seen_crawl_date"] = normalized_crawl_date
            new_record["last_seen_crawl_date"] = normalized_crawl_date
            new_record["created_at_utc"] = now_text
            new_record["updated_at_utc"] = now_text
            self._validate_attempt_counts(new_record)
            self._validate_state(new_record)

            target_partition = partition_for_gr_date(new_record.get("gr_date"))
            rows = self._read_partition(target_partition)
            rows.append(new_record)
            rows.sort(key=lambda item: str(item.get("unique_code", "")))
            self._write_partition(target_partition, rows)
            self._reindex_partition(target_partition)
            return UpsertResult(operation="inserted", partition=target_partition, unique_code=unique_code)

        source_partition = location.partition
        rows = self._read_partition(source_partition)
        current_record = rows[location.index]
        updated_record = copy.deepcopy(current_record)
        self._apply_mutable_update(updated_record, record, is_insert=False)

        if updated_record.get("state") != current_record.get("state"):
            StateMachine.validate_transition(
                str(current_record.get("state", STATE_FETCHED)),
                str(updated_record.get("state")),
            )

        if normalized_run_type == "monthly":
            existing_last_seen = parse_iso_date(updated_record.get("last_seen_crawl_date"))
            candidate_last_seen = parse_iso_date(normalized_crawl_date)
            if candidate_last_seen and (existing_last_seen is None or candidate_last_seen > existing_last_seen):
                updated_record["last_seen_crawl_date"] = candidate_last_seen.isoformat()

        self._validate_attempt_counts_non_decreasing(current_record, updated_record)
        updated_record["updated_at_utc"] = now_text
        self._validate_attempt_counts(updated_record)
        self._validate_state(updated_record)

        target_partition = partition_for_gr_date(updated_record.get("gr_date"))

        if target_partition == source_partition:
            rows[location.index] = updated_record
            rows.sort(key=lambda item: str(item.get("unique_code", "")))
            self._write_partition(source_partition, rows)
            self._reindex_partition(source_partition)
            return UpsertResult(operation="updated", partition=target_partition, unique_code=unique_code)

        source_rows = [row for row in rows if row.get("unique_code") != unique_code]
        target_rows = self._read_partition(target_partition)
        target_rows = [row for row in target_rows if row.get("unique_code") != unique_code]
        target_rows.append(updated_record)
        source_rows.sort(key=lambda item: str(item.get("unique_code", "")))
        target_rows.sort(key=lambda item: str(item.get("unique_code", "")))

        self._write_partition(source_partition, source_rows)
        self._write_partition(target_partition, target_rows)
        self._reindex_partition(source_partition)
        self._reindex_partition(target_partition)
        return UpsertResult(operation="moved", partition=target_partition, unique_code=unique_code)

    def apply_stage_result(
        self,
        *,
        unique_code: str,
        stage: str,
        success: bool,
        metadata: dict[str, Any] | None = None,
        error: str = "",
        has_document: bool = True,
        has_wayback_url: bool | None = None,
    ) -> UpsertResult:
        if stage not in STAGE_NAMES:
            raise LedgerError(f"Unknown stage: {stage}")

        existing = self.find(unique_code)
        if existing is None:
            raise RecordNotFoundError(f"Record not found: {unique_code}")

        record = copy.deepcopy(existing)
        metadata = metadata or {}
        attempts = record.setdefault("attempt_counts", {})
        current_attempts = attempts.get(stage, 0)
        if not isinstance(current_attempts, int):
            raise LedgerError(f"attempt_counts.{stage} must be int")
        if current_attempts >= 2:
            raise RetryLimitExceededError(
                f"Retry limit exceeded for unique_code={unique_code} stage={stage}"
            )
        attempts[stage] = current_attempts + 1

        if stage == STAGE_DOWNLOAD:
            download = record.setdefault("download", {})
            if success:
                download["status"] = "success"
                download["error"] = ""
                if "path" in metadata:
                    download["path"] = metadata["path"]
                if "hash" in metadata:
                    download["hash"] = metadata["hash"]
                if "size" in metadata:
                    download["size"] = metadata["size"]
                record["lfs_path"] = existing_file_ledger_path(download.get("path"))
            else:
                download["status"] = "failed"
                download["error"] = error or metadata.get("error", "download_failed")

        elif stage == STAGE_WAYBACK:
            wayback = record.setdefault("wayback", {})
            if success:
                wayback["status"] = "success"
                wayback["error"] = ""
                for key in (
                    "url",
                    "content_url",
                    "archive_time",
                    "archive_sha1",
                    "archive_length",
                    "archive_mimetype",
                    "archive_status_code",
                ):
                    if key in metadata:
                        wayback[key] = metadata[key]
            else:
                wayback["status"] = "failed"
                wayback["error"] = error or metadata.get("error", "wayback_upload_failed")

        elif stage == STAGE_ARCHIVE:
            archive = record.setdefault("archive", {})
            if success:
                archive["status"] = "success"
                archive["error"] = ""
                for key in ("identifier", "url"):
                    if key in metadata:
                        archive[key] = metadata[key]
            else:
                archive["status"] = "failed"
                archive["error"] = error or metadata.get("error", "archive_upload_failed")

        if stage == STAGE_ARCHIVE and has_wayback_url is None:
            wayback_url = str(record.get("wayback", {}).get("url", "")).strip()
            has_wayback_url = bool(wayback_url)

        next_state = StateMachine.next_state_for_stage(
            current_state=record.get("state", STATE_FETCHED),
            stage=stage,
            success=success,
            has_wayback_url=has_wayback_url,
            has_document=has_document,
        )
        StateMachine.validate_transition(record.get("state", STATE_FETCHED), next_state)
        record["state"] = next_state
        record["updated_at_utc"] = utc_now_text()

        return self.upsert(record)

    def _validate_state(self, record: dict[str, Any]) -> None:
        state = record.get("state")
        if not isinstance(state, str):
            raise LedgerError("record['state'] must be string")
        StateMachine.validate_state(state)

    def _validate_attempt_counts(self, record: dict[str, Any]) -> None:
        attempts = record.get("attempt_counts")
        if not isinstance(attempts, dict):
            raise LedgerError("record['attempt_counts'] must be object")
        for stage in STAGE_NAMES:
            value = attempts.get(stage, 0)
            if not isinstance(value, int):
                raise LedgerError(f"attempt_counts.{stage} must be int")
            if value < 0 or value > 2:
                raise LedgerError(f"attempt_counts.{stage} must be in [0,2], got {value}")

    def _validate_attempt_counts_non_decreasing(
        self,
        current_record: dict[str, Any],
        updated_record: dict[str, Any],
    ) -> None:
        current_attempts = current_record.get("attempt_counts", {})
        updated_attempts = updated_record.get("attempt_counts", {})
        if not isinstance(current_attempts, dict) or not isinstance(updated_attempts, dict):
            return
        for stage in STAGE_NAMES:
            current_value = current_attempts.get(stage, 0)
            updated_value = updated_attempts.get(stage, 0)
            if isinstance(current_value, int) and isinstance(updated_value, int) and updated_value < current_value:
                raise LedgerError(
                    f"attempt_counts.{stage} cannot decrease ({current_value} -> {updated_value})"
                )

    def _apply_mutable_update(self, target: dict[str, Any], incoming: dict[str, Any], *, is_insert: bool) -> None:
        for key, value in incoming.items():
            if key in IMMUTABLE_FIELDS:
                if is_insert:
                    target[key] = copy.deepcopy(value)
                else:
                    existing = target.get(key)
                    if existing is None or existing == "":
                        target[key] = copy.deepcopy(value)
                    elif value != existing:
                        raise ImmutableFieldUpdateError(f"Cannot change immutable field: {key}")
                continue

            if key in MUTABLE_FIELDS:
                if value is not None or key == "lfs_path":
                    target[key] = copy.deepcopy(value)

    def _partition_path(self, partition: str) -> Path:
        return self.ledger_dir / f"{partition}.jsonl"

    def _read_partition(self, partition: str) -> list[dict[str, Any]]:
        if partition in self._partition_cache:
            return self._partition_cache[partition]

        file_path = self._partition_path(partition)
        rows: list[dict[str, Any]] = []
        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    obj = json.loads(text)
                    if isinstance(obj, dict):
                        rows.append(obj)
        self._partition_cache[partition] = rows
        return rows

    def _write_partition(self, partition: str, rows: list[dict[str, Any]]) -> None:
        file_path = self._partition_path(partition)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = file_path.with_name(f".{file_path.name}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
        temp_path.replace(file_path)
        self._partition_cache[partition] = rows
