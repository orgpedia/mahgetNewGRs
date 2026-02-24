#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ledger_engine import (
    DuplicateUniqueCodeError,
    ImmutableFieldUpdateError,
    InvalidTransitionError,
    RecordNotFoundError,
    RetryLimitExceededError,
    StateMachine,
    UpsertResult,
    normalize_crawl_date,
    normalize_run_type,
    partition_for_gr_date,
    utc_now_text,
)


URL_NS = "urlinfos"
UPLOAD_NS = "uploadinfos"
PDF_NS = "pdfinfos"

REQUIRED_UPLOAD_STAGE_OBJECTS = ("download", "wayback", "archive")


@dataclass(frozen=True)
class RecordLocation:
    partition: str
    index: int


def _namespace_sort_key(file_path: Path) -> tuple[int, int, str]:
    stem = file_path.stem
    if stem.isdigit():
        return (0, -int(stem), stem)
    if stem == "unknown":
        return (2, 0, stem)
    return (1, 0, stem)


def _deepcopy_obj(value: Any) -> Any:
    return copy.deepcopy(value)


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _to_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    text = _normalize_text(value)
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


class InfoStore:
    """
    Split-ledger store for:
      - import/urlinfos/*.jsonl
      - import/uploadinfos/*.jsonl
      - import/pdfinfos/*.jsonl

    It presents a compatibility read model similar to the old monolithic ledger
    so existing stage code can be cut over incrementally.
    """

    def __init__(
        self,
        ledger_dir: str | Path = "import/grinfo",
        *,
        partitions: set[str] | None = None,
    ) -> None:
        requested = Path(ledger_dir).resolve()
        self.root_dir = self._resolve_root_dir(requested)
        self._partition_filter = self._normalize_partition_filter(partitions)
        self.url_dir = self.root_dir / URL_NS
        self.upload_dir = self.root_dir / UPLOAD_NS
        self.pdf_dir = self.root_dir / PDF_NS
        # Compatibility attribute expected by callers.
        self.ledger_dir = self.root_dir

        self.url_dir.mkdir(parents=True, exist_ok=True)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

        self._partition_cache: dict[str, dict[str, list[dict[str, Any]]]] = {
            URL_NS: {},
            UPLOAD_NS: {},
            PDF_NS: {},
        }
        self._index: dict[str, dict[str, RecordLocation]] = {
            URL_NS: {},
            UPLOAD_NS: {},
            PDF_NS: {},
        }
        self.refresh_index()

    @staticmethod
    def _normalize_partition_filter(partitions: set[str] | None) -> set[str] | None:
        if partitions is None:
            return None
        normalized = {str(value).strip() for value in partitions if str(value).strip()}
        return normalized or None

    def _is_partition_allowed(self, partition: str) -> bool:
        if self._partition_filter is None:
            return True
        return partition in self._partition_filter

    @staticmethod
    def _resolve_root_dir(requested: Path) -> Path:
        if (requested / URL_NS).exists() or (requested / UPLOAD_NS).exists() or (requested / PDF_NS).exists():
            return requested
        if requested.name in {"grinfo", URL_NS, UPLOAD_NS, PDF_NS}:
            candidate = requested.parent
            if (
                (candidate / URL_NS).exists()
                or (candidate / UPLOAD_NS).exists()
                or (candidate / PDF_NS).exists()
                or requested.name == "grinfo"
            ):
                return candidate
        return requested

    def _ns_dir(self, namespace: str) -> Path:
        if namespace == URL_NS:
            return self.url_dir
        if namespace == UPLOAD_NS:
            return self.upload_dir
        if namespace == PDF_NS:
            return self.pdf_dir
        raise ValueError(f"Unknown namespace: {namespace}")

    def _partition_path(self, namespace: str, partition: str) -> Path:
        return self._ns_dir(namespace) / f"{partition}.jsonl"

    def _row_key(self, row: dict[str, Any]) -> str:
        for field in ("record_key", "unique_code"):
            value = row.get(field)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _read_partition(self, namespace: str, partition: str) -> list[dict[str, Any]]:
        cache = self._partition_cache[namespace]
        if partition in cache:
            return cache[partition]

        path = self._partition_path(namespace, partition)
        rows: list[dict[str, Any]] = []
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    obj = json.loads(text)
                    if isinstance(obj, dict):
                        rows.append(obj)
        cache[partition] = rows
        return rows

    def _write_partition(self, namespace: str, partition: str, rows: list[dict[str, Any]]) -> None:
        path = self._partition_path(namespace, partition)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
        temp_path.replace(path)
        self._partition_cache[namespace][partition] = rows

    def _list_partitions_for_ns(self, namespace: str) -> list[str]:
        path = self._ns_dir(namespace)
        return sorted(
            {item.stem for item in path.glob("*.jsonl") if self._is_partition_allowed(item.stem)},
            key=lambda name: _namespace_sort_key(Path(f"{name}.jsonl")),
        )

    def list_partitions(self) -> list[str]:
        return self._list_partitions_for_ns(URL_NS)

    @classmethod
    def list_url_partitions(cls, ledger_dir: str | Path = "import/grinfo") -> list[str]:
        requested = Path(ledger_dir).resolve()
        root_dir = cls._resolve_root_dir(requested)
        url_dir = root_dir / URL_NS
        return sorted(
            {item.stem for item in url_dir.glob("*.jsonl")},
            key=lambda name: _namespace_sort_key(Path(f"{name}.jsonl")),
        )

    def refresh_index(self) -> None:
        for ns in (URL_NS, UPLOAD_NS, PDF_NS):
            self._partition_cache[ns] = {}
            self._index[ns] = {}
            for file_path in sorted(self._ns_dir(ns).glob("*.jsonl"), key=_namespace_sort_key):
                partition = file_path.stem
                if not self._is_partition_allowed(partition):
                    continue
                rows = self._read_partition(ns, partition)
                for idx, row in enumerate(rows):
                    key = self._row_key(row)
                    if not key:
                        continue
                    if key in self._index[ns]:
                        existing = self._index[ns][key]
                        raise DuplicateUniqueCodeError(
                            f"Duplicate record_key={key} in {ns} partitions "
                            f"{existing.partition} and {partition}"
                        )
                    self._index[ns][key] = RecordLocation(partition=partition, index=idx)

    def _reindex_partition(self, namespace: str, partition: str) -> None:
        ns_index = self._index[namespace]
        stale = [key for key, loc in ns_index.items() if loc.partition == partition]
        for key in stale:
            del ns_index[key]
        rows = self._read_partition(namespace, partition)
        for idx, row in enumerate(rows):
            key = self._row_key(row)
            if not key:
                continue
            existing = ns_index.get(key)
            if existing is not None and existing.partition != partition:
                raise DuplicateUniqueCodeError(
                    f"Duplicate record_key={key} in {namespace} partitions "
                    f"{existing.partition} and {partition}"
                )
            ns_index[key] = RecordLocation(partition=partition, index=idx)

    def _find_row(self, namespace: str, record_key: str) -> dict[str, Any] | None:
        location = self._index[namespace].get(record_key)
        if location is None:
            return None
        rows = self._read_partition(namespace, location.partition)
        return _deepcopy_obj(rows[location.index])

    def exists(self, unique_code: str) -> bool:
        key = _normalize_text(unique_code)
        return key in self._index[URL_NS]

    def _default_url_row(self, record_key: str, now_text: str, crawl_date: str, run_type: str) -> dict[str, Any]:
        return {
            "record_key": record_key,
            "unique_code": record_key,
            "title": "",
            "department_name": "",
            "department_code": "unknown",
            "gr_date": "",
            "source_url": "",
            "first_seen_crawl_date": crawl_date,
            "last_seen_crawl_date": crawl_date,
            "first_seen_run_type": run_type,
            "created_at_utc": now_text,
            "updated_at_utc": now_text,
        }

    def _default_upload_row(self, record_key: str, now_text: str) -> dict[str, Any]:
        return {
            "record_key": record_key,
            "state": "FETCHED",
            "download": {
                "status": "not_attempted",
                "error": "",
                "attempts": 0,
            },
            "wayback": {
                "status": "not_attempted",
                "url": "",
                "content_url": "",
                "archive_time": "",
                "archive_sha1": "",
                "archive_length": None,
                "archive_mimetype": "",
                "archive_status_code": "",
                "error": "",
                "attempts": 0,
            },
            "archive": {
                "status": "not_attempted",
                "identifier": "",
                "url": "",
                "error": "",
                "attempts": 0,
            },
            "hf": {
                "status": "not_attempted",
                "path": None,
                "hash": None,
                "backend": None,
                "commit_hash": None,
                "error": None,
                "synced_at_utc": None,
                "attempts": 0,
            },
            "created_at_utc": now_text,
            "updated_at_utc": now_text,
        }

    def _apply_url_patch(self, target: dict[str, Any], incoming: dict[str, Any], *, is_insert: bool) -> None:
        mutable = {
            "title",
            "department_name",
            "department_code",
            "gr_date",
            "source_url",
            "last_seen_crawl_date",
        }
        immutable = {"record_key", "unique_code", "created_at_utc", "first_seen_crawl_date", "first_seen_run_type"}
        for key, value in incoming.items():
            if key in immutable and not is_insert:
                existing = target.get(key)
                if existing not in (None, "", value):
                    raise ImmutableFieldUpdateError(f"Cannot change immutable field: {key}")
                continue
            if key in mutable and value is not None:
                target[key] = _deepcopy_obj(value)

    def _upload_attempt(self, upload_row: dict[str, Any], stage: str) -> int:
        obj = upload_row.get(stage, {})
        if isinstance(obj, dict):
            return _to_int(obj.get("attempts"), 0)
        return 0

    def _apply_upload_patch(self, target: dict[str, Any], incoming: dict[str, Any], now_text: str) -> None:
        if "state" in incoming and incoming.get("state") is not None:
            target["state"] = _normalize_text(incoming.get("state")) or target.get("state", "FETCHED")

        if "attempt_counts" in incoming and isinstance(incoming.get("attempt_counts"), dict):
            attempts = incoming["attempt_counts"]
            for stage in REQUIRED_UPLOAD_STAGE_OBJECTS:
                stage_obj = target.setdefault(stage, {})
                if isinstance(stage_obj, dict):
                    stage_obj["attempts"] = _to_int(attempts.get(stage), _to_int(stage_obj.get("attempts"), 0))
            hf_obj = target.setdefault("hf", {})
            if isinstance(hf_obj, dict):
                hf_stage_value = attempts.get("hf", attempts.get("lfs", hf_obj.get("attempts", 0)))
                hf_obj["attempts"] = _to_int(hf_stage_value, _to_int(hf_obj.get("attempts"), 0))

        for stage in ("download", "wayback", "archive", "hf"):
            value = incoming.get(stage)
            if isinstance(value, dict):
                stage_obj = target.setdefault(stage, {})
                if isinstance(stage_obj, dict):
                    for key, stage_value in value.items():
                        stage_obj[key] = _deepcopy_obj(stage_value)

        if "lfs_path" in incoming:
            hf_obj = target.setdefault("hf", {})
            if isinstance(hf_obj, dict):
                path_value = incoming.get("lfs_path")
                normalized = _normalize_text(path_value) if path_value is not None else ""
                hf_obj["path"] = normalized if normalized else None
                hf_obj["status"] = "success" if hf_obj["path"] else "not_attempted"
                hf_obj["synced_at_utc"] = now_text if hf_obj["path"] else None

    def _has_upload_patch(self, record: dict[str, Any]) -> bool:
        return any(key in record for key in ("state", "download", "wayback", "archive", "hf", "lfs_path", "attempt_counts"))

    def _pdf_row_from_pdf_info(
        self,
        record_key: str,
        pdf_info: Any,
        now_text: str,
        created_at: str,
        updated_at: str | None = None,
    ) -> dict[str, Any]:
        _ = updated_at
        base = {
            "record_key": record_key,
            "status": "not_attempted",
            "created_at_utc": created_at,
        }
        if not isinstance(pdf_info, dict):
            return base
        status = _normalize_text(pdf_info.get("status")) or "not_attempted"
        if status == "not_attempted":
            return base
        row = dict(base)
        row["status"] = status
        row["error"] = pdf_info.get("error")
        for key in (
            "file_size",
            "page_count",
            "pages_with_images",
            "has_any_page_image",
            "total_font_count",
            "fonts",
            "unresolved_word_count",
            "language",
        ):
            row[key] = _deepcopy_obj(pdf_info.get(key))
        return row

    def _upsert_namespace_row(self, namespace: str, record_key: str, partition: str, row: dict[str, Any]) -> None:
        loc = self._index[namespace].get(record_key)
        if loc is None:
            rows = self._read_partition(namespace, partition)
            rows.append(row)
            rows.sort(key=lambda item: self._row_key(item))
            self._write_partition(namespace, partition, rows)
            self._reindex_partition(namespace, partition)
            return

        if loc.partition == partition:
            rows = self._read_partition(namespace, partition)
            rows[loc.index] = row
            rows.sort(key=lambda item: self._row_key(item))
            self._write_partition(namespace, partition, rows)
            self._reindex_partition(namespace, partition)
            return

        source_rows = self._read_partition(namespace, loc.partition)
        source_rows = [item for item in source_rows if self._row_key(item) != record_key]
        self._write_partition(namespace, loc.partition, source_rows)
        self._reindex_partition(namespace, loc.partition)

        target_rows = self._read_partition(namespace, partition)
        target_rows = [item for item in target_rows if self._row_key(item) != record_key]
        target_rows.append(row)
        target_rows.sort(key=lambda item: self._row_key(item))
        self._write_partition(namespace, partition, target_rows)
        self._reindex_partition(namespace, partition)

    def _merge_record(
        self,
        record_key: str,
        url_row: dict[str, Any],
        upload_row: dict[str, Any] | None,
        pdf_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = _deepcopy_obj(url_row)
        row["record_key"] = record_key
        row["unique_code"] = _normalize_text(url_row.get("unique_code")) or record_key

        if upload_row is None:
            upload_row = self._default_upload_row(record_key, _normalize_text(url_row.get("updated_at_utc")) or utc_now_text())
        else:
            upload_row = _deepcopy_obj(upload_row)

        state = _normalize_text(upload_row.get("state")) or "FETCHED"
        download = upload_row.get("download") if isinstance(upload_row.get("download"), dict) else {}
        wayback = upload_row.get("wayback") if isinstance(upload_row.get("wayback"), dict) else {}
        archive = upload_row.get("archive") if isinstance(upload_row.get("archive"), dict) else {}
        hf = upload_row.get("hf") if isinstance(upload_row.get("hf"), dict) else {}

        download = dict(download)
        if "path" not in download:
            path = hf.get("path")
            download["path"] = path if isinstance(path, str) else ""
        if "hash" not in download:
            hash_value = hf.get("hash")
            download["hash"] = hash_value if isinstance(hash_value, str) else ""
        if "size" not in download:
            download["size"] = None

        row["state"] = state
        row["download"] = download
        row["wayback"] = wayback
        row["archive"] = archive
        row["hf"] = hf
        path_value = hf.get("path")
        row["lfs_path"] = path_value if isinstance(path_value, str) and path_value.strip() else None
        row["attempt_counts"] = {
            "download": _to_int(download.get("attempts"), 0),
            "wayback": _to_int(wayback.get("attempts"), 0),
            "archive": _to_int(archive.get("attempts"), 0),
        }

        if pdf_row is None:
            row["pdf_info"] = {"status": "not_attempted"}
        else:
            status = _normalize_text(pdf_row.get("status")) or "not_attempted"
            if status == "not_attempted":
                row["pdf_info"] = {"status": "not_attempted"}
            else:
                info = {
                    "status": status,
                    "error": pdf_row.get("error"),
                    "file_size": pdf_row.get("file_size"),
                    "page_count": pdf_row.get("page_count"),
                    "pages_with_images": pdf_row.get("pages_with_images"),
                    "has_any_page_image": pdf_row.get("has_any_page_image"),
                    "total_font_count": pdf_row.get("total_font_count"),
                    "fonts": pdf_row.get("fonts"),
                    "unresolved_word_count": pdf_row.get("unresolved_word_count"),
                    "language": pdf_row.get("language"),
                }
                row["pdf_info"] = info
        return row

    def iter_records(self) -> list[dict[str, Any]]:
        keys = sorted(self._index[URL_NS].keys())
        records: list[dict[str, Any]] = []
        for key in keys:
            found = self.find(key)
            if found is not None:
                records.append(found)
        return records

    def find(self, unique_code: str) -> dict[str, Any] | None:
        record_key = _normalize_text(unique_code)
        if not record_key:
            return None
        url_row = self._find_row(URL_NS, record_key)
        if url_row is None:
            return None
        upload_row = self._find_row(UPLOAD_NS, record_key)
        pdf_row = self._find_row(PDF_NS, record_key)
        return self._merge_record(record_key, url_row, upload_row, pdf_row)

    def insert(
        self,
        record: dict[str, Any],
        *,
        run_type: str | None = None,
        crawl_date: Any = None,
    ) -> UpsertResult:
        record_key = _normalize_text(record.get("record_key") or record.get("unique_code"))
        if not record_key:
            raise ValueError("insert requires record_key/unique_code")
        if record_key in self._index[URL_NS]:
            raise DuplicateUniqueCodeError(f"Record already exists: {record_key}")

        now_text = utc_now_text()
        normalized_run_type = normalize_run_type(run_type)
        normalized_crawl_date = normalize_crawl_date(crawl_date)

        url_row = self._default_url_row(record_key, now_text, normalized_crawl_date, normalized_run_type)
        self._apply_url_patch(url_row, record, is_insert=True)
        partition = partition_for_gr_date(url_row.get("gr_date"))
        self._upsert_namespace_row(URL_NS, record_key, partition, url_row)

        if self._has_upload_patch(record):
            upload_row = self._default_upload_row(record_key, now_text)
            self._apply_upload_patch(upload_row, record, now_text)
            self._upsert_namespace_row(UPLOAD_NS, record_key, partition, upload_row)

        if "pdf_info" in record:
            pdf_row = self._pdf_row_from_pdf_info(
                record_key,
                record.get("pdf_info"),
                now_text,
                created_at=url_row.get("created_at_utc", now_text),
            )
            self._upsert_namespace_row(PDF_NS, record_key, partition, pdf_row)

        return UpsertResult(operation="inserted", partition=partition, unique_code=record_key)

    def update(
        self,
        record: dict[str, Any],
        *,
        run_type: str | None = None,
        crawl_date: Any = None,
    ) -> UpsertResult:
        results = self.update_many([record], run_type=run_type, crawl_date=crawl_date)
        if not results:
            raise ValueError("update requires one record")
        return results[0]

    def update_many(
        self,
        records: list[dict[str, Any]],
        *,
        run_type: str | None = None,
        crawl_date: Any = None,
    ) -> list[UpsertResult]:
        if not records:
            return []

        now_text = utc_now_text()
        normalized_run_type = normalize_run_type(run_type)
        normalized_crawl_date = normalize_crawl_date(crawl_date)
        touched_partitions: set[tuple[str, str]] = set()
        results: list[UpsertResult] = []

        try:
            for record in records:
                record_key = _normalize_text(record.get("record_key") or record.get("unique_code"))
                if not record_key:
                    raise ValueError("update_many requires record_key/unique_code for every record")

                url_loc = self._index[URL_NS].get(record_key)
                if url_loc is None:
                    raise RecordNotFoundError(f"Record not found: {record_key}")

                url_rows = self._read_partition(URL_NS, url_loc.partition)
                if url_loc.index >= len(url_rows):
                    raise RecordNotFoundError(f"Record not found in URL partition index: {record_key}")
                current_url_row = url_rows[url_loc.index]
                if self._row_key(current_url_row) != record_key:
                    raise RecordNotFoundError(f"URL index mismatch for record_key={record_key}")

                url_row = _deepcopy_obj(current_url_row)
                self._apply_url_patch(url_row, record, is_insert=False)
                if normalized_run_type == "monthly":
                    existing_last_seen = _normalize_text(url_row.get("last_seen_crawl_date"))
                    if normalized_crawl_date and (not existing_last_seen or normalized_crawl_date > existing_last_seen):
                        url_row["last_seen_crawl_date"] = normalized_crawl_date
                target_partition = partition_for_gr_date(url_row.get("gr_date"))
                if target_partition != url_loc.partition:
                    raise InvalidTransitionError(
                        "update_many does not support repartition. "
                        f"record_key={record_key} current_partition={url_loc.partition} target_partition={target_partition}"
                    )

                url_rows[url_loc.index] = url_row
                touched_partitions.add((URL_NS, target_partition))

                has_upload_patch = self._has_upload_patch(record)
                upload_loc = self._index[UPLOAD_NS].get(record_key)
                upload_rows: list[dict[str, Any]] | None = None
                upload_row: dict[str, Any] | None = None
                if upload_loc is not None:
                    if upload_loc.partition != target_partition:
                        raise InvalidTransitionError(
                            "update_many does not support upload repartition. "
                            f"record_key={record_key} upload_partition={upload_loc.partition} target_partition={target_partition}"
                        )
                    upload_rows = self._read_partition(UPLOAD_NS, upload_loc.partition)
                    if upload_loc.index >= len(upload_rows):
                        raise RecordNotFoundError(f"Record not found in upload partition index: {record_key}")
                    current_upload_row = upload_rows[upload_loc.index]
                    if self._row_key(current_upload_row) != record_key:
                        raise RecordNotFoundError(f"Upload index mismatch for record_key={record_key}")
                    upload_row = _deepcopy_obj(current_upload_row)

                if has_upload_patch or upload_row is not None:
                    if upload_row is None:
                        upload_row = self._default_upload_row(record_key, now_text)
                    self._apply_upload_patch(upload_row, record, now_text)

                    if upload_loc is None:
                        upload_rows = self._read_partition(UPLOAD_NS, target_partition)
                        upload_rows.append(upload_row)
                        self._index[UPLOAD_NS][record_key] = RecordLocation(
                            partition=target_partition,
                            index=len(upload_rows) - 1,
                        )
                    else:
                        assert upload_rows is not None
                        upload_rows[upload_loc.index] = upload_row

                    touched_partitions.add((UPLOAD_NS, target_partition))

                if "pdf_info" in record:
                    pdf_loc = self._index[PDF_NS].get(record_key)
                    created_at = _normalize_text(url_row.get("created_at_utc")) or now_text
                    if pdf_loc is None:
                        pdf_row = self._pdf_row_from_pdf_info(record_key, record.get("pdf_info"), now_text, created_at)
                        pdf_rows = self._read_partition(PDF_NS, target_partition)
                        pdf_rows.append(pdf_row)
                        self._index[PDF_NS][record_key] = RecordLocation(
                            partition=target_partition,
                            index=len(pdf_rows) - 1,
                        )
                    else:
                        if pdf_loc.partition != target_partition:
                            raise InvalidTransitionError(
                                "update_many does not support pdf repartition. "
                                f"record_key={record_key} pdf_partition={pdf_loc.partition} target_partition={target_partition}"
                            )
                        pdf_rows = self._read_partition(PDF_NS, pdf_loc.partition)
                        if pdf_loc.index >= len(pdf_rows):
                            raise RecordNotFoundError(f"Record not found in pdf partition index: {record_key}")
                        current_pdf_row = pdf_rows[pdf_loc.index]
                        if self._row_key(current_pdf_row) != record_key:
                            raise RecordNotFoundError(f"PDF index mismatch for record_key={record_key}")
                        current_created_at = _normalize_text(current_pdf_row.get("created_at_utc"))
                        if current_created_at:
                            created_at = current_created_at
                        pdf_row = self._pdf_row_from_pdf_info(
                            record_key,
                            record.get("pdf_info"),
                            now_text,
                            created_at,
                            updated_at=_normalize_text(current_pdf_row.get("updated_at_utc")) or created_at,
                        )
                        pdf_rows[pdf_loc.index] = pdf_row

                    touched_partitions.add((PDF_NS, target_partition))
                else:
                    current_pdf_loc = self._index[PDF_NS].get(record_key)
                    if current_pdf_loc is not None and current_pdf_loc.partition != target_partition:
                        raise InvalidTransitionError(
                            "update_many does not support pdf repartition without pdf_info patch. "
                            f"record_key={record_key} pdf_partition={current_pdf_loc.partition} target_partition={target_partition}"
                        )

                results.append(UpsertResult(operation="updated", partition=target_partition, unique_code=record_key))

            for namespace, partition in sorted(touched_partitions):
                rows = self._read_partition(namespace, partition)
                self._write_partition(namespace, partition, rows)
        except Exception:
            # Rebuild in-memory index/cache from disk to discard staged in-memory mutations.
            self.refresh_index()
            raise

        return results

    def upsert(
        self,
        record: dict[str, Any],
        *,
        run_type: str | None = None,
        crawl_date: Any = None,
    ) -> UpsertResult:
        record_key = _normalize_text(record.get("record_key") or record.get("unique_code"))
        if not record_key:
            raise ValueError("upsert requires record_key/unique_code")

        now_text = utc_now_text()
        normalized_run_type = normalize_run_type(run_type)
        normalized_crawl_date = normalize_crawl_date(crawl_date)

        existing_url = self._find_row(URL_NS, record_key)
        if existing_url is None:
            url_row = self._default_url_row(record_key, now_text, normalized_crawl_date, normalized_run_type)
            self._apply_url_patch(url_row, record, is_insert=True)
            partition = partition_for_gr_date(url_row.get("gr_date"))
            self._upsert_namespace_row(URL_NS, record_key, partition, url_row)

            has_upload_patch = any(key in record for key in ("state", "download", "wayback", "archive", "hf", "lfs_path", "attempt_counts"))
            if has_upload_patch:
                upload_row = self._default_upload_row(record_key, now_text)
                self._apply_upload_patch(upload_row, record, now_text)
                self._upsert_namespace_row(UPLOAD_NS, record_key, partition, upload_row)

            if "pdf_info" in record:
                pdf_row = self._pdf_row_from_pdf_info(
                    record_key,
                    record.get("pdf_info"),
                    now_text,
                    created_at=url_row.get("created_at_utc", now_text),
                )
                self._upsert_namespace_row(PDF_NS, record_key, partition, pdf_row)

            return UpsertResult(operation="inserted", partition=partition, unique_code=record_key)

        url_row = _deepcopy_obj(existing_url)
        self._apply_url_patch(url_row, record, is_insert=False)
        if normalized_run_type == "monthly":
            existing_last_seen = _normalize_text(url_row.get("last_seen_crawl_date"))
            if normalized_crawl_date and (not existing_last_seen or normalized_crawl_date > existing_last_seen):
                url_row["last_seen_crawl_date"] = normalized_crawl_date
        target_partition = partition_for_gr_date(url_row.get("gr_date"))
        self._upsert_namespace_row(URL_NS, record_key, target_partition, url_row)

        upload_row = self._find_row(UPLOAD_NS, record_key)
        has_upload_patch = any(key in record for key in ("state", "download", "wayback", "archive", "hf", "lfs_path", "attempt_counts"))
        if has_upload_patch or upload_row is not None:
            if upload_row is None:
                upload_row = self._default_upload_row(record_key, now_text)
            self._apply_upload_patch(upload_row, record, now_text)
            self._upsert_namespace_row(UPLOAD_NS, record_key, target_partition, upload_row)

        if "pdf_info" in record:
            current_pdf = self._find_row(PDF_NS, record_key)
            created_at = _normalize_text((current_pdf or {}).get("created_at_utc")) or _normalize_text(url_row.get("created_at_utc")) or now_text
            existing_updated_at = _normalize_text((current_pdf or {}).get("updated_at_utc")) or created_at
            pdf_row = self._pdf_row_from_pdf_info(
                record_key,
                record.get("pdf_info"),
                now_text,
                created_at,
                updated_at=existing_updated_at,
            )
            self._upsert_namespace_row(PDF_NS, record_key, target_partition, pdf_row)
        else:
            current_pdf = self._find_row(PDF_NS, record_key)
            if current_pdf is not None:
                self._upsert_namespace_row(PDF_NS, record_key, target_partition, current_pdf)

        return UpsertResult(operation="updated", partition=target_partition, unique_code=record_key)

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
        record_key = _normalize_text(unique_code)
        existing = self.find(record_key)
        if existing is None:
            raise RecordNotFoundError(f"Record not found: {record_key}")

        metadata = metadata or {}
        url_partition = self._index[URL_NS][record_key].partition
        now_text = utc_now_text()

        upload_row = self._find_row(UPLOAD_NS, record_key)
        if upload_row is None:
            created_at = _normalize_text(existing.get("created_at_utc")) or now_text
            upload_row = self._default_upload_row(record_key, created_at)

        stage_obj = upload_row.get(stage)
        if not isinstance(stage_obj, dict):
            stage_obj = {}
            upload_row[stage] = stage_obj
        current_attempts = _to_int(stage_obj.get("attempts"), 0)
        if current_attempts >= 2:
            raise RetryLimitExceededError(f"Retry limit exceeded for unique_code={record_key} stage={stage}")
        stage_obj["attempts"] = current_attempts + 1

        if stage == "download":
            if success:
                stage_obj["status"] = "success"
                stage_obj["error"] = ""
                hf_obj = upload_row.setdefault("hf", {})
                if isinstance(hf_obj, dict):
                    if "path" in metadata:
                        path_value = _normalize_text(metadata.get("path"))
                        hf_obj["path"] = path_value or None
                        hf_obj["status"] = "success" if hf_obj["path"] else hf_obj.get("status", "not_attempted")
                        hf_obj["synced_at_utc"] = now_text if hf_obj.get("path") else hf_obj.get("synced_at_utc")
                    if "hash" in metadata:
                        hash_text = _normalize_text(metadata.get("hash"))
                        hf_obj["hash"] = hash_text or None
            else:
                stage_obj["status"] = "failed"
                stage_obj["error"] = error or _normalize_text(metadata.get("error")) or "download_failed"
        elif stage == "wayback":
            if success:
                stage_obj["status"] = "success"
                stage_obj["error"] = ""
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
                        stage_obj[key] = _deepcopy_obj(metadata[key])
            else:
                stage_obj["status"] = "failed"
                stage_obj["error"] = error or _normalize_text(metadata.get("error")) or "wayback_upload_failed"
        elif stage == "archive":
            if success:
                stage_obj["status"] = "success"
                stage_obj["error"] = ""
                for key in ("identifier", "url"):
                    if key in metadata:
                        stage_obj[key] = _deepcopy_obj(metadata[key])
            else:
                stage_obj["status"] = "failed"
                stage_obj["error"] = error or _normalize_text(metadata.get("error")) or "archive_upload_failed"
        else:
            raise InvalidTransitionError(f"Unknown stage: {stage}")

        if stage == "archive" and has_wayback_url is None:
            wayback_obj = upload_row.get("wayback", {})
            wayback_url = _normalize_text(wayback_obj.get("url") if isinstance(wayback_obj, dict) else "")
            has_wayback_url = bool(wayback_url)

        current_state = _normalize_text(upload_row.get("state")) or "FETCHED"
        next_state = StateMachine.next_state_for_stage(
            current_state=current_state,
            stage=stage,
            success=success,
            has_wayback_url=has_wayback_url,
            has_document=has_document,
        )
        #StateMachine.validate_transition(current_state, next_state)
        #upload_row["state"] = next_state

        self._upsert_namespace_row(UPLOAD_NS, record_key, url_partition, upload_row)
        return UpsertResult(operation="updated", partition=url_partition, unique_code=record_key)
