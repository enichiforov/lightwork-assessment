"""Log compactor: deduplicates, escalates, enriches and normalises log entries."""

from __future__ import annotations

import re
from collections.abc import Generator
from datetime import datetime
from typing import NamedTuple

_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?"
    r"$"
)
_VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class _LogEntry(NamedTuple):
    timestamp: datetime
    level: str
    fields: tuple[tuple[str, str], ...]
    raw_index: int


def _parse_timestamp(raw: str) -> datetime | None:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_line(line: str, index: int) -> _LogEntry | None:
    tokens = line.split()
    if len(tokens) < 2:
        return None

    ts = _parse_timestamp(tokens[0])
    if ts is None:
        return None

    level = tokens[1]
    if level not in _VALID_LEVELS:
        return None

    fields: dict[str, str] = {}
    for token in tokens[2:]:
        if "=" not in token:
            return None
        key, _, value = token.partition("=")
        if not key:
            return None
        fields[key] = value

    # Normalise user_id -> user
    if "user_id" in fields and "user" in fields:
        if fields["user_id"] != fields["user"]:
            return None  # conflict → malformed
        del fields["user_id"]
    elif "user_id" in fields:
        fields["user"] = fields.pop("user_id")

    # Enrichment: code 500-599 → ERROR
    code_val = fields.get("code")
    if code_val is not None:
        try:
            code_int = int(code_val)
            if 500 <= code_int <= 599:
                level = "ERROR"
        except ValueError:
            pass

    sorted_fields = tuple(sorted(fields.items()))
    return _LogEntry(ts, level, sorted_fields, index)


def _format_ts_range(start: datetime, end: datetime) -> str:
    if start == end:
        return start.isoformat()
    if start.date() == end.date():
        return f"{start.isoformat()}~{end.strftime('%H:%M:%S')}"
    return f"{start.isoformat()}~{end.isoformat()}"


def _format_entry(
    start: datetime,
    end: datetime,
    level: str,
    fields: tuple[tuple[str, str], ...],
    count: int,
) -> str:
    parts = [_format_ts_range(start, end), level]
    for key, value in fields:
        parts.append(f"{key}={value}")
    if count > 1:
        parts.append(f"(x {count})")
    return "   ".join(parts)


def compact_logs(
    file_path: str,
    dedup_window_seconds: int,
    error_threshold: int,
) -> Generator[str, None, None]:
    """Read logs from *file_path* and yield compacted log strings."""

    # Active groups keyed by (level, fields).
    # Value: [first_ts, last_ts, count, raw_index]
    groups: dict[tuple[str, tuple[tuple[str, str], ...]], list] = {}
    # Maintain insertion order so we can emit in stable order.
    order: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    def _flush_all() -> Generator[str, None, None]:
        # Sort by (first_ts, raw_index) for chronological + stable ordering.
        pending = []
        for key in order:
            if key not in groups:
                continue
            first_ts, last_ts, count, idx = groups[key]
            level, fields = key
            # Escalation
            if level == "ERROR" and count >= error_threshold:
                level = "CRITICAL"
            pending.append((first_ts, idx, level, fields, last_ts, count))
        pending.sort(key=lambda x: (x[0], x[1]))
        for first_ts, _, level, fields, last_ts, count in pending:
            yield _format_entry(first_ts, last_ts, level, fields, count)
        groups.clear()
        order.clear()

    def _try_flush_expired(
        current_ts: datetime,
    ) -> Generator[str, None, None]:
        """Flush groups whose window has expired relative to *current_ts*."""
        expired_keys: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        for key, (first_ts, last_ts, count, idx) in groups.items():
            diff = (current_ts - first_ts).total_seconds()
            if diff > dedup_window_seconds:
                expired_keys.append(key)

        if not expired_keys:
            return

        # Collect and sort expired entries.
        pending = []
        for key in expired_keys:
            first_ts, last_ts, count, idx = groups.pop(key)
            level, fields = key
            if level == "ERROR" and count >= error_threshold:
                level = "CRITICAL"
            pending.append((first_ts, idx, level, fields, last_ts, count))
        pending.sort(key=lambda x: (x[0], x[1]))

        # Rebuild order list without expired keys.
        expired_set = set(expired_keys)
        order[:] = [k for k in order if k not in expired_set]

        for first_ts, _, level, fields, last_ts, count in pending:
            yield _format_entry(first_ts, last_ts, level, fields, count)

    with open(file_path, encoding="utf-8") as fh:
        for raw_index, raw_line in enumerate(fh):
            line = raw_line.rstrip("\n\r")
            if not line:
                continue

            entry = _parse_line(line, raw_index)
            if entry is None:
                continue

            # Flush any groups that can no longer accept this entry.
            yield from _try_flush_expired(entry.timestamp)

            key = (entry.level, entry.fields)
            if key in groups:
                first_ts, last_ts, count, idx = groups[key]
                diff = (entry.timestamp - first_ts).total_seconds()
                if diff <= dedup_window_seconds:
                    groups[key] = [first_ts, entry.timestamp, count + 1, idx]
                else:
                    # Window exceeded — flush old group, start new.
                    level, fields = key
                    if level == "ERROR" and count >= error_threshold:
                        level = "CRITICAL"
                    yield _format_entry(first_ts, last_ts, level, fields, count)
                    groups[key] = [
                        entry.timestamp,
                        entry.timestamp,
                        1,
                        entry.raw_index,
                    ]
            else:
                groups[key] = [
                    entry.timestamp,
                    entry.timestamp,
                    1,
                    entry.raw_index,
                ]
                order.append(key)

    # Flush remaining groups.
    yield from _flush_all()
