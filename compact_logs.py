"""
Log compactor: deduplicates, escalates, enriches and normalises log entries.

Assumptions:
- A "level" token is any non-empty all-uppercase ASCII letter sequence.
  The spec says "includes but is not limited to DEBUG, INFO, WARNING, ERROR".
- Dedup window boundary is inclusive: diff <= dedup_window_seconds groups entries.
- Separator between output tokens is a single space.
"""

import re
from collections.abc import Generator
from datetime import datetime
from typing import NamedTuple


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
    if not re.fullmatch(r"[A-Z]+", level):
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
            if 500 <= int(code_val) <= 599:
                level = "ERROR"
        except ValueError:
            pass

    return _LogEntry(ts, level, tuple(sorted(fields.items())), index)


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
    parts.extend(f"{k}={v}" for k, v in fields)
    if count > 1:
        parts.append(f"(x{count})")
    return " ".join(parts)


def compact_logs(
    file_path: str,
    dedup_window_seconds: int,
    error_threshold: int,
) -> Generator[str, None, None]:
    """Read logs from *file_path* and yield compacted log strings."""

    # Active groups keyed by (level, fields).
    # Value: [first_ts, last_ts, count, raw_index]
    groups: dict[tuple[str, tuple[tuple[str, str], ...]], list] = {}
    # Insertion order for stable chronological output.
    order: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    def _emit(
        first_ts: datetime,
        last_ts: datetime,
        level: str,
        fields: tuple[tuple[str, str], ...],
        count: int,
    ) -> str:
        if level == "ERROR" and count >= error_threshold:
            level = "CRITICAL"
        return _format_entry(first_ts, last_ts, level, fields, count)

    def _flush_all() -> Generator[str, None, None]:
        pending = []
        for key in order:
            if key not in groups:
                continue
            first_ts, last_ts, count, idx = groups[key]
            level, fields = key
            pending.append((first_ts, idx, last_ts, level, fields, count))
        pending.sort(key=lambda x: (x[0], x[1]))
        for first_ts, _, last_ts, level, fields, count in pending:
            yield _emit(first_ts, last_ts, level, fields, count)
        groups.clear()
        order.clear()

    def _flush_expired(current_ts: datetime) -> Generator[str, None, None]:
        expired_keys = [
            key
            for key, (first_ts, *_) in groups.items()
            if (current_ts - first_ts).total_seconds() > dedup_window_seconds
        ]
        if not expired_keys:
            return

        pending = []
        for key in expired_keys:
            first_ts, last_ts, count, idx = groups.pop(key)
            level, fields = key
            pending.append((first_ts, idx, last_ts, level, fields, count))
        pending.sort(key=lambda x: (x[0], x[1]))

        expired_set = set(expired_keys)
        order[:] = [k for k in order if k not in expired_set]

        for first_ts, _, last_ts, level, fields, count in pending:
            yield _emit(first_ts, last_ts, level, fields, count)

    with open(file_path, encoding="utf-8") as fh:
        for raw_index, raw_line in enumerate(fh):
            line = raw_line.rstrip("\n\r")
            if not line:
                continue

            entry = _parse_line(line, raw_index)
            if entry is None:
                continue

            yield from _flush_expired(entry.timestamp)

            key = (entry.level, entry.fields)
            if key in groups:
                first_ts, last_ts, count, idx = groups[key]
                diff = (entry.timestamp - first_ts).total_seconds()
                if diff <= dedup_window_seconds:
                    groups[key] = [first_ts, entry.timestamp, count + 1, idx]
                else:
                    # Window exceeded — flush old group, start fresh.
                    yield _emit(first_ts, last_ts, entry.level, entry.fields, count)
                    groups[key] = [entry.timestamp, entry.timestamp, 1, entry.raw_index]
            else:
                groups[key] = [entry.timestamp, entry.timestamp, 1, entry.raw_index]
                order.append(key)

    yield from _flush_all()
