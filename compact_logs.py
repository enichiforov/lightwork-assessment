"""
Log compactor: deduplicates, escalates, enriches and normalises log entries.

Assumptions:
- Level is any non-empty sequence of uppercase ASCII letters. The spec states
  "includes but is not limited to DEBUG, INFO, WARNING, ERROR".
- Dedup window is inclusive: (last_ts - first_ts).total_seconds() <= window.
"""

import argparse
import dataclasses
import re
import sys
from collections.abc import Generator
from datetime import datetime
from enum import StrEnum


class Level(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


_Fields = tuple[tuple[str, str], ...]
_GroupKey = tuple[str, _Fields]


@dataclasses.dataclass(slots=True)
class _Group:
    first_ts: datetime
    last_ts: datetime
    count: int
    raw_index: int


def _parse_timestamp(raw: str) -> datetime | None:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_line(line: str, index: int) -> tuple[datetime, str, _Fields] | None:
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
                level = Level.ERROR
        except ValueError:
            pass

    return ts, level, tuple(sorted(fields.items()))


def _format_ts_range(start: datetime, end: datetime) -> str:
    if start == end:
        return start.isoformat()
    if start.date() == end.date():
        return f"{start.isoformat()}~{end.strftime('%H:%M:%S')}"
    return f"{start.isoformat()}~{end.isoformat()}"


def _format_entry(start: datetime, end: datetime, level: str, fields: _Fields, count: int) -> str:
    parts = [_format_ts_range(start, end), level, *(f"{k}={v}" for k, v in fields)]
    if count > 1:
        parts.append(f"(x{count})")
    return " ".join(parts)


def compact_logs(
    file_path: str,
    dedup_window_seconds: int,
    error_threshold: int,
) -> Generator[str, None, None]:
    """Read logs from *file_path* and yield compacted log strings."""

    groups: dict[_GroupKey, _Group] = {}
    order: list[_GroupKey] = []

    def _emit(key: _GroupKey, g: _Group) -> str:
        level, fields = key
        if level == Level.ERROR and g.count >= error_threshold:
            level = Level.CRITICAL
        return _format_entry(g.first_ts, g.last_ts, level, fields, g.count)

    def _emit_sorted(keys: list[_GroupKey]) -> Generator[str, None, None]:
        pending = sorted(keys, key=lambda k: (groups[k].first_ts, groups[k].raw_index))
        for key in pending:
            yield _emit(key, groups[key])

    def _flush_expired(current_ts: datetime) -> Generator[str, None, None]:
        expired = [
            k for k, g in groups.items()
            if (current_ts - g.first_ts).total_seconds() > dedup_window_seconds
        ]
        if not expired:
            return
        yield from _emit_sorted(expired)
        expired_set = set(expired)
        for k in expired:
            del groups[k]
        order[:] = [k for k in order if k not in expired_set]

    def _flush_all() -> Generator[str, None, None]:
        if groups:
            yield from _emit_sorted(order)
            groups.clear()
            order.clear()

    with open(file_path, encoding="utf-8") as fh:
        for raw_index, raw_line in enumerate(fh):
            line = raw_line.rstrip("\n\r")
            if not line:
                continue

            parsed = _parse_line(line, raw_index)
            if parsed is None:
                continue

            ts, level, fields = parsed
            yield from _flush_expired(ts)

            key: _GroupKey = (level, fields)
            if key in groups:
                g = groups[key]
                g.last_ts = ts
                g.count += 1
            else:
                groups[key] = _Group(first_ts=ts, last_ts=ts, count=1, raw_index=raw_index)
                order.append(key)

    yield from _flush_all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compact a structured log file by deduplicating and escalating entries."
    )
    parser.add_argument("file_path", help="Path to the log file")
    parser.add_argument(
        "--window",
        type=int,
        default=60,
        metavar="SECONDS",
        help="Deduplication window in seconds (default: 60)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=3,
        metavar="N",
        help="Error count threshold for CRITICAL escalation (default: 3)",
    )
    args = parser.parse_args()

    try:
        for entry in compact_logs(args.file_path, args.window, args.threshold):
            print(entry)
    except FileNotFoundError:
        print(f"error: file not found: {args.file_path}", file=sys.stderr)
        sys.exit(1)
