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


class _LogCompactor:
    def __init__(self, dedup_window_seconds: int, error_threshold: int) -> None:
        self._window = dedup_window_seconds
        self._threshold = error_threshold
        self._groups: dict[_GroupKey, _Group] = {}
        self._order: list[_GroupKey] = []

    @classmethod
    def process_file(
        cls, file_path: str, dedup_window_seconds: int, error_threshold: int
    ) -> Generator[str, None, None]:
        compactor = cls(dedup_window_seconds, error_threshold)
        with open(file_path, encoding="utf-8") as fh:
            for raw_index, raw_line in enumerate(fh):
                parsed = cls._parse_line(raw_line.rstrip("\n\r"), raw_index)
                if parsed is None:
                    continue
                yield from compactor._feed(*parsed, raw_index=raw_index)
        yield from compactor._flush_all()

    # ── Stateful instance methods ────────────────────────────────────────

    def _feed(
        self, ts: datetime, level: str, fields: _Fields, *, raw_index: int
    ) -> Generator[str, None, None]:
        yield from self._flush_expired(ts)
        key: _GroupKey = (level, fields)
        if key in self._groups:
            g = self._groups[key]
            g.last_ts = ts
            g.count += 1
        else:
            self._groups[key] = _Group(first_ts=ts, last_ts=ts, count=1, raw_index=raw_index)
            self._order.append(key)

    def _flush_expired(self, current_ts: datetime) -> Generator[str, None, None]:
        expired = [
            k for k, g in self._groups.items()
            if (current_ts - g.first_ts).total_seconds() > self._window
        ]
        if not expired:
            return
        yield from self._emit_sorted(expired)
        expired_set = set(expired)
        for k in expired:
            del self._groups[k]
        self._order[:] = [k for k in self._order if k not in expired_set]

    def _flush_all(self) -> Generator[str, None, None]:
        if self._groups:
            yield from self._emit_sorted(self._order)
            self._groups.clear()
            self._order.clear()

    def _emit_sorted(self, keys: list[_GroupKey]) -> Generator[str, None, None]:
        for key in sorted(keys, key=lambda k: (self._groups[k].first_ts, self._groups[k].raw_index)):
            g = self._groups[key]
            level, fields = key
            if level == Level.ERROR and g.count >= self._threshold:
                level = Level.CRITICAL
            yield self._render(g.first_ts, g.last_ts, level, fields, g.count)

    # ── Pure static helpers ──────────────────────────────────────────────

    @staticmethod
    def _parse_line(line: str, index: int) -> tuple[datetime, str, _Fields] | None:
        tokens = line.split()
        if len(tokens) < 2:
            return None

        ts: datetime | None = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                ts = datetime.strptime(tokens[0], fmt)
                break
            except ValueError:
                continue
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

        if "user_id" in fields and "user" in fields:
            if fields["user_id"] != fields["user"]:
                return None
            del fields["user_id"]
        elif "user_id" in fields:
            fields["user"] = fields.pop("user_id")

        code_val = fields.get("code")
        if code_val is not None:
            try:
                if 500 <= int(code_val) <= 599:
                    level = Level.ERROR
            except ValueError:
                pass

        return ts, level, tuple(sorted(fields.items()))

    @staticmethod
    def _render(start: datetime, end: datetime, level: str, fields: _Fields, count: int) -> str:
        if start == end:
            ts_range = start.isoformat()
        elif start.date() == end.date():
            ts_range = f"{start.isoformat()}~{end.strftime('%H:%M:%S')}"
        else:
            ts_range = f"{start.isoformat()}~{end.isoformat()}"

        parts = [ts_range, level, *(f"{k}={v}" for k, v in fields)]
        if count > 1:
            parts.append(f"(x{count})")
        return " ".join(parts)


def compact_logs(
    file_path: str,
    dedup_window_seconds: int,
    error_threshold: int,
) -> Generator[str, None, None]:
    """Read logs from *file_path* and yield compacted log strings."""
    return _LogCompactor.process_file(file_path, dedup_window_seconds, error_threshold)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compact a structured log file by deduplicating and escalating entries."
    )
    parser.add_argument("file_path", help="Path to the log file")
    parser.add_argument("--window", type=int, default=60, metavar="SECONDS",
                        help="Deduplication window in seconds (default: 60)")
    parser.add_argument("--threshold", type=int, default=3, metavar="N",
                        help="Error count threshold for CRITICAL escalation (default: 3)")
    args = parser.parse_args()

    try:
        for entry in compact_logs(args.file_path, args.window, args.threshold):
            print(entry)
    except FileNotFoundError:
        print(f"error: file not found: {args.file_path}", file=sys.stderr)
        sys.exit(1)
