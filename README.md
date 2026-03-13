# Log Compactor

A streaming log compactor that deduplicates, escalates, and enriches structured log entries.

## Requirements

Python 3.11+ · standard library only

## Usage

### As a library

```python
from compact_logs import compact_logs

for line in compact_logs("app.log", dedup_window_seconds=10, error_threshold=3):
    print(line)
```

### From the command line

```bash
python compact_logs.py app.log
python compact_logs.py app.log --window 30 --threshold 5
```

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--window SECONDS` | `60` | Deduplication window size |
| `--threshold N` | `3` | ERROR count before escalating to CRITICAL |

### Running tests

```bash
python -m unittest test_compact_logs -v
```

---

## Input format

One log entry per line, in chronological order:

```
<ISO-8601 timestamp> <LEVEL> [key=value ...]
```

Example:

```
2024-01-01T10:00:00 INFO user=alice action=login
2024-01-01T10:00:01 INFO action=login user=alice
2024-01-01T10:00:05 INFO user=alice action=login
2024-01-01T10:02:00 ERROR user=alice action=upload code=500
2024-01-01T10:02:01 ERROR action=upload user=alice code=500
2024-01-01T10:10:00 INFO user=bob action=login
```

## Output format

```
<start>[~<end>] <LEVEL> <sorted fields> [(xN)]
```

- `~<end>` omitted for single entries
- End shows only the time part when start and end share the same date
- `(xN)` omitted when count is 1

Output for the example above (`--window 10 --threshold 2`):

```
2024-01-01T10:00:00~10:00:05 INFO action=login user=alice (x3)
2024-01-01T10:02:00~10:02:01 CRITICAL action=upload code=500 user=alice (x2)
2024-01-01T10:10:00 INFO action=login user=bob
```

---

## Features

**Deduplication** — entries with identical level and fields within `--window` seconds are collapsed into one group.

**Error escalation** — when `--threshold` or more ERROR entries share the same group, the level is promoted to CRITICAL.

**Code enrichment** — any entry with `code` in the range 500–599 is overridden to ERROR before deduplication and escalation.

**Field normalisation** — `user_id` is treated as an alias for `user`. Conflicts (both present with different values) cause the line to be skipped.

**Malformed line handling** — lines missing a timestamp or level, with an unparseable timestamp, or with malformed `key=value` fields are silently skipped.

**Streaming** — reads the file line by line; memory usage is proportional to the number of distinct active groups, not file size.
