# Log Compactor

A streaming log compactor that deduplicates, escalates, and enriches structured log entries.

## Usage

```python
from compact_logs import compact_logs

for line in compact_logs("app.log", dedup_window_seconds=10, error_threshold=3):
    print(line)
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `file_path` | `str` | Path to the log file |
| `dedup_window_seconds` | `int` | Max seconds between duplicate entries to group them |
| `error_threshold` | `int` | Min ERROR count in a group to escalate to CRITICAL |

### Input format

One log entry per line:

```
<ISO-8601 timestamp> <LEVEL> [key=value ...]
```

Fields are unordered. The file must be in chronological order. No timezone support.

### Output format

```
<start>[~<end>] <LEVEL> <sorted fields> [(xN)]
```

- End timestamp omitted if there is only one entry in the group
- End timestamp shows only the time portion when start and end share the same date
- `(xN)` count omitted for single entries

### Example

Input:

```
2024-01-01T10:00:00 INFO user=alice action=login
2024-01-01T10:00:01 INFO action=login user=alice
2024-01-01T10:00:05 INFO user=alice action=login
2024-01-01T10:02:00 ERROR user=alice action=upload code=500
2024-01-01T10:02:01 ERROR action=upload user=alice code=500
2024-01-01T10:10:00 INFO user=bob action=login
```

Output (window=10, threshold=2):

```
2024-01-01T10:00:00~10:00:05 INFO action=login user=alice (x3)
2024-01-01T10:02:00~10:02:01 CRITICAL action=upload code=500 user=alice (x2)
2024-01-01T10:10:00 INFO action=login user=bob
```

## Features

**Deduplication** — Entries with identical level and fields within `dedup_window_seconds` are collapsed into one.

**Error escalation** — When `error_threshold` or more ERROR entries share the same group within the window, the group is promoted to CRITICAL.

**Code enrichment** — Any entry with `code` in the range 500–599 is overridden to ERROR before deduplication and escalation.

**Field normalization** — `user_id` is treated as an alias for `user`. Conflicts (both present with different values) cause the line to be skipped.

**Malformed line handling** — Lines missing a timestamp or level, with an unparseable timestamp, or with malformed `key=value` fields are silently skipped.

**Streaming** — Reads the file line by line; memory usage is proportional to the number of distinct active groups, not file size.

## Running tests

```bash
python -m unittest test_compact_logs -v
```

Requires Python 3.10+ (standard library only, no dependencies).
