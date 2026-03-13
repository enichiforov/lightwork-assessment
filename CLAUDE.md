# LightWork AI Python Assessment — Log Compactor

## Task
Build a "log compactor" — a single Python file `compact_logs.py` with function:

```python
def compact_logs(file_path: str, dedup_window_seconds: int, error_threshold: int) -> Generator[str, None, None]:
```

## Requirements

### Input
- Plain text file, one log entry per line, chronological order
- Format: `<ISO-8601 timestamp> <LEVEL> <key=value> <key=value> ...`
- LEVEL: DEBUG, INFO, WARNING, ERROR (all-caps)
- Fields unordered, not all present on every line, some lines malformed

### Rules to implement

1. **Deduplication** — Logs are duplicates if within `dedup_window_seconds`, same LEVEL, same key/value fields (excluding timestamp). Compact into single entry with `first_ts~last_ts` and `(x N)` count (omit if N==1). Output fields sorted alphabetically by key.

2. **Error escalation** — If `error_threshold` or more ERROR logs with same key/value group in same dedup window → escalate to CRITICAL.

3. **Enrichment** — If `code` field has int value 500-599, override level to ERROR before dedup/escalation (regardless of original level).

4. **Field normalization** — `user_id` is alias for `user`. Treat as equivalent for dedup. Output always `user` (never `user_id`). If both present with different values → malformed, skip.

5. **Ordering** — Chronological by earliest timestamp of group. Same start timestamp → stable (maintain input order).

6. **Malformed handling** — Skip lines with: missing timestamp/level, unparseable timestamp, fields not in key=value format, user/user_id conflict.

### Output format
```
<start_timestamp>[~<end_timestamp>]   <LEVEL>   <sorted fields>   [(x N)]
```
- If dates same: `2024-01-01T10:00:00~10:00:05` (end = time only)
- If dates differ: `2024-01-01T10:00:00~2024-01-02T10:00:05` (full timestamp)
- Separator between columns: 3 spaces

### Constraints
- Single `.py` file
- Python standard library only (no external deps)
- Must return generator of strings (not print)
- Type annotations required
- Memory efficient (streaming, support large files)
- Idiomatic modern Python, readable, maintainable
- Latest stable Python

### Example
Input:
```
2024-01-01T10:00:00   INFO   user=alice   action=login
2024-01-01T10:00:01   INFO   action=login   user=alice
2024-01-01T10:00:05   INFO   user=alice   action=login
2024-01-01T10:02:00   ERROR  user=alice   action=upload   code=500
2024-01-01T10:02:01   ERROR  action=upload   user=alice   code=500
2024-01-01T10:10:00   INFO   user=bob   action=login
```

Output (with dedup_window_seconds=10, error_threshold=2):
```
2024-01-01T10:00:00~10:00:05   INFO   action=login   user=alice   (x 3)
2024-01-01T10:02:00~10:02:01   CRITICAL   action=upload   code=500   user=alice   (x 2)
2024-01-01T10:10:00   INFO   action=login   user=bob
```

## GSD Mode
Use /gsd:quick — implement fast, clean, correct. Include comprehensive tests in a separate test file.
