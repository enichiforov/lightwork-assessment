"""Comprehensive tests for compact_logs."""

import os
import tempfile
import unittest

from compact_logs import compact_logs


def _run(lines: list[str], window: int = 10, threshold: int = 2) -> list[str]:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".log", delete=False, encoding="utf-8"
    ) as f:
        f.write("\n".join(lines) + "\n")
        path = f.name
    try:
        return list(compact_logs(path, window, threshold))
    finally:
        os.unlink(path)


class TestBasicDeduplication(unittest.TestCase):
    def test_example_from_spec(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice   action=login",
            "2024-01-01T10:00:01   INFO   action=login   user=alice",
            "2024-01-01T10:00:05   INFO   user=alice   action=login",
            "2024-01-01T10:02:00   ERROR  user=alice   action=upload   code=500",
            "2024-01-01T10:02:01   ERROR  action=upload   user=alice   code=500",
            "2024-01-01T10:10:00   INFO   user=bob   action=login",
        ]
        result = _run(lines, window=10, threshold=2)
        self.assertEqual(len(result), 3)
        self.assertIn("action=login", result[0])
        self.assertIn("user=alice", result[0])
        self.assertIn("(x 3)", result[0])
        self.assertIn("10:00:00~10:00:05", result[0])
        self.assertIn("INFO", result[0])

        self.assertIn("CRITICAL", result[1])
        self.assertIn("(x 2)", result[1])
        self.assertIn("code=500", result[1])

        self.assertIn("user=bob", result[2])
        self.assertNotIn("(x", result[2])

    def test_no_dedup_different_levels(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:01   WARNING   user=alice",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 2)

    def test_no_dedup_different_fields(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:01   INFO   user=bob",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 2)

    def test_no_dedup_outside_window(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:15   INFO   user=alice",
        ]
        result = _run(lines, window=10)
        self.assertEqual(len(result), 2)

    def test_count_omitted_for_single(self) -> None:
        lines = ["2024-01-01T10:00:00   INFO   user=alice"]
        result = _run(lines)
        self.assertEqual(len(result), 1)
        self.assertNotIn("(x", result[0])

    def test_fields_sorted_alphabetically(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   zebra=1   apple=2",
        ]
        result = _run(lines)
        idx_apple = result[0].index("apple=2")
        idx_zebra = result[0].index("zebra=1")
        self.assertLess(idx_apple, idx_zebra)


class TestEscalation(unittest.TestCase):
    def test_error_escalated_to_critical(self) -> None:
        lines = [
            "2024-01-01T10:00:00   ERROR   user=alice   code=500",
            "2024-01-01T10:00:01   ERROR   user=alice   code=500",
        ]
        result = _run(lines, threshold=2)
        self.assertEqual(len(result), 1)
        self.assertIn("CRITICAL", result[0])

    def test_error_not_escalated_below_threshold(self) -> None:
        lines = [
            "2024-01-01T10:00:00   ERROR   user=alice   code=500",
            "2024-01-01T10:00:01   ERROR   user=alice   code=500",
        ]
        result = _run(lines, threshold=3)
        self.assertEqual(len(result), 1)
        self.assertIn("ERROR", result[0])
        self.assertNotIn("CRITICAL", result[0])

    def test_escalation_only_for_errors(self) -> None:
        lines = [
            "2024-01-01T10:00:00   WARNING   user=alice",
            "2024-01-01T10:00:01   WARNING   user=alice",
            "2024-01-01T10:00:02   WARNING   user=alice",
        ]
        result = _run(lines, threshold=2)
        self.assertEqual(len(result), 1)
        self.assertIn("WARNING", result[0])
        self.assertNotIn("CRITICAL", result[0])


class TestEnrichment(unittest.TestCase):
    def test_code_500_overrides_info_to_error(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice   code=503",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)
        self.assertIn("ERROR", result[0])

    def test_code_599_overrides(self) -> None:
        lines = [
            "2024-01-01T10:00:00   DEBUG   code=599",
        ]
        result = _run(lines)
        self.assertIn("ERROR", result[0])

    def test_code_499_no_override(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   code=499",
        ]
        result = _run(lines)
        self.assertIn("INFO", result[0])

    def test_code_600_no_override(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   code=600",
        ]
        result = _run(lines)
        self.assertIn("INFO", result[0])

    def test_enrichment_then_escalation(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice   code=500",
            "2024-01-01T10:00:01   INFO   user=alice   code=500",
        ]
        result = _run(lines, threshold=2)
        self.assertEqual(len(result), 1)
        self.assertIn("CRITICAL", result[0])


class TestNormalization(unittest.TestCase):
    def test_user_id_normalized_to_user(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user_id=alice",
        ]
        result = _run(lines)
        self.assertIn("user=alice", result[0])
        self.assertNotIn("user_id", result[0])

    def test_user_id_dedup_with_user(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:01   INFO   user_id=alice",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)
        self.assertIn("(x 2)", result[0])

    def test_user_user_id_conflict_skipped(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice   user_id=bob",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 0)

    def test_user_user_id_same_value_ok(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice   user_id=alice",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)
        self.assertIn("user=alice", result[0])
        self.assertNotIn("user_id", result[0])


class TestMalformedInput(unittest.TestCase):
    def test_missing_timestamp(self) -> None:
        lines = ["INFO   user=alice"]
        result = _run(lines)
        self.assertEqual(len(result), 0)

    def test_missing_level(self) -> None:
        lines = ["2024-01-01T10:00:00"]
        result = _run(lines)
        self.assertEqual(len(result), 0)

    def test_invalid_timestamp(self) -> None:
        lines = ["not-a-date   INFO   user=alice"]
        result = _run(lines)
        self.assertEqual(len(result), 0)

    def test_invalid_field_format(self) -> None:
        lines = ["2024-01-01T10:00:00   INFO   notakeyvalue"]
        result = _run(lines)
        self.assertEqual(len(result), 0)

    def test_empty_file(self) -> None:
        result = _run([])
        self.assertEqual(len(result), 0)

    def test_blank_lines_skipped(self) -> None:
        lines = [
            "",
            "2024-01-01T10:00:00   INFO   user=alice",
            "",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)

    def test_invalid_level(self) -> None:
        lines = ["2024-01-01T10:00:00   TRACE   user=alice"]
        result = _run(lines)
        self.assertEqual(len(result), 0)


class TestOrdering(unittest.TestCase):
    def test_chronological_order(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:01   WARNING   user=bob",
            "2024-01-01T10:00:02   ERROR   user=charlie   code=500",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 3)
        self.assertIn("alice", result[0])
        self.assertIn("bob", result[1])
        self.assertIn("charlie", result[2])

    def test_stable_order_same_timestamp(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   action=first",
            "2024-01-01T10:00:00   WARNING   action=second",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 2)
        self.assertIn("first", result[0])
        self.assertIn("second", result[1])


class TestTimestampFormat(unittest.TestCase):
    def test_same_date_short_end(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:05   INFO   user=alice",
        ]
        result = _run(lines)
        self.assertIn("10:00:00~10:00:05", result[0])
        self.assertNotIn("2024-01-01T10:00:05", result[0])

    def test_different_date_full_end(self) -> None:
        lines = [
            "2024-01-01T23:59:55   INFO   user=alice",
            "2024-01-02T00:00:02   INFO   user=alice",
        ]
        result = _run(lines, window=60)
        self.assertIn("2024-01-02T00:00:02", result[0])

    def test_single_entry_no_range(self) -> None:
        lines = ["2024-01-01T10:00:00   INFO   user=alice"]
        result = _run(lines)
        self.assertNotIn("~", result[0])

    def test_fractional_seconds_parsed(self) -> None:
        lines = [
            "2024-01-01T10:00:00.123   INFO   user=alice",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)


class TestEdgeCases(unittest.TestCase):
    def test_window_boundary_exact(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:10   INFO   user=alice",
        ]
        # Exactly at window boundary (10 seconds, window=10) — should deduplicate
        result = _run(lines, window=10)
        self.assertEqual(len(result), 1)

    def test_window_boundary_exceeded(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   user=alice",
            "2024-01-01T10:00:11   INFO   user=alice",
        ]
        result = _run(lines, window=10)
        self.assertEqual(len(result), 2)

    def test_large_count(self) -> None:
        lines = [
            f"2024-01-01T10:00:{i:02d}   INFO   user=alice"
            for i in range(10)
        ]
        result = _run(lines, window=60)
        self.assertEqual(len(result), 1)
        self.assertIn("(x 10)", result[0])

    def test_code_non_integer_ignored(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO   code=abc",
        ]
        result = _run(lines)
        self.assertIn("INFO", result[0])

    def test_no_fields_valid(self) -> None:
        lines = [
            "2024-01-01T10:00:00   INFO",
        ]
        result = _run(lines)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "2024-01-01T10:00:00   INFO")


if __name__ == "__main__":
    unittest.main()
