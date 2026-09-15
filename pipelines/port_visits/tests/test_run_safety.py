from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run import ClickHouse, batch_id, enforce_row_limit

START = datetime(2026, 9, 14, 8, tzinfo=timezone.utc)
END = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)


class EnforceRowLimitTests(unittest.TestCase):
    def test_below_limit_proceeds(self):
        enforce_row_limit(499999, 500000)

    def test_at_limit_proceeds(self):
        enforce_row_limit(500000, 500000)

    def test_above_limit_fails_with_both_numbers(self):
        with self.assertRaisesRegex(RuntimeError, r'8,123,456.*5,000,000'):
            enforce_row_limit(8123456, 5000000)


class BatchIdTests(unittest.TestCase):
    def test_same_window_same_id(self):
        self.assertEqual(batch_id(START, END), batch_id(START, END))

    def test_same_window_different_timezone_representation_same_id(self):
        # +00:00 and an equivalent offset must normalize to the same UTC identity.
        start_plus_one = datetime(2026, 9, 14, 9, tzinfo=timezone(timedelta(hours=1)))
        self.assertEqual(batch_id(START, END), batch_id(start_plus_one, END))

    def test_different_window_different_id(self):
        other_end = datetime(2026, 9, 16, 8, tzinfo=timezone.utc)
        self.assertNotEqual(batch_id(START, END), batch_id(START, other_end))

    def test_repeated_publishing_of_same_batch_is_idempotent(self):
        # No wall-clock/row-count input, so re-invoking for the same window is deterministic.
        ids = {batch_id(START, END) for _ in range(3)}
        self.assertEqual(len(ids), 1)


class PositionsQueryTests(unittest.TestCase):
    def test_source_alias_precedes_final(self):
        with patch.dict(os.environ, {'CLICKHOUSE_PASSWORD': 'unused'}):
            ch = ClickHouse()
        with patch.object(ch, 'query', return_value='') as mocked_query:
            ch.positions(START, END, max_rows=10)
        sql = mocked_query.call_args.args[0]
        self.assertIn('FROM raw.ais_positions AS source FINAL', sql)
        self.assertNotIn('FROM raw.ais_positions FINAL AS source', sql)


if __name__ == '__main__':
    unittest.main()
