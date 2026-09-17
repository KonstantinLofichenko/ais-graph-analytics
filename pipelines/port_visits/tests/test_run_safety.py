from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sys
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run import ClickHouse, canonical_run_id, enforce_row_limit, validate_run_window

START = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)
END = datetime(2026, 9, 16, 8, tzinfo=timezone.utc)

class EnforceRowLimitTests(unittest.TestCase):
    def test_below_limit_proceeds(self):
        enforce_row_limit(499999, 500000)

    def test_at_limit_proceeds(self):
        enforce_row_limit(500000, 500000)

    def test_above_limit_fails_with_both_numbers(self):
        with self.assertRaisesRegex(RuntimeError, r'8,123,456.*5,000,000'):
            enforce_row_limit(8123456, 5000000)


class CanonicalRunIdTests(unittest.TestCase):
    def test_utc_start_has_canonical_format(self):
        self.assertEqual(canonical_run_id(START), '2026-09-15T08:00:00Z')

    def test_equivalent_offset_has_same_id(self):
        offset_start = datetime(2026, 9, 15, 11, tzinfo=timezone(timedelta(hours=3)))
        self.assertEqual(canonical_run_id(offset_start), '2026-09-15T08:00:00Z')

    def test_retry_has_same_id(self):
        self.assertEqual({canonical_run_id(START) for _ in range(3)}, {'2026-09-15T08:00:00Z'})

    def test_different_start_seconds_have_different_ids(self):
        self.assertNotEqual(canonical_run_id(START), canonical_run_id(START + timedelta(seconds=1)))

    def test_fractional_seconds_are_omitted_without_rounding(self):
        self.assertEqual(canonical_run_id(START.replace(microsecond=999999)), '2026-09-15T08:00:00Z')

    def test_naive_start_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'window_start must include a timezone'):
            canonical_run_id(START.replace(tzinfo=None))


class RunWindowValidationTests(unittest.TestCase):
    def setUp(self):
        self.ch = Mock()
        self.run_id = canonical_run_id(START)

    def stored_window(self, start, end):
        self.ch.query.return_value = json.dumps(dict(
            window_start=start.isoformat(), window_end=end.isoformat())) + '\n'

    def test_new_run_allows_any_window_length(self):
        self.ch.query.return_value = ''
        for hours in (24, 36, 48, 5.5):
            with self.subTest(hours=hours):
                validate_run_window(self.ch, self.run_id, START, START + timedelta(hours=hours))
        sql, params = self.ch.query.call_args.args
        self.assertIn('FROM analytics.port_visit_runs AS r FINAL', sql)
        self.assertIn('WHERE r.run_id = {run_id:String}', sql)
        self.assertNotIn('param_window_end', params)
        self.assertEqual(params, {'param_run_id': self.run_id})

    def test_same_window_retry_allows_equivalent_utc_offsets(self):
        for hours in (24, 36, 48, 5.5):
            with self.subTest(hours=hours):
                end = START + timedelta(hours=hours)
                self.stored_window(START, end)
                offset = timezone(timedelta(hours=3))
                retry_start, retry_end = START.astimezone(offset), end.astimezone(offset)
                self.assertEqual(canonical_run_id(retry_start), self.run_id)
                validate_run_window(self.ch, self.run_id, retry_start, retry_end)

    def test_same_start_with_different_end_fails(self):
        self.stored_window(START, END)
        requested_end = END + timedelta(hours=12)
        with self.assertRaisesRegex(RuntimeError, 'already exists for window') as error:
            validate_run_window(self.ch, self.run_id, START, requested_end)
        message = str(error.exception)
        for value in (self.run_id, END.isoformat(), requested_end.isoformat()):
            self.assertIn(value, message)

    def test_bounds_must_match_at_full_precision(self):
        start = START.replace(microsecond=123456)
        end = END.replace(microsecond=654321)
        self.stored_window(start, end)
        validate_run_window(self.ch, self.run_id, start, end)
        for requested_start, requested_end in (
                (start + timedelta(microseconds=1), end),
                (start, end + timedelta(microseconds=1))):
            with self.subTest(start=requested_start, end=requested_end):
                self.assertEqual(canonical_run_id(requested_start), self.run_id)
                with self.assertRaisesRegex(RuntimeError, 'only one analysis window'):
                    validate_run_window(self.ch, self.run_id, requested_start, requested_end)


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
