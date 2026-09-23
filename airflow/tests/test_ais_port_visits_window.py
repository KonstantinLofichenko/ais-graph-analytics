from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from ais_port_visits_window import resolve_window


class ResolveWindowTests(unittest.TestCase):
    def setUp(self):
        self.run_start = datetime(2026, 9, 14, 6, 56, 27, tzinfo=timezone.utc)

    def test_explicit_start_end_max_rows(self):
        conf = {'start': '2026-09-14', 'end': '2026-09-15', 'max_rows': 1500000}
        start, end, max_rows, source = resolve_window(conf, self.run_start, '24', '500000')
        self.assertEqual(start, datetime(2026, 9, 14, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 9, 15, tzinfo=timezone.utc))
        self.assertEqual(max_rows, 1500000)
        self.assertEqual(source, 'dag_run.conf')

    def test_explicit_start_end_max_rows_omitted(self):
        conf = {'start': '2026-09-14', 'end': '2026-09-15'}
        _, _, max_rows, source = resolve_window(conf, self.run_start, '24', '500000')
        self.assertEqual(max_rows, 500000)
        self.assertEqual(source, 'dag_run.conf')

    def test_explicit_multi_day_or_null_window_rejected(self):
        for conf in ({'start': '2026-09-01', 'end': '2026-09-03'},
                     {'start': None, 'end': None},
                     {'start': '2026-09-01T00:00:00Z', 'end': '2026-09-02T00:00:00Z'}):
            with self.assertRaises(ValueError):
                resolve_window(conf, self.run_start, '24', '500000')

    def test_no_start_end_uses_rolling_default(self):
        start, end, max_rows, source = resolve_window({}, self.run_start, '24', '500000')
        self.assertEqual(end, self.run_start)
        self.assertEqual(start, self.run_start - timedelta(hours=24))
        self.assertEqual(max_rows, 500000)
        self.assertEqual(source, 'rolling-default')

    def test_start_without_end_fails(self):
        with self.assertRaises(ValueError):
            resolve_window({'start': '2026-09-14'}, self.run_start, '24', '500000')

    def test_end_without_start_fails(self):
        with self.assertRaises(ValueError):
            resolve_window({'end': '2026-09-15'}, self.run_start, '24', '500000')

    def test_start_after_or_equal_end_fails(self):
        for start, end in (('2026-09-15', '2026-09-14'),
                            ('2026-09-14', '2026-09-14')):
            with self.assertRaises(ValueError):
                resolve_window({'start': start, 'end': end}, self.run_start, '24', '500000')

    def test_invalid_date_fails(self):
        conf = {'start': 'not-a-timestamp', 'end': '2026-09-15'}
        with self.assertRaises(ValueError):
            resolve_window(conf, self.run_start, '24', '500000')

    def test_timestamp_without_timezone_fails(self):
        conf = {'start': '2026-09-14T08:00:00', 'end': '2026-09-15'}
        with self.assertRaises(ValueError):
            resolve_window(conf, self.run_start, '24', '500000')

    def test_max_rows_not_positive_fails(self):
        conf = {'start': '2026-09-14', 'end': '2026-09-15', 'max_rows': 0}
        with self.assertRaises(ValueError):
            resolve_window(conf, self.run_start, '24', '500000')

    def test_max_rows_invalid_type_fails(self):
        conf = {'start': '2026-09-14', 'end': '2026-09-15', 'max_rows': 'many'}
        with self.assertRaises(ValueError):
            resolve_window(conf, self.run_start, '24', '500000')

    def test_rolling_default_invalid_window_env_fails(self):
        with self.assertRaises(ValueError):
            resolve_window({}, self.run_start, '0', '500000')

    def test_rolling_default_invalid_max_rows_env_fails(self):
        with self.assertRaises(ValueError):
            resolve_window({}, self.run_start, '24', 'abc')


if __name__ == '__main__':
    unittest.main()
