import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from pipelines.hais.ingest import FILE, HaisClickHouse, discover, load_file


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'hais_2026-09-01.snappy.parquet'
        self.path.write_bytes(b'sample')
        self.item = discover('2026-09-01', '2026-09-01', self.root)[0]
        self.ch = Mock()

    def events(self):
        return [json.loads(call.args[0].split('\n', 1)[1])
                for call in self.ch.query.call_args_list
                if call.args[0].startswith('INSERT INTO raw.hais_ingestion_runs')]

    def test_dates_and_inclusive_discovery(self):
        for start, end in [(None, None), ('2026-9-1', '2026-09-01'),
                           ('2026-09-01T00:00:00Z', '2026-09-01'),
                           ('2026-02-30', '2026-09-01'), ('2026-09-02', '2026-09-01')]:
            with self.assertRaises(ValueError):
                discover(start, end, self.root)
        with self.assertRaises(FileNotFoundError):
            discover('2026-09-01', '2026-09-02', self.root)
        (self.root / 'hais_2026-09-02.snappy.parquet').write_bytes(b'next')
        self.assertEqual(len(discover('2026-09-01', '2026-09-02', self.root)), 2)

    def test_existing_sample_success_skips_without_insert(self):
        # Sparse local stand-in: no Parquet parsing or real ingestion.
        with self.path.open('wb') as f:
            f.truncate(33836469)
        item = discover('2026-09-01', '2026-09-01', self.root)[0]
        self.ch.query.return_value = '1\n'
        self.assertEqual(load_file(item, self.root, self.ch)['status'], 'skipped')
        self.ch.query.assert_called_once()
        sql, params = self.ch.query.call_args.args
        self.assertIn("status = 'success'", sql)
        self.assertEqual(params['param_file_size'], '33836469')
        self.assertEqual(params['param_file_name'], self.path.name)

    def test_success_preserves_source_and_records_source_count(self):
        self.ch.query.side_effect = ['0', '0', '', '1227749', '', '']
        result = load_file(self.item, self.root, self.ch)
        self.assertEqual(result['row_count'], 1227749)
        self.assertFalse(any('count() FROM raw.hais_positions' in c.args[0]
                             for c in self.ch.query.call_args_list))
        events = self.events()
        self.assertEqual([e['status'] for e in events], ['running', 'success'])
        self.assertIsNone(events[0]['row_count'])
        self.assertIsNone(events[0]['completed_at'])
        self.assertEqual(events[0]['started_at'], events[1]['started_at'])
        self.assertEqual(events[1]['row_count'], 1227749)
        sql, params = self.ch.query.call_args_list[4].args
        self.assertIn('SELECT date_time_utc, mmsi', sql)
        self.assertIn('FROM file(', sql)
        for forbidden in ('geometry', 'ingested_at', 'DISTINCT', 'FINAL'):
            self.assertNotIn(forbidden, sql)
        self.assertEqual(params, {'param_file_path': 'hais/' + self.path.name})
        self.assertTrue(sql.endswith(' FROM ' + FILE))
        count_sql, count_params = self.ch.query.call_args_list[3].args
        self.assertEqual(count_sql, 'SELECT count() FROM ' + FILE + ' FORMAT TabSeparated')
        self.assertEqual(count_params, params)

    def test_exact_file_sql_embeds_schema_with_escaped_utc(self):
        expected = r"""file({file_path:String}, 'Parquet', 'date_time_utc DateTime64(6, \'UTC\'), mmsi UInt32,
longitude Float64, latitude Float64, status Nullable(Int32),
course_over_ground Nullable(Float64), true_heading Nullable(Int32),
speed_over_ground Nullable(Float64), rate_of_turn Nullable(Float64),
maneuvre Nullable(Int32), data_source Nullable(String), ais_class Nullable(String),
msg_type Nullable(Int32)')"""
        self.assertEqual(FILE, expected)
        self.assertNotIn('{schema:String}', FILE)

    def test_changed_size_is_a_new_identity(self):
        self.path.write_bytes(b'new version')
        item = discover('2026-09-01', '2026-09-01', self.root)[0]
        self.ch.query.side_effect = ['0', '0', '', '0', '', '']
        self.assertEqual(load_file(item, self.root, self.ch)['row_count'], 0)
        self.assertEqual(self.ch.query.call_args_list[0].args[1]['param_file_size'], '11')

    def test_uncertain_attempt_blocks_reload(self):
        self.ch.query.side_effect = ['0', '1']
        with self.assertRaisesRegex(RuntimeError, 'reconcile'):
            load_file(self.item, self.root, self.ch)
        self.assertEqual(self.events(), [])

    def test_insert_failure_records_failed_unknown_count(self):
        self.ch.query.side_effect = ['0', '0', '', '10', RuntimeError('timeout'), '']
        with self.assertRaisesRegex(RuntimeError, 'timeout'):
            load_file(self.item, self.root, self.ch)
        self.assertEqual(self.events()[-1]['status'], 'failed')
        self.assertIsNone(self.events()[-1]['row_count'])
        self.assertIsNotNone(self.events()[-1]['completed_at'])

    def test_file_changed_during_insert_fails_without_success(self):
        def query(sql, params=None):
            if sql.startswith('INSERT INTO raw.hais_positions '):
                self.path.write_bytes(b'changed during insert')
            return '0'
        self.ch.query.side_effect = query
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            load_file(self.item, self.root, self.ch)
        self.assertEqual(self.events()[-1]['status'], 'failed')

    def test_file_changed_during_count_prevents_insert(self):
        def query(sql, params=None):
            if sql.startswith('SELECT count() FROM file('):
                self.path.write_bytes(b'changed during count')
            return '0'
        self.ch.query.side_effect = query
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            load_file(self.item, self.root, self.ch)
        self.assertFalse(any(c.args[0].startswith('INSERT INTO raw.hais_positions ')
                             for c in self.ch.query.call_args_list))
        self.assertEqual(self.events()[-1]['status'], 'failed')

    def test_http_error_contains_bounded_response(self):
        ch = HaisClickHouse.__new__(HaisClickHouse)
        ch.url = 'http://example.invalid'
        ch.session = Mock()
        ch.session.post.return_value = Mock(ok=False, status_code=500,
                                           text='DB::Exception: ' + 'x' * 3000)
        with self.assertRaises(RuntimeError) as error:
            ch.query('SELECT 1')
        message = str(error.exception)
        self.assertIn('HTTP 500', message)
        self.assertEqual(message.split('\n', 1)[1],
                         ch.session.post.return_value.text[:2000])

    def test_changed_file_rejected_before_database_access(self):
        self.path.write_bytes(b'changed')
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            load_file(self.item, self.root, self.ch)
        self.ch.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
