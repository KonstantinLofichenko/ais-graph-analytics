"""Completed-run handoff tests; no ClickHouse or Neo4j connections."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.port_visits import run


class RunMetadataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / 'pipelines/port_visits').mkdir(parents=True)
        self.ports = self.root / 'ports.json'
        self.ports.write_text(json.dumps([dict(
            port_id='WPI:1', name='Test', country='NO',
            latitude=60.0, longitude=5.0, radius_m=1500.0)]))
        self.result = self.root / 'result.json'
        self.argv = ['run.py', '--ports', str(self.ports),
                     '--start', '2026-09-14T11:00:00+03:00',
                     '--end', '2026-09-15T08:00:00+00:00',
                     '--apply', '--result-json', str(self.result)]

    def invoke(self, publish_error=None, extra_args=(), rows=()):
        with patch.object(run, 'ROOT', self.root), \
                patch.object(run, 'load_dotenv'), \
                patch.object(run, 'ClickHouse') as clickhouse, \
                patch.object(run, 'publish', side_effect=publish_error) as publish, \
                patch.object(sys, 'argv', [*self.argv, *extra_args]), redirect_stdout(io.StringIO()):
            clickhouse.return_value.count_positions.return_value = len(rows)
            clickhouse.return_value.positions.return_value = list(rows)
            run.main()
            clickhouse.return_value.session.close.assert_called_once_with()
            return publish

    def test_completed_metadata_reuses_published_id_and_normalized_window(self):
        first_publish = self.invoke()
        first = json.loads(self.result.read_text())

        second_publish = self.invoke()
        second = json.loads(self.result.read_text())

        start = datetime(2026, 9, 14, 8, tzinfo=timezone.utc)
        end = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)

        ports = json.loads(self.ports.read_text())

        dataset_hash = run.digest(ports)

        parameters = {
            'version': run.OWNER,
            'min_stay': 1200,
            'max_gap': 900,
            'max_speed': 3,
        }

        expected = dict(
            run_id='2026-09-14T08:00:00Z',
            window_start=start.isoformat(),
            window_end=end.isoformat(),
        )

        self.assertEqual(first, expected)
        self.assertEqual(second, expected)

        for publish in (first_publish, second_publish):
            args = publish.call_args.args

            self.assertEqual(args[4], expected['run_id'])
            self.assertEqual(args[5], dataset_hash)
            self.assertEqual(args[6:8], (start, end))
            self.assertEqual(args[8], parameters)

        self.assertLess(self.result.stat().st_size, 256)

    def test_dataset_parameters_and_source_rows_do_not_change_id(self):
        original = self.invoke().call_args.args
        ports = json.loads(self.ports.read_text())
        ports.append(dict(ports[0], port_id='WPI:2', latitude=61.0))
        self.ports.write_text(json.dumps(ports))
        changed_dataset = self.invoke().call_args.args
        self.assertNotEqual(original[5], changed_dataset[5])

        changed_parameters = self.invoke(extra_args=(
            '--min-stay-minutes', '30', '--max-gap-minutes', '10',
            '--max-speed-knots', '2')).call_args.args
        self.assertNotEqual(original[8], changed_parameters[8])

        changed_rows = self.invoke(rows=[dict(
            mmsi=123456789, msgtime='2026-09-14T08:00:00Z',
            latitude=60.0, longitude=5.0, speed_over_ground=0.5)]).call_args.args
        self.assertNotEqual(original[9]['source_rows'], changed_rows[9]['source_rows'])

        for args in (original, changed_dataset, changed_parameters, changed_rows):
            self.assertEqual(args[4], '2026-09-14T08:00:00Z')
        self.assertEqual(json.loads(self.result.read_text())['window_end'], '2026-09-15T08:00:00+00:00')

    def test_subsecond_window_is_preserved_separately_from_run_id(self):
        self.invoke(extra_args=('--start', '2026-09-14T11:00:00.123456+03:00',
                                '--end', '2026-09-15T08:00:00.654321Z'))
        self.assertEqual(json.loads(self.result.read_text()), dict(
            run_id='2026-09-14T08:00:00Z', window_start='2026-09-14T08:00:00.123456+00:00',
            window_end='2026-09-15T08:00:00.654321+00:00'))

    def test_failed_publication_does_not_emit_completed_metadata(self):
        with self.assertRaisesRegex(RuntimeError, 'publication failed'):
            self.invoke(RuntimeError('publication failed'))
        self.assertFalse(self.result.exists())

    def test_preview_cannot_emit_completed_metadata(self):
        self.argv.remove('--apply')
        with patch.object(sys, 'argv', self.argv), \
                patch.object(run, 'ClickHouse') as clickhouse, \
                patch('sys.stderr', new_callable=io.StringIO) as stderr:
            with self.assertRaises(SystemExit) as error:
                run.main()
        self.assertEqual(error.exception.code, 2)
        self.assertIn('--result-json requires --apply', stderr.getvalue())
        clickhouse.assert_not_called()
        self.assertFalse(self.result.exists())


class PublicationRunIdTests(unittest.TestCase):
    def test_execution_time_changes_versions_but_preserves_id_across_stores(self):
        start = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        ports = [dict(port_id='WPI:1')]
        visits = [dict(visit_id='visit-hash', port_id='WPI:1')]
        counts = [dict(mmsi=123456789, port_id='WPI:1', visit_count=1)]
        stats = dict(source_rows=3)

        for attempt, executed_at in enumerate((end, end + timedelta(days=2))):
            with self.subTest(executed_at=executed_at), \
                    patch.object(run, 'datetime', wraps=datetime) as clock, \
                    patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused'}), \
                    patch.object(run.GraphDatabase, 'driver') as driver:
                clock.now.return_value = executed_at
                ch, tx = Mock(), Mock()
                ch.query.return_value = '' if attempt == 0 else json.dumps({
                    'window_start': run.date_string(start),
                    'window_end': run.date_string(end),
                }) + '\n'
                session = driver.return_value.__enter__.return_value.session.return_value.__enter__.return_value
                session.execute_write.side_effect = lambda operation, *args: operation(tx, *args)
                run.publish(ch, ports, visits, counts, run.canonical_run_id(start),
                            'dataset-hash', start, end, {}, stats)

                ch.query.assert_called_once()
                ch.upsert_ports.assert_called_once_with(ports, 'dataset-hash', executed_at)
                inserts = {call.args[0]: call.args[1] for call in ch.insert.call_args_list}
                self.assertEqual(inserts['port_visits'][0]['run_id'], '2026-09-15T08:00:00Z')
                self.assertEqual(inserts['port_visits'][0]['updated_at'], executed_at)
                completed = inserts['port_visit_runs'][0]
                self.assertEqual(completed['run_id'], '2026-09-15T08:00:00Z')
                self.assertEqual((completed['window_start'], completed['window_end']), (start, end))
                self.assertEqual(completed['completed_at'], executed_at)
                graph_writes = [call for call in tx.run.call_args_list if 'run' in call.kwargs]
                self.assertEqual(len(graph_writes), 2)  # VISITED and Port metadata.
                for call in graph_writes:
                    self.assertEqual(call.kwargs['run'], '2026-09-15T08:00:00Z')
                    self.assertEqual((call.kwargs['start'], call.kwargs['end']),
                                     (start.isoformat(), end.isoformat()))

    def test_existing_run_with_different_end_blocks_all_publication_writes(self):
        start = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        run_id = run.canonical_run_id(start)
        ch = Mock()
        ch.query.return_value = json.dumps({
            'window_start': run.date_string(start),
            'window_end': run.date_string(start + timedelta(hours=12)),
        }) + '\n'

        with patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused'}), \
                patch.object(run.GraphDatabase, 'driver') as driver:
            with self.assertRaisesRegex(RuntimeError, 'already exists') as raised:
                run.publish(ch, [], [], [], run_id, 'dataset-hash', start, end,
                            {}, dict(source_rows=0))

        self.assertIn(run_id, str(raised.exception))
        self.assertIn('requested', str(raised.exception))
        ch.query.assert_called_once()
        ch.upsert_ports.assert_not_called()
        ch.insert.assert_not_called()
        driver.assert_not_called()

    def test_window_lookup_failure_blocks_all_publication_writes(self):
        start = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        failure = RuntimeError('ClickHouse lookup failed')
        ch = Mock()
        ch.query.side_effect = failure

        with patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused'}), \
                patch.object(run.GraphDatabase, 'driver') as driver:
            with self.assertRaises(RuntimeError) as raised:
                run.publish(ch, [], [], [], run.canonical_run_id(start),
                            'dataset-hash', start, end, {}, dict(source_rows=0))

        self.assertIs(raised.exception, failure)
        ch.query.assert_called_once()
        ch.upsert_ports.assert_not_called()
        ch.insert.assert_not_called()
        driver.assert_not_called()


if __name__ == '__main__':
    unittest.main()
