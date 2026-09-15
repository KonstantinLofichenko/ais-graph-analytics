"""Completed-run handoff tests; no ClickHouse or Neo4j connections."""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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

    def invoke(self, publish_error=None):
        with patch.object(run, 'ROOT', self.root), \
                patch.object(run, 'load_dotenv'), \
                patch.object(run, 'ClickHouse') as clickhouse, \
                patch.object(run, 'publish', side_effect=publish_error) as publish, \
                patch.object(sys, 'argv', self.argv), redirect_stdout(io.StringIO()):
            clickhouse.return_value.count_positions.return_value = 0
            clickhouse.return_value.positions.return_value = []
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
            run_id=run.batch_id(
                start,
                end,
                dataset_hash,
                parameters,
            ),
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


if __name__ == '__main__':
    unittest.main()
