"""Run in the project's Airflow image; parsing does not execute task bodies."""
from pathlib import Path
import unittest

from airflow.dag_processing.dagbag import DagBag


class HaisDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag()
        cls.bag.process_file(str(Path(__file__).resolve().parents[1] /
                                 'dags/ais_hais_historical_ingestion.py'), safe_mode=False)
        cls.dag = cls.bag.dags['ais_hais_historical_ingestion']

    def test_parse_and_serial_fail_fast_wiring(self):
        self.assertFalse(self.bag.import_errors)
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertTrue(self.dag.fail_fast)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.max_active_tasks, 1)
        self.assertEqual(set(self.dag.task_ids), {'discover_files', 'ingest_file'})
        self.assertEqual(self.dag.get_task('ingest_file').upstream_task_ids, {'discover_files'})
        self.assertEqual(self.dag.get_task('ingest_file').retries, 0)

    def test_date_params_are_required_and_reject_timestamps(self):
        for key in ('start_date', 'end_date'):
            param = self.dag.params.get_param(key)
            with self.assertRaises(Exception):
                param.resolve()
            with self.assertRaises(Exception):
                param.resolve('2026-09-01T00:00:00Z')
            self.assertEqual(param.resolve('2026-09-01'), '2026-09-01')


if __name__ == '__main__':
    unittest.main()
