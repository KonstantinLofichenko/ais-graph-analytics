"""Airflow 3 operator tests: intercept trigger requests, never launch child DAGs."""
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import DagRunTriggerException
from airflow.dag_processing.dagbag import DagBag
from ais_analytics_sequence import CHILDREN, DailyAnalyticsOperator
from ais_port_visits_window import daily_windows, resolve_window
from pipelines.port_visits.run import canonical_run_id


class DailyAnalyticsTests(unittest.TestCase):
    def context(self, end='2026-09-17'):
        ti = Mock()
        ti.get_dag.return_value.is_paused = False
        return {'dag_run': SimpleNamespace(conf={'start': '2026-09-01', 'end': end,
                                                 'max_rows': 5000000}),
                'run_id': 'manual__test', 'ti': ti}

    def operator(self):
        return DailyAnalyticsOperator(task_id='daily_analytics', openlineage_inject_parent_info=False)

    def test_one_day_and_16_days(self):
        windows = daily_windows(self.context()['dag_run'].conf)
        self.assertEqual(len(windows), 16)
        self.assertEqual(windows[0], {'start': '2026-09-01', 'end': '2026-09-02', 'max_rows': 5000000})
        self.assertEqual(windows[-1]['start'], '2026-09-16')
        self.assertEqual(windows[-1]['end'], '2026-09-17')
        self.assertEqual(daily_windows(self.context('2026-09-02')['dag_run'].conf), windows[:1])
        for previous, current in zip(windows, windows[1:]):
            self.assertEqual(previous['end'], current['start'])
        for window in windows:
            start, end, limit, _ = resolve_window(window, None, None, None)
            self.assertEqual(start.isoformat(), window['start'] + 'T00:00:00+00:00')
            self.assertEqual(end.isoformat(), window['end'] + 'T00:00:00+00:00')
            self.assertEqual(canonical_run_id(start), window['start'] + 'T00:00:00Z')
            self.assertEqual(limit, 5000000)

    def test_invalid_ranges(self):
        for conf in ({}, {'start': '2026-09-01'}, {'start': None, 'end': None},
                     {'start': '2026-9-01', 'end': '2026-09-02'},
                     {'start': '2026-02-30', 'end': '2026-09-02'},
                     {'start': '2026-09-01T00:00:00Z', 'end': '2026-09-02'},
                     {'start': '2026-09-02', 'end': '2026-09-02'},
                     {'start': '2026-09-03', 'end': '2026-09-02'}):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                daily_windows(conf)

    def test_full_sequence_survives_fresh_instance_each_resume(self):
        context = self.context()
        event = None
        windows = daily_windows(context['dag_run'].conf)
        for step in range(48):
            # The real provider emits an SDK request; no scheduler/API executes it.
            with self.assertRaises(DagRunTriggerException) as caught:
                if event is None:
                    self.operator().execute(context)
                else:
                    self.operator().execute_complete(context, event)
            request = caught.exception
            self.assertEqual(request.trigger_dag_id, CHILDREN[step % 3])
            self.assertEqual(request.conf, windows[step // 3] if step % 3 == 0 else None)
            self.assertTrue(request.wait_for_completion)
            self.assertTrue(request.deferrable)
            self.assertFalse(request.reset_dag_run)
            child_id = request.dag_run_id
            event = ('trigger', {'dag_id': request.trigger_dag_id,
                                 'run_ids': [child_id], child_id: 'success'})
        self.assertEqual(self.operator().execute_complete(context, event), {'completed_days': 16})

    def test_failure_or_unexpected_completion_never_advances(self):
        context = self.context()
        for step in range(6):
            child_id = f'manual__test__step_{step}'
            for state in ('failed', 'running', None):
                event = ('trigger', {'dag_id': CHILDREN[step % 3], 'run_ids': [child_id], child_id: state})
                with self.assertRaises(AirflowException):
                    self.operator().execute_complete(context, event)
        with self.assertRaises(AirflowException):
            self.operator().execute_complete(context, ('trigger', {'run_ids': ['unrelated']}))

    def test_dag_parses(self):
        bag = DagBag(collect_dags=False)
        bag.process_file(str(Path(__file__).resolve().parents[1] / 'dags/ais_analytics_pipeline.py'), safe_mode=False)
        self.assertFalse(bag.import_errors)
        dag = bag.dags['ais_analytics_pipeline']
        self.assertIsNone(dag.schedule)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.task_ids, ['daily_analytics'])
        self.assertEqual(dag.get_task('daily_analytics').retries, 0)


if __name__ == '__main__':
    unittest.main()
