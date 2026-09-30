"""Offline checks for the consolidated dbt, Neo4j, and AI orchestration."""
from copy import copy
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import DagRunTriggerException
from daily_ais_window import resolve_activity_window
from ais_port_visits_window import resolve_window
from pipelines.port_visits.run import canonical_run_id


class DailyAisPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(collect_dags=False)
        cls.bag.process_file(
            str(Path(__file__).resolve().parents[1] / 'dags/daily_ais_pipeline.py'),
            safe_mode=False,
        )
        cls.dag = cls.bag.dags['daily_ais_pipeline']

    def context(self, conf=None, started_at=None):
        window = resolve_activity_window(conf, started_at or datetime(2026, 9, 29, tzinfo=timezone.utc))
        ti = Mock()
        ti.xcom_pull.return_value = window
        ti.get_dag.return_value.is_paused = False
        return {'dag_run': SimpleNamespace(conf=conf, logical_date=None),
                'run_id': 'manual__test', 'ti': ti}

    def rendered_task(self, name, context):
        task = copy(self.dag.get_task(name))
        task.render_template_fields(context)
        return task

    def test_master_has_full_success_dependency_chain(self):
        self.assertFalse(self.bag.import_errors)
        self.assertEqual(self.dag.schedule, '0 2 * * *')
        self.assertEqual(str(self.dag.timezone), 'UTC')
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        chain = ['resolve_activity_date', 'dbt_core', 'dbt_vessel_daily_features',
                 'ais_port_visits', 'ais_gds_metrics', 'ais_graph_metrics_export',
                 'dbt_graph_models', 'dbt_vessel_daily_anomalies', 'ai_enrichment',
                 'dbt_vessel_daily_enriched', 'dbt_tests']
        self.assertEqual({task.task_id: task.upstream_task_ids for task in self.dag.tasks},
                         {name: {chain[i-1]} if i else set() for i, name in enumerate(chain)})
        self.assertTrue(all(str(task.trigger_rule) == 'TriggerRule.ALL_SUCCESS' for task in self.dag.tasks))

    def test_obsolete_master_is_absent_and_child_dags_remain_manual(self):
        directory = Path(__file__).resolve().parents[1] / 'dags'
        self.assertFalse((directory / 'ais_analytics_pipeline.py').exists())
        self.assertFalse((directory / 'ais_analytics_sequence.py').exists())
        bag = DagBag(collect_dags=False)
        for path in directory.glob('*.py'):
            bag.process_file(str(path), safe_mode=False)
        self.assertFalse(bag.import_errors)
        self.assertNotIn('ais_analytics_pipeline', bag.dags)
        self.assertIn('daily_ais_pipeline', bag.dags)
        for name, dag in bag.dags.items():
            if name != 'daily_ais_pipeline':
                self.assertIsNone(dag.schedule, name)

    def test_default_day_is_frozen_for_graph_and_ai_across_midnight(self):
        context = self.context(started_at=datetime(2026, 9, 29, 23, 59, tzinfo=timezone.utc))
        # Rendering later uses the captured XCom, never wall-clock/logical-date arithmetic.
        ai = self.rendered_task('ai_enrichment', context)
        graph = self.rendered_task('ais_port_visits', context)
        self.assertEqual(ai.bash_command, 'python -m pipelines.ai_enrichment.enrich_vessels --date 2026-09-28')
        self.assertEqual(graph.conf, {'start': '2026-09-28', 'end': '2026-09-29'})
        self.assertNotIn('--limit', ai.bash_command)

    def test_historical_date_is_shared_without_a_logical_date(self):
        for day, end in [('2026-09-27', '2026-09-28'), ('2024-02-29', '2024-03-01')]:
            with self.subTest(day=day):
                context = self.context({'activity_date': day, 'max_rows': 5000000})
                ai = self.rendered_task('ai_enrichment', context)
                graph = self.rendered_task('ais_port_visits', context)
                self.assertEqual(ai.bash_command, f'python -m pipelines.ai_enrichment.enrich_vessels --date {day}')
                self.assertNotIn('--limit', ai.bash_command)
                self.assertEqual(graph.conf, {'start': day, 'end': end, 'max_rows': 5000000})
                start, stop, limit, _ = resolve_window(graph.conf, None, None, None)
                self.assertEqual(canonical_run_id(start), day + 'T00:00:00Z')
                self.assertEqual(stop - start, timedelta(days=1))
                self.assertEqual(limit, 5000000)
                self.assertIsNone(self.rendered_task('ais_gds_metrics', context).conf)
                self.assertIsNone(self.rendered_task('ais_graph_metrics_export', context).conf)

    def test_default_date_normalizes_run_start_to_utc(self):
        started = datetime(2026, 9, 29, 1, tzinfo=timezone(timedelta(hours=3)))
        self.assertEqual(resolve_activity_window({}, started)['activity_date'], '2026-09-27')

    def test_invalid_dates_fail_before_any_processing(self):
        for day in ('09/27/2026', '2026-9-27', 'foo', '2026-09-27;echo bad',
                    '2026-09-27; rm -rf /', '2026-09-27$(echo bad)',
                    '2026-09-27\n', '2026-02-29', '2026-09-31',
                    '2026-09-27T00:00:00Z', '', None, 20260927, [], {}):
            with self.subTest(day=day), self.assertRaisesRegex(ValueError, 'valid YYYY-MM-DD date'):
                resolve_activity_window({'activity_date': day}, None)

    def test_invalid_limits_and_legacy_range_are_rejected(self):
        for value in (0, -1, 1.5, True, '5000000'):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'positive integer'):
                self.context({'activity_date': '2026-09-27', 'max_rows': value})
        with self.assertRaisesRegex(ValueError, 'start/end ranges'):
            self.context({'start': '2026-09-01', 'end': '2026-09-17'})

    def test_child_triggers_wait_and_reject_failures(self):
        context = self.context({'activity_date': '2026-09-27'})
        for name in ('ais_port_visits', 'ais_gds_metrics', 'ais_graph_metrics_export'):
            with self.subTest(child=name):
                task = self.rendered_task(name, context)
                task.openlineage_inject_parent_info = False
                self.assertEqual(task.retries, 0)
                self.assertEqual(task.allowed_states, ['success'])
                self.assertEqual(task.failed_states, ['failed'])
                self.assertTrue(task.fail_when_dag_is_paused)
                self.assertFalse(task.reset_dag_run)
                with self.assertRaises(DagRunTriggerException) as caught:
                    task.execute(context)
                request = caught.exception
                self.assertEqual(request.trigger_dag_id, name)
                self.assertEqual(request.dag_run_id, 'manual__test__' + name)
                self.assertTrue(request.wait_for_completion)
                self.assertTrue(request.deferrable)
                with self.assertRaises(AirflowException):
                    task.execute_complete(context, ('trigger', {'dag_id': name,
                        'run_ids': [request.dag_run_id], request.dag_run_id: 'failed'}))

    def test_dbt_models_are_not_accidentally_rebuilt(self):
        self.assertEqual(self.dag.get_task('dbt_core').bash_command.count('--select +vessels'), 1)
        selections = {'dbt_vessel_daily_features': 'vessel_daily_features',
                      'dbt_graph_models': 'current_port_visits',
                      'dbt_vessel_daily_anomalies': 'vessel_daily_anomalies int_vessel_ai_candidates',
                      'dbt_vessel_daily_enriched': 'int_vessel_ai_current_inputs vessel_daily_enriched'}
        for name, model in selections.items():
            self.assertEqual(self.dag.get_task(name).bash_command,
                             f'dbt run --project-dir /opt/ais/dbt --select {model}')
        self.assertIn('vessel_daily_anomalies', self.dag.get_task('dbt_tests').bash_command)
        # Country enrichment remains separately provisioned and optional.
        self.assertNotIn('countries', ' '.join(self.dag.get_task(name).bash_command for name in selections))


if __name__ == '__main__':
    unittest.main()
