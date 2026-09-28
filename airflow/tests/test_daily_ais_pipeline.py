"""Validate the daily DAG with the datetime type supplied by Airflow 3 workers."""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest

from airflow.dag_processing.dagbag import DagBag
from airflow.sdk.execution_time import macros


class DailyAisPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag()
        cls.bag.process_file(
            str(Path(__file__).resolve().parents[1] / 'dags/daily_ais_pipeline.py'),
            safe_mode=False,
        )
        cls.dag = cls.bag.dags['daily_ais_pipeline']

    def test_manual_dag_has_success_dependency_chain(self):
        self.assertFalse(self.bag.import_errors)
        self.assertIsNone(self.dag.schedule)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(
            {
                task.task_id: task.upstream_task_ids
                for task in self.dag.tasks
            },
            {
                'dbt_core': set(),
                'dbt_vessel_daily_features': {'dbt_core'},
                'ai_enrichment': {'dbt_vessel_daily_features'},
                'dbt_vessel_daily_enriched': {'ai_enrichment'},
                'dbt_tests': {'dbt_vessel_daily_enriched'},
            },
        )
        self.assertTrue(all(str(task.trigger_rule) == 'TriggerRule.ALL_SUCCESS'
                            for task in self.dag.tasks))

    def test_ai_command_renders_previous_utc_date_from_standard_datetime(self):
        command = self.dag.get_task('ai_enrichment').bash_command
        rendered = self.dag.get_template_env().from_string(command).render(
            dag_run=SimpleNamespace(logical_date=datetime(2026, 9, 28, 17, 28,
                                                          tzinfo=timezone.utc)),
            macros=macros,
        )
        self.assertEqual(
            rendered,
            'python -m pipelines.ai_enrichment.enrich_vessels --date 2026-09-27',
        )
        self.assertNotIn('--limit', rendered)


if __name__ == '__main__':
    unittest.main()
