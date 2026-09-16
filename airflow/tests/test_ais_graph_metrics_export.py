"""Check the export DAG and task body without Airflow or database services."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import runpy
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


DAG_FILE = Path(__file__).resolve().parents[1] / 'dags' / 'ais_graph_metrics_export.py'


class GraphMetricsExportDagTests(unittest.TestCase):
    def setUp(self):
        self.dags = []
        self.tasks = []

        def dag(**options):
            def decorate(function):
                def build():
                    self.dags.append(options)
                    function()
                return build
            return decorate

        def task(**options):
            def decorate(function):
                def bind(*args, **kwargs):
                    call = SimpleNamespace(function=function, options=options,
                                           args=args, kwargs=kwargs)
                    self.tasks.append(call)
                    return call
                return bind
            return decorate

        sdk = ModuleType('airflow.sdk')
        sdk.dag = dag
        sdk.task = task
        airflow = ModuleType('airflow')
        airflow.sdk = sdk
        exporter = ModuleType('pipelines.graph_metrics.export')
        self.export_metrics = Mock()
        exporter.export_metrics = self.export_metrics
        modules_patch = patch.dict(sys.modules, {
            'airflow': airflow,
            'airflow.sdk': sdk,
            'pipelines.graph_metrics.export': exporter,
        })
        modules_patch.start()
        self.addCleanup(modules_patch.stop)
        path_patch = patch.object(sys, 'path', sys.path[:])
        path_patch.start()
        self.addCleanup(path_patch.stop)
        runpy.run_path(str(DAG_FILE))

    def test_single_manual_serial_dag_with_one_export_task(self):
        self.assertEqual(len(self.dags), 1)
        dag = self.dags[0]
        self.assertEqual(dag['dag_id'], 'ais_graph_metrics_export')
        self.assertEqual(dag['start_date'], datetime(2026, 9, 1, tzinfo=timezone.utc))
        self.assertIsNone(dag['schedule'])
        self.assertFalse(dag['catchup'])
        self.assertEqual(dag['max_active_runs'], 1)
        self.assertEqual(dag['max_active_tasks'], 1)
        self.assertEqual(dag['default_args']['retries'], 2)
        self.assertEqual(dag['default_args']['retry_delay'], timedelta(minutes=1))
        self.assertEqual(len(self.tasks), 1)
        task = self.tasks[0]
        self.assertEqual(task.function.__name__, 'export_port_metrics')
        self.assertEqual(task.options['execution_timeout'], timedelta(minutes=15))
        self.assertFalse(task.options['do_xcom_push'])
        self.assertEqual(task.args, ())
        self.assertEqual(task.kwargs, {})
        self.export_metrics.assert_not_called()

    def test_task_calls_exporter_without_returning_metrics_to_xcom(self):
        self.export_metrics.return_value = [{'port_id': 'WPI:23160', 'pagerank': 0.3}]
        self.assertIsNone(self.tasks[0].function())
        self.export_metrics.assert_called_once_with()
        self.assertEqual(sys.path[0], '/opt/ais')

    def test_export_failure_propagates_for_airflow_retry(self):
        failure = RuntimeError('export failed')
        self.export_metrics.side_effect = failure
        with self.assertRaises(RuntimeError) as raised:
            self.tasks[0].function()
        self.assertIs(raised.exception, failure)
        self.export_metrics.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
