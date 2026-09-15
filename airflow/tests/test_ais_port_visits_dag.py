"""Exercise the real DAG's wiring and task bodies without an Airflow installation."""
from datetime import datetime, timedelta, timezone
from functools import wraps
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


DAGS = Path(__file__).resolve().parents[1] / 'dags'


class TaskCall:
    def __init__(self, function, options, args, kwargs):
        self.function = function
        self.options = options
        self.args = args
        self.kwargs = kwargs
        self.task_id = options.get('task_id', function.__name__)
        self.upstream = {
            value.task_id for value in (*args, *kwargs.values())
            if isinstance(value, TaskCall)
        }


class DagHarness:
    """Record decorated calls as graph nodes; never run task bodies at import."""

    def __init__(self):
        self.dags = []
        self.calls = []
        self.context = Mock()
        self.sdk = ModuleType('airflow.sdk')
        self.sdk.dag = self.dag
        self.sdk.task = self.task
        self.sdk.get_current_context = self.context

    def dag(self, **options):
        def decorate(function):
            @wraps(function)
            def build(*args, **kwargs):
                self.dags.append(options)
                return function(*args, **kwargs)
            return build
        return decorate

    def task(self, **options):
        def decorate(function):
            @wraps(function)
            def bind(*args, **kwargs):
                call = TaskCall(function, options, args, kwargs)
                self.calls.append(call)
                return call
            return bind
        return decorate


class PortVisitsDagTests(unittest.TestCase):
    def setUp(self):
        # All import and path changes, including the task bodies' sys.path inserts,
        # are scoped to each test so the other test modules remain unaffected.
        path_patch = patch.object(sys, 'path', sys.path[:])
        path_patch.start()
        self.addCleanup(path_patch.stop)
        self.harness = DagHarness()
        airflow = ModuleType('airflow')
        airflow.sdk = self.harness.sdk
        window = ModuleType('ais_port_visits_window')
        window.__dict__.update(runpy.run_path(str(DAGS / 'ais_port_visits_window.py')))
        publisher = ModuleType('pipelines.port_connections.run')
        self.publish = Mock()
        publisher.publish_port_connections = self.publish
        download = ModuleType('download_ports')
        self.download = Mock()
        download.download = self.download
        modules_patch = patch.dict(sys.modules, {
            'airflow': airflow,
            'airflow.sdk': self.harness.sdk,
            'ais_port_visits_window': window,
            'pipelines.port_connections.run': publisher,
            'download_ports': download,
        })
        modules_patch.start()
        self.addCleanup(modules_patch.stop)
        runpy.run_path(str(DAGS / 'ais_port_visits.py'))
        self.tasks = {call.task_id: call for call in self.harness.calls}
        self.run_start = datetime(2026, 9, 15, 7, 13, 41, 123456, tzinfo=timezone.utc)
        self.ports = [{'port_id': 'WPI:23160', 'port_name': 'BERGEN'}]

    def context(self, conf):
        self.harness.context.return_value = {
            'dag_run': SimpleNamespace(conf=conf, start_date=self.run_start),
        }

    def test_single_manual_dag_has_exact_success_dependency_chain(self):
        self.assertEqual(len(self.harness.dags), 1)
        dag = self.harness.dags[0]
        self.assertEqual(dag['dag_id'], 'ais_port_visits')
        self.assertIsNone(dag['schedule'])
        self.assertFalse(dag['catchup'])
        self.assertEqual(dag['max_active_runs'], 1)
        self.assertEqual(dag['max_active_tasks'], 1)
        self.assertEqual(len(self.harness.calls), 3)
        self.assertEqual(set(self.tasks), {
            'download_ports', 'run_port_visit_pipeline', 'publish_port_connections',
        })
        download = self.tasks['download_ports']
        visits = self.tasks['run_port_visit_pipeline']
        connections = self.tasks['publish_port_connections']
        self.assertEqual(download.upstream, set())
        self.assertEqual(visits.upstream, {'download_ports'})
        self.assertEqual(connections.upstream, {'run_port_visit_pipeline'})
        self.assertEqual(visits.args, (download,))
        self.assertEqual(connections.args, (visits,))
        # An override such as all_done would permit publishing after failed visits.
        self.assertEqual(connections.options.get('trigger_rule', 'all_success'), 'all_success')
        self.assertEqual(dag.get('default_args', {}).get('trigger_rule', 'all_success'),
                         'all_success')
        self.download.assert_not_called()
        self.publish.assert_not_called()

    def test_download_returns_reference_records(self):
        self.download.return_value = self.ports
        self.assertIs(self.tasks['download_ports'].function(), self.ports)
        self.download.assert_called_once_with()

    def run_visits(self, expected_start, expected_end, expected_max_rows, expected_source):
        metadata = {
            'run_id': 'completed-visits-batch',
            'window_start': expected_start,
            'window_end': expected_end,
        }
        paths = []

        def complete(command, *, check):
            self.assertTrue(check)
            self.assertEqual(command[:2], [sys.executable, '/opt/ais/pipelines/port_visits/run.py'])
            self.assertIn('--apply', command)
            for flag, expected in (
                ('--start', expected_start), ('--end', expected_end),
                ('--max-rows', str(expected_max_rows)), ('--max-rows-source', expected_source),
            ):
                self.assertEqual(command[command.index(flag) + 1], expected)
            reference = Path(command[command.index('--ports') + 1])
            result = Path(command[command.index('--result-json') + 1])
            self.assertEqual(json.loads(reference.read_text()), self.ports)
            self.assertFalse(result.exists())
            paths.extend([reference, result])
            result.write_text(json.dumps(metadata))
            return subprocess.CompletedProcess(command, 0)

        with patch('subprocess.run', side_effect=complete) as run:
            actual = self.tasks['run_port_visit_pipeline'].function(self.ports)
        run.assert_called_once()
        self.assertEqual(actual, metadata)
        self.assertEqual(set(actual), {'run_id', 'window_start', 'window_end'})
        self.assertLess(len(json.dumps(actual)), 300)
        self.assertTrue(all(not path.exists() for path in paths))
        self.publish.assert_not_called()
        return actual

    def test_explicit_window_and_row_limit_reach_subprocess_and_small_xcom(self):
        self.context({
            'start': '2026-09-12T10:14:15.123456+03:00',
            'end': '2026-09-13T11:16:17.654321+03:00',
            'max_rows': 1500000,
        })
        with patch.dict(os.environ, {'PORT_VISITS_WINDOW_HOURS': '24',
                                     'PORT_VISITS_MAX_ROWS': '500000'}):
            self.run_visits('2026-09-12T07:14:15.123456+00:00',
                            '2026-09-13T08:16:17.654321+00:00', 1500000, 'dag_run.conf')

    def test_rolling_window_uses_same_run_start_and_environment_limit_on_retry(self):
        self.context({})
        expected_start = (self.run_start - timedelta(hours=36)).isoformat()
        with patch.dict(os.environ, {'PORT_VISITS_WINDOW_HOURS': '36',
                                     'PORT_VISITS_MAX_ROWS': '765432'}):
            first = self.run_visits(expected_start, self.run_start.isoformat(),
                                    765432, 'rolling-default')
            retry = self.run_visits(expected_start, self.run_start.isoformat(),
                                    765432, 'rolling-default')
        self.assertEqual(first, retry)

    def test_failed_subprocess_propagates_without_reading_result_or_publishing(self):
        self.context({})
        failure = subprocess.CalledProcessError(1, ['port-visits'])
        with patch.dict(os.environ, {'PORT_VISITS_WINDOW_HOURS': '24',
                                     'PORT_VISITS_MAX_ROWS': '500000'}), \
                patch('subprocess.run', side_effect=failure) as run, \
                patch.object(Path, 'read_text') as read_result:
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                self.tasks['run_port_visit_pipeline'].function(self.ports)
        self.assertIs(raised.exception, failure)
        run.assert_called_once()
        self.assertTrue(run.call_args.kwargs['check'])
        read_result.assert_not_called()
        self.publish.assert_not_called()

    def test_publisher_receives_successful_batch_metadata_unchanged(self):
        metadata = {
            'run_id': 'completed-visits-batch',
            'window_start': '2026-09-12T07:14:15.123456+00:00',
            'window_end': '2026-09-13T08:16:17.654321+00:00',
        }
        self.tasks['publish_port_connections'].function(metadata)
        self.publish.assert_called_once_with(metadata)
        self.assertIs(self.publish.call_args.args[0], metadata)


if __name__ == '__main__':
    unittest.main()
